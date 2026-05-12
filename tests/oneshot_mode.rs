// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;
use std::path::{Path, PathBuf};

use tempfile::tempdir;

use sparx::alert::{
    AlertV1, CountedStringV1, EntitiesV1, FileSpanV1, ReasonV1, TopFeatureV1,
    ALERT_SCHEMA_VERSION_V1,
};
use sparx::cli::route::route_command_v1;
use sparx::cli::{CommandV1, MigrateModeV1};
use sparx::config::load::default_config_v1;
use sparx::db::baseline_sketch::{DeviceStatsV1, WelfordF64V1};
use sparx::db::silence::{
    OPEN_SILENCE_FLAG_CLOSED_V1, OPEN_SILENCE_FLAG_OPEN_V1, SILENCE_SUBJECT_KIND_DEVICE_V1,
    SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1, SILENCE_SUBJECT_KIND_TENANT_V1,
};
use sparx::db::source_stream::SourceStreamStatsV1;
use sparx::db::tenant::TenantDeviceBaselineStateV1;
use sparx::ingest::{device_key_v1, file_key_v1};
use sparx::runtime::SparxRuntimeV1;
use sparx::sink::{jsonl_alert_path_v1, write_spool_alert_v1};
use sparx::types::{ConfidenceV1, FeatureFamilyV1, LabelV1};

fn temp_cfg_v1() -> sparx::config::ConfigV1 {
    let tmp = tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let _leaked = Box::leak(Box::new(tmp));

    let mut cfg = default_config_v1();
    cfg.sparx.data_root = root.join("state").display().to_string();
    cfg.sparx.tenant_root = root.join("watch").display().to_string();
    cfg.sparx.global_db_path = root.join("state/global.db").display().to_string();
    cfg.sparx.tenant_db_root = root.join("state/tenants").display().to_string();
    cfg.sparx.alert_out_root = root.join("state/alerts").display().to_string();
    cfg.output.sink = "jsonl".to_string();
    cfg.output.jsonl_flush_interval_s = 0;
    cfg
}

fn fixture_source_v1(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("tenants")
        .join("smoke")
        .join("devices")
        .join(name)
}

fn copy_fixture_device_v1(
    cfg: &sparx::config::ConfigV1,
    tenant_id: &str,
    device: &str,
    files: &[&str],
) {
    let device_dir = Path::new(&cfg.sparx.tenant_root)
        .join(tenant_id)
        .join(device);
    fs::create_dir_all(&device_dir).unwrap();
    for file in files {
        fs::copy(fixture_source_v1(file), device_dir.join(file)).unwrap();
    }
}

fn write_tenant_policy_v1(cfg: &sparx::config::ConfigV1, tenant_id: &str, body: &str) {
    let policy_dir = Path::new(&cfg.sparx.tenant_root)
        .join(tenant_id)
        .join(".sparx");
    fs::create_dir_all(&policy_dir).unwrap();
    fs::write(policy_dir.join("policy.toml"), body).unwrap();
}

fn append_future_observation_v1(
    cfg: &sparx::config::ConfigV1,
    tenant_id: &str,
    device: &str,
    file: &str,
) {
    let path = Path::new(&cfg.sparx.tenant_root)
        .join(tenant_id)
        .join(device)
        .join(file);
    let mut content = fs::read_to_string(&path).unwrap();
    content.push_str("<34>1 2099-01-05T13:05:05Z host1 sshd 111 ID48 - src_ip=10.2.3.4 user=alice action=login result=success\n");
    fs::write(path, content).unwrap();
}

fn append_custom_observation_v1(
    cfg: &sparx::config::ConfigV1,
    tenant_id: &str,
    device: &str,
    file: &str,
    line: &str,
) {
    let path = Path::new(&cfg.sparx.tenant_root)
        .join(tenant_id)
        .join(device)
        .join(file);
    let mut content = fs::read_to_string(&path).unwrap();
    content.push_str(line);
    if !line.ends_with('\n') {
        content.push('\n');
    }
    fs::write(path, content).unwrap();
}

fn write_custom_device_log_v1(
    cfg: &sparx::config::ConfigV1,
    tenant_id: &str,
    device: &str,
    file: &str,
    content: &str,
) {
    write_custom_device_bytes_v1(cfg, tenant_id, device, file, content.as_bytes());
}

fn write_custom_device_bytes_v1(
    cfg: &sparx::config::ConfigV1,
    tenant_id: &str,
    device: &str,
    file: &str,
    content: &[u8],
) {
    let device_dir = Path::new(&cfg.sparx.tenant_root)
        .join(tenant_id)
        .join(device);
    fs::create_dir_all(&device_dir).unwrap();
    fs::write(device_dir.join(file), content).unwrap();
}

fn sharp_drop_baseline_stats_v1() -> DeviceStatsV1 {
    DeviceStatsV1 {
        line_count: WelfordF64V1 {
            n: 12,
            mean: 100.0,
            m2: 0.0,
        },
        byte_count: WelfordF64V1 {
            n: 12,
            mean: 8000.0,
            m2: 0.0,
        },
        score_total: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        last_update_ts: 4_071_888_000,
    }
}

fn write_all_bucket_sharp_drop_baselines_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
    device_key: &str,
) -> Result<(), sparx::db::DbErrorV1> {
    runtime.with_tenant_db_v1(tenant_id, 0, |db| {
        for bucket in 0u8..48u8 {
            db.write_device_baseline_state_v1(&TenantDeviceBaselineStateV1 {
                device_key: device_key.to_string(),
                bucket,
                centroid: Vec::new(),
                stats: Some(sharp_drop_baseline_stats_v1()),
            })?;
        }
        Ok(())
    })
}

fn count_sharp_drop_alerts_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
) -> Result<usize, sparx::db::DbErrorV1> {
    runtime.with_tenant_db_v1(tenant_id, 0, |db| {
        let mut count = 0usize;
        for alert_id in db.list_primary_alert_ids_v1()? {
            if let Some(alert) = db.read_primary_alert_v1(&alert_id)? {
                if alert.reasons.iter().any(|reason| {
                    reason.code == "V_DROP"
                        && reason
                            .details
                            .iter()
                            .any(|(key, value)| key == "drop_kind" && value == "sharp_drop")
                }) {
                    count = count.saturating_add(1);
                }
            }
        }
        Ok(count)
    })
}

fn write_bad_gzip_device_v1(
    cfg: &sparx::config::ConfigV1,
    tenant_id: &str,
    device: &str,
    name: &str,
) {
    let device_dir = Path::new(&cfg.sparx.tenant_root)
        .join(tenant_id)
        .join(device);
    fs::create_dir_all(&device_dir).unwrap();
    fs::write(device_dir.join(name), b"not-a-gzip-stream\n").unwrap();
}

fn count_alert_lines_v1(root: &Path) -> usize {
    let mut count = 0usize;
    collect_alert_lines_v1(root, &mut count);
    count
}

fn collect_alert_lines_v1(path: &Path, count: &mut usize) {
    if !path.exists() {
        return;
    }
    let mut entries: Vec<PathBuf> = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for entry in entries {
        if entry.is_dir() {
            collect_alert_lines_v1(&entry, count);
        } else {
            let content = fs::read_to_string(entry).unwrap();
            *count = count.saturating_add(
                content
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .count(),
            );
        }
    }
}

fn count_vdrop_alerts_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
) -> Result<usize, sparx::db::DbErrorV1> {
    runtime.with_tenant_db_v1(tenant_id, 0, |db| {
        let mut count = 0usize;
        for alert_id in db.list_primary_alert_ids_v1()? {
            if let Some(alert) = db.read_primary_alert_v1(&alert_id)? {
                if alert.reasons.iter().any(|reason| reason.code == "V_DROP") {
                    count = count.saturating_add(1);
                }
            }
        }
        Ok(count)
    })
}

fn source_stream_vdrop_alerts_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
) -> Result<Vec<AlertV1>, sparx::db::DbErrorV1> {
    runtime.with_tenant_db_v1(tenant_id, 0, |db| {
        let mut alerts = Vec::new();
        for alert_id in db.list_primary_alert_ids_v1()? {
            if let Some(alert) = db.read_primary_alert_v1(&alert_id)? {
                if alert.reasons.iter().any(|reason| {
                    reason.code == "V_DROP"
                        && reason
                            .details
                            .iter()
                            .any(|(key, value)| key == "subject_kind" && value == "source_stream")
                }) {
                    alerts.push(alert);
                }
            }
        }
        alerts.sort_by(|a, b| a.alert_id.cmp(&b.alert_id));
        Ok(alerts)
    })
}

fn count_source_stream_vdrop_alerts_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
) -> Result<usize, sparx::db::DbErrorV1> {
    Ok(source_stream_vdrop_alerts_v1(runtime, tenant_id)?.len())
}

fn source_stream_sharp_drop_stats_v1() -> SourceStreamStatsV1 {
    SourceStreamStatsV1 {
        line_count: WelfordF64V1 {
            n: 12,
            mean: 100.0,
            m2: 0.0,
        },
        byte_count: WelfordF64V1 {
            n: 12,
            mean: 8000.0,
            m2: 0.0,
        },
        score_total: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        last_update_ts: 4_071_888_000,
    }
}

fn write_all_bucket_source_stream_sharp_drop_baselines_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
    device_key: &str,
    source_stream_id: &str,
) -> Result<(), sparx::db::DbErrorV1> {
    runtime.with_tenant_db_v1(tenant_id, 0, |db| {
        for bucket in 0u8..48u8 {
            db.write_source_stream_stats_v1(
                device_key,
                source_stream_id,
                bucket,
                &source_stream_sharp_drop_stats_v1(),
            )?;
        }
        Ok(())
    })
}

fn sample_spooled_alert_v1(tenant_id: &str, device_key: &str, alert_id: &str) -> AlertV1 {
    AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id: alert_id.to_string(),
        tenant_id: tenant_id.to_string(),
        device_key: device_key.to_string(),
        device_path: format!("{}/{}", tenant_id, device_key),
        window_start_ts: 1_700_000_000,
        window_end_ts: 1_700_000_060,
        window_size_s: 60,
        bucket: 17,
        label: LabelV1::Outlier,
        confidence: ConfidenceV1::High,
        cold_start: false,
        score_total: 0.95,
        score_rarity: 0.91,
        score_drift: 0.88,
        score_volume: 0.62,
        baseline_n_bucket: Some(12),
        baseline_centroid_norm: Some(1.25),
        reasons: vec![ReasonV1 {
            code: "R_NEW_FEATURE".to_string(),
            msg: "New feature observed in this window".to_string(),
            details: vec![("feature".to_string(), "w=failed".to_string())],
        }],
        top_features: vec![TopFeatureV1 {
            feature: "w=failed".to_string(),
            feature_id: 7,
            count: 3,
            family: FeatureFamilyV1::Word,
            tf_w: 0.41,
            idf: 1.11,
            contrib: 0.46,
        }],
        summary_analyst: "spooled alert summary".to_string(),
        summary_customer: "An unusual pattern was observed in this log window.".to_string(),
        entities: EntitiesV1 {
            src_ips: vec![CountedStringV1 {
                value: "10.0.0.1".to_string(),
                count: 3,
            }],
            dst_ips: Vec::new(),
            user_ids: Vec::new(),
            domains: Vec::new(),
            hosts: Vec::new(),
        },
        lines: 9,
        bytes: 1024,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
        provenance: vec![FileSpanV1 {
            file_rel: "app.log".to_string(),
            file_key: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            inode: 5,
            offset_start: 10,
            offset_end: 44,
            is_gzip: false,
        }],
        signature: format!("sig-{}", alert_id),
    }
}

#[test]
fn oneshot_single_tenant_pass_emits_alerts_and_advances_cursors_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log", "edge01.gz"]);

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    let log_file_key = file_key_v1("edge01.log");
    let gz_file_key = file_key_v1("edge01.gz");
    let log_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.log"),
    )?
    .len();
    let gz_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.gz"),
    )?
    .len();

    let log_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.read_cursor_v1(&device_key, &log_file_key)
    })?;
    let gz_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.read_cursor_v1(&device_key, &gz_file_key)
    })?;
    assert_eq!(log_cursor.unwrap().offset, log_size);
    assert_eq!(gz_cursor.unwrap().offset, gz_size);

    let (device_state, tenant_state) = runtime.with_tenant_db_v1("smoke", 0, |db| {
        Ok((
            db.read_device_expected_source_state_v1(&device_key)?,
            db.read_tenant_expected_source_state_v1()?,
        ))
    })?;
    let device_state = device_state.expect("device expected-source state");
    let tenant_state = tenant_state.expect("tenant expected-source state");
    assert_eq!(device_state.subject_kind_u8, SILENCE_SUBJECT_KIND_DEVICE_V1);
    assert_eq!(tenant_state.subject_kind_u8, SILENCE_SUBJECT_KIND_TENANT_V1);
    assert_eq!(device_state.window_size_s_u32, cfg.ingest.window_size_s);
    assert!(device_state.observed_windows_total_u64 >= 1);
    assert!(tenant_state.observed_windows_total_u64 >= device_state.observed_windows_total_u64);
    assert_eq!(
        device_state.last_seen_window_end_ts_i64,
        tenant_state.last_seen_window_end_ts_i64
    );
    Ok(())
}

#[test]
fn oneshot_bad_data_lines_stay_stable_and_status_json_remains_healthy_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.ingest.read_chunk_bytes = 8;
    cfg.ingest.max_line_len = 32;

    let mut content = Vec::new();
    content.extend_from_slice(b"\xff\xfe\x00not-syslog key=value action=login\n");
    content.extend_from_slice(
        b"<34>1 bad-ts host app proc msgid - user=\"unterminated src_ip=10.1.1.1\n",
    );
    content.extend_from_slice(b"{\"event\":\"login\",\"user\":");
    content.extend_from_slice(&[0xff, 0xfe, b'}', b'\n']);
    content.resize(content.len() + 256, b'a');
    content.extend_from_slice(
        b"\nCEF:0|vendor|product|version|sig|name|severity|src=10.0.0.1 msg=bad\\\n",
    );

    write_custom_device_bytes_v1(&cfg, "badtenant", "baddev", "bad.log", &content);

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "badtenant".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("badtenant", "baddev");
    let file_key = file_key_v1("bad.log");
    let file_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("badtenant")
            .join("baddev")
            .join("bad.log"),
    )?
    .len();
    let cursor = runtime.with_tenant_db_v1("badtenant", 0, |db| {
        db.read_cursor_v1(&device_key, &file_key)
    })?;
    assert_eq!(cursor.unwrap().offset, file_size);

    drop(runtime);

    let status = route_command_v1(&CommandV1::Status { json: true }, &cfg);
    assert_eq!(status.exit_code, 0, "stderr={:?}", status.msg_stderr);
    let status_json: serde_json::Value = serde_json::from_str(&status.msg_stdout.unwrap())?;
    assert_eq!(status_json["tenants"]["known_count"], serde_json::json!(1));
    assert_eq!(
        status_json["process"]["last_run_exit_code"],
        serde_json::json!(0)
    );
    Ok(())
}

#[test]
fn oneshot_runtime_emits_and_deduplicates_vdrop_alerts_v1() -> Result<(), Box<dyn std::error::Error>>
{
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 2);
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_evaluated_subjects_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_candidates_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_suppressed_candidates_total")?,
        None
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_alerts_emitted_total")?,
        Some(2)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("vdrop_last_evaluation_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("vdrop_tracked_subjects__smoke")?,
        Some(2.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("vdrop_open_silence_subjects__smoke")?,
        Some(2.0)
    );
    drop(runtime);

    let search = route_command_v1(
        &CommandV1::AlertsSearch {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            category: None,
            entity_kind: None,
            entity_value: None,
            contains: "V_DROP".to_string(),
        },
        &cfg,
    );
    assert_eq!(search.exit_code, 0, "stderr={:?}", search.msg_stderr);
    assert!(search.msg_stdout.unwrap().contains("count: 2"));

    let duplicate = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(duplicate.exit_code, 0, "stderr={:?}", duplicate.msg_stderr);
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 2);
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_evaluated_subjects_total")?,
        Some(4)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_candidates_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_suppressed_candidates_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_alerts_emitted_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("vdrop_open_silence_subjects__smoke")?,
        Some(2.0)
    );
    let device_key = device_key_v1("smoke", "edge01");
    let (device_open, tenant_open) = runtime.with_tenant_db_v1("smoke", 0, |db| {
        Ok((
            db.read_device_open_silence_state_v1(&device_key)?,
            db.read_tenant_open_silence_state_v1()?,
        ))
    })?;
    assert_eq!(
        device_open.as_ref().unwrap().state_flags_u8 & OPEN_SILENCE_FLAG_OPEN_V1,
        OPEN_SILENCE_FLAG_OPEN_V1
    );
    assert_eq!(
        tenant_open.as_ref().unwrap().state_flags_u8 & OPEN_SILENCE_FLAG_OPEN_V1,
        OPEN_SILENCE_FLAG_OPEN_V1
    );
    drop(runtime);

    append_future_observation_v1(&cfg, "smoke", "edge01", "edge01.log");
    let observed_again = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(
        observed_again.exit_code, 0,
        "stderr={:?}",
        observed_again.msg_stderr
    );

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 2);
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_evaluated_subjects_total")?,
        Some(7)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_candidates_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_suppressed_candidates_total")?,
        Some(5)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_alerts_emitted_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("vdrop_open_silence_subjects__smoke")?,
        Some(0.0)
    );
    let (device_open, tenant_open) = runtime.with_tenant_db_v1("smoke", 0, |db| {
        Ok((
            db.read_device_open_silence_state_v1(&device_key)?,
            db.read_tenant_open_silence_state_v1()?,
        ))
    })?;
    assert_eq!(
        device_open.as_ref().unwrap().state_flags_u8 & OPEN_SILENCE_FLAG_OPEN_V1,
        0
    );
    assert_eq!(
        tenant_open.as_ref().unwrap().state_flags_u8 & OPEN_SILENCE_FLAG_OPEN_V1,
        0
    );
    assert_eq!(
        device_open.as_ref().unwrap().state_flags_u8 & OPEN_SILENCE_FLAG_CLOSED_V1,
        OPEN_SILENCE_FLAG_CLOSED_V1
    );
    assert_eq!(
        tenant_open.as_ref().unwrap().state_flags_u8 & OPEN_SILENCE_FLAG_CLOSED_V1,
        OPEN_SILENCE_FLAG_CLOSED_V1
    );
    Ok(())
}

#[test]
fn oneshot_source_stream_gate_default_off_does_not_record_source_stream_state_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 2);
    assert_eq!(
        count_source_stream_vdrop_alerts_v1(&mut runtime, "smoke")?,
        0
    );
    let catalogs = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.list_source_stream_catalogs_for_device_v1(&device_key)
    })?;
    assert!(catalogs.is_empty());
    Ok(())
}

#[test]
fn oneshot_source_stream_gate_emits_runtime_hard_silence_alert_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    cfg.vdrop.source_stream_enabled = true;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 3);
    let source_alerts = source_stream_vdrop_alerts_v1(&mut runtime, "smoke")?;
    assert_eq!(source_alerts.len(), 1);
    let alert = &source_alerts[0];
    assert_eq!(alert.device_key, device_key);
    assert!(alert.device_path.starts_with("source_stream:"));
    assert!(alert.reasons.iter().any(|reason| {
        reason.code == "V_DROP"
            && reason
                .details
                .iter()
                .any(|(key, value)| key == "drop_kind" && value == "hard_silence")
            && reason
                .details
                .iter()
                .any(|(key, value)| key == "subject_kind" && value == "source_stream")
            && reason
                .details
                .iter()
                .any(|(key, value)| key == "source_path" && value == "edge01.log")
    }));

    let (catalogs, expected_states, open_states) = runtime.with_tenant_db_v1("smoke", 0, |db| {
        Ok((
            db.list_source_stream_catalogs_for_device_v1(&device_key)?,
            db.list_source_stream_expected_source_states_for_device_v1(&device_key)?,
            db.list_source_stream_open_silence_states_for_device_v1(&device_key)?,
        ))
    })?;
    assert_eq!(catalogs.len(), 1);
    assert_eq!(expected_states.len(), 1);
    assert_eq!(open_states.len(), 1);
    assert_eq!(
        expected_states[0].1.subject_kind_u8,
        SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
    );
    assert_eq!(
        open_states[0].1.state_flags_u8 & OPEN_SILENCE_FLAG_OPEN_V1,
        OPEN_SILENCE_FLAG_OPEN_V1
    );

    drop(runtime);

    let status = route_command_v1(&CommandV1::Status { json: true }, &cfg);
    assert_eq!(status.exit_code, 0, "stderr={:?}", status.msg_stderr);
    let value: serde_json::Value = serde_json::from_str(&status.msg_stdout.unwrap())?;
    assert_eq!(
        value["vdrop"]["source_stream_enabled"].as_bool(),
        Some(true)
    );
    assert_eq!(
        value["vdrop"]["source_stream_tracked_subjects"].as_u64(),
        Some(1)
    );
    assert_eq!(
        value["vdrop"]["source_stream_open_silence_subjects"].as_u64(),
        Some(1)
    );
    assert_eq!(
        value["vdrop"]["source_stream_open_drop_subjects"].as_u64(),
        Some(0)
    );
    assert_eq!(
        value["vdrop"]["source_stream_evaluated_subjects_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        value["vdrop"]["source_stream_candidates_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        value["vdrop"]["source_stream_suppressed_candidates_total"].as_u64(),
        Some(0)
    );
    assert_eq!(
        value["vdrop"]["source_stream_alerts_emitted_total"].as_u64(),
        Some(1)
    );
    assert!(value["vdrop"]["source_stream_last_evaluation_ts"]
        .as_u64()
        .is_some());
    let tenants = value["vdrop"]["tenants"].as_array().expect("vdrop tenants");
    assert_eq!(tenants.len(), 1);
    assert_eq!(
        tenants[0]["source_stream_tracked_subjects"].as_u64(),
        Some(1)
    );
    assert_eq!(
        tenants[0]["source_stream_open_silence_subjects"].as_u64(),
        Some(1)
    );
    assert_eq!(
        tenants[0]["source_stream_alerts_emitted_total"].as_u64(),
        Some(1)
    );
    Ok(())
}

#[test]
fn oneshot_tenant_policy_can_disable_source_stream_vdrop_runtime_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    cfg.vdrop.source_stream_enabled = true;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);
    write_tenant_policy_v1(
        &cfg,
        "smoke",
        "policy_version = 1\nvdrop_source_stream_enabled = false\n",
    );

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 2);
    assert_eq!(
        count_source_stream_vdrop_alerts_v1(&mut runtime, "smoke")?,
        0
    );
    let catalogs = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.list_source_stream_catalogs_for_device_v1(&device_key)
    })?;
    assert!(catalogs.is_empty());
    Ok(())
}

#[test]
fn oneshot_global_vdrop_disable_suppresses_runtime_alerts_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    cfg.vdrop.enabled = false;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 0);
    Ok(())
}

#[test]
fn oneshot_tenant_policy_can_disable_device_vdrop_subjects_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);
    write_tenant_policy_v1(
        &cfg,
        "smoke",
        "policy_version = 1\nvdrop_device_enabled = false\n",
    );

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 1);
    Ok(())
}

#[test]
fn oneshot_tenant_policy_overrides_global_vdrop_threshold_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    cfg.vdrop.min_expected_windows_missed = u32::MAX;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);
    write_tenant_policy_v1(
        &cfg,
        "smoke",
        "policy_version = 1\nvdrop_min_expected_windows_missed = 1\n",
    );

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 2);
    Ok(())
}

#[test]
fn oneshot_device_filter_limits_processing_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);
    copy_fixture_device_v1(&cfg, "smoke", "edge02", &["edge01.log"]);

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: Some("edge02".to_string()),
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let edge01_key = device_key_v1("smoke", "edge01");
    let edge02_key = device_key_v1("smoke", "edge02");
    let file_key = file_key_v1("edge01.log");

    let edge01_cursor =
        runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&edge01_key, &file_key))?;
    let edge02_cursor =
        runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&edge02_key, &file_key))?;
    assert!(edge01_cursor.is_none());
    assert!(edge02_cursor.is_some());
    Ok(())
}

#[test]
fn oneshot_time_filter_advances_cursor_without_emitting_alerts_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: Some(1_800_000_000),
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);
    assert_eq!(
        count_alert_lines_v1(Path::new(&cfg.sparx.alert_out_root)),
        0
    );

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    let file_key = file_key_v1("edge01.log");
    let size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.log"),
    )?
    .len();
    let cursor =
        runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&device_key, &file_key))?;
    assert_eq!(cursor.unwrap().offset, size);
    Ok(())
}

#[test]
fn oneshot_partial_device_failure_returns_exit6_v1() {
    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log", "edge01.gz"]);
    write_bad_gzip_device_v1(&cfg, "smoke", "edge02", "broken.gz");

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 6);
    let stderr = result.msg_stderr.unwrap();
    assert!(stderr.contains("edge02"));

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg).unwrap();
    let edge01_key = device_key_v1("smoke", "edge01");
    let edge01_log_key = file_key_v1("edge01.log");
    let edge01_gz_key = file_key_v1("edge01.gz");
    let edge01_log_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.log"),
    )
    .unwrap()
    .len();
    let edge01_gz_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.gz"),
    )
    .unwrap()
    .len();

    let edge01_log_cursor = runtime
        .with_tenant_db_v1("smoke", 0, |db| {
            db.read_cursor_v1(&edge01_key, &edge01_log_key)
        })
        .unwrap();
    let edge01_gz_cursor = runtime
        .with_tenant_db_v1("smoke", 0, |db| {
            db.read_cursor_v1(&edge01_key, &edge01_gz_key)
        })
        .unwrap();
    assert_eq!(edge01_log_cursor.unwrap().offset, edge01_log_size);
    assert_eq!(edge01_gz_cursor.unwrap().offset, edge01_gz_size);
}

#[test]
fn oneshot_replays_spooled_alerts_automatically_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    write_custom_device_log_v1(&cfg, "smoke", "device-a", "seed.log", "seed line\n");
    let alert = sample_spooled_alert_v1("smoke", "device-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let spool_path = write_spool_alert_v1(&cfg.sparx.data_root, &alert).unwrap();

    let result = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);
    assert!(!spool_path.exists());

    let out_path = jsonl_alert_path_v1(
        &cfg.sparx.alert_out_root,
        "smoke",
        "device-a",
        alert.window_start_ts,
        0,
    )
    .unwrap();
    let line = fs::read_to_string(out_path).unwrap();
    let value: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
    assert_eq!(
        value["alert_id"].as_str(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_automated_replay_attempts_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_spool_replayed_total")?,
        Some(1)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_spool_replay_fail_total")?,
        Some(0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_spool_writes_total")?,
        Some(0)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_last_automated_replay_attempt_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_last_automated_replay_replayed")?,
        Some(0.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_last_automated_replay_failed")?,
        Some(0.0)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_previous_snapshot_ts")?
        .is_some());
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_last_snapshot_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_previous_snapshot_backlog_files")?,
        Some(0.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_last_snapshot_backlog_files")?,
        Some(0.0)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_previous_counter_snapshot_ts")?
        .is_some());
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_last_counter_snapshot_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_previous_counter_snapshot_spool_replayed_total")?,
        Some(1)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_last_counter_snapshot_spool_replayed_total")?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_previous_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_last_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(2)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_history_start_counter_snapshot_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_history_start_counter_snapshot_spool_writes_total")?,
        Some(0)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_history_start_counter_snapshot_spool_replayed_total"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_history_start_counter_snapshot_spool_replay_fail_total"
        )?,
        Some(0)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_history_start_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_writes_total__smoke"
        )?,
        None
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_replayed_total__smoke"
        )?,
        None
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total__smoke"
        )?,
        None
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total__smoke"
        )?,
        None
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_tenant_previous_counter_snapshot_ts__smoke")?,
        None
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_tenant_last_counter_snapshot_ts__smoke")?,
        None
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_previous_counter_snapshot_spool_replayed_total__smoke"
        )?,
        None
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_last_counter_snapshot_spool_replayed_total__smoke"
        )?,
        None
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total__smoke"
        )?,
        None
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_last_counter_snapshot_automated_replay_attempts_total__smoke"
        )?,
        None
    );
    Ok(())
}

#[test]
fn oneshot_runtime_emits_device_sharp_drop_from_stats_baseline_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    cfg.vdrop.min_mature_windows = Some(2);
    cfg.vdrop.min_expected_lines = Some(10);

    write_custom_device_log_v1(
        &cfg,
        "smoke",
        "edge01",
        "edge01.log",
        "<34>1 2099-01-05T13:00:05Z host1 sshd 111 ID47 - src_ip=10.2.3.4 user=alice action=login result=success\n",
    );

    let initial = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(initial.exit_code, 0, "stderr={:?}", initial.msg_stderr);

    let device_key = device_key_v1("smoke", "edge01");
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    write_all_bucket_sharp_drop_baselines_v1(&mut runtime, "smoke", &device_key)?;
    drop(runtime);

    append_custom_observation_v1(
        &cfg,
        "smoke",
        "edge01",
        "edge01.log",
        "<34>1 2099-01-05T13:01:05Z host1 sshd 112 ID48 - src_ip=10.2.3.5 user=alice action=login result=success",
    );

    let sharp_drop = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(
        sharp_drop.exit_code, 0,
        "stderr={:?}",
        sharp_drop.msg_stderr
    );

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_sharp_drop_alerts_v1(&mut runtime, "smoke")?, 1);
    let (device_open_drop, tenant_open_drop) = runtime.with_tenant_db_v1("smoke", 0, |db| {
        Ok((
            db.read_device_open_drop_state_v1(&device_key)?,
            db.read_tenant_open_drop_state_v1()?,
        ))
    })?;
    assert!(device_open_drop.is_some());
    assert_eq!(tenant_open_drop, None);
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("vdrop_open_drop_subjects__smoke")?,
        Some(1.0)
    );
    assert_eq!(
        device_open_drop.unwrap().state_flags_u8 & sparx::db::silence::OPEN_DROP_FLAG_OPEN_V1,
        sparx::db::silence::OPEN_DROP_FLAG_OPEN_V1
    );
    Ok(())
}

#[test]
fn oneshot_source_stream_runtime_emits_sharp_drop_from_stats_baseline_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    cfg.vdrop.source_stream_enabled = true;
    cfg.vdrop.min_mature_windows = Some(2);
    cfg.vdrop.min_expected_lines = Some(10);

    write_custom_device_log_v1(
        &cfg,
        "smoke",
        "edge01",
        "edge01.log",
        "<34>1 2099-01-05T13:00:05Z host1 sshd 111 ID47 - src_ip=10.2.3.4 user=alice action=login result=success\n",
    );

    let initial = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(initial.exit_code, 0, "stderr={:?}", initial.msg_stderr);

    let device_key = device_key_v1("smoke", "edge01");
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let catalogs = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.list_source_stream_catalogs_for_device_v1(&device_key)
    })?;
    assert_eq!(catalogs.len(), 1);
    let source_stream_id = catalogs[0].source_stream_id.clone();
    write_all_bucket_source_stream_sharp_drop_baselines_v1(
        &mut runtime,
        "smoke",
        &device_key,
        &source_stream_id,
    )?;
    drop(runtime);

    append_custom_observation_v1(
        &cfg,
        "smoke",
        "edge01",
        "edge01.log",
        "<34>1 2099-01-05T13:01:05Z host1 sshd 112 ID48 - src_ip=10.2.3.5 user=alice action=login result=success",
    );

    let sharp_drop = route_command_v1(
        &CommandV1::OneShot {
            tenant_id: "smoke".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(
        sharp_drop.exit_code, 0,
        "stderr={:?}",
        sharp_drop.msg_stderr
    );

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let source_alerts = source_stream_vdrop_alerts_v1(&mut runtime, "smoke")?;
    let sharp_alerts: Vec<&AlertV1> = source_alerts
        .iter()
        .filter(|alert| {
            alert.reasons.iter().any(|reason| {
                reason.code == "V_DROP"
                    && reason
                        .details
                        .iter()
                        .any(|(key, value)| key == "drop_kind" && value == "sharp_drop")
                    && reason
                        .details
                        .iter()
                        .any(|(key, value)| key == "subject_kind" && value == "source_stream")
                    && reason
                        .details
                        .iter()
                        .any(|(key, value)| key == "source_stream_id" && value == &source_stream_id)
            })
        })
        .collect();
    assert_eq!(sharp_alerts.len(), 1);
    let alert = sharp_alerts[0];
    assert_eq!(alert.lines, 1);
    assert!(!alert.provenance.is_empty());
    let open_drop = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.read_source_stream_open_drop_state_v1(&device_key, &source_stream_id)
    })?;
    assert!(open_drop.is_some());
    assert_eq!(
        open_drop.unwrap().state_flags_u8 & sparx::db::silence::OPEN_DROP_FLAG_OPEN_V1,
        sparx::db::silence::OPEN_DROP_FLAG_OPEN_V1
    );
    Ok(())
}
