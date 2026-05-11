// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use sparx::alert::{
    AlertV1, CountedStringV1, EntitiesV1, FileSpanV1, ReasonV1, TopFeatureV1,
    ALERT_SCHEMA_VERSION_V1,
};
use sparx::sink::{
    enforce_spool_cap_v1, jsonl_alert_path_v1, jsonl_day_dir_v1, jsonl_file_name_v1,
    read_spooled_alert_v1, spool_alert_path_v1, spool_backlog_per_tenant_v1, write_spool_alert_v1,
    AlertSinkV1, JsonlAlertSinkV1, JsonlSinkConfigV1, SpoolConfigV1, SpoolEmitOutcomeV1,
    SpoolingJsonlAlertSinkV1, StdoutAlertSinkV1,
};
use sparx::types::{ConfidenceV1, FeatureFamilyV1, LabelV1};

fn sample_alert(window_start_ts: i64, window_end_ts: i64) -> AlertV1 {
    AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        tenant_id: "tenant-a".to_string(),
        device_key: "device-001".to_string(),
        device_path: "region/host1".to_string(),
        window_start_ts,
        window_end_ts,
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
        summary_analyst: "outlier score 0.950. Reasons: R_NEW_FEATURE. Top features: w=failed."
            .to_string(),
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
        signature: "cccccccccccccccccccccccccccccccc".to_string(),
    }
}

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(name);
    let _ = fs::remove_dir_all(&root);
    root
}

#[test]
fn jsonl_path_mapping_is_deterministic() {
    let ts = 1_700_000_000;
    let dir = jsonl_day_dir_v1("/tmp/out", "tenant-a", "device-001", ts).unwrap();
    let file = jsonl_file_name_v1("device-001", ts, 12).unwrap();
    let path = jsonl_alert_path_v1("/tmp/out", "tenant-a", "device-001", ts, 12).unwrap();

    assert_eq!(
        dir,
        PathBuf::from("/tmp/out/tenant=tenant-a/device=device-001/2023/11/14")
    );
    assert_eq!(file, "alerts_device-001_20231114_0012.jsonl");
    assert_eq!(
        path,
        PathBuf::from("/tmp/out/tenant=tenant-a/device=device-001/2023/11/14/alerts_device-001_20231114_0012.jsonl")
    );
}

#[test]
fn jsonl_sink_writes_valid_json_line_with_newline() {
    let root = temp_root("sparx_test_sink_jsonl_line");
    let cfg = JsonlSinkConfigV1 {
        alert_out_root: root.to_string_lossy().to_string(),
        jsonl_rotate_mb: 256,
        jsonl_flush_interval_s: 5,
    };
    let mut sink = JsonlAlertSinkV1::new(cfg);
    let alert = sample_alert(1_700_000_000, 1_700_000_060);

    let path = sink.emit_at_v1(&alert, 1_700_000_060).unwrap();
    sink.shutdown_v1().unwrap();

    let bytes = fs::read(&path).unwrap();
    assert!(bytes.ends_with(b"\n"));
    let line = String::from_utf8(bytes).unwrap();
    let trimmed = line.trim_end_matches('\n');
    let value: serde_json::Value = serde_json::from_str(trimmed).unwrap();
    assert_eq!(value["alert_id"], serde_json::Value::String(alert.alert_id));
    assert_eq!(
        value["tenant_id"],
        serde_json::Value::String("tenant-a".to_string())
    );
}

#[test]
fn jsonl_sink_rotates_on_size_exceeded() {
    let root = temp_root("sparx_test_sink_rotate_size");
    let cfg = JsonlSinkConfigV1 {
        alert_out_root: root.to_string_lossy().to_string(),
        jsonl_rotate_mb: 1,
        jsonl_flush_interval_s: 5,
    };
    let mut sink = JsonlAlertSinkV1::new(cfg);

    let mut first = sample_alert(1_700_000_000, 1_700_000_060);
    first.alert_id = "size-a".to_string();
    first.summary_analyst = "X".repeat(1_100_000);
    first.summary_customer = "Y".repeat(32);
    let first_path = sink.emit_at_v1(&first, 1_700_000_060).unwrap();
    assert_eq!(sink.current_seq_v1(), Some(0));

    let mut second = sample_alert(1_700_000_000, 1_700_000_120);
    second.alert_id = "size-b".to_string();
    let second_path = sink.emit_at_v1(&second, 1_700_000_120).unwrap();
    sink.shutdown_v1().unwrap();

    assert_ne!(first_path, second_path);
    assert!(first_path.ends_with("alerts_device-001_20231114_0000.jsonl"));
    assert!(second_path.ends_with("alerts_device-001_20231114_0001.jsonl"));
    assert!(first_path.is_file());
    assert!(second_path.is_file());
}

#[test]
fn jsonl_sink_rotates_on_day_boundary() {
    let root = temp_root("sparx_test_sink_rotate_day");
    let cfg = JsonlSinkConfigV1 {
        alert_out_root: root.to_string_lossy().to_string(),
        jsonl_rotate_mb: 256,
        jsonl_flush_interval_s: 5,
    };
    let mut sink = JsonlAlertSinkV1::new(cfg);

    let first = sample_alert(1_700_000_000, 1_700_000_060);
    let first_path = sink.emit_at_v1(&first, 1_700_000_060).unwrap();

    let mut second = sample_alert(1_700_086_400, 1_700_086_460);
    second.alert_id = "next-day".to_string();
    let second_path = sink.emit_at_v1(&second, 1_700_086_460).unwrap();
    sink.shutdown_v1().unwrap();

    assert_ne!(first_path.parent().unwrap(), second_path.parent().unwrap());
    assert!(first_path.ends_with("alerts_device-001_20231114_0000.jsonl"));
    assert!(second_path.ends_with("alerts_device-001_20231115_0000.jsonl"));
}

#[test]
fn spool_write_on_simulated_jsonl_failure() {
    let root = temp_root("sparx_test_sink_spool_write");
    fs::create_dir_all(&root).unwrap();
    let blocked = root.join("blocked-root");
    fs::write(&blocked, b"not a directory").unwrap();

    let jsonl_cfg = JsonlSinkConfigV1 {
        alert_out_root: blocked.to_string_lossy().to_string(),
        jsonl_rotate_mb: 256,
        jsonl_flush_interval_s: 5,
    };
    let spool_cfg = SpoolConfigV1 {
        data_root: root.to_string_lossy().to_string(),
        spool_max_mb: 2048,
    };
    let mut sink = SpoolingJsonlAlertSinkV1::new(jsonl_cfg, spool_cfg);
    let alert = sample_alert(1_700_000_000, 1_700_000_060);

    let outcome = sink.emit_at_v1(&alert, 1_700_000_060).unwrap();
    let spooled_path = match outcome {
        SpoolEmitOutcomeV1::Spooled { path } => path,
        other => panic!("expected spooled outcome, got {:?}", other),
    };

    assert_eq!(
        spooled_path,
        spool_alert_path_v1(
            &root.to_string_lossy(),
            "tenant-a",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        .unwrap()
    );
    assert!(spooled_path.is_file());
    let spooled_alert = read_spooled_alert_v1(&spooled_path).unwrap();
    assert_eq!(spooled_alert.alert_id, alert.alert_id);
    assert_eq!(sink.counters_v1().sink_spool_total, 1);
}

#[test]
fn spool_replay_succeeds_and_deletes_file() {
    let root = temp_root("sparx_test_sink_spool_replay");
    fs::create_dir_all(&root).unwrap();
    let jsonl_root = root.join("alerts-out");
    let mut alert = sample_alert(1_700_000_000, 1_700_000_060);
    alert.alert_id = "dddddddddddddddddddddddddddddddd".to_string();

    let spooled_path = write_spool_alert_v1(&root.to_string_lossy(), &alert).unwrap();
    assert!(spooled_path.is_file());

    let jsonl_cfg = JsonlSinkConfigV1 {
        alert_out_root: jsonl_root.to_string_lossy().to_string(),
        jsonl_rotate_mb: 256,
        jsonl_flush_interval_s: 5,
    };
    let spool_cfg = SpoolConfigV1 {
        data_root: root.to_string_lossy().to_string(),
        spool_max_mb: 2048,
    };
    let mut sink = SpoolingJsonlAlertSinkV1::new(jsonl_cfg, spool_cfg);

    let report = sink.replay_spooled_alerts_v1(1_700_000_120).unwrap();
    sink.shutdown_v1().unwrap();

    assert_eq!(report.replayed_paths, vec![spooled_path.clone()]);
    assert!(report.failed_paths.is_empty());
    assert!(!spooled_path.exists());
    assert_eq!(sink.counters_v1().sink_spool_replayed_total, 1);

    let out_path = jsonl_alert_path_v1(
        &jsonl_root.to_string_lossy(),
        "tenant-a",
        "device-001",
        1_700_000_000,
        0,
    )
    .unwrap();
    let line = fs::read_to_string(out_path).unwrap();
    let value: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
    assert_eq!(
        value["alert_id"],
        serde_json::Value::String("dddddddddddddddddddddddddddddddd".to_string())
    );
}

#[test]
fn spool_replay_can_be_bounded_per_pass() {
    let root = temp_root("sparx_test_sink_spool_replay_bounded");
    fs::create_dir_all(&root).unwrap();
    let jsonl_root = root.join("alerts-out");

    for suffix in ["a", "b", "c"] {
        let mut alert = sample_alert(1_700_000_000, 1_700_000_060);
        alert.alert_id = format!("0000000000000000000000000000000{}", suffix);
        write_spool_alert_v1(&root.to_string_lossy(), &alert).unwrap();
    }

    let jsonl_cfg = JsonlSinkConfigV1 {
        alert_out_root: jsonl_root.to_string_lossy().to_string(),
        jsonl_rotate_mb: 256,
        jsonl_flush_interval_s: 5,
    };
    let spool_cfg = SpoolConfigV1 {
        data_root: root.to_string_lossy().to_string(),
        spool_max_mb: 2048,
    };
    let mut sink = SpoolingJsonlAlertSinkV1::new(jsonl_cfg, spool_cfg);

    let report = sink
        .replay_spooled_alerts_limited_v1(1_700_000_120, 2)
        .unwrap();
    sink.shutdown_v1().unwrap();

    assert_eq!(report.replayed_paths.len(), 2);
    assert!(report.failed_paths.is_empty());
    let remaining = spool_alert_path_v1(
        &root.to_string_lossy(),
        "tenant-a",
        "0000000000000000000000000000000c",
    )
    .unwrap();
    assert!(remaining.exists());

    let out_path = jsonl_alert_path_v1(
        &jsonl_root.to_string_lossy(),
        "tenant-a",
        "device-001",
        1_700_000_000,
        0,
    )
    .unwrap();
    let content = fs::read_to_string(out_path).unwrap();
    let mut lines = content.lines();
    let first: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    let second: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(
        first["alert_id"],
        serde_json::Value::String("0000000000000000000000000000000a".to_string())
    );
    assert_eq!(
        second["alert_id"],
        serde_json::Value::String("0000000000000000000000000000000b".to_string())
    );
    assert!(lines.next().is_none());
}

#[test]
fn spool_caps_delete_oldest_and_are_deterministic() {
    let root = temp_root("sparx_test_sink_spool_caps");
    fs::create_dir_all(&root).unwrap();

    let mut alert_a = sample_alert(1_700_000_000, 1_700_000_060);
    alert_a.alert_id = "0000000000000000000000000000000a".to_string();
    alert_a.summary_analyst = "A".repeat(600_000);
    let path_a = write_spool_alert_v1(&root.to_string_lossy(), &alert_a).unwrap();

    let mut alert_b = sample_alert(1_700_000_000, 1_700_000_060);
    alert_b.alert_id = "0000000000000000000000000000000b".to_string();
    alert_b.summary_analyst = "B".repeat(600_000);
    let path_b = write_spool_alert_v1(&root.to_string_lossy(), &alert_b).unwrap();

    let mut alert_c = sample_alert(1_700_000_000, 1_700_000_060);
    alert_c.alert_id = "0000000000000000000000000000000c".to_string();
    alert_c.summary_analyst = "C".repeat(600_000);
    let path_c = write_spool_alert_v1(&root.to_string_lossy(), &alert_c).unwrap();

    let report = enforce_spool_cap_v1(&root.to_string_lossy(), 1).unwrap();

    assert!(report.bytes_before > report.bytes_after);
    assert_eq!(report.dropped_paths, vec![path_a.clone(), path_b.clone()]);
    assert!(!path_a.exists());
    assert!(!path_b.exists());
    assert!(path_c.exists());
}

#[test]
fn stdout_sink_emits_one_line_per_alert() {
    let mut sink = StdoutAlertSinkV1::new(Vec::<u8>::new());
    let mut first = sample_alert(1_700_000_000, 1_700_000_060);
    first.alert_id = "stdout-a".to_string();
    let mut second = sample_alert(1_700_000_060, 1_700_000_120);
    second.alert_id = "stdout-b".to_string();

    sink.emit(&first).unwrap();
    sink.emit(&second).unwrap();

    let out = String::from_utf8(sink.into_inner()).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 2);
    let first_json: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let second_json: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(
        first_json["alert_id"],
        serde_json::Value::String("stdout-a".to_string())
    );
    assert_eq!(
        second_json["alert_id"],
        serde_json::Value::String("stdout-b".to_string())
    );
}

#[test]
fn spool_backlog_per_tenant_is_deterministic_v1() {
    let root = temp_root("sparx_test_sink_spool_backlog_per_tenant");
    fs::create_dir_all(&root).unwrap();

    let mut alert_a = sample_alert(1_700_000_000, 1_700_000_060);
    alert_a.alert_id = "11111111111111111111111111111111".to_string();
    alert_a.tenant_id = "tenant-b".to_string();
    write_spool_alert_v1(&root.to_string_lossy(), &alert_a).unwrap();
    std::thread::sleep(Duration::from_secs(2));

    let mut alert_b = sample_alert(1_700_000_000, 1_700_000_060);
    alert_b.alert_id = "22222222222222222222222222222222".to_string();
    alert_b.tenant_id = "tenant-a".to_string();
    let path_b = write_spool_alert_v1(&root.to_string_lossy(), &alert_b).unwrap();

    let mut alert_c = sample_alert(1_700_000_000, 1_700_000_060);
    alert_c.alert_id = "33333333333333333333333333333333".to_string();
    alert_c.tenant_id = "tenant-a".to_string();
    let path_c = write_spool_alert_v1(&root.to_string_lossy(), &alert_c).unwrap();

    let summaries = spool_backlog_per_tenant_v1(&root.to_string_lossy()).unwrap();
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0].tenant_id, "tenant-a");
    assert_eq!(summaries[0].files, 2);
    assert_eq!(
        summaries[0].bytes,
        fs::metadata(path_b).unwrap().len() + fs::metadata(path_c).unwrap().len()
    );
    assert!(summaries[0].oldest_file_ts.is_some());
    assert!(summaries[0].oldest_age_s.is_some());
    assert_eq!(summaries[1].tenant_id, "tenant-b");
    assert_eq!(summaries[1].files, 1);
    assert!(summaries[1].oldest_file_ts.is_some());
    assert!(summaries[1].oldest_age_s.is_some());
    assert!(summaries[1].oldest_age_s.unwrap() >= 1);
    assert!(summaries[1].oldest_age_s.unwrap() >= summaries[0].oldest_age_s.unwrap());
}

#[test]
fn sink_paths_reject_unsafe_filesystem_components_v1() {
    let ts = 1_700_000_000;
    assert!(jsonl_day_dir_v1("/tmp/out", "../tenant", "device-001", ts).is_err());
    assert!(jsonl_day_dir_v1("/tmp/out", "tenant-a", "../device", ts).is_err());
    assert!(jsonl_file_name_v1("bad/device", ts, 0).is_err());
    assert!(spool_alert_path_v1("/tmp/out", "tenant-a", "bad/alert").is_err());
}

#[cfg(unix)]
#[test]
fn spool_replay_inventory_ignores_symlinked_files_v1() {
    use std::os::unix::fs::symlink;

    let root = temp_root("sparx_test_sink_spool_symlink_guard");
    fs::create_dir_all(&root).unwrap();
    let tenant_dir = root.join("spool").join("alerts").join("tenant=tenant-a");
    fs::create_dir_all(&tenant_dir).unwrap();
    let outside = root.join("outside.json");
    fs::write(&outside, b"{}").unwrap();
    symlink(&outside, tenant_dir.join("spool_symlink.json")).unwrap();

    let files = sparx::sink::sorted_spool_files_for_replay_v1(&root.to_string_lossy()).unwrap();
    assert!(files.is_empty());
    let summaries = spool_backlog_per_tenant_v1(&root.to_string_lossy()).unwrap();
    assert!(summaries.is_empty());
}
