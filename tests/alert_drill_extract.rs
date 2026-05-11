// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;
use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use tempfile::tempdir;

use sparx::alert::{
    AlertV1, CountedStringV1, EntitiesV1, FileSpanV1, ReasonV1, TopFeatureV1,
    ALERT_SCHEMA_VERSION_V1,
};
use sparx::cli::route::route_command_v1;
use sparx::cli::CommandV1;
use sparx::config::load::default_config_v1;
use sparx::runtime::SparxRuntimeV1;
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
    cfg.storage.tenant_db_max_open = 4;
    cfg.storage.tenant_db_idle_close_s = 30;
    cfg.ingest.read_chunk_bytes = 1024;
    cfg
}

fn sample_alert_v1(alert_id: &str, provenance: Vec<FileSpanV1>) -> AlertV1 {
    AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id: alert_id.to_string(),
        tenant_id: "tenant-a".to_string(),
        device_key: "device-001".to_string(),
        device_path: "device-a".to_string(),
        window_start_ts: 100,
        window_end_ts: 700,
        window_size_s: 600,
        bucket: 7,
        label: LabelV1::Outlier,
        confidence: ConfidenceV1::High,
        cold_start: false,
        score_total: 0.91,
        score_rarity: 0.85,
        score_drift: 0.90,
        score_volume: 0.77,
        baseline_n_bucket: Some(42),
        baseline_centroid_norm: Some(1.25),
        reasons: vec![ReasonV1 {
            code: "rare_feature_mix".to_string(),
            msg: "Rare weighted feature mix exceeded threshold".to_string(),
            details: vec![("user".to_string(), "alice".to_string())],
        }],
        top_features: vec![TopFeatureV1 {
            feature: "CANON:user=alice".to_string(),
            feature_id: 11,
            count: 3,
            family: FeatureFamilyV1::Canon,
            tf_w: 0.5,
            idf: 1.1,
            contrib: 0.55,
        }],
        summary_analyst: "Analyst summary".to_string(),
        summary_customer: "Customer summary".to_string(),
        entities: EntitiesV1 {
            src_ips: vec![CountedStringV1 {
                value: "10.0.0.1".to_string(),
                count: 2,
            }],
            dst_ips: Vec::new(),
            user_ids: vec![CountedStringV1 {
                value: "alice".to_string(),
                count: 3,
            }],
            domains: Vec::new(),
            hosts: Vec::new(),
        },
        lines: 12,
        bytes: 4096,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
        provenance,
        signature: format!("sig-{}", alert_id),
    }
}

fn seed_alert_v1(
    cfg: &sparx::config::ConfigV1,
    alert: &AlertV1,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = SparxRuntimeV1::open_from_config_v1(cfg)?;
    runtime.with_tenant_db_v1("tenant-a", 1_700_200_000, |db| {
        db.write_primary_alert_v1(alert)?;
        db.persist_sync_all_v1()
    })?;
    Ok(())
}

fn write_plain_log_v1(
    cfg: &sparx::config::ConfigV1,
    rel_name: &str,
    body: &str,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let path = std::path::Path::new(&cfg.sparx.tenant_root)
        .join("tenant-a")
        .join("device-a")
        .join(rel_name);
    fs::create_dir_all(path.parent().unwrap())?;
    fs::write(&path, body.as_bytes())?;
    Ok(path)
}

fn write_gzip_log_v1(
    cfg: &sparx::config::ConfigV1,
    rel_name: &str,
    body: &str,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let path = std::path::Path::new(&cfg.sparx.tenant_root)
        .join("tenant-a")
        .join("device-a")
        .join(rel_name);
    fs::create_dir_all(path.parent().unwrap())?;
    let file = fs::File::create(&path)?;
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(body.as_bytes())?;
    encoder.finish()?;
    Ok(path)
}

#[test]
fn alert_drill_reads_plain_span_and_enforces_max_lines_v1() -> Result<(), Box<dyn std::error::Error>>
{
    let cfg = temp_cfg_v1();
    let body = "alpha\nbravo\ncharlie\n";
    let bravo_start = body.find("bravo").unwrap() as u64;
    let plain_path = write_plain_log_v1(&cfg, "messages.log", body)?;
    let alert = sample_alert_v1(
        "alert-plain",
        vec![FileSpanV1 {
            file_rel: "messages.log".to_string(),
            file_key: "f-plain".to_string(),
            inode: 1,
            offset_start: bravo_start,
            offset_end: plain_path.metadata()?.len(),
            is_gzip: false,
        }],
    );
    seed_alert_v1(&cfg, &alert)?;

    let result = route_command_v1(
        &CommandV1::AlertDrill {
            tenant_id: "tenant-a".to_string(),
            alert_id: "alert-plain".to_string(),
            max_bytes: None,
            max_lines: Some(1),
        },
        &cfg,
    );
    assert_eq!(0, result.exit_code);
    let out = result.msg_stdout.unwrap();
    assert!(out.contains("spans_emitted: 1"));
    assert!(out.contains("lines_emitted: 1"));
    assert!(out.contains("bravo"));
    assert!(!out.contains("charlie"));
    Ok(())
}

#[test]
fn alert_drill_skips_gzip_span_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let gzip_path = write_gzip_log_v1(&cfg, "messages.log.gz", "zip-one\nzip-two\n")?;
    let alert = sample_alert_v1(
        "alert-gzip",
        vec![FileSpanV1 {
            file_rel: "messages.log.gz".to_string(),
            file_key: "f-gzip".to_string(),
            inode: 2,
            offset_start: 0,
            offset_end: gzip_path.metadata()?.len(),
            is_gzip: true,
        }],
    );
    seed_alert_v1(&cfg, &alert)?;

    let result = route_command_v1(
        &CommandV1::AlertDrill {
            tenant_id: "tenant-a".to_string(),
            alert_id: "alert-gzip".to_string(),
            max_bytes: None,
            max_lines: None,
        },
        &cfg,
    );
    assert_eq!(0, result.exit_code);
    let out = result.msg_stdout.unwrap();
    assert!(out.contains("gzip_spans_skipped: 1"));
    assert!(out.contains("gzip_skipped: true"));
    Ok(())
}

#[test]
fn alert_extract_writes_plain_and_gzip_ranges_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let plain_body = "plain-one\nplain-two\n";
    let plain_path = write_plain_log_v1(&cfg, "messages.log", plain_body)?;
    let gzip_body = "zip-one\nzip-two\n";
    let gzip_path = write_gzip_log_v1(&cfg, "messages.log.gz", gzip_body)?;
    let alert = sample_alert_v1(
        "alert-extract",
        vec![
            FileSpanV1 {
                file_rel: "messages.log".to_string(),
                file_key: "f-plain".to_string(),
                inode: 3,
                offset_start: 0,
                offset_end: plain_path.metadata()?.len(),
                is_gzip: false,
            },
            FileSpanV1 {
                file_rel: "messages.log.gz".to_string(),
                file_key: "f-gzip".to_string(),
                inode: 4,
                offset_start: 0,
                offset_end: gzip_path.metadata()?.len(),
                is_gzip: true,
            },
        ],
    );
    seed_alert_v1(&cfg, &alert)?;
    let out_path = std::path::Path::new(&cfg.sparx.data_root).join("extracts/alert.log");

    let result = route_command_v1(
        &CommandV1::AlertExtract {
            tenant_id: "tenant-a".to_string(),
            alert_id: "alert-extract".to_string(),
            out_path: out_path.display().to_string(),
            max_bytes: None,
            max_lines: None,
        },
        &cfg,
    );
    assert_eq!(0, result.exit_code);
    let out = result.msg_stdout.unwrap();
    assert!(out.contains("spans_written: 2"));
    let data = fs::read_to_string(&out_path)?;
    assert!(data.contains("plain-one"));
    assert!(data.contains("zip-one"));
    Ok(())
}

#[test]
fn alert_extract_missing_file_returns_exit_three_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let alert = sample_alert_v1(
        "alert-missing",
        vec![FileSpanV1 {
            file_rel: "missing.log".to_string(),
            file_key: "f-missing".to_string(),
            inode: 5,
            offset_start: 0,
            offset_end: 32,
            is_gzip: false,
        }],
    );
    seed_alert_v1(&cfg, &alert)?;
    let out_path = std::path::Path::new(&cfg.sparx.data_root).join("extracts/missing.log");

    let result = route_command_v1(
        &CommandV1::AlertExtract {
            tenant_id: "tenant-a".to_string(),
            alert_id: "alert-missing".to_string(),
            out_path: out_path.display().to_string(),
            max_bytes: None,
            max_lines: None,
        },
        &cfg,
    );
    assert_eq!(3, result.exit_code);
    assert!(result
        .msg_stderr
        .unwrap()
        .contains("alert extract io error"));
    Ok(())
}

#[test]
fn alert_drill_resolves_runtime_device_path_with_tenant_prefix_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let body = "alpha\nbravo\n";
    let plain_path = write_plain_log_v1(&cfg, "runtime.log", body)?;
    let mut alert = sample_alert_v1(
        "alert-runtime-path",
        vec![FileSpanV1 {
            file_rel: "runtime.log".to_string(),
            file_key: "f-runtime".to_string(),
            inode: 1,
            offset_start: 0,
            offset_end: plain_path.metadata()?.len(),
            is_gzip: false,
        }],
    );
    alert.device_path = "tenant-a/device-a".to_string();
    seed_alert_v1(&cfg, &alert)?;

    let result = route_command_v1(
        &CommandV1::AlertDrill {
            tenant_id: "tenant-a".to_string(),
            alert_id: "alert-runtime-path".to_string(),
            max_bytes: None,
            max_lines: Some(2),
        },
        &cfg,
    );
    assert_eq!(0, result.exit_code);
    let out = result.msg_stdout.unwrap();
    assert!(out.contains("spans_emitted: 1"));
    assert!(out.contains("alpha"));
    assert!(out.contains("bravo"));
    Ok(())
}

#[test]
fn alert_drill_rejects_provenance_path_traversal_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    write_plain_log_v1(&cfg, "safe.log", "alpha\n")?;
    let alert = sample_alert_v1(
        "alert-traversal",
        vec![FileSpanV1 {
            file_rel: "../outside.log".to_string(),
            file_key: "f-bad".to_string(),
            inode: 1,
            offset_start: 0,
            offset_end: 5,
            is_gzip: false,
        }],
    );
    seed_alert_v1(&cfg, &alert)?;

    let result = route_command_v1(
        &CommandV1::AlertDrill {
            tenant_id: "tenant-a".to_string(),
            alert_id: "alert-traversal".to_string(),
            max_bytes: None,
            max_lines: None,
        },
        &cfg,
    );
    assert_ne!(0, result.exit_code);
    assert!(result.msg_stderr.unwrap().contains("alert drill io error"));
    Ok(())
}

#[test]
fn alert_drill_resolves_source_stream_display_path_by_device_key_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let body = "source-one\nsource-two\n";
    let plain_path = write_plain_log_v1(&cfg, "source.log", body)?;
    let device_key = sparx::ingest::device_key_v1("tenant-a", "device-a");
    let mut alert = sample_alert_v1(
        "alert-source-stream-drill",
        vec![FileSpanV1 {
            file_rel: "source.log".to_string(),
            file_key: "f-source".to_string(),
            inode: 1,
            offset_start: 0,
            offset_end: plain_path.metadata()?.len(),
            is_gzip: false,
        }],
    );
    alert.device_key = device_key.clone();
    alert.device_path = format!("source_stream:{}/source.log", device_key);
    seed_alert_v1(&cfg, &alert)?;

    let result = route_command_v1(
        &CommandV1::AlertDrill {
            tenant_id: "tenant-a".to_string(),
            alert_id: "alert-source-stream-drill".to_string(),
            max_bytes: None,
            max_lines: Some(2),
        },
        &cfg,
    );
    assert_eq!(0, result.exit_code);
    let out = result.msg_stdout.unwrap();
    assert!(out.contains("spans_emitted: 1"));
    assert!(out.contains("source-one"));
    assert!(out.contains("source-two"));
    Ok(())
}
