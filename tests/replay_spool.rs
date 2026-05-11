// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;

use tempfile::tempdir;

use sparx::alert::{
    AlertV1, CountedStringV1, EntitiesV1, FileSpanV1, ReasonV1, TopFeatureV1,
    ALERT_SCHEMA_VERSION_V1,
};
use sparx::cli::route::route_command_v1;
use sparx::cli::CommandV1;
use sparx::config::load::default_config_v1;
use sparx::sink::{jsonl_alert_path_v1, spool_alert_dir_v1, write_spool_alert_v1};
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
    cfg
}

fn sample_alert_v1(tenant_id: &str, device_key: &str, alert_id: &str, summary: &str) -> AlertV1 {
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
        summary_analyst: summary.to_string(),
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
fn replay_spool_all_tenants_replays_and_deletes_files_v1() {
    let cfg = temp_cfg_v1();
    let alert_a = sample_alert_v1("tenant-a", "device-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "tenant a summary");
    let alert_b = sample_alert_v1("tenant-b", "device-b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "tenant b summary");

    let path_a = write_spool_alert_v1(&cfg.sparx.data_root, &alert_a).unwrap();
    let path_b = write_spool_alert_v1(&cfg.sparx.data_root, &alert_b).unwrap();

    let r = route_command_v1(&CommandV1::ReplaySpool { tenant_id: None }, &cfg);
    assert_eq!(0, r.exit_code);
    let stdout = r.msg_stdout.unwrap();
    assert!(stdout.contains("scope: all"));
    assert!(stdout.contains("replayed: 2"));
    assert!(stdout.contains("failed: 0"));
    assert!(r.msg_stderr.is_none());
    assert!(!path_a.exists());
    assert!(!path_b.exists());

    let out_a = jsonl_alert_path_v1(&cfg.sparx.alert_out_root, "tenant-a", "device-a", alert_a.window_start_ts, 0).unwrap();
    let out_b = jsonl_alert_path_v1(&cfg.sparx.alert_out_root, "tenant-b", "device-b", alert_b.window_start_ts, 0).unwrap();
    assert!(out_a.is_file());
    assert!(out_b.is_file());
}

#[test]
fn replay_spool_single_tenant_leaves_other_tenants_untouched_v1() {
    let cfg = temp_cfg_v1();
    let alert_a = sample_alert_v1("tenant-a", "device-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "tenant a summary");
    let alert_b = sample_alert_v1("tenant-b", "device-b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "tenant b summary");

    let path_a = write_spool_alert_v1(&cfg.sparx.data_root, &alert_a).unwrap();
    let path_b = write_spool_alert_v1(&cfg.sparx.data_root, &alert_b).unwrap();

    let r = route_command_v1(
        &CommandV1::ReplaySpool {
            tenant_id: Some("tenant-a".to_string()),
        },
        &cfg,
    );
    assert_eq!(0, r.exit_code);
    let stdout = r.msg_stdout.unwrap();
    assert!(stdout.contains("scope: tenant-a"));
    assert!(stdout.contains("replayed: 1"));
    assert!(stdout.contains("failed: 0"));
    assert!(!path_a.exists());
    assert!(path_b.exists());

    let out_a = jsonl_alert_path_v1(&cfg.sparx.alert_out_root, "tenant-a", "device-a", alert_a.window_start_ts, 0).unwrap();
    let out_b = jsonl_alert_path_v1(&cfg.sparx.alert_out_root, "tenant-b", "device-b", alert_b.window_start_ts, 0).unwrap();
    assert!(out_a.is_file());
    assert!(!out_b.exists());
}

#[test]
fn replay_spool_partial_failure_preserves_failed_files_v1() {
    let cfg = temp_cfg_v1();
    let alert = sample_alert_v1("tenant-a", "device-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "tenant a summary");
    let good_path = write_spool_alert_v1(&cfg.sparx.data_root, &alert).unwrap();
    let bad_dir = spool_alert_dir_v1(&cfg.sparx.data_root, "tenant-a").unwrap();
    fs::create_dir_all(&bad_dir).unwrap();
    let bad_path = bad_dir.join("spool_broken.json");
    fs::write(&bad_path, b"not-json").unwrap();

    let r = route_command_v1(&CommandV1::ReplaySpool { tenant_id: None }, &cfg);
    assert_eq!(6, r.exit_code);
    let stdout = r.msg_stdout.unwrap();
    assert!(stdout.contains("replayed: 1"));
    assert!(stdout.contains("failed: 1"));
    let stderr = r.msg_stderr.unwrap();
    assert!(stderr.contains("replay-spool partial failure"));
    assert!(stderr.contains(&bad_path.display().to_string()));
    assert!(!good_path.exists());
    assert!(bad_path.exists());

    let out_path = jsonl_alert_path_v1(&cfg.sparx.alert_out_root, "tenant-a", "device-a", alert.window_start_ts, 0).unwrap();
    assert!(out_path.is_file());
}

#[test]
fn replay_spool_is_deterministic_by_filename_v1() {
    let cfg = temp_cfg_v1();
    let alert_b = sample_alert_v1("tenant-a", "device-a", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "second summary");
    let alert_a = sample_alert_v1("tenant-a", "device-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "first summary");

    write_spool_alert_v1(&cfg.sparx.data_root, &alert_b).unwrap();
    write_spool_alert_v1(&cfg.sparx.data_root, &alert_a).unwrap();

    let r = route_command_v1(&CommandV1::ReplaySpool { tenant_id: None }, &cfg);
    assert_eq!(0, r.exit_code);

    let out_path = jsonl_alert_path_v1(&cfg.sparx.alert_out_root, "tenant-a", "device-a", alert_a.window_start_ts, 0).unwrap();
    let content = fs::read_to_string(out_path).unwrap();
    let mut lines = content.lines();
    let first: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    let second: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", first["alert_id"].as_str().unwrap());
    assert_eq!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", second["alert_id"].as_str().unwrap());
    assert!(lines.next().is_none());
}

#[test]
fn replay_spool_fails_closed_for_stdout_sink_v1() {
    let mut cfg = temp_cfg_v1();
    cfg.output.sink = "stdout".to_string();
    let alert = sample_alert_v1("tenant-a", "device-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "tenant a summary");
    let spool_path = write_spool_alert_v1(&cfg.sparx.data_root, &alert).unwrap();

    let r = route_command_v1(&CommandV1::ReplaySpool { tenant_id: None }, &cfg);
    assert_eq!(1, r.exit_code);
    assert!(r.msg_stdout.is_none());
    let stderr = r.msg_stderr.unwrap();
    assert!(stderr.contains("replay-spool requires output.sink=jsonl"));
    assert!(spool_path.exists());

    let out_path = jsonl_alert_path_v1(&cfg.sparx.alert_out_root, "tenant-a", "device-a", alert.window_start_ts, 0).unwrap();
    assert!(!out_path.exists());
}
