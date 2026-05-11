// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;
use std::io::Read;

use flate2::read::GzDecoder;
use serde_json::Value;
use tempfile::tempdir;

use sparx::alert::{
    encode_alert_v1, AlertV1, CountedStringV1, EntitiesV1, FileSpanV1, ReasonV1, TopFeatureV1,
    ALERT_SCHEMA_VERSION_V1,
};
use sparx::cli::route::route_command_v1;
use sparx::cli::{AlertCategoryFilterV1, AlertEntityKindFilterV1, CommandV1};
use sparx::config::load::default_config_v1;
use sparx::db::keys::key_tenant_alert_v1;
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
    cfg
}

fn sample_alert_v1(alert_id: &str, window_start_ts: i64, summary_analyst: &str) -> AlertV1 {
    AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id: alert_id.to_string(),
        tenant_id: "tenant-a".to_string(),
        device_key: "device-001".to_string(),
        device_path: "tenant-a/device-a".to_string(),
        window_start_ts,
        window_end_ts: window_start_ts + 600,
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
        summary_analyst: summary_analyst.to_string(),
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
        provenance: vec![FileSpanV1 {
            file_rel: "messages.log".to_string(),
            file_key: "f-001".to_string(),
            inode: 777,
            offset_start: 120,
            offset_end: 240,
            is_gzip: false,
        }],
        signature: format!("sig-{}", alert_id),
    }
}

fn seed_alerts_v1(
    cfg: &sparx::config::ConfigV1,
    alerts: &[AlertV1],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = SparxRuntimeV1::open_from_config_v1(cfg)?;
    runtime.with_tenant_db_v1("tenant-a", 1_700_200_000, |db| {
        for alert in alerts {
            db.write_primary_alert_v1(alert)?;
        }
        db.persist_sync_all_v1()
    })?;
    drop(runtime);
    Ok(())
}

#[test]
fn alerts_list_json_is_sorted_by_window_desc_then_alert_id_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    seed_alerts_v1(
        &cfg,
        &[
            sample_alert_v1("alert-b", 100, "second"),
            sample_alert_v1("alert-a", 100, "first same ts"),
            sample_alert_v1("alert-c", 200, "latest"),
        ],
    )?;

    let r = route_command_v1(
        &CommandV1::AlertsList {
            tenant_id: "tenant-a".to_string(),
            since: None,
            until: None,
            category: None,
            entity_kind: None,
            entity_value: None,
            json: true,
        },
        &cfg,
    );
    assert_eq!(0, r.exit_code);
    let value: Value = serde_json::from_str(&r.msg_stdout.unwrap())?;
    let alerts = value["alerts"].as_array().unwrap();
    assert_eq!(3, alerts.len());
    assert_eq!("alert-c", alerts[0]["alert_id"].as_str().unwrap());
    assert_eq!("alert-a", alerts[1]["alert_id"].as_str().unwrap());
    assert_eq!("alert-b", alerts[2]["alert_id"].as_str().unwrap());
    Ok(())
}

#[test]
fn alerts_show_json_and_missing_alert_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    seed_alerts_v1(&cfg, &[sample_alert_v1("alert-a", 100, "show me")])?;

    let ok = route_command_v1(
        &CommandV1::AlertsShow {
            tenant_id: "tenant-a".to_string(),
            alert_id: "alert-a".to_string(),
            json: true,
        },
        &cfg,
    );
    assert_eq!(0, ok.exit_code);
    let value: Value = serde_json::from_str(&ok.msg_stdout.unwrap())?;
    assert_eq!("alert-a", value["alert"]["alert_id"].as_str().unwrap());
    assert_eq!(
        "show me",
        value["alert"]["summary_analyst"].as_str().unwrap()
    );

    let missing = route_command_v1(
        &CommandV1::AlertsShow {
            tenant_id: "tenant-a".to_string(),
            alert_id: "missing".to_string(),
            json: false,
        },
        &cfg,
    );
    assert_eq!(1, missing.exit_code);
    assert!(missing
        .msg_stderr
        .unwrap()
        .contains("alert not found: missing"));
    Ok(())
}

#[test]
fn alerts_search_honors_time_filter_and_contains_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    seed_alerts_v1(
        &cfg,
        &[
            sample_alert_v1("alert-a", 100, "alice login"),
            sample_alert_v1("alert-b", 150, "bob login"),
            sample_alert_v1("alert-c", 200, "alice escalation"),
        ],
    )?;

    let r = route_command_v1(
        &CommandV1::AlertsSearch {
            tenant_id: "tenant-a".to_string(),
            since: Some(120),
            until: Some(250),
            category: None,
            entity_kind: None,
            entity_value: None,
            contains: "escalation".to_string(),
        },
        &cfg,
    );
    assert_eq!(0, r.exit_code);
    let out = r.msg_stdout.unwrap();
    assert!(out.contains("count: 1"));
    assert!(out.contains("alert-c"));
    assert!(!out.contains("alert-a"));
    assert!(!out.contains("alert-b"));
    Ok(())
}

#[test]
fn alerts_list_falls_back_when_time_index_is_incomplete_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    runtime.with_tenant_db_v1("tenant-a", 1_700_200_000, |db| {
        let indexed = sample_alert_v1("alert-indexed", 100, "indexed path");
        db.write_primary_alert_v1(&indexed)?;

        let legacy = sample_alert_v1("alert-legacy", 200, "legacy primary only");
        let encoded = encode_alert_v1(&legacy).map_err(|e| {
            sparx::db::DbErrorV1::new_v1(format!("legacy alert encode failed: {:?}", e))
        })?;
        db.put_raw_v1(key_tenant_alert_v1(&legacy.alert_id).as_bytes(), &encoded)?;
        db.persist_sync_all_v1()
    })?;
    drop(runtime);

    let r = route_command_v1(
        &CommandV1::AlertsList {
            tenant_id: "tenant-a".to_string(),
            since: None,
            until: None,
            category: None,
            entity_kind: None,
            entity_value: None,
            json: true,
        },
        &cfg,
    );
    assert_eq!(0, r.exit_code);
    let value: Value = serde_json::from_str(&r.msg_stdout.unwrap())?;
    let alerts = value["alerts"].as_array().unwrap();
    assert_eq!(2, alerts.len());
    assert_eq!("alert-legacy", alerts[0]["alert_id"].as_str().unwrap());
    assert_eq!("alert-indexed", alerts[1]["alert_id"].as_str().unwrap());
    Ok(())
}

#[test]
fn alerts_list_structured_category_filter_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let alert_a = sample_alert_v1("alert-a", 100, "outlier match");
    let mut alert_b = sample_alert_v1("alert-b", 200, "noise match");
    alert_b.label = LabelV1::NoiseSuspect;
    let mut alert_c = sample_alert_v1("alert-c", 300, "info match");
    alert_c.label = LabelV1::Info;
    seed_alerts_v1(&cfg, &[alert_a, alert_b, alert_c])?;

    let r = route_command_v1(
        &CommandV1::AlertsList {
            tenant_id: "tenant-a".to_string(),
            since: None,
            until: None,
            category: Some(AlertCategoryFilterV1::NoiseSuspect),
            entity_kind: None,
            entity_value: None,
            json: true,
        },
        &cfg,
    );
    assert_eq!(0, r.exit_code);
    let value: Value = serde_json::from_str(&r.msg_stdout.unwrap())?;
    let filters = value["filters"].as_object().unwrap();
    assert_eq!(
        Some("noise_suspect"),
        filters.get("category").and_then(|v| v.as_str())
    );
    let alerts = value["alerts"].as_array().unwrap();
    assert_eq!(1, alerts.len());
    assert_eq!("alert-b", alerts[0]["alert_id"].as_str().unwrap());
    Ok(())
}

#[test]
fn alerts_search_structured_entity_filter_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let alert_a = sample_alert_v1("alert-a", 100, "alice login");
    let mut alert_b = sample_alert_v1("alert-b", 200, "bob login");
    alert_b.entities.user_ids = vec![CountedStringV1 {
        value: "bob".to_string(),
        count: 1,
    }];
    let mut alert_c = sample_alert_v1("alert-c", 300, "alice escalation");
    alert_c.entities.user_ids.push(CountedStringV1 {
        value: "root".to_string(),
        count: 1,
    });
    seed_alerts_v1(&cfg, &[alert_a, alert_b, alert_c])?;

    let r = route_command_v1(
        &CommandV1::AlertsSearch {
            tenant_id: "tenant-a".to_string(),
            since: None,
            until: None,
            category: None,
            entity_kind: Some(AlertEntityKindFilterV1::UserId),
            entity_value: Some("alice".to_string()),
            contains: "alice".to_string(),
        },
        &cfg,
    );
    assert_eq!(0, r.exit_code);
    let out = r.msg_stdout.unwrap();
    assert!(out.contains("entity_kind: userid"));
    assert!(out.contains("entity_value: alice"));
    assert!(out.contains("alert-c"));
    assert!(out.contains("alert-a"));
    assert!(!out.contains("alert-b"));
    Ok(())
}

#[test]
fn alerts_list_falls_back_when_entity_index_is_incomplete_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    runtime.with_tenant_db_v1("tenant-a", 1_700_200_000, |db| {
        let indexed = sample_alert_v1("alert-indexed", 100, "indexed alice");
        db.write_primary_alert_v1(&indexed)?;

        let legacy = sample_alert_v1("alert-legacy", 200, "legacy alice");
        let encoded = encode_alert_v1(&legacy).map_err(|e| {
            sparx::db::DbErrorV1::new_v1(format!("legacy alert encode failed: {:?}", e))
        })?;
        db.put_raw_v1(key_tenant_alert_v1(&legacy.alert_id).as_bytes(), &encoded)?;
        db.persist_sync_all_v1()
    })?;
    drop(runtime);

    let r = route_command_v1(
        &CommandV1::AlertsList {
            tenant_id: "tenant-a".to_string(),
            since: None,
            until: None,
            category: None,
            entity_kind: Some(AlertEntityKindFilterV1::UserId),
            entity_value: Some("alice".to_string()),
            json: true,
        },
        &cfg,
    );
    assert_eq!(0, r.exit_code);
    let value: Value = serde_json::from_str(&r.msg_stdout.unwrap())?;
    let alerts = value["alerts"].as_array().unwrap();
    assert_eq!(2, alerts.len());
    assert_eq!("alert-legacy", alerts[0]["alert_id"].as_str().unwrap());
    assert_eq!("alert-indexed", alerts[1]["alert_id"].as_str().unwrap());
    Ok(())
}

#[test]
fn alerts_export_structured_category_filter_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let alert_a = sample_alert_v1("alert-a", 100, "outlier export");
    let mut alert_b = sample_alert_v1("alert-b", 200, "noise export");
    alert_b.label = LabelV1::NoiseSuspect;
    seed_alerts_v1(&cfg, &[alert_a, alert_b])?;

    let out_path = std::path::Path::new(&cfg.sparx.data_root).join("exports/filtered.jsonl");
    let r = route_command_v1(
        &CommandV1::AlertsExport {
            tenant_id: "tenant-a".to_string(),
            category: Some(AlertCategoryFilterV1::NoiseSuspect),
            entity_kind: None,
            entity_value: None,
            out_path: out_path.display().to_string(),
            gzip: false,
        },
        &cfg,
    );
    assert_eq!(0, r.exit_code);
    let data = fs::read_to_string(&out_path)?;
    let lines: Vec<&str> = data.lines().collect();
    assert_eq!(1, lines.len());
    let value: Value = serde_json::from_str(lines[0])?;
    assert_eq!("alert-b", value["alert_id"].as_str().unwrap());
    Ok(())
}

#[test]
fn alerts_export_supports_plain_and_gzip_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    seed_alerts_v1(
        &cfg,
        &[
            sample_alert_v1("alert-a", 100, "first"),
            sample_alert_v1("alert-b", 200, "second"),
        ],
    )?;

    let plain_path = std::path::Path::new(&cfg.sparx.data_root).join("exports/plain.jsonl");
    let gzip_path = std::path::Path::new(&cfg.sparx.data_root).join("exports/gzip.jsonl.gz");

    let plain = route_command_v1(
        &CommandV1::AlertsExport {
            tenant_id: "tenant-a".to_string(),
            category: None,
            entity_kind: None,
            entity_value: None,
            out_path: plain_path.display().to_string(),
            gzip: false,
        },
        &cfg,
    );
    assert_eq!(0, plain.exit_code);
    let plain_data = fs::read_to_string(&plain_path)?;
    let plain_lines: Vec<&str> = plain_data.lines().collect();
    assert_eq!(2, plain_lines.len());
    let first_plain: Value = serde_json::from_str(plain_lines[0])?;
    assert_eq!("alert-b", first_plain["alert_id"].as_str().unwrap());

    let gzip = route_command_v1(
        &CommandV1::AlertsExport {
            tenant_id: "tenant-a".to_string(),
            category: None,
            entity_kind: None,
            entity_value: None,
            out_path: gzip_path.display().to_string(),
            gzip: true,
        },
        &cfg,
    );
    assert_eq!(0, gzip.exit_code);
    let file = fs::File::open(&gzip_path)?;
    let mut decoder = GzDecoder::new(file);
    let mut gzip_data = String::new();
    decoder.read_to_string(&mut gzip_data)?;
    let gzip_lines: Vec<&str> = gzip_data.lines().collect();
    assert_eq!(2, gzip_lines.len());
    let first_gzip: Value = serde_json::from_str(gzip_lines[0])?;
    assert_eq!("alert-b", first_gzip["alert_id"].as_str().unwrap());
    Ok(())
}
