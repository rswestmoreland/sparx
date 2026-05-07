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

fn copy_fixture_device_v1(cfg: &sparx::config::ConfigV1, tenant_id: &str, device: &str, files: &[&str]) {
    let device_dir = Path::new(&cfg.sparx.tenant_root).join(tenant_id).join(device);
    fs::create_dir_all(&device_dir).unwrap();
    for file in files {
        fs::copy(fixture_source_v1(file), device_dir.join(file)).unwrap();
    }
}

fn write_bad_gzip_device_v1(cfg: &sparx::config::ConfigV1, tenant_id: &str, device: &str, name: &str) {
    let device_dir = Path::new(&cfg.sparx.tenant_root).join(tenant_id).join(device);
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
            *count = count.saturating_add(content.lines().filter(|line| !line.trim().is_empty()).count());
        }
    }
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
fn oneshot_single_tenant_pass_emits_alerts_and_advances_cursors_v1() -> Result<(), Box<dyn std::error::Error>> {
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
    let log_size = fs::metadata(Path::new(&cfg.sparx.tenant_root).join("smoke").join("edge01").join("edge01.log"))?.len();
    let gz_size = fs::metadata(Path::new(&cfg.sparx.tenant_root).join("smoke").join("edge01").join("edge01.gz"))?.len();

    let log_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&device_key, &log_file_key))?;
    let gz_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&device_key, &gz_file_key))?;
    assert_eq!(log_cursor.unwrap().offset, log_size);
    assert_eq!(gz_cursor.unwrap().offset, gz_size);
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

    let edge01_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&edge01_key, &file_key))?;
    let edge02_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&edge02_key, &file_key))?;
    assert!(edge01_cursor.is_none());
    assert!(edge02_cursor.is_some());
    Ok(())
}

#[test]
fn oneshot_time_filter_advances_cursor_without_emitting_alerts_v1() -> Result<(), Box<dyn std::error::Error>> {
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
    assert_eq!(count_alert_lines_v1(Path::new(&cfg.sparx.alert_out_root)), 0);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    let file_key = file_key_v1("edge01.log");
    let size = fs::metadata(Path::new(&cfg.sparx.tenant_root).join("smoke").join("edge01").join("edge01.log"))?.len();
    let cursor = runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&device_key, &file_key))?;
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
    let edge01_log_size = fs::metadata(Path::new(&cfg.sparx.tenant_root).join("smoke").join("edge01").join("edge01.log")).unwrap().len();
    let edge01_gz_size = fs::metadata(Path::new(&cfg.sparx.tenant_root).join("smoke").join("edge01").join("edge01.gz")).unwrap().len();

    let edge01_log_cursor = runtime
        .with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&edge01_key, &edge01_log_key))
        .unwrap();
    let edge01_gz_cursor = runtime
        .with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&edge01_key, &edge01_gz_key))
        .unwrap();
    assert_eq!(edge01_log_cursor.unwrap().offset, edge01_log_size);
    assert_eq!(edge01_gz_cursor.unwrap().offset, edge01_gz_size);
}

#[test]
fn oneshot_replays_spooled_alerts_automatically_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
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

    let out_path = jsonl_alert_path_v1(&cfg.sparx.alert_out_root, "smoke", "device-a", alert.window_start_ts, 0).unwrap();
    let line = fs::read_to_string(out_path).unwrap();
    let value: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
    assert_eq!(value["alert_id"].as_str(), Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    Ok(())
}
