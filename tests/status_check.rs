use tempfile::tempdir;

use sparx::cli::CommandV1;
use sparx::cli::route::{command_requires_config_v1, route_command_no_config_v1, route_command_v1};
use sparx::config::load::default_config_v1;
use sparx::db::{GlobalSchemaStateV1, GlobalTenantRecordV1};
use sparx::runtime::SparxRuntimeV1;
use sparx::sink::write_spool_alert_v1;

#[derive(serde::Serialize)]
struct ExpectedStatusRootsV1 {
    data_root: String,
    tenant_root: String,
    global_db_path: String,
    tenant_db_root: String,
    alert_out_root: String,
    spool_root: String,
}

#[derive(serde::Serialize)]
struct ExpectedStatusTenantCountsV1 {
    known_count: usize,
    active_count: usize,
}

#[derive(serde::Serialize)]
struct ExpectedStatusProcessStateV1 {
    last_run_start_ts: Option<i64>,
    last_run_end_ts: Option<i64>,
    last_run_exit_code: Option<i32>,
    last_run_host: Option<String>,
}

#[derive(serde::Serialize)]
struct ExpectedStatusRuntimeStateV1 {
    global_schema_version: Option<u32>,
    global_schema_created_ts: Option<i64>,
    global_schema_last_migrate_ts: Option<i64>,
}

#[derive(serde::Serialize)]
struct ExpectedStatusObservabilityV1 {
    prometheus_enabled: bool,
    prometheus_bind: String,
    prometheus_url: Option<String>,
    health_enabled: bool,
    health_bind: String,
    health_url: Option<String>,
}

#[derive(serde::Serialize)]
struct ExpectedStatusMetricsV1 {
    run_cycles_completed_total: u64,
    run_tenants_total: u64,
    run_tenants_processed_total: u64,
    run_tenants_skipped_total: u64,
    run_devices_processed_total: u64,
    run_devices_failed_total: u64,
    run_alerts_emitted_total: u64,
    run_last_cycle_tenants_total: Option<u64>,
    run_last_cycle_tenants_processed: Option<u64>,
    run_last_cycle_tenants_skipped: Option<u64>,
    run_last_cycle_devices_processed: Option<u64>,
    run_last_cycle_devices_failed: Option<u64>,
    run_last_cycle_alerts_emitted: Option<u64>,
    run_last_cycle_completed_ts: Option<u64>,
}

#[derive(serde::Serialize)]
struct ExpectedStatusRecoveryV1 {
    automated_replay_max_files_per_pass: u32,
    spool_backlog_files: u64,
    spool_backlog_bytes: u64,
}

#[derive(serde::Serialize)]
struct ExpectedStatusSnapshotV1 {
    version: String,
    mode: String,
    window_size_s: u32,
    sink: String,
    roots: ExpectedStatusRootsV1,
    tenants: ExpectedStatusTenantCountsV1,
    process: ExpectedStatusProcessStateV1,
    runtime: ExpectedStatusRuntimeStateV1,
    observability: ExpectedStatusObservabilityV1,
    metrics: ExpectedStatusMetricsV1,
    recovery: ExpectedStatusRecoveryV1,
}

fn sample_spooled_alert_v1() -> sparx::alert::AlertV1 {
    sparx::alert::AlertV1 {
        schema_version: 1,
        alert_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        tenant_id: "tenant-z".to_string(),
        device_key: "device-z".to_string(),
        window_start_ts: 1_700_200_000,
        window_end_ts: 1_700_200_060,
        explain_version: 1,
        score: 0.95,
        confidence: sparx::types::ConfidenceV1::High,
        label: "Anomaly".to_string(),
        score_shape: 0.80,
        score_volume: 0.70,
        baseline_n_bucket: Some(3),
        baseline_centroid_norm: Some(1.2),
        reasons: vec![],
        top_features: vec![],
        summary_analyst: "spooled alert".to_string(),
        summary_customer: "An unusual pattern was observed in this log window.".to_string(),
        entities: sparx::alert::EntitiesV1 {
            src_ips: vec![],
            dst_ips: vec![],
            user_ids: vec![],
            domains: vec![],
            hosts: vec![],
        },
        lines: 1,
        bytes: 64,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
        provenance: vec![sparx::alert::FileSpanV1 {
            file_rel: "app.log".to_string(),
            file_key: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            inode: 1,
            offset_start: 0,
            offset_end: 1,
            is_gzip: false,
        }],
        signature: "sig-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
    }
}

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
    cfg
}

fn expected_status_text_v1(cfg: &sparx::config::ConfigV1) -> String {
    let spool_root = std::path::Path::new(&cfg.sparx.data_root)
        .join("spool")
        .join("alerts")
        .display()
        .to_string();
    format!(
        "sparx status
version: sparx 0.0.0
mode: {}
window_size_s: {}
sink: {}
roots.data_root: {}
roots.tenant_root: {}
roots.global_db_path: {}
roots.tenant_db_root: {}
roots.alert_out_root: {}
roots.spool_root: {}
tenants.known_count: 0
tenants.active_count: 0
process.last_run_start_ts: null
process.last_run_end_ts: null
process.last_run_exit_code: null
process.last_run_host: null
runtime.global_schema_version: null
runtime.global_schema_created_ts: null
runtime.global_schema_last_migrate_ts: null
observability.prometheus_enabled: {}
observability.prometheus_bind: {}
observability.prometheus_url: {}
observability.health_enabled: {}
observability.health_bind: {}
observability.health_url: {}
metrics.run_cycles_completed_total: 0
metrics.run_tenants_total: 0
metrics.run_tenants_processed_total: 0
metrics.run_tenants_skipped_total: 0
metrics.run_devices_processed_total: 0
metrics.run_devices_failed_total: 0
metrics.run_alerts_emitted_total: 0
metrics.run_last_cycle_tenants_total: null
metrics.run_last_cycle_tenants_processed: null
metrics.run_last_cycle_tenants_skipped: null
metrics.run_last_cycle_devices_processed: null
metrics.run_last_cycle_devices_failed: null
metrics.run_last_cycle_alerts_emitted: null
metrics.run_last_cycle_completed_ts: null
recovery.automated_replay_max_files_per_pass: {}
recovery.spool_backlog_files: 0
recovery.spool_backlog_bytes: 0
",
        cfg.sparx.mode,
        cfg.ingest.window_size_s,
        cfg.output.sink,
        cfg.sparx.data_root,
        cfg.sparx.tenant_root,
        cfg.sparx.global_db_path,
        cfg.sparx.tenant_db_root,
        cfg.sparx.alert_out_root,
        spool_root,
        cfg.metrics.prometheus_enabled,
        cfg.metrics.prometheus_bind,
        format!("http://{}{}", cfg.metrics.prometheus_bind, "/metrics"),
        cfg.metrics.health_enabled,
        cfg.metrics.health_bind,
        format!("http://{}{}", cfg.metrics.health_bind, "/healthz"),
        cfg.output.automated_replay_max_files_per_pass,
    )
}

#[test]
fn status_text_empty_global_db_is_deterministic_v1() {
    let cfg = temp_cfg_v1();
    let r = route_command_v1(&CommandV1::Status { json: false }, &cfg);
    assert_eq!(r.exit_code, 0);
    assert_eq!(expected_status_text_v1(&cfg), r.msg_stdout.unwrap());
}

#[test]
fn status_json_populated_runtime_state_is_deterministic_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;

    runtime.write_global_schema_state_v1(&GlobalSchemaStateV1 {
        version: 1,
        created_ts: 1_700_003_000,
        last_migrate_ts: 1_700_003_010,
    })?;
    runtime.mark_process_start_v1(1_700_003_100, "edge-lab-02")?;
    runtime.mark_process_end_v1(1_700_003_160, 6)?;

    runtime.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: "tenant-b".to_string(),
        created_ts: 1_700_003_200,
        last_seen_ts: 1_700_003_220,
        status: 0,
        tenant_root_rel: Some("tenant-b".to_string()),
        tenant_db_path: Some(runtime.tenant_paths_v1("tenant-b").tenant_db_dir),
        alert_out_root: Some(runtime.tenant_paths_v1("tenant-b").alert_out_dir),
    })?;
    runtime.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: "tenant-a".to_string(),
        created_ts: 1_700_003_201,
        last_seen_ts: 1_700_003_221,
        status: 1,
        tenant_root_rel: Some("tenant-a".to_string()),
        tenant_db_path: Some(runtime.tenant_paths_v1("tenant-a").tenant_db_dir),
        alert_out_root: Some(runtime.tenant_paths_v1("tenant-a").alert_out_dir),
    })?;
    runtime.set_tenant_active_index_v1("tenant-a", true)?;
    let spool_path = write_spool_alert_v1(&cfg.sparx.data_root, &sample_spooled_alert_v1()).unwrap();
    let spool_backlog_bytes = std::fs::metadata(&spool_path)?.len();
    drop(runtime);

    let spool_root = std::path::Path::new(&cfg.sparx.data_root)
        .join("spool")
        .join("alerts")
        .display()
        .to_string();
    let expected = ExpectedStatusSnapshotV1 {
        version: "sparx 0.0.0".to_string(),
        mode: cfg.sparx.mode.clone(),
        window_size_s: cfg.ingest.window_size_s,
        sink: cfg.output.sink.clone(),
        roots: ExpectedStatusRootsV1 {
            data_root: cfg.sparx.data_root.clone(),
            tenant_root: cfg.sparx.tenant_root.clone(),
            global_db_path: cfg.sparx.global_db_path.clone(),
            tenant_db_root: cfg.sparx.tenant_db_root.clone(),
            alert_out_root: cfg.sparx.alert_out_root.clone(),
            spool_root,
        },
        tenants: ExpectedStatusTenantCountsV1 {
            known_count: 2,
            active_count: 1,
        },
        process: ExpectedStatusProcessStateV1 {
            last_run_start_ts: Some(1_700_003_100),
            last_run_end_ts: Some(1_700_003_160),
            last_run_exit_code: Some(6),
            last_run_host: Some("edge-lab-02".to_string()),
        },
        runtime: ExpectedStatusRuntimeStateV1 {
            global_schema_version: Some(1),
            global_schema_created_ts: Some(1_700_003_000),
            global_schema_last_migrate_ts: Some(1_700_003_010),
        },
        observability: ExpectedStatusObservabilityV1 {
            prometheus_enabled: cfg.metrics.prometheus_enabled,
            prometheus_bind: cfg.metrics.prometheus_bind.clone(),
            prometheus_url: Some(format!("http://{}{}", cfg.metrics.prometheus_bind, "/metrics")),
            health_enabled: cfg.metrics.health_enabled,
            health_bind: cfg.metrics.health_bind.clone(),
            health_url: Some(format!("http://{}{}", cfg.metrics.health_bind, "/healthz")),
        },
        metrics: ExpectedStatusMetricsV1 {
            run_cycles_completed_total: 0,
            run_tenants_total: 0,
            run_tenants_processed_total: 0,
            run_tenants_skipped_total: 0,
            run_devices_processed_total: 0,
            run_devices_failed_total: 0,
            run_alerts_emitted_total: 0,
            run_last_cycle_tenants_total: None,
            run_last_cycle_tenants_processed: None,
            run_last_cycle_tenants_skipped: None,
            run_last_cycle_devices_processed: None,
            run_last_cycle_devices_failed: None,
            run_last_cycle_alerts_emitted: None,
            run_last_cycle_completed_ts: None,
        },
        recovery: ExpectedStatusRecoveryV1 {
            automated_replay_max_files_per_pass: cfg.output.automated_replay_max_files_per_pass,
            spool_backlog_files: 1,
            spool_backlog_bytes,
        },
    };

    let r = route_command_v1(&CommandV1::Status { json: true }, &cfg);
    assert_eq!(r.exit_code, 0);
    assert_eq!(serde_json::to_string(&expected)?, r.msg_stdout.unwrap());
    Ok(())
}

#[test]
fn status_reports_db_error_when_runtime_open_fails_v1() {
    let tmp = tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let _leaked = Box::leak(Box::new(tmp));

    let mut cfg = default_config_v1();
    cfg.sparx.data_root = root.join("state").display().to_string();
    cfg.sparx.tenant_root = root.join("watch").display().to_string();
    cfg.sparx.global_db_path = root.join("state/global.db").display().to_string();
    cfg.sparx.tenant_db_root = root.join("state/tenants").display().to_string();
    cfg.sparx.alert_out_root = root.join("state/alerts").display().to_string();

    std::fs::create_dir_all(root.join("state")).expect("state dir");
    std::fs::write(root.join("state/global.db"), b"not-a-directory").expect("global db file");

    let r = route_command_v1(&CommandV1::Status { json: false }, &cfg);
    assert_eq!(r.exit_code, 4);
    assert!(r.msg_stderr.unwrap().contains("status db error"));
}

#[test]
fn config_check_creates_spool_dir() {
    let mut cfg = default_config_v1();
    let td = std::env::temp_dir().join("sparx_test_cfg_check");
    let _ = std::fs::remove_dir_all(&td);

    cfg.sparx.data_root = td.to_string_lossy().to_string();
    cfg.sparx.global_db_path = format!("{}/global.db", cfg.sparx.data_root);
    cfg.sparx.tenant_db_root = format!("{}/tenants", cfg.sparx.data_root);
    cfg.sparx.alert_out_root = format!("{}/alerts", cfg.sparx.data_root);
    cfg.sparx.tenant_root = std::env::temp_dir().to_string_lossy().to_string();

    let r = route_command_v1(&CommandV1::ConfigCheck, &cfg);
    assert_eq!(r.exit_code, 0);

    let global_db = td.join("global.db");
    let tenant_db_root = td.join("tenants");
    let spool = td.join("spool").join("alerts");
    assert!(global_db.is_dir());
    assert!(tenant_db_root.is_dir());
    assert!(spool.is_dir());
}

#[test]
fn version_and_validate_fixtures_do_not_require_config() {
    assert!(!command_requires_config_v1(&CommandV1::Version));
    assert!(!command_requires_config_v1(&CommandV1::ValidateFixtures {
        fixture_root: "/tmp/fx".to_string(),
    }));
    assert!(command_requires_config_v1(&CommandV1::Run { migrate: sparx::cli::MigrateModeV1::Auto }));
    assert!(command_requires_config_v1(&CommandV1::TenantPurge {
        tenant_id: "t1".to_string(),
        force: false,
    }));
}

#[test]
fn run_reports_db_error_when_runtime_open_fails_v1() {
    let mut cfg = default_config_v1();
    let root = tempdir().expect("tempdir");
    cfg.sparx.data_root = root.path().join("state").display().to_string();
    cfg.sparx.global_db_path = root.path().join("state/global.db").display().to_string();
    cfg.sparx.tenant_db_root = root.path().join("state/tenants").display().to_string();
    cfg.sparx.alert_out_root = root.path().join("state/alerts").display().to_string();
    cfg.sparx.tenant_root = root.path().join("watch").display().to_string();

    std::fs::create_dir_all(root.path().join("state")).expect("state dir");
    std::fs::write(root.path().join("state/global.db"), b"not-a-directory").expect("global db file");

    let r = route_command_v1(&CommandV1::Run { migrate: sparx::cli::MigrateModeV1::Auto }, &cfg);
    assert_eq!(r.exit_code, 4);
    assert!(r.msg_stderr.unwrap().contains("run db error"));
}

#[test]
fn no_config_router_rejects_config_required_commands() {
    let r = route_command_no_config_v1(&CommandV1::Status { json: false });
    assert_eq!(r.exit_code, 5);
    assert!(r.msg_stderr.unwrap().contains("command requires config: status"));
}
