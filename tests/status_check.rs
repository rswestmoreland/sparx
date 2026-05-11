// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use tempfile::tempdir;

use sparx::cli::route::{command_requires_config_v1, route_command_no_config_v1, route_command_v1};
use sparx::cli::CommandV1;
use sparx::config::load::default_config_v1;
use sparx::db::{GlobalSchemaStateV1, GlobalTenantRecordV1};
use sparx::runtime::SparxRuntimeV1;
use sparx::sink::write_spool_alert_v1;

fn sample_spooled_alert_v1() -> sparx::alert::AlertV1 {
    sparx::alert::AlertV1 {
        schema_version: 1,
        alert_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        tenant_id: "tenant-z".to_string(),
        device_key: "device-z".to_string(),
        device_path: "tenant-z/device-z".to_string(),
        window_start_ts: 1_700_200_000,
        window_end_ts: 1_700_200_060,
        window_size_s: 60,
        bucket: 7,
        label: sparx::types::LabelV1::Outlier,
        confidence: sparx::types::ConfidenceV1::High,
        cold_start: false,
        score_total: 0.95,
        score_rarity: 0.80,
        score_drift: 0.75,
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
    let prometheus_url = format!("http://{}/metrics", cfg.metrics.prometheus_bind);
    let health_url = format!("http://{}/healthz", cfg.metrics.health_bind);
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
vdrop.enabled: true
vdrop.device_enabled: true
vdrop.tenant_enabled: true
vdrop.source_stream_enabled: false
vdrop.min_expected_windows_missed: 1
vdrop.min_mature_windows: null
vdrop.min_expected_lines: null
vdrop.tracked_subjects: null
vdrop.open_silence_subjects: null
vdrop.open_drop_subjects: null
vdrop.evaluated_subjects_total: 0
vdrop.candidates_total: 0
vdrop.suppressed_candidates_total: 0
vdrop.alerts_emitted_total: 0
vdrop.last_evaluation_ts: null
vdrop.source_stream_tracked_subjects: null
vdrop.source_stream_open_silence_subjects: null
vdrop.source_stream_open_drop_subjects: null
vdrop.source_stream_evaluated_subjects_total: 0
vdrop.source_stream_candidates_total: 0
vdrop.source_stream_suppressed_candidates_total: 0
vdrop.source_stream_alerts_emitted_total: 0
vdrop.source_stream_last_evaluation_ts: null
vdrop.tenants: 0
recovery.automated_replay_max_files_per_pass: {}
recovery.automated_replay_interval_s: {}
recovery.spool_max_mb: {}
recovery.spool_backlog_files: 0
recovery.spool_backlog_bytes: 0
recovery.spool_oldest_file_ts: null
recovery.spool_oldest_age_s: null
recovery.stale_backlog: false
recovery.stale_backlog_tenants: 0
recovery.spool_backlog_tenants: 0
recovery.spool_writes_total: 0
recovery.spool_replayed_total: 0
recovery.spool_replay_fail_total: 0
recovery.spool_drop_total: 0
recovery.automated_replay_attempts_total: 0
recovery.last_automated_replay_attempt_ts: null
recovery.last_automated_replay_replayed: null
recovery.last_automated_replay_failed: null
recovery.previous_snapshot_ts: null
recovery.last_snapshot_ts: null
recovery.snapshot_interval_s: null
recovery.backlog_files_trend_delta: null
recovery.backlog_bytes_trend_delta: null
recovery.backlog_trend_direction: unknown
recovery.previous_counter_snapshot_ts: null
recovery.last_counter_snapshot_ts: null
recovery.counter_snapshot_interval_s: null
recovery.history_start_counter_snapshot_ts: null
recovery.history_counter_snapshot_interval_s: null
recovery.history_spool_write_rate_per_s: null
recovery.history_spool_replayed_rate_per_s: null
recovery.history_spool_replay_fail_rate_per_s: null
recovery.history_automated_replay_attempt_rate_per_s: null
recovery.spool_write_rate_per_s: null
recovery.spool_replayed_rate_per_s: null
recovery.spool_replay_fail_rate_per_s: null
recovery.automated_replay_attempt_rate_per_s: null
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
        prometheus_url,
        cfg.metrics.health_enabled,
        cfg.metrics.health_bind,
        health_url,
        cfg.output.automated_replay_max_files_per_pass,
        cfg.output.automated_replay_interval_s,
        cfg.output.spool_max_mb,
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
fn status_json_populated_runtime_state_is_deterministic_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.output.automated_replay_interval_s = 3600;
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
    let spool_path_z =
        write_spool_alert_v1(&cfg.sparx.data_root, &sample_spooled_alert_v1()).unwrap();
    let spool_backlog_bytes_z = std::fs::metadata(&spool_path_z)?.len();
    let mut tenant_a_alert = sample_spooled_alert_v1();
    tenant_a_alert.alert_id = "cccccccccccccccccccccccccccccccc".to_string();
    tenant_a_alert.tenant_id = "tenant-a".to_string();
    let spool_path_a = write_spool_alert_v1(&cfg.sparx.data_root, &tenant_a_alert).unwrap();
    let spool_backlog_bytes_a = std::fs::metadata(&spool_path_a)?.len();
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_spool_writes_total", 4)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_spool_replayed_total", 3)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_spool_replay_fail_total", 1)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_spool_drop_total", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_automated_replay_attempts_total", 5)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_automated_replay_attempt_ts", 1_700_003_299)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("recovery_last_automated_replay_replayed", 7.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("recovery_last_automated_replay_failed", 2.0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_previous_snapshot_ts", 1_700_003_250)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("recovery_previous_snapshot_backlog_files", 5.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("recovery_previous_snapshot_backlog_bytes", 900.0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_snapshot_ts", 1_700_003_310)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("recovery_last_snapshot_backlog_files", 3.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("recovery_last_snapshot_backlog_bytes", 600.0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_previous_counter_snapshot_ts", 1_700_003_240)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_previous_counter_snapshot_spool_writes_total", 1)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_previous_counter_snapshot_spool_replayed_total", 1)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_previous_counter_snapshot_spool_replay_fail_total",
        0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_previous_counter_snapshot_automated_replay_attempts_total",
        2,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_counter_snapshot_ts", 1_700_003_300)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_counter_snapshot_spool_writes_total", 4)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_counter_snapshot_spool_replayed_total", 3)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_counter_snapshot_spool_replay_fail_total", 1)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_last_counter_snapshot_automated_replay_attempts_total",
        5,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_history_start_counter_snapshot_ts", 1_700_003_000)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_history_start_counter_snapshot_spool_writes_total",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_history_start_counter_snapshot_spool_replayed_total",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_history_start_counter_snapshot_spool_replay_fail_total",
        0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_history_start_counter_snapshot_automated_replay_attempts_total",
        2,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_snapshot_ts__tenant-a",
        1_700_003_250,
    )?;
    runtime.global_db_v1().write_metric_gauge_v1(
        "recovery_tenant_previous_snapshot_backlog_files__tenant-a",
        4.0,
    )?;
    runtime.global_db_v1().write_metric_gauge_v1(
        "recovery_tenant_previous_snapshot_backlog_bytes__tenant-a",
        700.0,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_tenant_last_snapshot_ts__tenant-a", 1_700_003_310)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("recovery_tenant_last_snapshot_backlog_files__tenant-a", 1.0)?;
    runtime.global_db_v1().write_metric_gauge_v1(
        "recovery_tenant_last_snapshot_backlog_bytes__tenant-a",
        200.0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_ts__tenant-a",
        1_700_003_240,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_writes_total__tenant-a",
        2,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_replayed_total__tenant-a",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_replay_fail_total__tenant-a",
        0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total__tenant-a",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_ts__tenant-a",
        1_700_003_300,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_writes_total__tenant-a",
        4,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_replayed_total__tenant-a",
        3,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_replay_fail_total__tenant-a",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_automated_replay_attempts_total__tenant-a",
        3,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_ts__tenant-a",
        1_700_003_000,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_writes_total__tenant-a",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_replayed_total__tenant-a",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total__tenant-a",
        0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total__tenant-a",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_snapshot_ts__tenant-z",
        1_700_003_250,
    )?;
    runtime.global_db_v1().write_metric_gauge_v1(
        "recovery_tenant_previous_snapshot_backlog_files__tenant-z",
        1.0,
    )?;
    runtime.global_db_v1().write_metric_gauge_v1(
        "recovery_tenant_previous_snapshot_backlog_bytes__tenant-z",
        200.0,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_tenant_last_snapshot_ts__tenant-z", 1_700_003_310)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("recovery_tenant_last_snapshot_backlog_files__tenant-z", 1.0)?;
    runtime.global_db_v1().write_metric_gauge_v1(
        "recovery_tenant_last_snapshot_backlog_bytes__tenant-z",
        200.0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_ts__tenant-z",
        1_700_003_240,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_writes_total__tenant-z",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_replayed_total__tenant-z",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_replay_fail_total__tenant-z",
        0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total__tenant-z",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_ts__tenant-z",
        1_700_003_300,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_writes_total__tenant-z",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_replayed_total__tenant-z",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_replay_fail_total__tenant-z",
        0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_automated_replay_attempts_total__tenant-z",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_ts__tenant-z",
        1_700_003_000,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_writes_total__tenant-z",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_replayed_total__tenant-z",
        1,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total__tenant-z",
        0,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total__tenant-z",
        1,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_evaluated_subjects_total", 5)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_candidates_total", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_suppressed_candidates_total", 3)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_alerts_emitted_total", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_last_evaluation_ts", 1_700_003_333)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_evaluated_subjects_total", 4)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_candidates_total", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_suppressed_candidates_total", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_alerts_emitted_total", 1)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_last_evaluation_ts", 1_700_003_333)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_tracked_subjects__tenant-a", 3.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_open_silence_subjects__tenant-a", 1.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_open_drop_subjects__tenant-a", 2.0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_evaluated_subjects_total__tenant-a", 3)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_candidates_total__tenant-a", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_suppressed_candidates_total__tenant-a", 1)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_alerts_emitted_total__tenant-a", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_last_evaluation_ts__tenant-a", 1_700_003_333)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_source_stream_tracked_subjects__tenant-a", 2.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_source_stream_open_silence_subjects__tenant-a", 1.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_source_stream_open_drop_subjects__tenant-a", 1.0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_evaluated_subjects_total__tenant-a", 3)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_candidates_total__tenant-a", 2)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "vdrop_source_stream_suppressed_candidates_total__tenant-a",
        1,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_alerts_emitted_total__tenant-a", 1)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "vdrop_source_stream_last_evaluation_ts__tenant-a",
        1_700_003_333,
    )?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_tracked_subjects__tenant-b", 0.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_open_silence_subjects__tenant-b", 0.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_open_drop_subjects__tenant-b", 0.0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_evaluated_subjects_total__tenant-b", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_candidates_total__tenant-b", 0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_suppressed_candidates_total__tenant-b", 2)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_alerts_emitted_total__tenant-b", 0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_last_evaluation_ts__tenant-b", 1_700_003_222)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_source_stream_tracked_subjects__tenant-b", 0.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_source_stream_open_silence_subjects__tenant-b", 0.0)?;
    runtime
        .global_db_v1()
        .write_metric_gauge_v1("vdrop_source_stream_open_drop_subjects__tenant-b", 0.0)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_evaluated_subjects_total__tenant-b", 1)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_candidates_total__tenant-b", 0)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "vdrop_source_stream_suppressed_candidates_total__tenant-b",
        1,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("vdrop_source_stream_alerts_emitted_total__tenant-b", 0)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "vdrop_source_stream_last_evaluation_ts__tenant-b",
        1_700_003_222,
    )?;
    drop(runtime);

    let spool_root = std::path::Path::new(&cfg.sparx.data_root)
        .join("spool")
        .join("alerts")
        .display()
        .to_string();

    let r = route_command_v1(&CommandV1::Status { json: true }, &cfg);
    assert_eq!(r.exit_code, 0);
    let value: serde_json::Value = serde_json::from_str(&r.msg_stdout.unwrap())?;
    assert_eq!(value["version"].as_str(), Some("sparx 0.0.0"));
    assert_eq!(value["mode"].as_str(), Some(cfg.sparx.mode.as_str()));
    assert_eq!(
        value["window_size_s"].as_u64(),
        Some(u64::from(cfg.ingest.window_size_s))
    );
    assert_eq!(value["sink"].as_str(), Some(cfg.output.sink.as_str()));
    assert_eq!(
        value["roots"]["data_root"].as_str(),
        Some(cfg.sparx.data_root.as_str())
    );
    assert_eq!(
        value["roots"]["tenant_root"].as_str(),
        Some(cfg.sparx.tenant_root.as_str())
    );
    assert_eq!(
        value["roots"]["global_db_path"].as_str(),
        Some(cfg.sparx.global_db_path.as_str())
    );
    assert_eq!(
        value["roots"]["tenant_db_root"].as_str(),
        Some(cfg.sparx.tenant_db_root.as_str())
    );
    assert_eq!(
        value["roots"]["alert_out_root"].as_str(),
        Some(cfg.sparx.alert_out_root.as_str())
    );
    assert_eq!(
        value["roots"]["spool_root"].as_str(),
        Some(spool_root.as_str())
    );
    assert_eq!(value["tenants"]["known_count"].as_u64(), Some(2));
    assert_eq!(value["tenants"]["active_count"].as_u64(), Some(1));
    assert_eq!(
        value["process"]["last_run_start_ts"].as_i64(),
        Some(1_700_003_100)
    );
    assert_eq!(
        value["process"]["last_run_end_ts"].as_i64(),
        Some(1_700_003_160)
    );
    assert_eq!(value["process"]["last_run_exit_code"].as_i64(), Some(6));
    assert_eq!(
        value["process"]["last_run_host"].as_str(),
        Some("edge-lab-02")
    );
    assert_eq!(value["runtime"]["global_schema_version"].as_u64(), Some(1));
    assert_eq!(
        value["runtime"]["global_schema_created_ts"].as_i64(),
        Some(1_700_003_000)
    );
    assert_eq!(
        value["runtime"]["global_schema_last_migrate_ts"].as_i64(),
        Some(1_700_003_010)
    );
    assert_eq!(
        value["observability"]["prometheus_enabled"].as_bool(),
        Some(cfg.metrics.prometheus_enabled)
    );
    assert_eq!(
        value["observability"]["prometheus_bind"].as_str(),
        Some(cfg.metrics.prometheus_bind.as_str())
    );
    assert_eq!(
        value["observability"]["prometheus_url"].as_str(),
        Some(format!("http://{}{}", cfg.metrics.prometheus_bind, "/metrics").as_str())
    );
    assert_eq!(
        value["observability"]["health_enabled"].as_bool(),
        Some(cfg.metrics.health_enabled)
    );
    assert_eq!(
        value["observability"]["health_bind"].as_str(),
        Some(cfg.metrics.health_bind.as_str())
    );
    assert_eq!(
        value["observability"]["health_url"].as_str(),
        Some(format!("http://{}{}", cfg.metrics.health_bind, "/healthz").as_str())
    );
    assert_eq!(
        value["metrics"]["run_cycles_completed_total"].as_u64(),
        Some(0)
    );
    assert_eq!(value["vdrop"]["enabled"].as_bool(), Some(true));
    assert_eq!(value["vdrop"]["device_enabled"].as_bool(), Some(true));
    assert_eq!(value["vdrop"]["tenant_enabled"].as_bool(), Some(true));
    assert_eq!(
        value["vdrop"]["source_stream_enabled"].as_bool(),
        Some(false)
    );
    assert_eq!(
        value["vdrop"]["min_expected_windows_missed"].as_u64(),
        Some(1)
    );
    assert_eq!(value["vdrop"]["min_mature_windows"].as_u64(), None);
    assert_eq!(value["vdrop"]["min_expected_lines"].as_u64(), None);
    assert_eq!(value["vdrop"]["tracked_subjects"].as_u64(), Some(3));
    assert_eq!(value["vdrop"]["open_silence_subjects"].as_u64(), Some(1));
    assert_eq!(value["vdrop"]["open_drop_subjects"].as_u64(), Some(2));
    assert_eq!(value["vdrop"]["evaluated_subjects_total"].as_u64(), Some(5));
    assert_eq!(value["vdrop"]["candidates_total"].as_u64(), Some(2));
    assert_eq!(
        value["vdrop"]["suppressed_candidates_total"].as_u64(),
        Some(3)
    );
    assert_eq!(value["vdrop"]["alerts_emitted_total"].as_u64(), Some(2));
    assert_eq!(
        value["vdrop"]["last_evaluation_ts"].as_u64(),
        Some(1_700_003_333)
    );
    assert_eq!(
        value["vdrop"]["source_stream_tracked_subjects"].as_u64(),
        Some(2)
    );
    assert_eq!(
        value["vdrop"]["source_stream_open_silence_subjects"].as_u64(),
        Some(1)
    );
    assert_eq!(
        value["vdrop"]["source_stream_open_drop_subjects"].as_u64(),
        Some(1)
    );
    assert_eq!(
        value["vdrop"]["source_stream_evaluated_subjects_total"].as_u64(),
        Some(4)
    );
    assert_eq!(
        value["vdrop"]["source_stream_candidates_total"].as_u64(),
        Some(2)
    );
    assert_eq!(
        value["vdrop"]["source_stream_suppressed_candidates_total"].as_u64(),
        Some(2)
    );
    assert_eq!(
        value["vdrop"]["source_stream_alerts_emitted_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        value["vdrop"]["source_stream_last_evaluation_ts"].as_u64(),
        Some(1_700_003_333)
    );
    let vdrop_tenants = value["vdrop"]["tenants"]
        .as_array()
        .expect("vdrop tenant array");
    assert_eq!(vdrop_tenants.len(), 2);
    assert_eq!(vdrop_tenants[0]["tenant_id"].as_str(), Some("tenant-a"));
    assert_eq!(vdrop_tenants[0]["tracked_subjects"].as_u64(), Some(3));
    assert_eq!(vdrop_tenants[0]["open_silence_subjects"].as_u64(), Some(1));
    assert_eq!(vdrop_tenants[0]["open_drop_subjects"].as_u64(), Some(2));
    assert_eq!(
        vdrop_tenants[0]["evaluated_subjects_total"].as_u64(),
        Some(3)
    );
    assert_eq!(vdrop_tenants[0]["candidates_total"].as_u64(), Some(2));
    assert_eq!(
        vdrop_tenants[0]["suppressed_candidates_total"].as_u64(),
        Some(1)
    );
    assert_eq!(vdrop_tenants[0]["alerts_emitted_total"].as_u64(), Some(2));
    assert_eq!(
        vdrop_tenants[0]["last_evaluation_ts"].as_u64(),
        Some(1_700_003_333)
    );
    assert_eq!(
        vdrop_tenants[0]["source_stream_tracked_subjects"].as_u64(),
        Some(2)
    );
    assert_eq!(
        vdrop_tenants[0]["source_stream_open_silence_subjects"].as_u64(),
        Some(1)
    );
    assert_eq!(
        vdrop_tenants[0]["source_stream_open_drop_subjects"].as_u64(),
        Some(1)
    );
    assert_eq!(
        vdrop_tenants[0]["source_stream_evaluated_subjects_total"].as_u64(),
        Some(3)
    );
    assert_eq!(
        vdrop_tenants[0]["source_stream_candidates_total"].as_u64(),
        Some(2)
    );
    assert_eq!(
        vdrop_tenants[0]["source_stream_suppressed_candidates_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        vdrop_tenants[0]["source_stream_alerts_emitted_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        vdrop_tenants[0]["source_stream_last_evaluation_ts"].as_u64(),
        Some(1_700_003_333)
    );
    assert_eq!(vdrop_tenants[1]["tenant_id"].as_str(), Some("tenant-b"));
    assert_eq!(vdrop_tenants[1]["tracked_subjects"].as_u64(), Some(0));
    assert_eq!(vdrop_tenants[1]["open_silence_subjects"].as_u64(), Some(0));
    assert_eq!(vdrop_tenants[1]["open_drop_subjects"].as_u64(), Some(0));
    assert_eq!(
        vdrop_tenants[1]["evaluated_subjects_total"].as_u64(),
        Some(2)
    );
    assert_eq!(vdrop_tenants[1]["candidates_total"].as_u64(), Some(0));
    assert_eq!(
        vdrop_tenants[1]["suppressed_candidates_total"].as_u64(),
        Some(2)
    );
    assert_eq!(vdrop_tenants[1]["alerts_emitted_total"].as_u64(), Some(0));
    assert_eq!(
        vdrop_tenants[1]["last_evaluation_ts"].as_u64(),
        Some(1_700_003_222)
    );
    assert_eq!(
        vdrop_tenants[1]["source_stream_tracked_subjects"].as_u64(),
        Some(0)
    );
    assert_eq!(
        vdrop_tenants[1]["source_stream_open_silence_subjects"].as_u64(),
        Some(0)
    );
    assert_eq!(
        vdrop_tenants[1]["source_stream_open_drop_subjects"].as_u64(),
        Some(0)
    );
    assert_eq!(
        vdrop_tenants[1]["source_stream_evaluated_subjects_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        vdrop_tenants[1]["source_stream_candidates_total"].as_u64(),
        Some(0)
    );
    assert_eq!(
        vdrop_tenants[1]["source_stream_suppressed_candidates_total"].as_u64(),
        Some(1)
    );
    assert_eq!(
        vdrop_tenants[1]["source_stream_alerts_emitted_total"].as_u64(),
        Some(0)
    );
    assert_eq!(
        vdrop_tenants[1]["source_stream_last_evaluation_ts"].as_u64(),
        Some(1_700_003_222)
    );
    assert_eq!(
        value["recovery"]["automated_replay_max_files_per_pass"].as_u64(),
        Some(u64::from(cfg.output.automated_replay_max_files_per_pass))
    );
    assert_eq!(
        value["recovery"]["automated_replay_interval_s"].as_u64(),
        Some(u64::from(cfg.output.automated_replay_interval_s))
    );
    assert_eq!(
        value["recovery"]["spool_max_mb"].as_u64(),
        Some(u64::from(cfg.output.spool_max_mb))
    );
    assert_eq!(value["recovery"]["spool_backlog_files"].as_u64(), Some(2));
    assert_eq!(
        value["recovery"]["spool_backlog_bytes"].as_u64(),
        Some(spool_backlog_bytes_a + spool_backlog_bytes_z)
    );
    assert!(value["recovery"]["spool_oldest_file_ts"].as_u64().is_some());
    assert!(value["recovery"]["spool_oldest_age_s"].as_u64().is_some());
    assert_eq!(value["recovery"]["stale_backlog"].as_bool(), Some(false));
    assert_eq!(value["recovery"]["stale_backlog_tenants"].as_u64(), Some(0));
    let tenants = value["recovery"]["spool_backlog_tenants"]
        .as_array()
        .expect("tenant array");
    assert_eq!(tenants.len(), 2);
    assert_eq!(tenants[0]["tenant_id"].as_str(), Some("tenant-a"));
    assert_eq!(tenants[0]["files"].as_u64(), Some(1));
    assert_eq!(tenants[0]["bytes"].as_u64(), Some(spool_backlog_bytes_a));
    assert!(tenants[0]["oldest_file_ts"].as_u64().is_some());
    assert!(tenants[0]["oldest_age_s"].as_u64().is_some());
    assert_eq!(tenants[0]["stale"].as_bool(), Some(false));
    assert_eq!(
        tenants[0]["previous_snapshot_ts"].as_u64(),
        Some(1_700_003_250)
    );
    assert_eq!(tenants[0]["last_snapshot_ts"].as_u64(), Some(1_700_003_310));
    assert_eq!(tenants[0]["snapshot_interval_s"].as_u64(), Some(60));
    assert_eq!(tenants[0]["backlog_files_trend_delta"].as_i64(), Some(-3));
    assert_eq!(tenants[0]["backlog_bytes_trend_delta"].as_i64(), Some(-500));
    assert_eq!(tenants[0]["backlog_trend_direction"].as_str(), Some("down"));
    assert_eq!(
        tenants[0]["previous_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_240)
    );
    assert_eq!(
        tenants[0]["last_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_300)
    );
    assert_eq!(tenants[0]["counter_snapshot_interval_s"].as_u64(), Some(60));
    assert_eq!(
        tenants[0]["history_start_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_000)
    );
    assert_eq!(
        tenants[0]["history_counter_snapshot_interval_s"].as_u64(),
        Some(300)
    );
    assert!(
        (tenants[0]["history_spool_write_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.01)
            .abs()
            < 0.000001
    );
    assert!(
        (tenants[0]["history_spool_replayed_rate_per_s"]
            .as_f64()
            .unwrap()
            - (2.0 / 300.0))
            .abs()
            < 0.000001
    );
    assert!(
        (tenants[0]["history_spool_replay_fail_rate_per_s"]
            .as_f64()
            .unwrap()
            - (1.0 / 300.0))
            .abs()
            < 0.000001
    );
    assert!(
        (tenants[0]["history_automated_replay_attempt_rate_per_s"]
            .as_f64()
            .unwrap()
            - (2.0 / 300.0))
            .abs()
            < 0.000001
    );
    assert!(
        (tenants[0]["spool_write_rate_per_s"].as_f64().unwrap() - (2.0 / 60.0)).abs() < 0.000001
    );
    assert!(
        (tenants[0]["spool_replayed_rate_per_s"].as_f64().unwrap() - (2.0 / 60.0)).abs() < 0.000001
    );
    assert!(
        (tenants[0]["spool_replay_fail_rate_per_s"].as_f64().unwrap() - (1.0 / 60.0)).abs()
            < 0.000001
    );
    assert!(
        (tenants[0]["automated_replay_attempt_rate_per_s"]
            .as_f64()
            .unwrap()
            - (2.0 / 60.0))
            .abs()
            < 0.000001
    );
    assert_eq!(tenants[1]["tenant_id"].as_str(), Some("tenant-z"));
    assert_eq!(tenants[1]["files"].as_u64(), Some(1));
    assert_eq!(tenants[1]["bytes"].as_u64(), Some(spool_backlog_bytes_z));
    assert!(tenants[1]["oldest_file_ts"].as_u64().is_some());
    assert!(tenants[1]["oldest_age_s"].as_u64().is_some());
    assert_eq!(tenants[1]["stale"].as_bool(), Some(false));
    assert_eq!(
        tenants[1]["previous_snapshot_ts"].as_u64(),
        Some(1_700_003_250)
    );
    assert_eq!(tenants[1]["last_snapshot_ts"].as_u64(), Some(1_700_003_310));
    assert_eq!(tenants[1]["snapshot_interval_s"].as_u64(), Some(60));
    assert_eq!(tenants[1]["backlog_files_trend_delta"].as_i64(), Some(0));
    assert_eq!(tenants[1]["backlog_bytes_trend_delta"].as_i64(), Some(0));
    assert_eq!(tenants[1]["backlog_trend_direction"].as_str(), Some("flat"));
    assert_eq!(
        tenants[1]["previous_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_240)
    );
    assert_eq!(
        tenants[1]["last_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_300)
    );
    assert_eq!(tenants[1]["counter_snapshot_interval_s"].as_u64(), Some(60));
    assert_eq!(
        tenants[1]["history_start_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_000)
    );
    assert_eq!(
        tenants[1]["history_counter_snapshot_interval_s"].as_u64(),
        Some(300)
    );
    assert!(
        (tenants[1]["history_spool_write_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.0)
            .abs()
            < 0.000001
    );
    assert!(
        (tenants[1]["history_spool_replayed_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.0)
            .abs()
            < 0.000001
    );
    assert!(
        (tenants[1]["history_spool_replay_fail_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.0)
            .abs()
            < 0.000001
    );
    assert!(
        (tenants[1]["history_automated_replay_attempt_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.0)
            .abs()
            < 0.000001
    );
    assert!((tenants[1]["spool_write_rate_per_s"].as_f64().unwrap() - 0.0).abs() < 0.000001);
    assert!((tenants[1]["spool_replayed_rate_per_s"].as_f64().unwrap() - 0.0).abs() < 0.000001);
    assert!((tenants[1]["spool_replay_fail_rate_per_s"].as_f64().unwrap() - 0.0).abs() < 0.000001);
    assert!(
        (tenants[1]["automated_replay_attempt_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.0)
            .abs()
            < 0.000001
    );
    assert_eq!(value["recovery"]["spool_writes_total"].as_u64(), Some(4));
    assert_eq!(value["recovery"]["spool_replayed_total"].as_u64(), Some(3));
    assert_eq!(
        value["recovery"]["spool_replay_fail_total"].as_u64(),
        Some(1)
    );
    assert_eq!(value["recovery"]["spool_drop_total"].as_u64(), Some(2));
    assert_eq!(
        value["recovery"]["automated_replay_attempts_total"].as_u64(),
        Some(5)
    );
    assert_eq!(
        value["recovery"]["last_automated_replay_attempt_ts"].as_u64(),
        Some(1_700_003_299)
    );
    assert_eq!(
        value["recovery"]["last_automated_replay_replayed"].as_u64(),
        Some(7)
    );
    assert_eq!(
        value["recovery"]["last_automated_replay_failed"].as_u64(),
        Some(2)
    );
    assert_eq!(
        value["recovery"]["previous_snapshot_ts"].as_u64(),
        Some(1_700_003_250)
    );
    assert_eq!(
        value["recovery"]["last_snapshot_ts"].as_u64(),
        Some(1_700_003_310)
    );
    assert_eq!(value["recovery"]["snapshot_interval_s"].as_u64(), Some(60));
    assert_eq!(
        value["recovery"]["backlog_files_trend_delta"].as_i64(),
        Some(-2)
    );
    assert_eq!(
        value["recovery"]["backlog_bytes_trend_delta"].as_i64(),
        Some(-300)
    );
    assert_eq!(
        value["recovery"]["backlog_trend_direction"].as_str(),
        Some("down")
    );
    assert_eq!(
        value["recovery"]["previous_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_240)
    );
    assert_eq!(
        value["recovery"]["last_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_300)
    );
    assert_eq!(
        value["recovery"]["counter_snapshot_interval_s"].as_u64(),
        Some(60)
    );
    assert_eq!(
        value["recovery"]["history_start_counter_snapshot_ts"].as_u64(),
        Some(1_700_003_000)
    );
    assert_eq!(
        value["recovery"]["history_counter_snapshot_interval_s"].as_u64(),
        Some(300)
    );
    assert!(
        (value["recovery"]["history_spool_write_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.01)
            .abs()
            < 0.000001
    );
    assert!(
        (value["recovery"]["history_spool_replayed_rate_per_s"]
            .as_f64()
            .unwrap()
            - (2.0 / 300.0))
            .abs()
            < 0.000001
    );
    assert!(
        (value["recovery"]["history_spool_replay_fail_rate_per_s"]
            .as_f64()
            .unwrap()
            - (1.0 / 300.0))
            .abs()
            < 0.000001
    );
    assert!(
        (value["recovery"]["history_automated_replay_attempt_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.01)
            .abs()
            < 0.000001
    );
    assert!(
        (value["recovery"]["spool_write_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.05)
            .abs()
            < 0.000001
    );
    assert!(
        (value["recovery"]["spool_replayed_rate_per_s"]
            .as_f64()
            .unwrap()
            - (2.0 / 60.0))
            .abs()
            < 0.000001
    );
    assert!(
        (value["recovery"]["spool_replay_fail_rate_per_s"]
            .as_f64()
            .unwrap()
            - (1.0 / 60.0))
            .abs()
            < 0.000001
    );
    assert!(
        (value["recovery"]["automated_replay_attempt_rate_per_s"]
            .as_f64()
            .unwrap()
            - 0.05)
            .abs()
            < 0.000001
    );
    Ok(())
}

#[test]
fn status_json_suppresses_reset_like_recovery_rate_windows_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;

    write_spool_alert_v1(&cfg.sparx.data_root, &sample_spooled_alert_v1()).unwrap();

    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_previous_counter_snapshot_ts", 200)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_previous_counter_snapshot_spool_writes_total", 10)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_previous_counter_snapshot_spool_replayed_total",
        10,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_previous_counter_snapshot_spool_replay_fail_total",
        10,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_previous_counter_snapshot_automated_replay_attempts_total",
        10,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_counter_snapshot_ts", 100)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_counter_snapshot_spool_writes_total", 5)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_counter_snapshot_spool_replayed_total", 5)?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_last_counter_snapshot_spool_replay_fail_total", 5)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_last_counter_snapshot_automated_replay_attempts_total",
        5,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_history_start_counter_snapshot_ts", 50)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_history_start_counter_snapshot_spool_writes_total",
        9,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_history_start_counter_snapshot_spool_replayed_total",
        9,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_history_start_counter_snapshot_spool_replay_fail_total",
        9,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_history_start_counter_snapshot_automated_replay_attempts_total",
        9,
    )?;

    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_ts__tenant-z",
        200,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_writes_total__tenant-z",
        10,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_replayed_total__tenant-z",
        10,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_spool_replay_fail_total__tenant-z",
        10,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total__tenant-z",
        10,
    )?;
    runtime
        .global_db_v1()
        .write_metric_counter_v1("recovery_tenant_last_counter_snapshot_ts__tenant-z", 100)?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_writes_total__tenant-z",
        5,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_replayed_total__tenant-z",
        5,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_spool_replay_fail_total__tenant-z",
        5,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_last_counter_snapshot_automated_replay_attempts_total__tenant-z",
        5,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_ts__tenant-z",
        50,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_writes_total__tenant-z",
        9,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_replayed_total__tenant-z",
        9,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total__tenant-z",
        9,
    )?;
    runtime.global_db_v1().write_metric_counter_v1(
        "recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total__tenant-z",
        9,
    )?;
    drop(runtime);

    let r = route_command_v1(&CommandV1::Status { json: true }, &cfg);
    assert_eq!(r.exit_code, 0);
    let value: serde_json::Value = serde_json::from_str(&r.msg_stdout.unwrap())?;
    assert!(value["recovery"]["counter_snapshot_interval_s"].is_null());
    assert_eq!(
        value["recovery"]["history_counter_snapshot_interval_s"].as_u64(),
        Some(50)
    );
    assert!(value["recovery"]["spool_write_rate_per_s"].is_null());
    assert!(value["recovery"]["spool_replayed_rate_per_s"].is_null());
    assert!(value["recovery"]["spool_replay_fail_rate_per_s"].is_null());
    assert!(value["recovery"]["automated_replay_attempt_rate_per_s"].is_null());
    assert!(value["recovery"]["history_spool_write_rate_per_s"].is_null());
    assert!(value["recovery"]["history_spool_replayed_rate_per_s"].is_null());
    assert!(value["recovery"]["history_spool_replay_fail_rate_per_s"].is_null());
    assert!(value["recovery"]["history_automated_replay_attempt_rate_per_s"].is_null());

    let tenants = value["recovery"]["spool_backlog_tenants"]
        .as_array()
        .expect("tenant array");
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0]["tenant_id"].as_str(), Some("tenant-z"));
    assert!(tenants[0]["counter_snapshot_interval_s"].is_null());
    assert_eq!(
        tenants[0]["history_counter_snapshot_interval_s"].as_u64(),
        Some(50)
    );
    assert!(tenants[0]["spool_write_rate_per_s"].is_null());
    assert!(tenants[0]["spool_replayed_rate_per_s"].is_null());
    assert!(tenants[0]["spool_replay_fail_rate_per_s"].is_null());
    assert!(tenants[0]["automated_replay_attempt_rate_per_s"].is_null());
    assert!(tenants[0]["history_spool_write_rate_per_s"].is_null());
    assert!(tenants[0]["history_spool_replayed_rate_per_s"].is_null());
    assert!(tenants[0]["history_spool_replay_fail_rate_per_s"].is_null());
    assert!(tenants[0]["history_automated_replay_attempt_rate_per_s"].is_null());
    Ok(())
}

#[test]
fn status_json_surfaces_stale_backlog_when_oldest_age_exceeds_interval_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = temp_cfg_v1();
    cfg.output.automated_replay_interval_s = 1;

    write_spool_alert_v1(&cfg.sparx.data_root, &sample_spooled_alert_v1()).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));

    let r = route_command_v1(&CommandV1::Status { json: true }, &cfg);
    assert_eq!(r.exit_code, 0);
    let value: serde_json::Value = serde_json::from_str(&r.msg_stdout.unwrap())?;
    assert_eq!(value["recovery"]["stale_backlog"].as_bool(), Some(true));
    assert_eq!(value["recovery"]["stale_backlog_tenants"].as_u64(), Some(1));
    assert!(value["recovery"]["spool_oldest_age_s"].as_u64().unwrap() >= 1);
    let tenants = value["recovery"]["spool_backlog_tenants"]
        .as_array()
        .expect("tenant array");
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0]["tenant_id"].as_str(), Some("tenant-z"));
    assert_eq!(tenants[0]["stale"].as_bool(), Some(true));
    assert!(tenants[0]["oldest_age_s"].as_u64().unwrap() >= 1);
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
    assert!(command_requires_config_v1(&CommandV1::Run {
        migrate: sparx::cli::MigrateModeV1::Auto
    }));
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
    std::fs::write(root.path().join("state/global.db"), b"not-a-directory")
        .expect("global db file");

    let r = route_command_v1(
        &CommandV1::Run {
            migrate: sparx::cli::MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 4);
    assert!(r.msg_stderr.unwrap().contains("run db error"));
}

#[test]
fn no_config_router_rejects_config_required_commands() {
    let r = route_command_no_config_v1(&CommandV1::Status { json: false });
    assert_eq!(r.exit_code, 5);
    assert!(r
        .msg_stderr
        .unwrap()
        .contains("command requires config: status"));
}
