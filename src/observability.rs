// Observability helpers for status, metrics, and health.
//
// Phase 13a adds a real endpoint-backed observability surface that remains
// grounded in data Sparx already knows about its runtime, process state, and
// completed run-cycle totals.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use serde::Serialize;

use crate::config::ConfigV1;
use crate::db::layout::FilesystemLayoutV1;
use crate::db::{DbErrorV1, GlobalDbV1};
use crate::runtime::SparxRuntimeV1;
use crate::sink::spool_backlog_summary_v1;

pub const METRIC_RUN_CYCLES_COMPLETED_TOTAL_V1: &str = "run_cycles_completed_total";
pub const METRIC_RUN_TENANTS_TOTAL_V1: &str = "run_tenants_total";
pub const METRIC_RUN_TENANTS_PROCESSED_TOTAL_V1: &str = "run_tenants_processed_total";
pub const METRIC_RUN_TENANTS_SKIPPED_TOTAL_V1: &str = "run_tenants_skipped_total";
pub const METRIC_RUN_DEVICES_PROCESSED_TOTAL_V1: &str = "run_devices_processed_total";
pub const METRIC_RUN_DEVICES_FAILED_TOTAL_V1: &str = "run_devices_failed_total";
pub const METRIC_RUN_ALERTS_EMITTED_TOTAL_V1: &str = "run_alerts_emitted_total";
pub const METRIC_RUN_LAST_CYCLE_TENANTS_TOTAL_V1: &str = "run_last_cycle_tenants_total";
pub const METRIC_RUN_LAST_CYCLE_TENANTS_PROCESSED_V1: &str = "run_last_cycle_tenants_processed";
pub const METRIC_RUN_LAST_CYCLE_TENANTS_SKIPPED_V1: &str = "run_last_cycle_tenants_skipped";
pub const METRIC_RUN_LAST_CYCLE_DEVICES_PROCESSED_V1: &str = "run_last_cycle_devices_processed";
pub const METRIC_RUN_LAST_CYCLE_DEVICES_FAILED_V1: &str = "run_last_cycle_devices_failed";
pub const METRIC_RUN_LAST_CYCLE_ALERTS_EMITTED_V1: &str = "run_last_cycle_alerts_emitted";
pub const METRIC_RUN_LAST_CYCLE_COMPLETED_TS_V1: &str = "run_last_cycle_completed_ts";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatusRootsV1 {
    pub data_root: String,
    pub tenant_root: String,
    pub global_db_path: String,
    pub tenant_db_root: String,
    pub alert_out_root: String,
    pub spool_root: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatusTenantCountsV1 {
    pub known_count: usize,
    pub active_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatusProcessStateViewV1 {
    pub last_run_start_ts: Option<i64>,
    pub last_run_end_ts: Option<i64>,
    pub last_run_exit_code: Option<i32>,
    pub last_run_host: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatusRuntimeStateViewV1 {
    pub global_schema_version: Option<u32>,
    pub global_schema_created_ts: Option<i64>,
    pub global_schema_last_migrate_ts: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatusObservabilityConfigV1 {
    pub prometheus_enabled: bool,
    pub prometheus_bind: String,
    pub prometheus_url: Option<String>,
    pub health_enabled: bool,
    pub health_bind: String,
    pub health_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatusRunMetricsV1 {
    pub run_cycles_completed_total: u64,
    pub run_tenants_total: u64,
    pub run_tenants_processed_total: u64,
    pub run_tenants_skipped_total: u64,
    pub run_devices_processed_total: u64,
    pub run_devices_failed_total: u64,
    pub run_alerts_emitted_total: u64,
    pub run_last_cycle_tenants_total: Option<u64>,
    pub run_last_cycle_tenants_processed: Option<u64>,
    pub run_last_cycle_tenants_skipped: Option<u64>,
    pub run_last_cycle_devices_processed: Option<u64>,
    pub run_last_cycle_devices_failed: Option<u64>,
    pub run_last_cycle_alerts_emitted: Option<u64>,
    pub run_last_cycle_completed_ts: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatusRecoveryViewV1 {
    pub automated_replay_max_files_per_pass: u32,
    pub spool_backlog_files: u64,
    pub spool_backlog_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StatusSnapshotV1 {
    pub version: String,
    pub mode: String,
    pub window_size_s: u32,
    pub sink: String,
    pub roots: StatusRootsV1,
    pub tenants: StatusTenantCountsV1,
    pub process: StatusProcessStateViewV1,
    pub runtime: StatusRuntimeStateViewV1,
    pub observability: StatusObservabilityConfigV1,
    pub metrics: StatusRunMetricsV1,
    pub recovery: StatusRecoveryViewV1,
}

pub fn build_status_snapshot_from_runtime_v1(
    cfg: &ConfigV1,
    runtime: &SparxRuntimeV1,
) -> Result<StatusSnapshotV1, DbErrorV1> {
    build_status_snapshot_from_parts_v1(cfg, runtime.layout_v1(), runtime.global_db_v1())
}

pub fn build_status_snapshot_from_parts_v1(
    cfg: &ConfigV1,
    layout: &FilesystemLayoutV1,
    global_db: &GlobalDbV1,
) -> Result<StatusSnapshotV1, DbErrorV1> {
    let process_state = global_db.read_process_state_v1()?;
    let schema_state = global_db.read_schema_state_v1()?;
    let known_tenants = global_db.list_known_tenant_ids_v1()?;
    let active_tenants = global_db.list_active_tenants_v1()?;
    let spool_backlog = spool_backlog_summary_v1(&cfg.sparx.data_root)
        .map_err(|e| DbErrorV1::new_v1(format!("failed to read spool backlog summary: {}", e.msg)))?;

    Ok(StatusSnapshotV1 {
        version: "sparx 0.0.0".to_string(),
        mode: cfg.sparx.mode.clone(),
        window_size_s: cfg.ingest.window_size_s,
        sink: cfg.output.sink.clone(),
        roots: StatusRootsV1 {
            data_root: layout.data_root_v1().display().to_string(),
            tenant_root: layout.tenant_root_v1().display().to_string(),
            global_db_path: layout.global_db_path_v1().display().to_string(),
            tenant_db_root: layout.tenant_db_root_v1().display().to_string(),
            alert_out_root: layout.alert_out_root_v1().display().to_string(),
            spool_root: layout.spool_root_v1().display().to_string(),
        },
        tenants: StatusTenantCountsV1 {
            known_count: known_tenants.len(),
            active_count: active_tenants.len(),
        },
        process: StatusProcessStateViewV1 {
            last_run_start_ts: process_state.last_run_start_ts,
            last_run_end_ts: process_state.last_run_end_ts,
            last_run_exit_code: process_state.last_run_exit_code,
            last_run_host: process_state.last_run_host,
        },
        runtime: StatusRuntimeStateViewV1 {
            global_schema_version: schema_state.as_ref().map(|s| s.version),
            global_schema_created_ts: schema_state.as_ref().map(|s| s.created_ts),
            global_schema_last_migrate_ts: schema_state.as_ref().map(|s| s.last_migrate_ts),
        },
        observability: StatusObservabilityConfigV1 {
            prometheus_enabled: cfg.metrics.prometheus_enabled,
            prometheus_bind: cfg.metrics.prometheus_bind.clone(),
            prometheus_url: endpoint_url_v1(
                cfg.metrics.prometheus_enabled,
                &cfg.metrics.prometheus_bind,
                endpoint_path_v1(EndpointKindV1::Prometheus),
            ),
            health_enabled: cfg.metrics.health_enabled,
            health_bind: cfg.metrics.health_bind.clone(),
            health_url: endpoint_url_v1(
                cfg.metrics.health_enabled,
                &cfg.metrics.health_bind,
                endpoint_path_v1(EndpointKindV1::Health),
            ),
        },
        metrics: StatusRunMetricsV1 {
            run_cycles_completed_total: global_db.read_metric_counter_v1(METRIC_RUN_CYCLES_COMPLETED_TOTAL_V1)?.unwrap_or(0),
            run_tenants_total: global_db.read_metric_counter_v1(METRIC_RUN_TENANTS_TOTAL_V1)?.unwrap_or(0),
            run_tenants_processed_total: global_db.read_metric_counter_v1(METRIC_RUN_TENANTS_PROCESSED_TOTAL_V1)?.unwrap_or(0),
            run_tenants_skipped_total: global_db.read_metric_counter_v1(METRIC_RUN_TENANTS_SKIPPED_TOTAL_V1)?.unwrap_or(0),
            run_devices_processed_total: global_db.read_metric_counter_v1(METRIC_RUN_DEVICES_PROCESSED_TOTAL_V1)?.unwrap_or(0),
            run_devices_failed_total: global_db.read_metric_counter_v1(METRIC_RUN_DEVICES_FAILED_TOTAL_V1)?.unwrap_or(0),
            run_alerts_emitted_total: global_db.read_metric_counter_v1(METRIC_RUN_ALERTS_EMITTED_TOTAL_V1)?.unwrap_or(0),
            run_last_cycle_tenants_total: read_u64_gauge_as_int_v1(global_db, METRIC_RUN_LAST_CYCLE_TENANTS_TOTAL_V1)?,
            run_last_cycle_tenants_processed: read_u64_gauge_as_int_v1(global_db, METRIC_RUN_LAST_CYCLE_TENANTS_PROCESSED_V1)?,
            run_last_cycle_tenants_skipped: read_u64_gauge_as_int_v1(global_db, METRIC_RUN_LAST_CYCLE_TENANTS_SKIPPED_V1)?,
            run_last_cycle_devices_processed: read_u64_gauge_as_int_v1(global_db, METRIC_RUN_LAST_CYCLE_DEVICES_PROCESSED_V1)?,
            run_last_cycle_devices_failed: read_u64_gauge_as_int_v1(global_db, METRIC_RUN_LAST_CYCLE_DEVICES_FAILED_V1)?,
            run_last_cycle_alerts_emitted: read_u64_gauge_as_int_v1(global_db, METRIC_RUN_LAST_CYCLE_ALERTS_EMITTED_V1)?,
            run_last_cycle_completed_ts: global_db.read_metric_counter_v1(METRIC_RUN_LAST_CYCLE_COMPLETED_TS_V1)?,
        },
        recovery: StatusRecoveryViewV1 {
            automated_replay_max_files_per_pass: cfg.output.automated_replay_max_files_per_pass,
            spool_backlog_files: spool_backlog.files,
            spool_backlog_bytes: spool_backlog.bytes,
        },
    })
}

fn read_u64_gauge_as_int_v1(global_db: &GlobalDbV1, name: &str) -> Result<Option<u64>, DbErrorV1> {
    let value = match global_db.read_metric_gauge_v1(name)? {
        Some(value) => value,
        None => return Ok(None),
    };
    if !value.is_finite() || value < 0.0 {
        return Ok(None);
    }
    let rounded = value.round();
    if (rounded - value).abs() > f64::EPSILON {
        return Ok(None);
    }
    Ok(Some(rounded as u64))
}

pub fn format_status_text_v1(snapshot: &StatusSnapshotV1) -> String {
    let mut out = String::new();
    out.push_str("sparx status\n");
    out.push_str(&format!("version: {}\n", snapshot.version));
    out.push_str(&format!("mode: {}\n", snapshot.mode));
    out.push_str(&format!("window_size_s: {}\n", snapshot.window_size_s));
    out.push_str(&format!("sink: {}\n", snapshot.sink));
    out.push_str(&format!("roots.data_root: {}\n", snapshot.roots.data_root));
    out.push_str(&format!("roots.tenant_root: {}\n", snapshot.roots.tenant_root));
    out.push_str(&format!("roots.global_db_path: {}\n", snapshot.roots.global_db_path));
    out.push_str(&format!("roots.tenant_db_root: {}\n", snapshot.roots.tenant_db_root));
    out.push_str(&format!("roots.alert_out_root: {}\n", snapshot.roots.alert_out_root));
    out.push_str(&format!("roots.spool_root: {}\n", snapshot.roots.spool_root));
    out.push_str(&format!("tenants.known_count: {}\n", snapshot.tenants.known_count));
    out.push_str(&format!("tenants.active_count: {}\n", snapshot.tenants.active_count));
    out.push_str(&format!("process.last_run_start_ts: {}\n", format_option_i64_v1(snapshot.process.last_run_start_ts)));
    out.push_str(&format!("process.last_run_end_ts: {}\n", format_option_i64_v1(snapshot.process.last_run_end_ts)));
    out.push_str(&format!("process.last_run_exit_code: {}\n", format_option_i32_v1(snapshot.process.last_run_exit_code)));
    out.push_str(&format!("process.last_run_host: {}\n", format_option_str_v1(snapshot.process.last_run_host.as_deref())));
    out.push_str(&format!("runtime.global_schema_version: {}\n", format_option_u32_v1(snapshot.runtime.global_schema_version)));
    out.push_str(&format!("runtime.global_schema_created_ts: {}\n", format_option_i64_v1(snapshot.runtime.global_schema_created_ts)));
    out.push_str(&format!("runtime.global_schema_last_migrate_ts: {}\n", format_option_i64_v1(snapshot.runtime.global_schema_last_migrate_ts)));
    out.push_str(&format!("observability.prometheus_enabled: {}\n", snapshot.observability.prometheus_enabled));
    out.push_str(&format!("observability.prometheus_bind: {}\n", snapshot.observability.prometheus_bind));
    out.push_str(&format!("observability.prometheus_url: {}\n", format_option_str_v1(snapshot.observability.prometheus_url.as_deref())));
    out.push_str(&format!("observability.health_enabled: {}\n", snapshot.observability.health_enabled));
    out.push_str(&format!("observability.health_bind: {}\n", snapshot.observability.health_bind));
    out.push_str(&format!("observability.health_url: {}\n", format_option_str_v1(snapshot.observability.health_url.as_deref())));
    out.push_str(&format!("metrics.run_cycles_completed_total: {}\n", snapshot.metrics.run_cycles_completed_total));
    out.push_str(&format!("metrics.run_tenants_total: {}\n", snapshot.metrics.run_tenants_total));
    out.push_str(&format!("metrics.run_tenants_processed_total: {}\n", snapshot.metrics.run_tenants_processed_total));
    out.push_str(&format!("metrics.run_tenants_skipped_total: {}\n", snapshot.metrics.run_tenants_skipped_total));
    out.push_str(&format!("metrics.run_devices_processed_total: {}\n", snapshot.metrics.run_devices_processed_total));
    out.push_str(&format!("metrics.run_devices_failed_total: {}\n", snapshot.metrics.run_devices_failed_total));
    out.push_str(&format!("metrics.run_alerts_emitted_total: {}\n", snapshot.metrics.run_alerts_emitted_total));
    out.push_str(&format!("metrics.run_last_cycle_tenants_total: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_tenants_total)));
    out.push_str(&format!("metrics.run_last_cycle_tenants_processed: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_tenants_processed)));
    out.push_str(&format!("metrics.run_last_cycle_tenants_skipped: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_tenants_skipped)));
    out.push_str(&format!("metrics.run_last_cycle_devices_processed: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_devices_processed)));
    out.push_str(&format!("metrics.run_last_cycle_devices_failed: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_devices_failed)));
    out.push_str(&format!("metrics.run_last_cycle_alerts_emitted: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_alerts_emitted)));
    out.push_str(&format!("metrics.run_last_cycle_completed_ts: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_completed_ts)));
    out.push_str(&format!(
        "recovery.automated_replay_max_files_per_pass: {}\n",
        snapshot.recovery.automated_replay_max_files_per_pass
    ));
    out.push_str(&format!("recovery.spool_backlog_files: {}\n", snapshot.recovery.spool_backlog_files));
    out.push_str(&format!("recovery.spool_backlog_bytes: {}\n", snapshot.recovery.spool_backlog_bytes));
    out
}

pub fn format_prometheus_text_v1(snapshot: &StatusSnapshotV1) -> String {
    let mut out = String::new();
    out.push_str("# HELP sparx_status_info Static status metadata for the running Sparx instance.\n");
    out.push_str("# TYPE sparx_status_info gauge\n");
    out.push_str(&format!(
        "sparx_status_info{{version=\"{}\",mode=\"{}\",sink=\"{}\"}} 1\n",
        escape_prometheus_label_v1(&snapshot.version),
        escape_prometheus_label_v1(&snapshot.mode),
        escape_prometheus_label_v1(&snapshot.sink),
    ));
    out.push_str("# TYPE sparx_status_window_size_seconds gauge\n");
    out.push_str(&format!("sparx_status_window_size_seconds {}\n", snapshot.window_size_s));
    out.push_str("# TYPE sparx_status_tenants_known_total gauge\n");
    out.push_str(&format!("sparx_status_tenants_known_total {}\n", snapshot.tenants.known_count));
    out.push_str("# TYPE sparx_status_tenants_active_total gauge\n");
    out.push_str(&format!("sparx_status_tenants_active_total {}\n", snapshot.tenants.active_count));
    out.push_str("# TYPE sparx_observability_prometheus_enabled gauge\n");
    out.push_str(&format!("sparx_observability_prometheus_enabled {}\n", if snapshot.observability.prometheus_enabled { 1 } else { 0 }));
    out.push_str("# TYPE sparx_observability_health_enabled gauge\n");
    out.push_str(&format!("sparx_observability_health_enabled {}\n", if snapshot.observability.health_enabled { 1 } else { 0 }));
    append_optional_i64_metric_v1(&mut out, "sparx_process_last_run_start_ts", snapshot.process.last_run_start_ts);
    append_optional_i64_metric_v1(&mut out, "sparx_process_last_run_end_ts", snapshot.process.last_run_end_ts);
    append_optional_i32_metric_v1(&mut out, "sparx_process_last_run_exit_code", snapshot.process.last_run_exit_code);
    append_optional_u32_metric_v1(&mut out, "sparx_runtime_global_schema_version", snapshot.runtime.global_schema_version);
    append_optional_i64_metric_v1(&mut out, "sparx_runtime_global_schema_created_ts", snapshot.runtime.global_schema_created_ts);
    append_optional_i64_metric_v1(&mut out, "sparx_runtime_global_schema_last_migrate_ts", snapshot.runtime.global_schema_last_migrate_ts);
    out.push_str("# TYPE sparx_run_cycles_completed_total counter\n");
    out.push_str(&format!("sparx_run_cycles_completed_total {}\n", snapshot.metrics.run_cycles_completed_total));
    out.push_str("# TYPE sparx_run_tenants_total counter\n");
    out.push_str(&format!("sparx_run_tenants_total {}\n", snapshot.metrics.run_tenants_total));
    out.push_str("# TYPE sparx_run_tenants_processed_total counter\n");
    out.push_str(&format!("sparx_run_tenants_processed_total {}\n", snapshot.metrics.run_tenants_processed_total));
    out.push_str("# TYPE sparx_run_tenants_skipped_total counter\n");
    out.push_str(&format!("sparx_run_tenants_skipped_total {}\n", snapshot.metrics.run_tenants_skipped_total));
    out.push_str("# TYPE sparx_run_devices_processed_total counter\n");
    out.push_str(&format!("sparx_run_devices_processed_total {}\n", snapshot.metrics.run_devices_processed_total));
    out.push_str("# TYPE sparx_run_devices_failed_total counter\n");
    out.push_str(&format!("sparx_run_devices_failed_total {}\n", snapshot.metrics.run_devices_failed_total));
    out.push_str("# TYPE sparx_run_alerts_emitted_total counter\n");
    out.push_str(&format!("sparx_run_alerts_emitted_total {}\n", snapshot.metrics.run_alerts_emitted_total));
    append_optional_u64_metric_v1(&mut out, "sparx_run_last_cycle_tenants_total", snapshot.metrics.run_last_cycle_tenants_total);
    append_optional_u64_metric_v1(&mut out, "sparx_run_last_cycle_tenants_processed", snapshot.metrics.run_last_cycle_tenants_processed);
    append_optional_u64_metric_v1(&mut out, "sparx_run_last_cycle_tenants_skipped", snapshot.metrics.run_last_cycle_tenants_skipped);
    append_optional_u64_metric_v1(&mut out, "sparx_run_last_cycle_devices_processed", snapshot.metrics.run_last_cycle_devices_processed);
    append_optional_u64_metric_v1(&mut out, "sparx_run_last_cycle_devices_failed", snapshot.metrics.run_last_cycle_devices_failed);
    append_optional_u64_metric_v1(&mut out, "sparx_run_last_cycle_alerts_emitted", snapshot.metrics.run_last_cycle_alerts_emitted);
    append_optional_u64_metric_v1(&mut out, "sparx_run_last_cycle_completed_ts", snapshot.metrics.run_last_cycle_completed_ts);
    out.push_str("# TYPE sparx_recovery_automated_replay_max_files_per_pass gauge\n");
    out.push_str(&format!(
        "sparx_recovery_automated_replay_max_files_per_pass {}\n",
        snapshot.recovery.automated_replay_max_files_per_pass
    ));
    out.push_str("# TYPE sparx_recovery_spool_backlog_files gauge\n");
    out.push_str(&format!(
        "sparx_recovery_spool_backlog_files {}\n",
        snapshot.recovery.spool_backlog_files
    ));
    out.push_str("# TYPE sparx_recovery_spool_backlog_bytes gauge\n");
    out.push_str(&format!(
        "sparx_recovery_spool_backlog_bytes {}\n",
        snapshot.recovery.spool_backlog_bytes
    ));
    out
}

pub fn format_health_text_v1(snapshot: &StatusSnapshotV1) -> String {
    let mut out = String::new();
    out.push_str("sparx health\n");
    out.push_str("status: ok\n");
    out.push_str(&format!("mode: {}\n", snapshot.mode));
    out.push_str(&format!("run_cycles_completed_total: {}\n", snapshot.metrics.run_cycles_completed_total));
    out.push_str(&format!("run_last_cycle_completed_ts: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_completed_ts)));
    out.push_str(&format!("spool_backlog_files: {}\n", snapshot.recovery.spool_backlog_files));
    out.push_str(&format!("spool_backlog_bytes: {}\n", snapshot.recovery.spool_backlog_bytes));
    out.push_str(&format!(
        "automated_replay_max_files_per_pass: {}\n",
        snapshot.recovery.automated_replay_max_files_per_pass
    ));
    out
}

fn append_optional_i64_metric_v1(out: &mut String, name: &str, value: Option<i64>) {
    if let Some(value) = value {
        out.push_str("# TYPE ");
        out.push_str(name);
        out.push_str(" gauge\n");
        out.push_str(&format!("{} {}\n", name, value));
    }
}

fn append_optional_i32_metric_v1(out: &mut String, name: &str, value: Option<i32>) {
    if let Some(value) = value {
        out.push_str("# TYPE ");
        out.push_str(name);
        out.push_str(" gauge\n");
        out.push_str(&format!("{} {}\n", name, value));
    }
}

fn append_optional_u32_metric_v1(out: &mut String, name: &str, value: Option<u32>) {
    if let Some(value) = value {
        out.push_str("# TYPE ");
        out.push_str(name);
        out.push_str(" gauge\n");
        out.push_str(&format!("{} {}\n", name, value));
    }
}

fn append_optional_u64_metric_v1(out: &mut String, name: &str, value: Option<u64>) {
    if let Some(value) = value {
        out.push_str("# TYPE ");
        out.push_str(name);
        out.push_str(" gauge\n");
        out.push_str(&format!("{} {}\n", name, value));
    }
}

fn escape_prometheus_label_v1(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn format_option_i64_v1(value: Option<i64>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "null".to_string(),
    }
}

fn format_option_i32_v1(value: Option<i32>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "null".to_string(),
    }
}

fn format_option_u32_v1(value: Option<u32>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "null".to_string(),
    }
}

fn format_option_u64_v1(value: Option<u64>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "null".to_string(),
    }
}

fn format_option_str_v1(value: Option<&str>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "null".to_string(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EndpointKindV1 {
    Prometheus,
    Health,
}

pub struct ObservabilityServerV1 {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    local_addr: String,
}

impl ObservabilityServerV1 {
    fn start_v1(
        kind: EndpointKindV1,
        bind: &str,
        cfg: ConfigV1,
        layout: FilesystemLayoutV1,
        global_db: GlobalDbV1,
    ) -> Result<Self, String> {
        let listener = TcpListener::bind(bind)
            .map_err(|e| format!("failed to bind {} endpoint at {}: {}", endpoint_name_v1(kind), bind, e))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to set nonblocking {} endpoint at {}: {}", endpoint_name_v1(kind), bind, e))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| format!("failed to read local address for {} endpoint at {}: {}", endpoint_name_v1(kind), bind, e))?
            .to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            while !stop_thread.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _addr)) => {
                        let _ = handle_endpoint_request_v1(kind, &cfg, &layout, &global_db, &mut stream);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_e) => {
                        thread::sleep(Duration::from_millis(25));
                    }
                }
            }
        });
        Ok(Self {
            stop,
            handle: Some(handle),
            local_addr,
        })
    }

    pub fn local_addr_v1(&self) -> &str {
        &self.local_addr
    }

    pub fn shutdown_v1(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.local_addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub struct ObservabilityServersV1 {
    pub prometheus: Option<ObservabilityServerV1>,
    pub health: Option<ObservabilityServerV1>,
}

impl ObservabilityServersV1 {
    pub fn start_from_runtime_v1(cfg: &ConfigV1, runtime: &SparxRuntimeV1) -> Result<Self, String> {
        let layout = runtime.layout_v1().clone();
        let global_db = runtime.global_db_v1().clone();
        let mut prometheus = None;
        let mut health = None;

        if cfg.metrics.prometheus_enabled {
            prometheus = Some(ObservabilityServerV1::start_v1(
                EndpointKindV1::Prometheus,
                &cfg.metrics.prometheus_bind,
                cfg.clone(),
                layout.clone(),
                global_db.clone(),
            )?);
        }

        if cfg.metrics.health_enabled {
            match ObservabilityServerV1::start_v1(
                EndpointKindV1::Health,
                &cfg.metrics.health_bind,
                cfg.clone(),
                layout.clone(),
                global_db.clone(),
            ) {
                Ok(server) => {
                    health = Some(server);
                }
                Err(e) => {
                    if let Some(server) = prometheus.as_mut() {
                        server.shutdown_v1();
                    }
                    return Err(e);
                }
            }
        }

        Ok(Self { prometheus, health })
    }

    pub fn shutdown_v1(&mut self) {
        if let Some(server) = self.prometheus.as_mut() {
            server.shutdown_v1();
        }
        if let Some(server) = self.health.as_mut() {
            server.shutdown_v1();
        }
    }
}

fn endpoint_name_v1(kind: EndpointKindV1) -> &'static str {
    match kind {
        EndpointKindV1::Prometheus => "prometheus",
        EndpointKindV1::Health => "health",
    }
}

fn endpoint_path_v1(kind: EndpointKindV1) -> &'static str {
    match kind {
        EndpointKindV1::Prometheus => "/metrics",
        EndpointKindV1::Health => "/healthz",
    }
}

fn endpoint_url_v1(enabled: bool, bind: &str, path: &str) -> Option<String> {
    if !enabled {
        return None;
    }
    Some(format!("http://{}{}", bind, path))
}

fn handle_endpoint_request_v1(
    kind: EndpointKindV1,
    cfg: &ConfigV1,
    layout: &FilesystemLayoutV1,
    global_db: &GlobalDbV1,
    stream: &mut TcpStream,
) -> Result<(), String> {
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .map_err(|e| format!("failed to set endpoint read timeout: {}", e))?;
    let mut buf = [0u8; 4096];
    let read = stream.read(&mut buf).map_err(|e| format!("endpoint read failed: {}", e))?;
    let request = String::from_utf8_lossy(&buf[..read]).to_string();
    let first_line = request.lines().next().unwrap_or("");
    let expected_path = endpoint_path_v1(kind);

    if !first_line.starts_with("GET ") {
        write_http_response_v1(stream, 405, "text/plain; charset=utf-8", "method not allowed\n")?;
        return Ok(());
    }

    let actual_path = first_line.split_whitespace().nth(1).unwrap_or("/");
    if actual_path != expected_path {
        write_http_response_v1(stream, 404, "text/plain; charset=utf-8", "not found\n")?;
        return Ok(());
    }

    let snapshot = match build_status_snapshot_from_parts_v1(cfg, layout, global_db) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let body = format!("snapshot error: {}\n", e);
            write_http_response_v1(stream, 503, "text/plain; charset=utf-8", &body)?;
            return Ok(());
        }
    };

    match kind {
        EndpointKindV1::Prometheus => write_http_response_v1(
            stream,
            200,
            "text/plain; version=0.0.4; charset=utf-8",
            &format_prometheus_text_v1(&snapshot),
        )?,
        EndpointKindV1::Health => write_http_response_v1(
            stream,
            200,
            "text/plain; charset=utf-8",
            &format_health_text_v1(&snapshot),
        )?,
    }
    Ok(())
}

fn write_http_response_v1(
    stream: &mut TcpStream,
    status_code: u16,
    content_type: &str,
    body: &str,
) -> Result<(), String> {
    let reason = match status_code {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status_code,
        reason,
        content_type,
        body.as_bytes().len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .map_err(|e| format!("endpoint write failed: {}", e))?;
    let _ = stream.flush();
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}
