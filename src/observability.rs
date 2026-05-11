// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Observability helpers for status, metrics, and health.
//
// This module builds one shared status snapshot and formats that snapshot for
// text status, JSON status, Prometheus metrics, and health output.

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
use crate::sink::{spool_backlog_per_tenant_v1, spool_backlog_summary_v1};

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
pub const METRIC_RECOVERY_SPOOL_WRITES_TOTAL_V1: &str = "recovery_spool_writes_total";
pub const METRIC_RECOVERY_SPOOL_REPLAYED_TOTAL_V1: &str = "recovery_spool_replayed_total";
pub const METRIC_RECOVERY_SPOOL_REPLAY_FAIL_TOTAL_V1: &str = "recovery_spool_replay_fail_total";
pub const METRIC_RECOVERY_SPOOL_DROP_TOTAL_V1: &str = "recovery_spool_drop_total";
pub const METRIC_RECOVERY_AUTOMATED_REPLAY_ATTEMPTS_TOTAL_V1: &str = "recovery_automated_replay_attempts_total";
pub const METRIC_RECOVERY_LAST_AUTOMATED_REPLAY_ATTEMPT_TS_V1: &str = "recovery_last_automated_replay_attempt_ts";
pub const METRIC_RECOVERY_LAST_AUTOMATED_REPLAY_REPLAYED_V1: &str = "recovery_last_automated_replay_replayed";
pub const METRIC_RECOVERY_LAST_AUTOMATED_REPLAY_FAILED_V1: &str = "recovery_last_automated_replay_failed";
pub const METRIC_RECOVERY_PREVIOUS_SNAPSHOT_TS_V1: &str = "recovery_previous_snapshot_ts";
pub const METRIC_RECOVERY_PREVIOUS_SNAPSHOT_BACKLOG_FILES_V1: &str = "recovery_previous_snapshot_backlog_files";
pub const METRIC_RECOVERY_PREVIOUS_SNAPSHOT_BACKLOG_BYTES_V1: &str = "recovery_previous_snapshot_backlog_bytes";
pub const METRIC_RECOVERY_LAST_SNAPSHOT_TS_V1: &str = "recovery_last_snapshot_ts";
pub const METRIC_RECOVERY_LAST_SNAPSHOT_BACKLOG_FILES_V1: &str = "recovery_last_snapshot_backlog_files";
pub const METRIC_RECOVERY_LAST_SNAPSHOT_BACKLOG_BYTES_V1: &str = "recovery_last_snapshot_backlog_bytes";
pub const METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_TS_V1: &str = "recovery_previous_counter_snapshot_ts";
pub const METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_SPOOL_WRITES_TOTAL_V1: &str = "recovery_previous_counter_snapshot_spool_writes_total";
pub const METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_SPOOL_REPLAYED_TOTAL_V1: &str = "recovery_previous_counter_snapshot_spool_replayed_total";
pub const METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_SPOOL_REPLAY_FAIL_TOTAL_V1: &str = "recovery_previous_counter_snapshot_spool_replay_fail_total";
pub const METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_AUTOMATED_REPLAY_ATTEMPTS_TOTAL_V1: &str = "recovery_previous_counter_snapshot_automated_replay_attempts_total";
pub const METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_TS_V1: &str = "recovery_last_counter_snapshot_ts";
pub const METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_SPOOL_WRITES_TOTAL_V1: &str = "recovery_last_counter_snapshot_spool_writes_total";
pub const METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_SPOOL_REPLAYED_TOTAL_V1: &str = "recovery_last_counter_snapshot_spool_replayed_total";
pub const METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_SPOOL_REPLAY_FAIL_TOTAL_V1: &str = "recovery_last_counter_snapshot_spool_replay_fail_total";
pub const METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_AUTOMATED_REPLAY_ATTEMPTS_TOTAL_V1: &str = "recovery_last_counter_snapshot_automated_replay_attempts_total";
pub const METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_TS_V1: &str = "recovery_history_start_counter_snapshot_ts";
pub const METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_SPOOL_WRITES_TOTAL_V1: &str = "recovery_history_start_counter_snapshot_spool_writes_total";
pub const METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_SPOOL_REPLAYED_TOTAL_V1: &str = "recovery_history_start_counter_snapshot_spool_replayed_total";
pub const METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_SPOOL_REPLAY_FAIL_TOTAL_V1: &str = "recovery_history_start_counter_snapshot_spool_replay_fail_total";
pub const METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_AUTOMATED_REPLAY_ATTEMPTS_TOTAL_V1: &str = "recovery_history_start_counter_snapshot_automated_replay_attempts_total";
pub const METRIC_VDROP_TRACKED_SUBJECTS_V1: &str = "vdrop_tracked_subjects";
pub const METRIC_VDROP_OPEN_SILENCE_SUBJECTS_V1: &str = "vdrop_open_silence_subjects";
pub const METRIC_VDROP_OPEN_DROP_SUBJECTS_V1: &str = "vdrop_open_drop_subjects";
pub const METRIC_VDROP_EVALUATED_SUBJECTS_TOTAL_V1: &str = "vdrop_evaluated_subjects_total";
pub const METRIC_VDROP_CANDIDATES_TOTAL_V1: &str = "vdrop_candidates_total";
pub const METRIC_VDROP_SUPPRESSED_CANDIDATES_TOTAL_V1: &str = "vdrop_suppressed_candidates_total";
pub const METRIC_VDROP_ALERTS_EMITTED_TOTAL_V1: &str = "vdrop_alerts_emitted_total";
pub const METRIC_VDROP_LAST_EVALUATION_TS_V1: &str = "vdrop_last_evaluation_ts";
pub const METRIC_VDROP_SOURCE_STREAM_TRACKED_SUBJECTS_V1: &str = "vdrop_source_stream_tracked_subjects";
pub const METRIC_VDROP_SOURCE_STREAM_OPEN_SILENCE_SUBJECTS_V1: &str = "vdrop_source_stream_open_silence_subjects";
pub const METRIC_VDROP_SOURCE_STREAM_OPEN_DROP_SUBJECTS_V1: &str = "vdrop_source_stream_open_drop_subjects";
pub const METRIC_VDROP_SOURCE_STREAM_EVALUATED_SUBJECTS_TOTAL_V1: &str = "vdrop_source_stream_evaluated_subjects_total";
pub const METRIC_VDROP_SOURCE_STREAM_CANDIDATES_TOTAL_V1: &str = "vdrop_source_stream_candidates_total";
pub const METRIC_VDROP_SOURCE_STREAM_SUPPRESSED_CANDIDATES_TOTAL_V1: &str = "vdrop_source_stream_suppressed_candidates_total";
pub const METRIC_VDROP_SOURCE_STREAM_ALERTS_EMITTED_TOTAL_V1: &str = "vdrop_source_stream_alerts_emitted_total";
pub const METRIC_VDROP_SOURCE_STREAM_LAST_EVALUATION_TS_V1: &str = "vdrop_source_stream_last_evaluation_ts";

fn recovery_tenant_metric_name_v1(prefix: &str, tenant_id: &str) -> String {
    format!("{}__{}", prefix, tenant_id)
}

pub fn metric_recovery_tenant_previous_snapshot_ts_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_previous_snapshot_ts", tenant_id)
}

pub fn metric_recovery_tenant_previous_snapshot_backlog_files_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_previous_snapshot_backlog_files", tenant_id)
}

pub fn metric_recovery_tenant_previous_snapshot_backlog_bytes_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_previous_snapshot_backlog_bytes", tenant_id)
}

pub fn metric_recovery_tenant_last_snapshot_ts_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_last_snapshot_ts", tenant_id)
}

pub fn metric_recovery_tenant_last_snapshot_backlog_files_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_last_snapshot_backlog_files", tenant_id)
}

pub fn metric_recovery_tenant_last_snapshot_backlog_bytes_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_last_snapshot_backlog_bytes", tenant_id)
}


pub fn metric_recovery_tenant_spool_writes_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_spool_writes_total", tenant_id)
}

pub fn metric_recovery_tenant_spool_replayed_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_spool_replayed_total", tenant_id)
}

pub fn metric_recovery_tenant_spool_replay_fail_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_spool_replay_fail_total", tenant_id)
}

pub fn metric_recovery_tenant_automated_replay_attempts_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_automated_replay_attempts_total", tenant_id)
}

pub fn metric_recovery_tenant_previous_counter_snapshot_ts_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_previous_counter_snapshot_ts", tenant_id)
}

pub fn metric_recovery_tenant_previous_counter_snapshot_spool_writes_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_previous_counter_snapshot_spool_writes_total", tenant_id)
}

pub fn metric_recovery_tenant_previous_counter_snapshot_spool_replayed_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_previous_counter_snapshot_spool_replayed_total", tenant_id)
}

pub fn metric_recovery_tenant_previous_counter_snapshot_spool_replay_fail_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_previous_counter_snapshot_spool_replay_fail_total", tenant_id)
}

pub fn metric_recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total", tenant_id)
}

pub fn metric_recovery_tenant_last_counter_snapshot_ts_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_last_counter_snapshot_ts", tenant_id)
}

pub fn metric_recovery_tenant_last_counter_snapshot_spool_writes_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_last_counter_snapshot_spool_writes_total", tenant_id)
}

pub fn metric_recovery_tenant_last_counter_snapshot_spool_replayed_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_last_counter_snapshot_spool_replayed_total", tenant_id)
}

pub fn metric_recovery_tenant_last_counter_snapshot_spool_replay_fail_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_last_counter_snapshot_spool_replay_fail_total", tenant_id)
}

pub fn metric_recovery_tenant_last_counter_snapshot_automated_replay_attempts_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_last_counter_snapshot_automated_replay_attempts_total", tenant_id)
}

pub fn metric_recovery_tenant_history_start_counter_snapshot_ts_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_history_start_counter_snapshot_ts", tenant_id)
}

pub fn metric_recovery_tenant_history_start_counter_snapshot_spool_writes_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_history_start_counter_snapshot_spool_writes_total", tenant_id)
}

pub fn metric_recovery_tenant_history_start_counter_snapshot_spool_replayed_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_history_start_counter_snapshot_spool_replayed_total", tenant_id)
}

pub fn metric_recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total", tenant_id)
}

pub fn metric_recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total", tenant_id)
}

pub fn metric_vdrop_tenant_tracked_subjects_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_tracked_subjects", tenant_id)
}

pub fn metric_vdrop_tenant_open_silence_subjects_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_open_silence_subjects", tenant_id)
}

pub fn metric_vdrop_tenant_open_drop_subjects_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_open_drop_subjects", tenant_id)
}

pub fn metric_vdrop_tenant_evaluated_subjects_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_evaluated_subjects_total", tenant_id)
}

pub fn metric_vdrop_tenant_candidates_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_candidates_total", tenant_id)
}

pub fn metric_vdrop_tenant_suppressed_candidates_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_suppressed_candidates_total", tenant_id)
}

pub fn metric_vdrop_tenant_alerts_emitted_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_alerts_emitted_total", tenant_id)
}

pub fn metric_vdrop_tenant_last_evaluation_ts_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_last_evaluation_ts", tenant_id)
}

pub fn metric_vdrop_tenant_source_stream_tracked_subjects_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_source_stream_tracked_subjects", tenant_id)
}

pub fn metric_vdrop_tenant_source_stream_open_silence_subjects_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_source_stream_open_silence_subjects", tenant_id)
}

pub fn metric_vdrop_tenant_source_stream_open_drop_subjects_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_source_stream_open_drop_subjects", tenant_id)
}

pub fn metric_vdrop_tenant_source_stream_evaluated_subjects_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_source_stream_evaluated_subjects_total", tenant_id)
}

pub fn metric_vdrop_tenant_source_stream_candidates_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_source_stream_candidates_total", tenant_id)
}

pub fn metric_vdrop_tenant_source_stream_suppressed_candidates_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_source_stream_suppressed_candidates_total", tenant_id)
}

pub fn metric_vdrop_tenant_source_stream_alerts_emitted_total_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_source_stream_alerts_emitted_total", tenant_id)
}

pub fn metric_vdrop_tenant_source_stream_last_evaluation_ts_v1(tenant_id: &str) -> String {
    recovery_tenant_metric_name_v1("vdrop_source_stream_last_evaluation_ts", tenant_id)
}

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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StatusRecoveryTenantBacklogV1 {
    pub tenant_id: String,
    pub files: u64,
    pub bytes: u64,
    pub oldest_file_ts: Option<u64>,
    pub oldest_age_s: Option<u64>,
    pub stale: bool,
    pub previous_snapshot_ts: Option<u64>,
    pub last_snapshot_ts: Option<u64>,
    pub snapshot_interval_s: Option<u64>,
    pub backlog_files_trend_delta: Option<i64>,
    pub backlog_bytes_trend_delta: Option<i64>,
    pub backlog_trend_direction: String,
    pub previous_counter_snapshot_ts: Option<u64>,
    pub last_counter_snapshot_ts: Option<u64>,
    pub counter_snapshot_interval_s: Option<u64>,
    pub history_start_counter_snapshot_ts: Option<u64>,
    pub history_counter_snapshot_interval_s: Option<u64>,
    pub history_spool_write_rate_per_s: Option<f64>,
    pub history_spool_replayed_rate_per_s: Option<f64>,
    pub history_spool_replay_fail_rate_per_s: Option<f64>,
    pub history_automated_replay_attempt_rate_per_s: Option<f64>,
    pub spool_write_rate_per_s: Option<f64>,
    pub spool_replayed_rate_per_s: Option<f64>,
    pub spool_replay_fail_rate_per_s: Option<f64>,
    pub automated_replay_attempt_rate_per_s: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StatusRecoveryViewV1 {
    pub automated_replay_max_files_per_pass: u32,
    pub automated_replay_interval_s: u32,
    pub spool_max_mb: u32,
    pub spool_backlog_files: u64,
    pub spool_backlog_bytes: u64,
    pub spool_oldest_file_ts: Option<u64>,
    pub spool_oldest_age_s: Option<u64>,
    pub stale_backlog: bool,
    pub stale_backlog_tenants: u64,
    pub spool_backlog_tenants: Vec<StatusRecoveryTenantBacklogV1>,
    pub spool_writes_total: u64,
    pub spool_replayed_total: u64,
    pub spool_replay_fail_total: u64,
    pub spool_drop_total: u64,
    pub automated_replay_attempts_total: u64,
    pub last_automated_replay_attempt_ts: Option<u64>,
    pub last_automated_replay_replayed: Option<u64>,
    pub last_automated_replay_failed: Option<u64>,
    pub previous_snapshot_ts: Option<u64>,
    pub last_snapshot_ts: Option<u64>,
    pub snapshot_interval_s: Option<u64>,
    pub backlog_files_trend_delta: Option<i64>,
    pub backlog_bytes_trend_delta: Option<i64>,
    pub backlog_trend_direction: String,
    pub previous_counter_snapshot_ts: Option<u64>,
    pub last_counter_snapshot_ts: Option<u64>,
    pub counter_snapshot_interval_s: Option<u64>,
    pub history_start_counter_snapshot_ts: Option<u64>,
    pub history_counter_snapshot_interval_s: Option<u64>,
    pub history_spool_write_rate_per_s: Option<f64>,
    pub history_spool_replayed_rate_per_s: Option<f64>,
    pub history_spool_replay_fail_rate_per_s: Option<f64>,
    pub history_automated_replay_attempt_rate_per_s: Option<f64>,
    pub spool_write_rate_per_s: Option<f64>,
    pub spool_replayed_rate_per_s: Option<f64>,
    pub spool_replay_fail_rate_per_s: Option<f64>,
    pub automated_replay_attempt_rate_per_s: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StatusVDropTenantDiagnosticsV1 {
    pub tenant_id: String,
    pub tracked_subjects: Option<u64>,
    pub open_silence_subjects: Option<u64>,
    pub open_drop_subjects: Option<u64>,
    pub evaluated_subjects_total: u64,
    pub candidates_total: u64,
    pub suppressed_candidates_total: u64,
    pub alerts_emitted_total: u64,
    pub last_evaluation_ts: Option<u64>,
    pub source_stream_tracked_subjects: Option<u64>,
    pub source_stream_open_silence_subjects: Option<u64>,
    pub source_stream_open_drop_subjects: Option<u64>,
    pub source_stream_evaluated_subjects_total: u64,
    pub source_stream_candidates_total: u64,
    pub source_stream_suppressed_candidates_total: u64,
    pub source_stream_alerts_emitted_total: u64,
    pub source_stream_last_evaluation_ts: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StatusVDropDiagnosticsV1 {
    pub enabled: bool,
    pub device_enabled: bool,
    pub tenant_enabled: bool,
    pub source_stream_enabled: bool,
    pub min_expected_windows_missed: u32,
    pub min_mature_windows: Option<u64>,
    pub min_expected_lines: Option<u64>,
    pub tracked_subjects: Option<u64>,
    pub open_silence_subjects: Option<u64>,
    pub open_drop_subjects: Option<u64>,
    pub evaluated_subjects_total: u64,
    pub candidates_total: u64,
    pub suppressed_candidates_total: u64,
    pub alerts_emitted_total: u64,
    pub last_evaluation_ts: Option<u64>,
    pub source_stream_tracked_subjects: Option<u64>,
    pub source_stream_open_silence_subjects: Option<u64>,
    pub source_stream_open_drop_subjects: Option<u64>,
    pub source_stream_evaluated_subjects_total: u64,
    pub source_stream_candidates_total: u64,
    pub source_stream_suppressed_candidates_total: u64,
    pub source_stream_alerts_emitted_total: u64,
    pub source_stream_last_evaluation_ts: Option<u64>,
    pub tenants: Vec<StatusVDropTenantDiagnosticsV1>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
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
    pub vdrop: StatusVDropDiagnosticsV1,
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
    let spool_backlog_tenants = spool_backlog_per_tenant_v1(&cfg.sparx.data_root)
        .map_err(|e| DbErrorV1::new_v1(format!("failed to read per-tenant spool backlog summary: {}", e.msg)))?;
    let mapped_spool_backlog_tenants: Vec<StatusRecoveryTenantBacklogV1> = spool_backlog_tenants
        .into_iter()
        .map(|tenant| {
            let previous_snapshot_ts = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_previous_snapshot_ts_v1(&tenant.tenant_id),
            )?;
            let previous_snapshot_backlog_files = read_u64_gauge_as_int_v1(
                global_db,
                &metric_recovery_tenant_previous_snapshot_backlog_files_v1(&tenant.tenant_id),
            )?;
            let previous_snapshot_backlog_bytes = read_u64_gauge_as_int_v1(
                global_db,
                &metric_recovery_tenant_previous_snapshot_backlog_bytes_v1(&tenant.tenant_id),
            )?;
            let last_snapshot_ts = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_last_snapshot_ts_v1(&tenant.tenant_id),
            )?;
            let last_snapshot_backlog_files = read_u64_gauge_as_int_v1(
                global_db,
                &metric_recovery_tenant_last_snapshot_backlog_files_v1(&tenant.tenant_id),
            )?;
            let last_snapshot_backlog_bytes = read_u64_gauge_as_int_v1(
                global_db,
                &metric_recovery_tenant_last_snapshot_backlog_bytes_v1(&tenant.tenant_id),
            )?;
            let previous_counter_snapshot_ts = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_previous_counter_snapshot_ts_v1(&tenant.tenant_id),
            )?;
            let previous_counter_snapshot_spool_writes_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_previous_counter_snapshot_spool_writes_total_v1(&tenant.tenant_id),
            )?;
            let previous_counter_snapshot_spool_replayed_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_previous_counter_snapshot_spool_replayed_total_v1(&tenant.tenant_id),
            )?;
            let previous_counter_snapshot_spool_replay_fail_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_previous_counter_snapshot_spool_replay_fail_total_v1(&tenant.tenant_id),
            )?;
            let previous_counter_snapshot_automated_replay_attempts_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total_v1(&tenant.tenant_id),
            )?;
            let last_counter_snapshot_ts = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_last_counter_snapshot_ts_v1(&tenant.tenant_id),
            )?;
            let last_counter_snapshot_spool_writes_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_last_counter_snapshot_spool_writes_total_v1(&tenant.tenant_id),
            )?;
            let last_counter_snapshot_spool_replayed_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_last_counter_snapshot_spool_replayed_total_v1(&tenant.tenant_id),
            )?;
            let last_counter_snapshot_spool_replay_fail_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_last_counter_snapshot_spool_replay_fail_total_v1(&tenant.tenant_id),
            )?;
            let last_counter_snapshot_automated_replay_attempts_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_last_counter_snapshot_automated_replay_attempts_total_v1(&tenant.tenant_id),
            )?;
            let history_start_counter_snapshot_ts = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_history_start_counter_snapshot_ts_v1(&tenant.tenant_id),
            )?;
            let history_start_counter_snapshot_spool_writes_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_history_start_counter_snapshot_spool_writes_total_v1(&tenant.tenant_id),
            )?;
            let history_start_counter_snapshot_spool_replayed_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_history_start_counter_snapshot_spool_replayed_total_v1(&tenant.tenant_id),
            )?;
            let history_start_counter_snapshot_spool_replay_fail_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total_v1(&tenant.tenant_id),
            )?;
            let history_start_counter_snapshot_automated_replay_attempts_total = global_db.read_metric_counter_v1(
                &metric_recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total_v1(&tenant.tenant_id),
            )?;
            let snapshot_interval_s = diff_nonnegative_u64_v1(last_snapshot_ts, previous_snapshot_ts);
            let backlog_files_trend_delta = diff_signed_u64_v1(
                last_snapshot_backlog_files,
                previous_snapshot_backlog_files,
            );
            let backlog_bytes_trend_delta = diff_signed_u64_v1(
                last_snapshot_backlog_bytes,
                previous_snapshot_backlog_bytes,
            );
            let backlog_trend_direction = recovery_trend_direction_v1(
                backlog_files_trend_delta,
                backlog_bytes_trend_delta,
            )
            .to_string();
            let last_counter_snapshot = RecoveryCounterSnapshotViewV1 {
                ts: last_counter_snapshot_ts,
                spool_writes_total: last_counter_snapshot_spool_writes_total,
                spool_replayed_total: last_counter_snapshot_spool_replayed_total,
                spool_replay_fail_total: last_counter_snapshot_spool_replay_fail_total,
                automated_replay_attempts_total: last_counter_snapshot_automated_replay_attempts_total,
            };
            let previous_counter_snapshot = RecoveryCounterSnapshotViewV1 {
                ts: previous_counter_snapshot_ts,
                spool_writes_total: previous_counter_snapshot_spool_writes_total,
                spool_replayed_total: previous_counter_snapshot_spool_replayed_total,
                spool_replay_fail_total: previous_counter_snapshot_spool_replay_fail_total,
                automated_replay_attempts_total: previous_counter_snapshot_automated_replay_attempts_total,
            };
            let history_start_counter_snapshot = RecoveryCounterSnapshotViewV1 {
                ts: history_start_counter_snapshot_ts,
                spool_writes_total: history_start_counter_snapshot_spool_writes_total,
                spool_replayed_total: history_start_counter_snapshot_spool_replayed_total,
                spool_replay_fail_total: history_start_counter_snapshot_spool_replay_fail_total,
                automated_replay_attempts_total: history_start_counter_snapshot_automated_replay_attempts_total,
            };
            let counter_rate = recovery_counter_rate_view_v1(
                previous_counter_snapshot,
                last_counter_snapshot,
            );
            let history_counter_rate = recovery_counter_rate_view_v1(
                history_start_counter_snapshot,
                last_counter_snapshot,
            );
            let stale = stale_backlog_v1(tenant.oldest_age_s, cfg.output.automated_replay_interval_s);
            Ok(StatusRecoveryTenantBacklogV1 {
                tenant_id: tenant.tenant_id,
                files: tenant.files,
                bytes: tenant.bytes,
                oldest_file_ts: tenant.oldest_file_ts,
                oldest_age_s: tenant.oldest_age_s,
                stale,
                previous_snapshot_ts,
                last_snapshot_ts,
                snapshot_interval_s,
                backlog_files_trend_delta,
                backlog_bytes_trend_delta,
                backlog_trend_direction,
                previous_counter_snapshot_ts,
                last_counter_snapshot_ts,
                counter_snapshot_interval_s: counter_rate.interval_s,
                history_start_counter_snapshot_ts,
                history_counter_snapshot_interval_s: history_counter_rate.interval_s,
                history_spool_write_rate_per_s: history_counter_rate.spool_write_rate_per_s,
                history_spool_replayed_rate_per_s: history_counter_rate.spool_replayed_rate_per_s,
                history_spool_replay_fail_rate_per_s: history_counter_rate.spool_replay_fail_rate_per_s,
                history_automated_replay_attempt_rate_per_s: history_counter_rate.automated_replay_attempt_rate_per_s,
                spool_write_rate_per_s: counter_rate.spool_write_rate_per_s,
                spool_replayed_rate_per_s: counter_rate.spool_replayed_rate_per_s,
                spool_replay_fail_rate_per_s: counter_rate.spool_replay_fail_rate_per_s,
                automated_replay_attempt_rate_per_s: counter_rate.automated_replay_attempt_rate_per_s,
            })
        })
        .collect::<Result<Vec<_>, DbErrorV1>>()?;
    let vdrop_tenants: Vec<StatusVDropTenantDiagnosticsV1> = known_tenants
        .iter()
        .map(|tenant_id| {
            Ok(StatusVDropTenantDiagnosticsV1 {
                tenant_id: tenant_id.clone(),
                tracked_subjects: read_u64_gauge_as_int_v1(
                    global_db,
                    &metric_vdrop_tenant_tracked_subjects_v1(tenant_id),
                )?,
                open_silence_subjects: read_u64_gauge_as_int_v1(
                    global_db,
                    &metric_vdrop_tenant_open_silence_subjects_v1(tenant_id),
                )?,
                open_drop_subjects: read_u64_gauge_as_int_v1(
                    global_db,
                    &metric_vdrop_tenant_open_drop_subjects_v1(tenant_id),
                )?,
                evaluated_subjects_total: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_evaluated_subjects_total_v1(tenant_id))?
                    .unwrap_or(0),
                candidates_total: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_candidates_total_v1(tenant_id))?
                    .unwrap_or(0),
                suppressed_candidates_total: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_suppressed_candidates_total_v1(tenant_id))?
                    .unwrap_or(0),
                alerts_emitted_total: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_alerts_emitted_total_v1(tenant_id))?
                    .unwrap_or(0),
                last_evaluation_ts: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_last_evaluation_ts_v1(tenant_id))?,
                source_stream_tracked_subjects: read_u64_gauge_as_int_v1(
                    global_db,
                    &metric_vdrop_tenant_source_stream_tracked_subjects_v1(tenant_id),
                )?,
                source_stream_open_silence_subjects: read_u64_gauge_as_int_v1(
                    global_db,
                    &metric_vdrop_tenant_source_stream_open_silence_subjects_v1(tenant_id),
                )?,
                source_stream_open_drop_subjects: read_u64_gauge_as_int_v1(
                    global_db,
                    &metric_vdrop_tenant_source_stream_open_drop_subjects_v1(tenant_id),
                )?,
                source_stream_evaluated_subjects_total: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_source_stream_evaluated_subjects_total_v1(tenant_id))?
                    .unwrap_or(0),
                source_stream_candidates_total: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_source_stream_candidates_total_v1(tenant_id))?
                    .unwrap_or(0),
                source_stream_suppressed_candidates_total: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_source_stream_suppressed_candidates_total_v1(tenant_id))?
                    .unwrap_or(0),
                source_stream_alerts_emitted_total: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_source_stream_alerts_emitted_total_v1(tenant_id))?
                    .unwrap_or(0),
                source_stream_last_evaluation_ts: global_db
                    .read_metric_counter_v1(&metric_vdrop_tenant_source_stream_last_evaluation_ts_v1(tenant_id))?,
            })
        })
        .collect::<Result<Vec<_>, DbErrorV1>>()?;
    let vdrop_tracked_subjects = sum_optional_u64_v1(
        vdrop_tenants.iter().map(|tenant| tenant.tracked_subjects),
    );
    let vdrop_open_silence_subjects = sum_optional_u64_v1(
        vdrop_tenants.iter().map(|tenant| tenant.open_silence_subjects),
    );
    let vdrop_open_drop_subjects = sum_optional_u64_v1(
        vdrop_tenants.iter().map(|tenant| tenant.open_drop_subjects),
    );
    let vdrop_source_stream_tracked_subjects = sum_optional_u64_v1(
        vdrop_tenants.iter().map(|tenant| tenant.source_stream_tracked_subjects),
    );
    let vdrop_source_stream_open_silence_subjects = sum_optional_u64_v1(
        vdrop_tenants.iter().map(|tenant| tenant.source_stream_open_silence_subjects),
    );
    let vdrop_source_stream_open_drop_subjects = sum_optional_u64_v1(
        vdrop_tenants.iter().map(|tenant| tenant.source_stream_open_drop_subjects),
    );
    let stale_backlog_tenants = mapped_spool_backlog_tenants.iter().filter(|tenant| tenant.stale).count() as u64;
    let stale_backlog = stale_backlog_v1(spool_backlog.oldest_age_s, cfg.output.automated_replay_interval_s);
    let previous_snapshot_ts = global_db.read_metric_counter_v1(METRIC_RECOVERY_PREVIOUS_SNAPSHOT_TS_V1)?;
    let last_snapshot_ts = global_db.read_metric_counter_v1(METRIC_RECOVERY_LAST_SNAPSHOT_TS_V1)?;
    let previous_snapshot_backlog_files = read_u64_gauge_as_int_v1(
        global_db,
        METRIC_RECOVERY_PREVIOUS_SNAPSHOT_BACKLOG_FILES_V1,
    )?;
    let previous_snapshot_backlog_bytes = read_u64_gauge_as_int_v1(
        global_db,
        METRIC_RECOVERY_PREVIOUS_SNAPSHOT_BACKLOG_BYTES_V1,
    )?;
    let last_snapshot_backlog_files = read_u64_gauge_as_int_v1(
        global_db,
        METRIC_RECOVERY_LAST_SNAPSHOT_BACKLOG_FILES_V1,
    )?;
    let last_snapshot_backlog_bytes = read_u64_gauge_as_int_v1(
        global_db,
        METRIC_RECOVERY_LAST_SNAPSHOT_BACKLOG_BYTES_V1,
    )?;
    let snapshot_interval_s = diff_nonnegative_u64_v1(last_snapshot_ts, previous_snapshot_ts);
    let backlog_files_trend_delta = diff_signed_u64_v1(last_snapshot_backlog_files, previous_snapshot_backlog_files);
    let backlog_bytes_trend_delta = diff_signed_u64_v1(last_snapshot_backlog_bytes, previous_snapshot_backlog_bytes);
    let backlog_trend_direction = recovery_trend_direction_v1(
        backlog_files_trend_delta,
        backlog_bytes_trend_delta,
    )
    .to_string();
    let previous_counter_snapshot_ts = global_db.read_metric_counter_v1(METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_TS_V1)?;
    let last_counter_snapshot_ts = global_db.read_metric_counter_v1(METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_TS_V1)?;
    let previous_counter_snapshot_spool_writes_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_SPOOL_WRITES_TOTAL_V1)?;
    let previous_counter_snapshot_spool_replayed_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_SPOOL_REPLAYED_TOTAL_V1)?;
    let previous_counter_snapshot_spool_replay_fail_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_SPOOL_REPLAY_FAIL_TOTAL_V1)?;
    let previous_counter_snapshot_automated_replay_attempts_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_PREVIOUS_COUNTER_SNAPSHOT_AUTOMATED_REPLAY_ATTEMPTS_TOTAL_V1)?;
    let last_counter_snapshot_spool_writes_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_SPOOL_WRITES_TOTAL_V1)?;
    let last_counter_snapshot_spool_replayed_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_SPOOL_REPLAYED_TOTAL_V1)?;
    let last_counter_snapshot_spool_replay_fail_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_SPOOL_REPLAY_FAIL_TOTAL_V1)?;
    let last_counter_snapshot_automated_replay_attempts_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_LAST_COUNTER_SNAPSHOT_AUTOMATED_REPLAY_ATTEMPTS_TOTAL_V1)?;
    let history_start_counter_snapshot_ts = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_TS_V1)?;
    let history_start_counter_snapshot_spool_writes_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_SPOOL_WRITES_TOTAL_V1)?;
    let history_start_counter_snapshot_spool_replayed_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_SPOOL_REPLAYED_TOTAL_V1)?;
    let history_start_counter_snapshot_spool_replay_fail_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_SPOOL_REPLAY_FAIL_TOTAL_V1)?;
    let history_start_counter_snapshot_automated_replay_attempts_total = global_db
        .read_metric_counter_v1(METRIC_RECOVERY_HISTORY_START_COUNTER_SNAPSHOT_AUTOMATED_REPLAY_ATTEMPTS_TOTAL_V1)?;
    let last_counter_snapshot = RecoveryCounterSnapshotViewV1 {
        ts: last_counter_snapshot_ts,
        spool_writes_total: last_counter_snapshot_spool_writes_total,
        spool_replayed_total: last_counter_snapshot_spool_replayed_total,
        spool_replay_fail_total: last_counter_snapshot_spool_replay_fail_total,
        automated_replay_attempts_total: last_counter_snapshot_automated_replay_attempts_total,
    };
    let previous_counter_snapshot = RecoveryCounterSnapshotViewV1 {
        ts: previous_counter_snapshot_ts,
        spool_writes_total: previous_counter_snapshot_spool_writes_total,
        spool_replayed_total: previous_counter_snapshot_spool_replayed_total,
        spool_replay_fail_total: previous_counter_snapshot_spool_replay_fail_total,
        automated_replay_attempts_total: previous_counter_snapshot_automated_replay_attempts_total,
    };
    let history_start_counter_snapshot = RecoveryCounterSnapshotViewV1 {
        ts: history_start_counter_snapshot_ts,
        spool_writes_total: history_start_counter_snapshot_spool_writes_total,
        spool_replayed_total: history_start_counter_snapshot_spool_replayed_total,
        spool_replay_fail_total: history_start_counter_snapshot_spool_replay_fail_total,
        automated_replay_attempts_total: history_start_counter_snapshot_automated_replay_attempts_total,
    };
    let counter_rate = recovery_counter_rate_view_v1(
        previous_counter_snapshot,
        last_counter_snapshot,
    );
    let history_counter_rate = recovery_counter_rate_view_v1(
        history_start_counter_snapshot,
        last_counter_snapshot,
    );

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
            automated_replay_interval_s: cfg.output.automated_replay_interval_s,
            spool_max_mb: cfg.output.spool_max_mb,
            spool_backlog_files: spool_backlog.files,
            spool_backlog_bytes: spool_backlog.bytes,
            spool_oldest_file_ts: spool_backlog.oldest_file_ts,
            spool_oldest_age_s: spool_backlog.oldest_age_s,
            stale_backlog,
            stale_backlog_tenants,
            spool_backlog_tenants: mapped_spool_backlog_tenants,
            spool_writes_total: global_db
                .read_metric_counter_v1(METRIC_RECOVERY_SPOOL_WRITES_TOTAL_V1)?
                .unwrap_or(0),
            spool_replayed_total: global_db
                .read_metric_counter_v1(METRIC_RECOVERY_SPOOL_REPLAYED_TOTAL_V1)?
                .unwrap_or(0),
            spool_replay_fail_total: global_db
                .read_metric_counter_v1(METRIC_RECOVERY_SPOOL_REPLAY_FAIL_TOTAL_V1)?
                .unwrap_or(0),
            spool_drop_total: global_db
                .read_metric_counter_v1(METRIC_RECOVERY_SPOOL_DROP_TOTAL_V1)?
                .unwrap_or(0),
            automated_replay_attempts_total: global_db
                .read_metric_counter_v1(METRIC_RECOVERY_AUTOMATED_REPLAY_ATTEMPTS_TOTAL_V1)?
                .unwrap_or(0),
            last_automated_replay_attempt_ts: global_db
                .read_metric_counter_v1(METRIC_RECOVERY_LAST_AUTOMATED_REPLAY_ATTEMPT_TS_V1)?,
            last_automated_replay_replayed: read_u64_gauge_as_int_v1(
                global_db,
                METRIC_RECOVERY_LAST_AUTOMATED_REPLAY_REPLAYED_V1,
            )?,
            last_automated_replay_failed: read_u64_gauge_as_int_v1(
                global_db,
                METRIC_RECOVERY_LAST_AUTOMATED_REPLAY_FAILED_V1,
            )?,
            previous_snapshot_ts,
            last_snapshot_ts,
            snapshot_interval_s,
            backlog_files_trend_delta,
            backlog_bytes_trend_delta,
            backlog_trend_direction,
            previous_counter_snapshot_ts,
            last_counter_snapshot_ts,
            counter_snapshot_interval_s: counter_rate.interval_s,
            history_start_counter_snapshot_ts,
            history_counter_snapshot_interval_s: history_counter_rate.interval_s,
            history_spool_write_rate_per_s: history_counter_rate.spool_write_rate_per_s,
            history_spool_replayed_rate_per_s: history_counter_rate.spool_replayed_rate_per_s,
            history_spool_replay_fail_rate_per_s: history_counter_rate.spool_replay_fail_rate_per_s,
            history_automated_replay_attempt_rate_per_s: history_counter_rate.automated_replay_attempt_rate_per_s,
            spool_write_rate_per_s: counter_rate.spool_write_rate_per_s,
            spool_replayed_rate_per_s: counter_rate.spool_replayed_rate_per_s,
            spool_replay_fail_rate_per_s: counter_rate.spool_replay_fail_rate_per_s,
            automated_replay_attempt_rate_per_s: counter_rate.automated_replay_attempt_rate_per_s,
        },
        vdrop: StatusVDropDiagnosticsV1 {
            enabled: cfg.vdrop.enabled,
            device_enabled: cfg.vdrop.device_enabled,
            tenant_enabled: cfg.vdrop.tenant_enabled,
            source_stream_enabled: cfg.vdrop.source_stream_enabled,
            min_expected_windows_missed: cfg.vdrop.min_expected_windows_missed,
            min_mature_windows: cfg.vdrop.min_mature_windows,
            min_expected_lines: cfg.vdrop.min_expected_lines,
            tracked_subjects: vdrop_tracked_subjects,
            open_silence_subjects: vdrop_open_silence_subjects,
            open_drop_subjects: vdrop_open_drop_subjects,
            evaluated_subjects_total: global_db.read_metric_counter_v1(METRIC_VDROP_EVALUATED_SUBJECTS_TOTAL_V1)?.unwrap_or(0),
            candidates_total: global_db.read_metric_counter_v1(METRIC_VDROP_CANDIDATES_TOTAL_V1)?.unwrap_or(0),
            suppressed_candidates_total: global_db.read_metric_counter_v1(METRIC_VDROP_SUPPRESSED_CANDIDATES_TOTAL_V1)?.unwrap_or(0),
            alerts_emitted_total: global_db.read_metric_counter_v1(METRIC_VDROP_ALERTS_EMITTED_TOTAL_V1)?.unwrap_or(0),
            last_evaluation_ts: global_db.read_metric_counter_v1(METRIC_VDROP_LAST_EVALUATION_TS_V1)?,
            source_stream_tracked_subjects: vdrop_source_stream_tracked_subjects,
            source_stream_open_silence_subjects: vdrop_source_stream_open_silence_subjects,
            source_stream_open_drop_subjects: vdrop_source_stream_open_drop_subjects,
            source_stream_evaluated_subjects_total: global_db
                .read_metric_counter_v1(METRIC_VDROP_SOURCE_STREAM_EVALUATED_SUBJECTS_TOTAL_V1)?
                .unwrap_or(0),
            source_stream_candidates_total: global_db
                .read_metric_counter_v1(METRIC_VDROP_SOURCE_STREAM_CANDIDATES_TOTAL_V1)?
                .unwrap_or(0),
            source_stream_suppressed_candidates_total: global_db
                .read_metric_counter_v1(METRIC_VDROP_SOURCE_STREAM_SUPPRESSED_CANDIDATES_TOTAL_V1)?
                .unwrap_or(0),
            source_stream_alerts_emitted_total: global_db
                .read_metric_counter_v1(METRIC_VDROP_SOURCE_STREAM_ALERTS_EMITTED_TOTAL_V1)?
                .unwrap_or(0),
            source_stream_last_evaluation_ts: global_db
                .read_metric_counter_v1(METRIC_VDROP_SOURCE_STREAM_LAST_EVALUATION_TS_V1)?,
            tenants: vdrop_tenants,
        },
    })
}

fn sum_optional_u64_v1<I>(values: I) -> Option<u64>
where
    I: IntoIterator<Item = Option<u64>>,
{
    let mut saw_any = false;
    let mut total = 0u64;
    for value in values {
        if let Some(value) = value {
            saw_any = true;
            total = total.saturating_add(value);
        }
    }
    if saw_any { Some(total) } else { None }
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

fn diff_signed_u64_v1(current: Option<u64>, previous: Option<u64>) -> Option<i64> {
    match (current, previous) {
        (Some(current), Some(previous)) => {
            if current >= previous {
                Some((current - previous) as i64)
            } else {
                Some(-((previous - current) as i64))
            }
        }
        _ => None,
    }
}

fn diff_nonnegative_u64_v1(current: Option<u64>, previous: Option<u64>) -> Option<u64> {
    match (current, previous) {
        (Some(current), Some(previous)) if current >= previous => Some(current - previous),
        _ => None,
    }
}

fn rate_per_second_v1(current: Option<u64>, previous: Option<u64>, interval_s: Option<u64>) -> Option<f64> {
    match (diff_nonnegative_u64_v1(current, previous), interval_s) {
        (Some(delta), Some(interval)) if interval > 0 => Some((delta as f64) / (interval as f64)),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct RecoveryCounterSnapshotViewV1 {
    ts: Option<u64>,
    spool_writes_total: Option<u64>,
    spool_replayed_total: Option<u64>,
    spool_replay_fail_total: Option<u64>,
    automated_replay_attempts_total: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct RecoveryCounterRateViewV1 {
    interval_s: Option<u64>,
    spool_write_rate_per_s: Option<f64>,
    spool_replayed_rate_per_s: Option<f64>,
    spool_replay_fail_rate_per_s: Option<f64>,
    automated_replay_attempt_rate_per_s: Option<f64>,
}

fn recovery_counter_rate_view_v1(
    start: RecoveryCounterSnapshotViewV1,
    end: RecoveryCounterSnapshotViewV1,
) -> RecoveryCounterRateViewV1 {
    let interval_s = diff_nonnegative_u64_v1(end.ts, start.ts);
    RecoveryCounterRateViewV1 {
        interval_s,
        spool_write_rate_per_s: rate_per_second_v1(
            end.spool_writes_total,
            start.spool_writes_total,
            interval_s,
        ),
        spool_replayed_rate_per_s: rate_per_second_v1(
            end.spool_replayed_total,
            start.spool_replayed_total,
            interval_s,
        ),
        spool_replay_fail_rate_per_s: rate_per_second_v1(
            end.spool_replay_fail_total,
            start.spool_replay_fail_total,
            interval_s,
        ),
        automated_replay_attempt_rate_per_s: rate_per_second_v1(
            end.automated_replay_attempts_total,
            start.automated_replay_attempts_total,
            interval_s,
        ),
    }
}

fn recovery_trend_direction_v1(backlog_files_trend_delta: Option<i64>, backlog_bytes_trend_delta: Option<i64>) -> &'static str {
    match (backlog_files_trend_delta, backlog_bytes_trend_delta) {
        (Some(files_delta), Some(bytes_delta)) => {
            if files_delta > 0 || bytes_delta > 0 {
                "up"
            } else if files_delta < 0 || bytes_delta < 0 {
                "down"
            } else {
                "flat"
            }
        }
        _ => "unknown",
    }
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
    out.push_str(&format!("vdrop.enabled: {}\n", snapshot.vdrop.enabled));
    out.push_str(&format!("vdrop.device_enabled: {}\n", snapshot.vdrop.device_enabled));
    out.push_str(&format!("vdrop.tenant_enabled: {}\n", snapshot.vdrop.tenant_enabled));
    out.push_str(&format!("vdrop.source_stream_enabled: {}\n", snapshot.vdrop.source_stream_enabled));
    out.push_str(&format!("vdrop.min_expected_windows_missed: {}\n", snapshot.vdrop.min_expected_windows_missed));
    out.push_str(&format!("vdrop.min_mature_windows: {}\n", format_option_u64_v1(snapshot.vdrop.min_mature_windows)));
    out.push_str(&format!("vdrop.min_expected_lines: {}\n", format_option_u64_v1(snapshot.vdrop.min_expected_lines)));
    out.push_str(&format!("vdrop.tracked_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.tracked_subjects)));
    out.push_str(&format!("vdrop.open_silence_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.open_silence_subjects)));
    out.push_str(&format!("vdrop.open_drop_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.open_drop_subjects)));
    out.push_str(&format!("vdrop.evaluated_subjects_total: {}\n", snapshot.vdrop.evaluated_subjects_total));
    out.push_str(&format!("vdrop.candidates_total: {}\n", snapshot.vdrop.candidates_total));
    out.push_str(&format!("vdrop.suppressed_candidates_total: {}\n", snapshot.vdrop.suppressed_candidates_total));
    out.push_str(&format!("vdrop.alerts_emitted_total: {}\n", snapshot.vdrop.alerts_emitted_total));
    out.push_str(&format!("vdrop.last_evaluation_ts: {}\n", format_option_u64_v1(snapshot.vdrop.last_evaluation_ts)));
    out.push_str(&format!("vdrop.source_stream_tracked_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.source_stream_tracked_subjects)));
    out.push_str(&format!("vdrop.source_stream_open_silence_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.source_stream_open_silence_subjects)));
    out.push_str(&format!("vdrop.source_stream_open_drop_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.source_stream_open_drop_subjects)));
    out.push_str(&format!("vdrop.source_stream_evaluated_subjects_total: {}\n", snapshot.vdrop.source_stream_evaluated_subjects_total));
    out.push_str(&format!("vdrop.source_stream_candidates_total: {}\n", snapshot.vdrop.source_stream_candidates_total));
    out.push_str(&format!("vdrop.source_stream_suppressed_candidates_total: {}\n", snapshot.vdrop.source_stream_suppressed_candidates_total));
    out.push_str(&format!("vdrop.source_stream_alerts_emitted_total: {}\n", snapshot.vdrop.source_stream_alerts_emitted_total));
    out.push_str(&format!("vdrop.source_stream_last_evaluation_ts: {}\n", format_option_u64_v1(snapshot.vdrop.source_stream_last_evaluation_ts)));
    out.push_str(&format!("vdrop.tenants: {}\n", snapshot.vdrop.tenants.len()));
    for (idx, tenant) in snapshot.vdrop.tenants.iter().enumerate() {
        out.push_str(&format!("vdrop.tenant[{}].tenant_id: {}\n", idx, tenant.tenant_id));
        out.push_str(&format!("vdrop.tenant[{}].tracked_subjects: {}\n", idx, format_option_u64_v1(tenant.tracked_subjects)));
        out.push_str(&format!("vdrop.tenant[{}].open_silence_subjects: {}\n", idx, format_option_u64_v1(tenant.open_silence_subjects)));
        out.push_str(&format!("vdrop.tenant[{}].open_drop_subjects: {}\n", idx, format_option_u64_v1(tenant.open_drop_subjects)));
        out.push_str(&format!("vdrop.tenant[{}].evaluated_subjects_total: {}\n", idx, tenant.evaluated_subjects_total));
        out.push_str(&format!("vdrop.tenant[{}].candidates_total: {}\n", idx, tenant.candidates_total));
        out.push_str(&format!("vdrop.tenant[{}].suppressed_candidates_total: {}\n", idx, tenant.suppressed_candidates_total));
        out.push_str(&format!("vdrop.tenant[{}].alerts_emitted_total: {}\n", idx, tenant.alerts_emitted_total));
        out.push_str(&format!("vdrop.tenant[{}].last_evaluation_ts: {}\n", idx, format_option_u64_v1(tenant.last_evaluation_ts)));
        out.push_str(&format!("vdrop.tenant[{}].source_stream_tracked_subjects: {}\n", idx, format_option_u64_v1(tenant.source_stream_tracked_subjects)));
        out.push_str(&format!("vdrop.tenant[{}].source_stream_open_silence_subjects: {}\n", idx, format_option_u64_v1(tenant.source_stream_open_silence_subjects)));
        out.push_str(&format!("vdrop.tenant[{}].source_stream_open_drop_subjects: {}\n", idx, format_option_u64_v1(tenant.source_stream_open_drop_subjects)));
        out.push_str(&format!("vdrop.tenant[{}].source_stream_evaluated_subjects_total: {}\n", idx, tenant.source_stream_evaluated_subjects_total));
        out.push_str(&format!("vdrop.tenant[{}].source_stream_candidates_total: {}\n", idx, tenant.source_stream_candidates_total));
        out.push_str(&format!("vdrop.tenant[{}].source_stream_suppressed_candidates_total: {}\n", idx, tenant.source_stream_suppressed_candidates_total));
        out.push_str(&format!("vdrop.tenant[{}].source_stream_alerts_emitted_total: {}\n", idx, tenant.source_stream_alerts_emitted_total));
        out.push_str(&format!("vdrop.tenant[{}].source_stream_last_evaluation_ts: {}\n", idx, format_option_u64_v1(tenant.source_stream_last_evaluation_ts)));
    }
    out.push_str(&format!("recovery.automated_replay_max_files_per_pass: {}\n", snapshot.recovery.automated_replay_max_files_per_pass));
    out.push_str(&format!("recovery.automated_replay_interval_s: {}\n", snapshot.recovery.automated_replay_interval_s));
    out.push_str(&format!("recovery.spool_max_mb: {}\n", snapshot.recovery.spool_max_mb));
    out.push_str(&format!("recovery.spool_backlog_files: {}\n", snapshot.recovery.spool_backlog_files));
    out.push_str(&format!("recovery.spool_backlog_bytes: {}\n", snapshot.recovery.spool_backlog_bytes));
    out.push_str(&format!("recovery.spool_oldest_file_ts: {}\n", format_option_u64_v1(snapshot.recovery.spool_oldest_file_ts)));
    out.push_str(&format!("recovery.spool_oldest_age_s: {}\n", format_option_u64_v1(snapshot.recovery.spool_oldest_age_s)));
    out.push_str(&format!("recovery.stale_backlog: {}\n", snapshot.recovery.stale_backlog));
    out.push_str(&format!("recovery.stale_backlog_tenants: {}\n", snapshot.recovery.stale_backlog_tenants));
    out.push_str(&format!("recovery.spool_backlog_tenants: {}\n", snapshot.recovery.spool_backlog_tenants.len()));
    for (idx, tenant) in snapshot.recovery.spool_backlog_tenants.iter().enumerate() {
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].tenant_id: {}\n", idx, tenant.tenant_id));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].files: {}\n", idx, tenant.files));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].bytes: {}\n", idx, tenant.bytes));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].oldest_file_ts: {}\n", idx, format_option_u64_v1(tenant.oldest_file_ts)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].oldest_age_s: {}\n", idx, format_option_u64_v1(tenant.oldest_age_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].stale: {}\n", idx, tenant.stale));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].previous_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.previous_snapshot_ts)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].last_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.last_snapshot_ts)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].snapshot_interval_s: {}\n", idx, format_option_u64_v1(tenant.snapshot_interval_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].backlog_files_trend_delta: {}\n", idx, format_option_i64_v1(tenant.backlog_files_trend_delta)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].backlog_bytes_trend_delta: {}\n", idx, format_option_i64_v1(tenant.backlog_bytes_trend_delta)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].backlog_trend_direction: {}\n", idx, tenant.backlog_trend_direction));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].previous_counter_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.previous_counter_snapshot_ts)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].last_counter_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.last_counter_snapshot_ts)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].counter_snapshot_interval_s: {}\n", idx, format_option_u64_v1(tenant.counter_snapshot_interval_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].history_start_counter_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.history_start_counter_snapshot_ts)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].history_counter_snapshot_interval_s: {}\n", idx, format_option_u64_v1(tenant.history_counter_snapshot_interval_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].history_spool_write_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.history_spool_write_rate_per_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].history_spool_replayed_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.history_spool_replayed_rate_per_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].history_spool_replay_fail_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.history_spool_replay_fail_rate_per_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].history_automated_replay_attempt_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.history_automated_replay_attempt_rate_per_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].spool_write_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.spool_write_rate_per_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].spool_replayed_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.spool_replayed_rate_per_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].spool_replay_fail_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.spool_replay_fail_rate_per_s)));
        out.push_str(&format!("recovery.spool_backlog_tenant[{}].automated_replay_attempt_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.automated_replay_attempt_rate_per_s)));
    }
    out.push_str(&format!("recovery.spool_writes_total: {}\n", snapshot.recovery.spool_writes_total));
    out.push_str(&format!("recovery.spool_replayed_total: {}\n", snapshot.recovery.spool_replayed_total));
    out.push_str(&format!("recovery.spool_replay_fail_total: {}\n", snapshot.recovery.spool_replay_fail_total));
    out.push_str(&format!("recovery.spool_drop_total: {}\n", snapshot.recovery.spool_drop_total));
    out.push_str(&format!("recovery.automated_replay_attempts_total: {}\n", snapshot.recovery.automated_replay_attempts_total));
    out.push_str(&format!("recovery.last_automated_replay_attempt_ts: {}\n", format_option_u64_v1(snapshot.recovery.last_automated_replay_attempt_ts)));
    out.push_str(&format!("recovery.last_automated_replay_replayed: {}\n", format_option_u64_v1(snapshot.recovery.last_automated_replay_replayed)));
    out.push_str(&format!("recovery.last_automated_replay_failed: {}\n", format_option_u64_v1(snapshot.recovery.last_automated_replay_failed)));
    out.push_str(&format!("recovery.previous_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.previous_snapshot_ts)));
    out.push_str(&format!("recovery.last_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.last_snapshot_ts)));
    out.push_str(&format!("recovery.snapshot_interval_s: {}\n", format_option_u64_v1(snapshot.recovery.snapshot_interval_s)));
    out.push_str(&format!("recovery.backlog_files_trend_delta: {}\n", format_option_i64_v1(snapshot.recovery.backlog_files_trend_delta)));
    out.push_str(&format!("recovery.backlog_bytes_trend_delta: {}\n", format_option_i64_v1(snapshot.recovery.backlog_bytes_trend_delta)));
    out.push_str(&format!("recovery.backlog_trend_direction: {}\n", snapshot.recovery.backlog_trend_direction));
    out.push_str(&format!("recovery.previous_counter_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.previous_counter_snapshot_ts)));
    out.push_str(&format!("recovery.last_counter_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.last_counter_snapshot_ts)));
    out.push_str(&format!("recovery.counter_snapshot_interval_s: {}\n", format_option_u64_v1(snapshot.recovery.counter_snapshot_interval_s)));
    out.push_str(&format!("recovery.history_start_counter_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.history_start_counter_snapshot_ts)));
    out.push_str(&format!("recovery.history_counter_snapshot_interval_s: {}\n", format_option_u64_v1(snapshot.recovery.history_counter_snapshot_interval_s)));
    out.push_str(&format!("recovery.history_spool_write_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.history_spool_write_rate_per_s)));
    out.push_str(&format!("recovery.history_spool_replayed_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.history_spool_replayed_rate_per_s)));
    out.push_str(&format!("recovery.history_spool_replay_fail_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.history_spool_replay_fail_rate_per_s)));
    out.push_str(&format!("recovery.history_automated_replay_attempt_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.history_automated_replay_attempt_rate_per_s)));
    out.push_str(&format!("recovery.spool_write_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.spool_write_rate_per_s)));
    out.push_str(&format!("recovery.spool_replayed_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.spool_replayed_rate_per_s)));
    out.push_str(&format!("recovery.spool_replay_fail_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.spool_replay_fail_rate_per_s)));
    out.push_str(&format!("recovery.automated_replay_attempt_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.automated_replay_attempt_rate_per_s)));
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
    out.push_str("# TYPE sparx_vdrop_enabled gauge\n");
    out.push_str(&format!("sparx_vdrop_enabled {}\n", if snapshot.vdrop.enabled { 1 } else { 0 }));
    out.push_str("# TYPE sparx_vdrop_device_enabled gauge\n");
    out.push_str(&format!("sparx_vdrop_device_enabled {}\n", if snapshot.vdrop.device_enabled { 1 } else { 0 }));
    out.push_str("# TYPE sparx_vdrop_tenant_enabled gauge\n");
    out.push_str(&format!("sparx_vdrop_tenant_enabled {}\n", if snapshot.vdrop.tenant_enabled { 1 } else { 0 }));
    out.push_str("# TYPE sparx_vdrop_source_stream_enabled gauge\n");
    out.push_str(&format!("sparx_vdrop_source_stream_enabled {}\n", if snapshot.vdrop.source_stream_enabled { 1 } else { 0 }));
    out.push_str("# TYPE sparx_vdrop_min_expected_windows_missed gauge\n");
    out.push_str(&format!("sparx_vdrop_min_expected_windows_missed {}\n", snapshot.vdrop.min_expected_windows_missed));
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_min_mature_windows", snapshot.vdrop.min_mature_windows);
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_min_expected_lines", snapshot.vdrop.min_expected_lines);
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_tracked_subjects", snapshot.vdrop.tracked_subjects);
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_open_silence_subjects", snapshot.vdrop.open_silence_subjects);
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_open_drop_subjects", snapshot.vdrop.open_drop_subjects);
    out.push_str("# TYPE sparx_vdrop_evaluated_subjects_total counter\n");
    out.push_str(&format!("sparx_vdrop_evaluated_subjects_total {}\n", snapshot.vdrop.evaluated_subjects_total));
    out.push_str("# TYPE sparx_vdrop_candidates_total counter\n");
    out.push_str(&format!("sparx_vdrop_candidates_total {}\n", snapshot.vdrop.candidates_total));
    out.push_str("# TYPE sparx_vdrop_suppressed_candidates_total counter\n");
    out.push_str(&format!("sparx_vdrop_suppressed_candidates_total {}\n", snapshot.vdrop.suppressed_candidates_total));
    out.push_str("# TYPE sparx_vdrop_alerts_emitted_total counter\n");
    out.push_str(&format!("sparx_vdrop_alerts_emitted_total {}\n", snapshot.vdrop.alerts_emitted_total));
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_last_evaluation_ts", snapshot.vdrop.last_evaluation_ts);
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_source_stream_tracked_subjects", snapshot.vdrop.source_stream_tracked_subjects);
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_source_stream_open_silence_subjects", snapshot.vdrop.source_stream_open_silence_subjects);
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_source_stream_open_drop_subjects", snapshot.vdrop.source_stream_open_drop_subjects);
    out.push_str("# TYPE sparx_vdrop_source_stream_evaluated_subjects_total counter\n");
    out.push_str(&format!("sparx_vdrop_source_stream_evaluated_subjects_total {}\n", snapshot.vdrop.source_stream_evaluated_subjects_total));
    out.push_str("# TYPE sparx_vdrop_source_stream_candidates_total counter\n");
    out.push_str(&format!("sparx_vdrop_source_stream_candidates_total {}\n", snapshot.vdrop.source_stream_candidates_total));
    out.push_str("# TYPE sparx_vdrop_source_stream_suppressed_candidates_total counter\n");
    out.push_str(&format!("sparx_vdrop_source_stream_suppressed_candidates_total {}\n", snapshot.vdrop.source_stream_suppressed_candidates_total));
    out.push_str("# TYPE sparx_vdrop_source_stream_alerts_emitted_total counter\n");
    out.push_str(&format!("sparx_vdrop_source_stream_alerts_emitted_total {}\n", snapshot.vdrop.source_stream_alerts_emitted_total));
    append_optional_u64_metric_v1(&mut out, "sparx_vdrop_source_stream_last_evaluation_ts", snapshot.vdrop.source_stream_last_evaluation_ts);
    out.push_str("# TYPE sparx_vdrop_tracked_subjects_by_tenant gauge\n");
    for tenant in &snapshot.vdrop.tenants {
        if let Some(value) = tenant.tracked_subjects {
            out.push_str(&format!(
                "sparx_vdrop_tracked_subjects_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_vdrop_open_silence_subjects_by_tenant gauge\n");
    for tenant in &snapshot.vdrop.tenants {
        if let Some(value) = tenant.open_silence_subjects {
            out.push_str(&format!(
                "sparx_vdrop_open_silence_subjects_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_vdrop_open_drop_subjects_by_tenant gauge\n");
    for tenant in &snapshot.vdrop.tenants {
        if let Some(value) = tenant.open_drop_subjects {
            out.push_str(&format!(
                "sparx_vdrop_open_drop_subjects_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_vdrop_evaluated_subjects_total_by_tenant counter\n");
    for tenant in &snapshot.vdrop.tenants {
        out.push_str(&format!(
            "sparx_vdrop_evaluated_subjects_total_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.evaluated_subjects_total
        ));
    }
    out.push_str("# TYPE sparx_vdrop_candidates_total_by_tenant counter\n");
    for tenant in &snapshot.vdrop.tenants {
        out.push_str(&format!(
            "sparx_vdrop_candidates_total_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.candidates_total
        ));
    }
    out.push_str("# TYPE sparx_vdrop_suppressed_candidates_total_by_tenant counter\n");
    for tenant in &snapshot.vdrop.tenants {
        out.push_str(&format!(
            "sparx_vdrop_suppressed_candidates_total_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.suppressed_candidates_total
        ));
    }
    out.push_str("# TYPE sparx_vdrop_alerts_emitted_total_by_tenant counter\n");
    for tenant in &snapshot.vdrop.tenants {
        out.push_str(&format!(
            "sparx_vdrop_alerts_emitted_total_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.alerts_emitted_total
        ));
    }
    out.push_str("# TYPE sparx_vdrop_last_evaluation_ts_by_tenant gauge\n");
    for tenant in &snapshot.vdrop.tenants {
        if let Some(value) = tenant.last_evaluation_ts {
            out.push_str(&format!(
                "sparx_vdrop_last_evaluation_ts_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_vdrop_source_stream_tracked_subjects_by_tenant gauge\n");
    for tenant in &snapshot.vdrop.tenants {
        if let Some(value) = tenant.source_stream_tracked_subjects {
            out.push_str(&format!(
                "sparx_vdrop_source_stream_tracked_subjects_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_vdrop_source_stream_open_silence_subjects_by_tenant gauge\n");
    for tenant in &snapshot.vdrop.tenants {
        if let Some(value) = tenant.source_stream_open_silence_subjects {
            out.push_str(&format!(
                "sparx_vdrop_source_stream_open_silence_subjects_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_vdrop_source_stream_open_drop_subjects_by_tenant gauge\n");
    for tenant in &snapshot.vdrop.tenants {
        if let Some(value) = tenant.source_stream_open_drop_subjects {
            out.push_str(&format!(
                "sparx_vdrop_source_stream_open_drop_subjects_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_vdrop_source_stream_evaluated_subjects_total_by_tenant counter\n");
    for tenant in &snapshot.vdrop.tenants {
        out.push_str(&format!(
            "sparx_vdrop_source_stream_evaluated_subjects_total_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.source_stream_evaluated_subjects_total
        ));
    }
    out.push_str("# TYPE sparx_vdrop_source_stream_candidates_total_by_tenant counter\n");
    for tenant in &snapshot.vdrop.tenants {
        out.push_str(&format!(
            "sparx_vdrop_source_stream_candidates_total_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.source_stream_candidates_total
        ));
    }
    out.push_str("# TYPE sparx_vdrop_source_stream_suppressed_candidates_total_by_tenant counter\n");
    for tenant in &snapshot.vdrop.tenants {
        out.push_str(&format!(
            "sparx_vdrop_source_stream_suppressed_candidates_total_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.source_stream_suppressed_candidates_total
        ));
    }
    out.push_str("# TYPE sparx_vdrop_source_stream_alerts_emitted_total_by_tenant counter\n");
    for tenant in &snapshot.vdrop.tenants {
        out.push_str(&format!(
            "sparx_vdrop_source_stream_alerts_emitted_total_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.source_stream_alerts_emitted_total
        ));
    }
    out.push_str("# TYPE sparx_vdrop_source_stream_last_evaluation_ts_by_tenant gauge\n");
    for tenant in &snapshot.vdrop.tenants {
        if let Some(value) = tenant.source_stream_last_evaluation_ts {
            out.push_str(&format!(
                "sparx_vdrop_source_stream_last_evaluation_ts_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_automated_replay_max_files_per_pass gauge\n");
    out.push_str(&format!("sparx_recovery_automated_replay_max_files_per_pass {}\n", snapshot.recovery.automated_replay_max_files_per_pass));
    out.push_str("# TYPE sparx_recovery_automated_replay_interval_seconds gauge\n");
    out.push_str(&format!("sparx_recovery_automated_replay_interval_seconds {}\n", snapshot.recovery.automated_replay_interval_s));
    out.push_str("# TYPE sparx_recovery_spool_max_megabytes gauge\n");
    out.push_str(&format!("sparx_recovery_spool_max_megabytes {}\n", snapshot.recovery.spool_max_mb));
    out.push_str("# TYPE sparx_recovery_spool_backlog_files gauge\n");
    out.push_str(&format!("sparx_recovery_spool_backlog_files {}\n", snapshot.recovery.spool_backlog_files));
    out.push_str("# TYPE sparx_recovery_spool_backlog_bytes gauge\n");
    out.push_str(&format!("sparx_recovery_spool_backlog_bytes {}\n", snapshot.recovery.spool_backlog_bytes));
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_spool_oldest_file_ts", snapshot.recovery.spool_oldest_file_ts);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_spool_oldest_age_seconds", snapshot.recovery.spool_oldest_age_s);
    out.push_str("# TYPE sparx_recovery_stale_backlog gauge\n");
    out.push_str(&format!("sparx_recovery_stale_backlog {}\n", if snapshot.recovery.stale_backlog { 1 } else { 0 }));
    out.push_str("# TYPE sparx_recovery_stale_backlog_tenants gauge\n");
    out.push_str(&format!("sparx_recovery_stale_backlog_tenants {}\n", snapshot.recovery.stale_backlog_tenants));
    out.push_str("# TYPE sparx_recovery_spool_backlog_tenants gauge\n");
    out.push_str(&format!("sparx_recovery_spool_backlog_tenants {}\n", snapshot.recovery.spool_backlog_tenants.len()));
    out.push_str("# TYPE sparx_recovery_spool_backlog_files_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        out.push_str(&format!(
            "sparx_recovery_spool_backlog_files_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.files
        ));
    }
    out.push_str("# TYPE sparx_recovery_spool_backlog_bytes_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        out.push_str(&format!(
            "sparx_recovery_spool_backlog_bytes_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            tenant.bytes
        ));
    }
    out.push_str("# TYPE sparx_recovery_spool_oldest_age_seconds_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(age) = tenant.oldest_age_s {
            out.push_str(&format!(
                "sparx_recovery_spool_oldest_age_seconds_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                age
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_stale_backlog_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        out.push_str(&format!(
            "sparx_recovery_stale_backlog_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            if tenant.stale { 1 } else { 0 }
        ));
    }
    out.push_str("# TYPE sparx_recovery_previous_snapshot_ts_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.previous_snapshot_ts {
            out.push_str(&format!(
                "sparx_recovery_previous_snapshot_ts_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_last_snapshot_ts_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.last_snapshot_ts {
            out.push_str(&format!(
                "sparx_recovery_last_snapshot_ts_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_snapshot_interval_seconds_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.snapshot_interval_s {
            out.push_str(&format!(
                "sparx_recovery_snapshot_interval_seconds_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_backlog_files_trend_delta_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.backlog_files_trend_delta {
            out.push_str(&format!(
                "sparx_recovery_backlog_files_trend_delta_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_backlog_bytes_trend_delta_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.backlog_bytes_trend_delta {
            out.push_str(&format!(
                "sparx_recovery_backlog_bytes_trend_delta_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_backlog_trend_direction_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        out.push_str(&format!(
            "sparx_recovery_backlog_trend_direction_by_tenant{{tenant_id=\"{}\"}} {}\n",
            escape_prometheus_label_v1(&tenant.tenant_id),
            recovery_trend_direction_value_v1(&tenant.backlog_trend_direction)
        ));
    }
    out.push_str("# TYPE sparx_recovery_previous_counter_snapshot_ts_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.previous_counter_snapshot_ts {
            out.push_str(&format!(
                "sparx_recovery_previous_counter_snapshot_ts_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_last_counter_snapshot_ts_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.last_counter_snapshot_ts {
            out.push_str(&format!(
                "sparx_recovery_last_counter_snapshot_ts_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_counter_snapshot_interval_seconds_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.counter_snapshot_interval_s {
            out.push_str(&format!(
                "sparx_recovery_counter_snapshot_interval_seconds_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_history_start_counter_snapshot_ts_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.history_start_counter_snapshot_ts {
            out.push_str(&format!(
                "sparx_recovery_history_start_counter_snapshot_ts_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_history_counter_snapshot_interval_seconds_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.history_counter_snapshot_interval_s {
            out.push_str(&format!(
                "sparx_recovery_history_counter_snapshot_interval_seconds_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_history_spool_write_rate_per_second_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.history_spool_write_rate_per_s {
            out.push_str(&format!(
                "sparx_recovery_history_spool_write_rate_per_second_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_history_spool_replayed_rate_per_second_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.history_spool_replayed_rate_per_s {
            out.push_str(&format!(
                "sparx_recovery_history_spool_replayed_rate_per_second_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_history_spool_replay_fail_rate_per_second_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.history_spool_replay_fail_rate_per_s {
            out.push_str(&format!(
                "sparx_recovery_history_spool_replay_fail_rate_per_second_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_history_automated_replay_attempt_rate_per_second_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.history_automated_replay_attempt_rate_per_s {
            out.push_str(&format!(
                "sparx_recovery_history_automated_replay_attempt_rate_per_second_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_spool_write_rate_per_second_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.spool_write_rate_per_s {
            out.push_str(&format!(
                "sparx_recovery_spool_write_rate_per_second_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_spool_replayed_rate_per_second_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.spool_replayed_rate_per_s {
            out.push_str(&format!(
                "sparx_recovery_spool_replayed_rate_per_second_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_spool_replay_fail_rate_per_second_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.spool_replay_fail_rate_per_s {
            out.push_str(&format!(
                "sparx_recovery_spool_replay_fail_rate_per_second_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_automated_replay_attempt_rate_per_second_by_tenant gauge\n");
    for tenant in &snapshot.recovery.spool_backlog_tenants {
        if let Some(value) = tenant.automated_replay_attempt_rate_per_s {
            out.push_str(&format!(
                "sparx_recovery_automated_replay_attempt_rate_per_second_by_tenant{{tenant_id=\"{}\"}} {}\n",
                escape_prometheus_label_v1(&tenant.tenant_id),
                value
            ));
        }
    }
    out.push_str("# TYPE sparx_recovery_spool_writes_total counter\n");
    out.push_str(&format!("sparx_recovery_spool_writes_total {}\n", snapshot.recovery.spool_writes_total));
    out.push_str("# TYPE sparx_recovery_spool_replayed_total counter\n");
    out.push_str(&format!("sparx_recovery_spool_replayed_total {}\n", snapshot.recovery.spool_replayed_total));
    out.push_str("# TYPE sparx_recovery_spool_replay_fail_total counter\n");
    out.push_str(&format!("sparx_recovery_spool_replay_fail_total {}\n", snapshot.recovery.spool_replay_fail_total));
    out.push_str("# TYPE sparx_recovery_spool_drop_total counter\n");
    out.push_str(&format!("sparx_recovery_spool_drop_total {}\n", snapshot.recovery.spool_drop_total));
    out.push_str("# TYPE sparx_recovery_automated_replay_attempts_total counter\n");
    out.push_str(&format!("sparx_recovery_automated_replay_attempts_total {}\n", snapshot.recovery.automated_replay_attempts_total));
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_last_automated_replay_attempt_ts", snapshot.recovery.last_automated_replay_attempt_ts);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_last_automated_replay_replayed", snapshot.recovery.last_automated_replay_replayed);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_last_automated_replay_failed", snapshot.recovery.last_automated_replay_failed);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_previous_snapshot_ts", snapshot.recovery.previous_snapshot_ts);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_last_snapshot_ts", snapshot.recovery.last_snapshot_ts);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_snapshot_interval_seconds", snapshot.recovery.snapshot_interval_s);
    append_optional_i64_metric_v1(&mut out, "sparx_recovery_backlog_files_trend_delta", snapshot.recovery.backlog_files_trend_delta);
    append_optional_i64_metric_v1(&mut out, "sparx_recovery_backlog_bytes_trend_delta", snapshot.recovery.backlog_bytes_trend_delta);
    out.push_str("# TYPE sparx_recovery_backlog_trend_direction gauge\n");
    out.push_str(&format!("sparx_recovery_backlog_trend_direction {}\n", recovery_trend_direction_value_v1(&snapshot.recovery.backlog_trend_direction)));
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_previous_counter_snapshot_ts", snapshot.recovery.previous_counter_snapshot_ts);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_last_counter_snapshot_ts", snapshot.recovery.last_counter_snapshot_ts);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_counter_snapshot_interval_seconds", snapshot.recovery.counter_snapshot_interval_s);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_history_start_counter_snapshot_ts", snapshot.recovery.history_start_counter_snapshot_ts);
    append_optional_u64_metric_v1(&mut out, "sparx_recovery_history_counter_snapshot_interval_seconds", snapshot.recovery.history_counter_snapshot_interval_s);
    append_optional_f64_metric_v1(&mut out, "sparx_recovery_history_spool_write_rate_per_second", snapshot.recovery.history_spool_write_rate_per_s);
    append_optional_f64_metric_v1(&mut out, "sparx_recovery_history_spool_replayed_rate_per_second", snapshot.recovery.history_spool_replayed_rate_per_s);
    append_optional_f64_metric_v1(&mut out, "sparx_recovery_history_spool_replay_fail_rate_per_second", snapshot.recovery.history_spool_replay_fail_rate_per_s);
    append_optional_f64_metric_v1(&mut out, "sparx_recovery_history_automated_replay_attempt_rate_per_second", snapshot.recovery.history_automated_replay_attempt_rate_per_s);
    append_optional_f64_metric_v1(&mut out, "sparx_recovery_spool_write_rate_per_second", snapshot.recovery.spool_write_rate_per_s);
    append_optional_f64_metric_v1(&mut out, "sparx_recovery_spool_replayed_rate_per_second", snapshot.recovery.spool_replayed_rate_per_s);
    append_optional_f64_metric_v1(&mut out, "sparx_recovery_spool_replay_fail_rate_per_second", snapshot.recovery.spool_replay_fail_rate_per_s);
    append_optional_f64_metric_v1(&mut out, "sparx_recovery_automated_replay_attempt_rate_per_second", snapshot.recovery.automated_replay_attempt_rate_per_s);
    out
}

pub fn format_health_text_v1(snapshot: &StatusSnapshotV1) -> String {
    let mut out = String::new();
    out.push_str("status: ok\n");
    out.push_str(&format!("version: {}\n", snapshot.version));
    out.push_str(&format!("mode: {}\n", snapshot.mode));
    out.push_str(&format!("run_cycles_completed_total: {}\n", snapshot.metrics.run_cycles_completed_total));
    out.push_str(&format!("run_last_cycle_completed_ts: {}\n", format_option_u64_v1(snapshot.metrics.run_last_cycle_completed_ts)));
    out.push_str(&format!("spool_backlog_files: {}\n", snapshot.recovery.spool_backlog_files));
    out.push_str(&format!("spool_backlog_bytes: {}\n", snapshot.recovery.spool_backlog_bytes));
    out.push_str(&format!("spool_oldest_file_ts: {}\n", format_option_u64_v1(snapshot.recovery.spool_oldest_file_ts)));
    out.push_str(&format!("spool_oldest_age_s: {}\n", format_option_u64_v1(snapshot.recovery.spool_oldest_age_s)));
    out.push_str(&format!("stale_backlog: {}\n", snapshot.recovery.stale_backlog));
    out.push_str(&format!("stale_backlog_tenants: {}\n", snapshot.recovery.stale_backlog_tenants));
    out.push_str(&format!("spool_backlog_tenants: {}\n", snapshot.recovery.spool_backlog_tenants.len()));
    for (idx, tenant) in snapshot.recovery.spool_backlog_tenants.iter().enumerate() {
        out.push_str(&format!("spool_backlog_tenant[{}].tenant_id: {}\n", idx, tenant.tenant_id));
        out.push_str(&format!("spool_backlog_tenant[{}].files: {}\n", idx, tenant.files));
        out.push_str(&format!("spool_backlog_tenant[{}].bytes: {}\n", idx, tenant.bytes));
        out.push_str(&format!("spool_backlog_tenant[{}].oldest_file_ts: {}\n", idx, format_option_u64_v1(tenant.oldest_file_ts)));
        out.push_str(&format!("spool_backlog_tenant[{}].oldest_age_s: {}\n", idx, format_option_u64_v1(tenant.oldest_age_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].stale: {}\n", idx, tenant.stale));
        out.push_str(&format!("spool_backlog_tenant[{}].previous_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.previous_snapshot_ts)));
        out.push_str(&format!("spool_backlog_tenant[{}].last_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.last_snapshot_ts)));
        out.push_str(&format!("spool_backlog_tenant[{}].snapshot_interval_s: {}\n", idx, format_option_u64_v1(tenant.snapshot_interval_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].backlog_files_trend_delta: {}\n", idx, format_option_i64_v1(tenant.backlog_files_trend_delta)));
        out.push_str(&format!("spool_backlog_tenant[{}].backlog_bytes_trend_delta: {}\n", idx, format_option_i64_v1(tenant.backlog_bytes_trend_delta)));
        out.push_str(&format!("spool_backlog_tenant[{}].backlog_trend_direction: {}\n", idx, tenant.backlog_trend_direction));
        out.push_str(&format!("spool_backlog_tenant[{}].previous_counter_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.previous_counter_snapshot_ts)));
        out.push_str(&format!("spool_backlog_tenant[{}].last_counter_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.last_counter_snapshot_ts)));
        out.push_str(&format!("spool_backlog_tenant[{}].counter_snapshot_interval_s: {}\n", idx, format_option_u64_v1(tenant.counter_snapshot_interval_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].history_start_counter_snapshot_ts: {}\n", idx, format_option_u64_v1(tenant.history_start_counter_snapshot_ts)));
        out.push_str(&format!("spool_backlog_tenant[{}].history_counter_snapshot_interval_s: {}\n", idx, format_option_u64_v1(tenant.history_counter_snapshot_interval_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].history_spool_write_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.history_spool_write_rate_per_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].history_spool_replayed_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.history_spool_replayed_rate_per_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].history_spool_replay_fail_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.history_spool_replay_fail_rate_per_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].history_automated_replay_attempt_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.history_automated_replay_attempt_rate_per_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].spool_write_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.spool_write_rate_per_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].spool_replayed_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.spool_replayed_rate_per_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].spool_replay_fail_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.spool_replay_fail_rate_per_s)));
        out.push_str(&format!("spool_backlog_tenant[{}].automated_replay_attempt_rate_per_s: {}\n", idx, format_option_f64_v1(tenant.automated_replay_attempt_rate_per_s)));
    }
    out.push_str(&format!("spool_writes_total: {}\n", snapshot.recovery.spool_writes_total));
    out.push_str(&format!("spool_replayed_total: {}\n", snapshot.recovery.spool_replayed_total));
    out.push_str(&format!("spool_replay_fail_total: {}\n", snapshot.recovery.spool_replay_fail_total));
    out.push_str(&format!("spool_drop_total: {}\n", snapshot.recovery.spool_drop_total));
    out.push_str(&format!("automated_replay_attempts_total: {}\n", snapshot.recovery.automated_replay_attempts_total));
    out.push_str(&format!("last_automated_replay_attempt_ts: {}\n", format_option_u64_v1(snapshot.recovery.last_automated_replay_attempt_ts)));
    out.push_str(&format!("last_automated_replay_replayed: {}\n", format_option_u64_v1(snapshot.recovery.last_automated_replay_replayed)));
    out.push_str(&format!("last_automated_replay_failed: {}\n", format_option_u64_v1(snapshot.recovery.last_automated_replay_failed)));
    out.push_str(&format!("previous_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.previous_snapshot_ts)));
    out.push_str(&format!("last_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.last_snapshot_ts)));
    out.push_str(&format!("snapshot_interval_s: {}\n", format_option_u64_v1(snapshot.recovery.snapshot_interval_s)));
    out.push_str(&format!("backlog_files_trend_delta: {}\n", format_option_i64_v1(snapshot.recovery.backlog_files_trend_delta)));
    out.push_str(&format!("backlog_bytes_trend_delta: {}\n", format_option_i64_v1(snapshot.recovery.backlog_bytes_trend_delta)));
    out.push_str(&format!("backlog_trend_direction: {}\n", snapshot.recovery.backlog_trend_direction));
    out.push_str(&format!("previous_counter_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.previous_counter_snapshot_ts)));
    out.push_str(&format!("last_counter_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.last_counter_snapshot_ts)));
    out.push_str(&format!("counter_snapshot_interval_s: {}\n", format_option_u64_v1(snapshot.recovery.counter_snapshot_interval_s)));
    out.push_str(&format!("history_start_counter_snapshot_ts: {}\n", format_option_u64_v1(snapshot.recovery.history_start_counter_snapshot_ts)));
    out.push_str(&format!("history_counter_snapshot_interval_s: {}\n", format_option_u64_v1(snapshot.recovery.history_counter_snapshot_interval_s)));
    out.push_str(&format!("history_spool_write_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.history_spool_write_rate_per_s)));
    out.push_str(&format!("history_spool_replayed_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.history_spool_replayed_rate_per_s)));
    out.push_str(&format!("history_spool_replay_fail_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.history_spool_replay_fail_rate_per_s)));
    out.push_str(&format!("history_automated_replay_attempt_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.history_automated_replay_attempt_rate_per_s)));
    out.push_str(&format!("spool_write_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.spool_write_rate_per_s)));
    out.push_str(&format!("spool_replayed_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.spool_replayed_rate_per_s)));
    out.push_str(&format!("spool_replay_fail_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.spool_replay_fail_rate_per_s)));
    out.push_str(&format!("automated_replay_attempt_rate_per_s: {}\n", format_option_f64_v1(snapshot.recovery.automated_replay_attempt_rate_per_s)));
    out.push_str(&format!("vdrop_enabled: {}\n", snapshot.vdrop.enabled));
    out.push_str(&format!("vdrop_device_enabled: {}\n", snapshot.vdrop.device_enabled));
    out.push_str(&format!("vdrop_tenant_enabled: {}\n", snapshot.vdrop.tenant_enabled));
    out.push_str(&format!("vdrop_source_stream_enabled: {}\n", snapshot.vdrop.source_stream_enabled));
    out.push_str(&format!("vdrop_tracked_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.tracked_subjects)));
    out.push_str(&format!("vdrop_open_silence_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.open_silence_subjects)));
    out.push_str(&format!("vdrop_open_drop_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.open_drop_subjects)));
    out.push_str(&format!("vdrop_evaluated_subjects_total: {}\n", snapshot.vdrop.evaluated_subjects_total));
    out.push_str(&format!("vdrop_candidates_total: {}\n", snapshot.vdrop.candidates_total));
    out.push_str(&format!("vdrop_suppressed_candidates_total: {}\n", snapshot.vdrop.suppressed_candidates_total));
    out.push_str(&format!("vdrop_alerts_emitted_total: {}\n", snapshot.vdrop.alerts_emitted_total));
    out.push_str(&format!("vdrop_last_evaluation_ts: {}\n", format_option_u64_v1(snapshot.vdrop.last_evaluation_ts)));
    out.push_str(&format!("vdrop_source_stream_tracked_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.source_stream_tracked_subjects)));
    out.push_str(&format!("vdrop_source_stream_open_silence_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.source_stream_open_silence_subjects)));
    out.push_str(&format!("vdrop_source_stream_open_drop_subjects: {}\n", format_option_u64_v1(snapshot.vdrop.source_stream_open_drop_subjects)));
    out.push_str(&format!("vdrop_source_stream_evaluated_subjects_total: {}\n", snapshot.vdrop.source_stream_evaluated_subjects_total));
    out.push_str(&format!("vdrop_source_stream_candidates_total: {}\n", snapshot.vdrop.source_stream_candidates_total));
    out.push_str(&format!("vdrop_source_stream_suppressed_candidates_total: {}\n", snapshot.vdrop.source_stream_suppressed_candidates_total));
    out.push_str(&format!("vdrop_source_stream_alerts_emitted_total: {}\n", snapshot.vdrop.source_stream_alerts_emitted_total));
    out.push_str(&format!("vdrop_source_stream_last_evaluation_ts: {}\n", format_option_u64_v1(snapshot.vdrop.source_stream_last_evaluation_ts)));
    out.push_str(&format!("automated_replay_max_files_per_pass: {}\n", snapshot.recovery.automated_replay_max_files_per_pass));
    out.push_str(&format!("automated_replay_interval_s: {}\n", snapshot.recovery.automated_replay_interval_s));
    out.push_str(&format!("spool_max_mb: {}\n", snapshot.recovery.spool_max_mb));
    out
}

fn append_optional_f64_metric_v1(out: &mut String, name: &str, value: Option<f64>) {
    if let Some(value) = value {
        out.push_str("# TYPE ");
        out.push_str(name);
        out.push_str(" gauge\n");
        out.push_str(&format!("{} {:.6}\n", name, value));
    }
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

fn stale_backlog_v1(oldest_age_s: Option<u64>, replay_interval_s: u32) -> bool {
    match oldest_age_s {
        Some(age) => age >= u64::from(replay_interval_s),
        None => false,
    }
}

fn recovery_trend_direction_value_v1(direction: &str) -> i8 {
    match direction {
        "up" => 1,
        "down" => -1,
        "flat" => 0,
        _ => 0,
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

fn format_option_f64_v1(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{:.6}", v),
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
