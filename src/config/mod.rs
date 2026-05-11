// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Config types for sparx.
// See: contracts/28_config_schema_v0_1.md
//
// ConfigV1 is the final effective configuration after parse, merge, defaults,
// environment overrides, CLI overrides, and validation.
//
// ASCII-only.

use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct ConfigV1 {
    pub sparx: SparxSectionV1,
    pub ingest: IngestSectionV1,
    pub features: FeaturesSectionV1,
    pub baseline: BaselineSectionV1,
    pub scoring: ScoringSectionV1,
    pub caps: CapsSectionV1,
    pub storage: StorageSectionV1,
    pub output: OutputSectionV1,
    pub metrics: MetricsSectionV1,
    pub vdrop: VDropSectionV1,
}

#[derive(Clone, Debug)]
pub struct SparxSectionV1 {
    pub data_root: String,
    pub tenant_root: String,
    pub global_db_path: String,
    pub tenant_db_root: String,
    pub alert_out_root: String,
    pub pid_file: Option<String>,
    pub mode: String,       // "daemon" | "oneshot"
    pub log_level: String,  // "error".."trace"
    pub log_format: String, // "text" | "json"
}

#[derive(Clone, Debug)]
pub struct IngestSectionV1 {
    pub window_size_s: u32,
    pub max_emit_latency_s: u32,
    pub poll_interval_ms: u32,
    pub max_open_files: u32,
    pub follow_symlinks: bool,
    pub read_chunk_bytes: u32,
    pub gzip_enabled: bool,
    pub gzip_suffixes: Vec<String>,
    pub prefer_plain_when_both: bool,
    pub max_line_len: u32,
    pub max_tokens_per_line: u32,
    pub max_kv_per_line: u32,
    pub max_words_from_quoted_value: u32,
}

#[derive(Clone, Debug)]
pub struct FeaturesSectionV1 {
    pub dict_enabled: bool,
    pub dict_max_entries: u32,
    pub hash_space_bits: u8,     // reserved continuity field in v0.1
    pub dict_gc_interval_s: u32, // reserved continuity field in v0.1
}

#[derive(Clone, Debug)]
pub struct BaselineSectionV1 {
    pub baseline_days: u32,
    pub baseline_min_days: u32,
    pub df_bucket_count: u32,
    pub df_ring_slots: u32,
    pub df_buckets_per_slot: u32,
}

#[derive(Clone, Debug)]
pub struct ScoringSectionV1 {
    pub outlier_threshold: f32,
    pub noise_threshold: f32,
    pub cold_start_days: u32, // active scoring maturity floor for bucket baselines
    pub min_lines_per_window: u32, // active alert suppression floor for low-volume windows
}

#[derive(Clone, Debug)]
pub struct CapsSectionV1 {
    pub max_features_per_window: u32,
    pub max_word_features_per_window: u32,
    pub max_shape_features_per_window: u32,
    pub max_syslog_features_per_window: u32,
    pub max_srcips: u32,
    pub max_dstips: u32,
    pub max_userids: u32,
    pub max_domains: u32,
    pub max_hosts: u32,
}

#[derive(Clone, Debug)]
pub struct StorageSectionV1 {
    pub global_db_open_files: i32,      // reserved continuity field in v0.1
    pub global_db_write_buffer_mb: u32, // reserved continuity field in v0.1
    pub tenant_db_open_files: i32,      // reserved continuity field in v0.1
    pub tenant_db_write_buffer_mb: u32, // reserved continuity field in v0.1
    pub tenant_db_max_background_jobs: i32, // reserved continuity field in v0.1
    pub tenant_db_max_open: u32,        // active tenant-handle lifecycle control
    pub tenant_db_idle_close_s: u32,    // active tenant-handle lifecycle control
}

#[derive(Clone, Debug)]
pub struct OutputSectionV1 {
    pub sink: String, // "jsonl" | "stdout"
    pub jsonl_rotate_mb: u32,
    pub jsonl_flush_interval_s: u32,
    pub include_debug_fields: bool,
    pub automated_replay_max_files_per_pass: u32, // active bounded deterministic automated replay pass size
    pub automated_replay_interval_s: u32, // active minimum seconds between daemon replay attempts
    pub spool_max_mb: u32, // active deterministic spool cap for helper-backed jsonl recovery
}

#[derive(Clone, Debug)]
pub struct MetricsSectionV1 {
    pub prometheus_enabled: bool, // serves /metrics during run when enabled
    pub prometheus_bind: String,  // bind address for the Prometheus text endpoint
    pub health_enabled: bool,     // serves /healthz during run when enabled
    pub health_bind: String,      // bind address for the health endpoint
}

#[derive(Clone, Debug)]
pub struct VDropSectionV1 {
    pub enabled: bool,
    pub device_enabled: bool,
    pub tenant_enabled: bool,
    pub source_stream_enabled: bool,
    pub min_expected_windows_missed: u32,
    pub min_mature_windows: Option<u64>,
    pub min_expected_lines: Option<u64>,
}

// Overrides provided by CLI flags (highest precedence).
#[derive(Clone, Debug, Default)]
pub struct CliOverridesV1 {
    pub config_path: Option<String>,
    pub watch_root: Option<String>, // overrides tenant_root
    pub state_root: Option<String>, // overrides data_root (and derived paths)
    pub log_level: Option<String>,
    pub log_format: Option<String>,
}

// Parseable config file model (all fields optional).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlRootV1 {
    pub sparx: Option<TomlSparxV1>,
    pub ingest: Option<TomlIngestV1>,
    pub features: Option<TomlFeaturesV1>,
    pub baseline: Option<TomlBaselineV1>,
    pub scoring: Option<TomlScoringV1>,
    pub caps: Option<TomlCapsV1>,
    pub storage: Option<TomlStorageV1>,
    pub output: Option<TomlOutputV1>,
    pub metrics: Option<TomlMetricsV1>,
    pub vdrop: Option<TomlVDropV1>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlSparxV1 {
    pub data_root: Option<String>,
    pub tenant_root: Option<String>,
    pub global_db_path: Option<String>,
    pub tenant_db_root: Option<String>,
    pub alert_out_root: Option<String>,
    pub pid_file: Option<String>,
    pub mode: Option<String>,
    pub log_level: Option<String>,
    pub log_format: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlIngestV1 {
    pub window_size_s: Option<u32>,
    pub max_emit_latency_s: Option<u32>,
    pub poll_interval_ms: Option<u32>,
    pub max_open_files: Option<u32>,
    pub follow_symlinks: Option<bool>,
    pub read_chunk_bytes: Option<u32>,
    pub gzip_enabled: Option<bool>,
    pub gzip_suffixes: Option<Vec<String>>,
    pub prefer_plain_when_both: Option<bool>,
    pub max_line_len: Option<u32>,
    pub max_tokens_per_line: Option<u32>,
    pub max_kv_per_line: Option<u32>,
    pub max_words_from_quoted_value: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlFeaturesV1 {
    pub dict_enabled: Option<bool>,
    pub dict_max_entries: Option<u32>,
    pub hash_space_bits: Option<u8>,
    pub dict_gc_interval_s: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlBaselineV1 {
    pub baseline_days: Option<u32>,
    pub baseline_min_days: Option<u32>,
    pub df_bucket_count: Option<u32>,
    pub df_ring_slots: Option<u32>,
    pub df_buckets_per_slot: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlScoringV1 {
    pub outlier_threshold: Option<f32>,
    pub noise_threshold: Option<f32>,
    pub cold_start_days: Option<u32>,
    pub min_lines_per_window: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlCapsV1 {
    pub max_features_per_window: Option<u32>,
    pub max_word_features_per_window: Option<u32>,
    pub max_shape_features_per_window: Option<u32>,
    pub max_syslog_features_per_window: Option<u32>,
    pub max_srcips: Option<u32>,
    pub max_dstips: Option<u32>,
    pub max_userids: Option<u32>,
    pub max_domains: Option<u32>,
    pub max_hosts: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlStorageV1 {
    pub global_db_open_files: Option<i32>,
    pub global_db_write_buffer_mb: Option<u32>,
    pub tenant_db_open_files: Option<i32>,
    pub tenant_db_write_buffer_mb: Option<u32>,
    pub tenant_db_max_background_jobs: Option<i32>,
    pub tenant_db_max_open: Option<u32>,
    pub tenant_db_idle_close_s: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlOutputV1 {
    pub sink: Option<String>,
    pub jsonl_rotate_mb: Option<u32>,
    pub jsonl_flush_interval_s: Option<u32>,
    pub include_debug_fields: Option<bool>,
    pub automated_replay_max_files_per_pass: Option<u32>,
    pub automated_replay_interval_s: Option<u32>,
    pub spool_max_mb: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlMetricsV1 {
    pub prometheus_enabled: Option<bool>,
    pub prometheus_bind: Option<String>,
    pub health_enabled: Option<bool>,
    pub health_bind: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct TomlVDropV1 {
    pub enabled: Option<bool>,
    pub device_enabled: Option<bool>,
    pub tenant_enabled: Option<bool>,
    pub source_stream_enabled: Option<bool>,
    pub min_expected_windows_missed: Option<u32>,
    pub min_mature_windows: Option<u64>,
    pub min_expected_lines: Option<u64>,
}

pub mod load;
pub mod validate;
