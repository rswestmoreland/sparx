// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Config load and merge logic.
// See: contracts/28_config_schema_v0_1.md
// Implements file, environment, and CLI overrides with deterministic defaults.

use std::env;
use std::fs;

use super::{
    BaselineSectionV1, CapsSectionV1, CliOverridesV1, ConfigV1, FeaturesSectionV1, IngestSectionV1,
    MetricsSectionV1, OutputSectionV1, ScoringSectionV1, SparxSectionV1, StorageSectionV1,
    TomlRootV1, VDropSectionV1,
};

#[derive(Clone, Debug)]
pub struct ConfigErrorV1 {
    pub msg: String,
}

fn env_str(name: &str) -> Option<String> {
    env::var(name).ok()
}

fn env_bool(name: &str) -> Option<bool> {
    env_str(name).and_then(|v| {
        let s = v.trim().to_ascii_lowercase();
        match s.as_str() {
            "1" | "true" | "yes" | "y" | "on" => Some(true),
            "0" | "false" | "no" | "n" | "off" => Some(false),
            _ => None,
        }
    })
}

fn env_u32(name: &str) -> Option<u32> {
    env_str(name).and_then(|v| v.trim().parse::<u32>().ok())
}

fn env_u8(name: &str) -> Option<u8> {
    env_str(name).and_then(|v| v.trim().parse::<u8>().ok())
}

fn env_f32(name: &str) -> Option<f32> {
    env_str(name).and_then(|v| v.trim().parse::<f32>().ok())
}

fn env_u64(name: &str) -> Option<u64> {
    env_str(name).and_then(|v| v.trim().parse::<u64>().ok())
}

fn split_csv(v: &str) -> Vec<String> {
    v.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub fn default_config_v1() -> ConfigV1 {
    // Defaults must match contracts/28_config_schema_v0_1.md.
    let data_root = "/var/lib/sparx".to_string();
    let tenant_root = "/var/log/tenants".to_string();

    let sparx = SparxSectionV1 {
        data_root: data_root.clone(),
        tenant_root: tenant_root.clone(),
        global_db_path: format!("{}/global.db", data_root),
        tenant_db_root: format!("{}/tenants", data_root),
        alert_out_root: format!("{}/alerts", data_root),
        pid_file: Some("/run/sparx.pid".to_string()),
        mode: "daemon".to_string(),
        log_level: "info".to_string(),
        log_format: "text".to_string(),
    };

    let ingest = IngestSectionV1 {
        window_size_s: 60,
        max_emit_latency_s: 600,
        poll_interval_ms: 1000,
        max_open_files: 4096,
        follow_symlinks: false,
        read_chunk_bytes: 262144,
        gzip_enabled: true,
        gzip_suffixes: vec![".gz".to_string(), ".gzip".to_string()],
        prefer_plain_when_both: true,
        max_line_len: 16384,
        max_tokens_per_line: 256,
        max_kv_per_line: 64,
        max_words_from_quoted_value: 32,
    };

    let features = FeaturesSectionV1 {
        dict_enabled: true,
        dict_max_entries: 2_000_000,
        hash_space_bits: 26,
        dict_gc_interval_s: 3600,
    };

    let baseline = BaselineSectionV1 {
        baseline_days: 7,
        baseline_min_days: 1,
        df_bucket_count: 48,
        df_ring_slots: 7,
        df_buckets_per_slot: 48,
    };

    let scoring = ScoringSectionV1 {
        outlier_threshold: 0.85,
        noise_threshold: 0.65,
        cold_start_days: 2,
        min_lines_per_window: 10,
    };

    let caps = CapsSectionV1 {
        max_features_per_window: 50_000,
        max_word_features_per_window: 20_000,
        max_shape_features_per_window: 20_000,
        max_syslog_features_per_window: 2_000,
        max_srcips: 64,
        max_dstips: 64,
        max_userids: 128,
        max_domains: 128,
        max_hosts: 128,
    };

    let storage = StorageSectionV1 {
        global_db_open_files: 256,
        global_db_write_buffer_mb: 64,
        tenant_db_open_files: 512,
        tenant_db_write_buffer_mb: 128,
        tenant_db_max_background_jobs: 4,
        tenant_db_max_open: 64,
        tenant_db_idle_close_s: 60,
    };

    let output = OutputSectionV1 {
        sink: "jsonl".to_string(),
        jsonl_rotate_mb: 256,
        jsonl_flush_interval_s: 5,
        include_debug_fields: false,
        automated_replay_max_files_per_pass: 128,
        automated_replay_interval_s: 1,
        spool_max_mb: 2048,
    };

    let metrics = MetricsSectionV1 {
        prometheus_enabled: true,
        prometheus_bind: "127.0.0.1:9898".to_string(),
        health_enabled: true,
        health_bind: "127.0.0.1:9899".to_string(),
    };

    let vdrop = VDropSectionV1 {
        enabled: true,
        device_enabled: true,
        tenant_enabled: true,
        source_stream_enabled: false,
        min_expected_windows_missed: 3,
        min_mature_windows: None,
        min_expected_lines: None,
    };

    ConfigV1 {
        sparx,
        ingest,
        features,
        baseline,
        scoring,
        caps,
        storage,
        output,
        metrics,
        vdrop,
    }
}

fn read_toml_file(path: &str) -> Result<TomlRootV1, ConfigErrorV1> {
    let bytes = fs::read(path).map_err(|e| ConfigErrorV1 {
        msg: format!("config read failed: {}: {}", path, e),
    })?;
    let s = String::from_utf8_lossy(&bytes).to_string();
    toml::from_str::<TomlRootV1>(&s).map_err(|e| ConfigErrorV1 {
        msg: format!("config parse failed: {}: {}", path, e),
    })
}

pub fn load_config_v1(cli: &CliOverridesV1) -> Result<ConfigV1, ConfigErrorV1> {
    // Determine config path.
    let cfg_path = cli
        .config_path
        .clone()
        .unwrap_or_else(|| "/etc/sparx/sparx.toml".to_string());

    // Start with defaults.
    let mut cfg = default_config_v1();

    // Apply config file if present (best effort: if file missing, keep defaults).
    // NOTE: contract does not explicitly require failure on missing file.
    if fs::metadata(&cfg_path).is_ok() {
        let t = read_toml_file(&cfg_path)?;
        apply_toml(&mut cfg, &t);
    }

    // Apply environment variables.
    apply_env(&mut cfg);

    // Apply CLI overrides (highest precedence).
    apply_cli(&mut cfg, cli);

    Ok(cfg)
}

fn apply_toml(cfg: &mut ConfigV1, t: &TomlRootV1) {
    if let Some(s) = &t.sparx {
        if let Some(v) = &s.data_root {
            cfg.sparx.data_root = v.clone();
            cfg.sparx.global_db_path = format!("{}/global.db", cfg.sparx.data_root);
            cfg.sparx.tenant_db_root = format!("{}/tenants", cfg.sparx.data_root);
            cfg.sparx.alert_out_root = format!("{}/alerts", cfg.sparx.data_root);
        }
        if let Some(v) = &s.tenant_root {
            cfg.sparx.tenant_root = v.clone();
        }
        if let Some(v) = &s.global_db_path {
            cfg.sparx.global_db_path = v.clone();
        }
        if let Some(v) = &s.tenant_db_root {
            cfg.sparx.tenant_db_root = v.clone();
        }
        if let Some(v) = &s.alert_out_root {
            cfg.sparx.alert_out_root = v.clone();
        }
        if s.pid_file.is_some() {
            cfg.sparx.pid_file = s.pid_file.clone();
        }
        if let Some(v) = &s.mode {
            cfg.sparx.mode = v.clone();
        }
        if let Some(v) = &s.log_level {
            cfg.sparx.log_level = v.clone();
        }
        if let Some(v) = &s.log_format {
            cfg.sparx.log_format = v.clone();
        }
    }

    if let Some(i) = &t.ingest {
        if let Some(v) = i.window_size_s {
            cfg.ingest.window_size_s = v;
        }
        if let Some(v) = i.max_emit_latency_s {
            cfg.ingest.max_emit_latency_s = v;
        }
        if let Some(v) = i.poll_interval_ms {
            cfg.ingest.poll_interval_ms = v;
        }
        if let Some(v) = i.max_open_files {
            cfg.ingest.max_open_files = v;
        }
        if let Some(v) = i.follow_symlinks {
            cfg.ingest.follow_symlinks = v;
        }
        if let Some(v) = i.read_chunk_bytes {
            cfg.ingest.read_chunk_bytes = v;
        }
        if let Some(v) = i.gzip_enabled {
            cfg.ingest.gzip_enabled = v;
        }
        if let Some(v) = &i.gzip_suffixes {
            cfg.ingest.gzip_suffixes = v.clone();
        }
        if let Some(v) = i.prefer_plain_when_both {
            cfg.ingest.prefer_plain_when_both = v;
        }
        if let Some(v) = i.max_line_len {
            cfg.ingest.max_line_len = v;
        }
        if let Some(v) = i.max_tokens_per_line {
            cfg.ingest.max_tokens_per_line = v;
        }
        if let Some(v) = i.max_kv_per_line {
            cfg.ingest.max_kv_per_line = v;
        }
        if let Some(v) = i.max_words_from_quoted_value {
            cfg.ingest.max_words_from_quoted_value = v;
        }
    }

    if let Some(f) = &t.features {
        if let Some(v) = f.dict_enabled {
            cfg.features.dict_enabled = v;
        }
        if let Some(v) = f.dict_max_entries {
            cfg.features.dict_max_entries = v;
        }
        if let Some(v) = f.hash_space_bits {
            cfg.features.hash_space_bits = v;
        }
        if let Some(v) = f.dict_gc_interval_s {
            cfg.features.dict_gc_interval_s = v;
        }
    }

    if let Some(b) = &t.baseline {
        if let Some(v) = b.baseline_days {
            cfg.baseline.baseline_days = v;
        }
        if let Some(v) = b.baseline_min_days {
            cfg.baseline.baseline_min_days = v;
        }
        if let Some(v) = b.df_bucket_count {
            cfg.baseline.df_bucket_count = v;
        }
        if let Some(v) = b.df_ring_slots {
            cfg.baseline.df_ring_slots = v;
        }
        if let Some(v) = b.df_buckets_per_slot {
            cfg.baseline.df_buckets_per_slot = v;
        }
    }

    if let Some(s) = &t.scoring {
        if let Some(v) = s.outlier_threshold {
            cfg.scoring.outlier_threshold = v;
        }
        if let Some(v) = s.noise_threshold {
            cfg.scoring.noise_threshold = v;
        }
        if let Some(v) = s.cold_start_days {
            cfg.scoring.cold_start_days = v;
        }
        if let Some(v) = s.min_lines_per_window {
            cfg.scoring.min_lines_per_window = v;
        }
    }

    if let Some(c) = &t.caps {
        if let Some(v) = c.max_features_per_window {
            cfg.caps.max_features_per_window = v;
        }
        if let Some(v) = c.max_word_features_per_window {
            cfg.caps.max_word_features_per_window = v;
        }
        if let Some(v) = c.max_shape_features_per_window {
            cfg.caps.max_shape_features_per_window = v;
        }
        if let Some(v) = c.max_syslog_features_per_window {
            cfg.caps.max_syslog_features_per_window = v;
        }
        if let Some(v) = c.max_srcips {
            cfg.caps.max_srcips = v;
        }
        if let Some(v) = c.max_dstips {
            cfg.caps.max_dstips = v;
        }
        if let Some(v) = c.max_userids {
            cfg.caps.max_userids = v;
        }
        if let Some(v) = c.max_domains {
            cfg.caps.max_domains = v;
        }
        if let Some(v) = c.max_hosts {
            cfg.caps.max_hosts = v;
        }
    }

    if let Some(s) = &t.storage {
        if let Some(v) = s.global_db_open_files {
            cfg.storage.global_db_open_files = v;
        }
        if let Some(v) = s.global_db_write_buffer_mb {
            cfg.storage.global_db_write_buffer_mb = v;
        }
        if let Some(v) = s.tenant_db_open_files {
            cfg.storage.tenant_db_open_files = v;
        }
        if let Some(v) = s.tenant_db_write_buffer_mb {
            cfg.storage.tenant_db_write_buffer_mb = v;
        }
        if let Some(v) = s.tenant_db_max_background_jobs {
            cfg.storage.tenant_db_max_background_jobs = v;
        }
        if let Some(v) = s.tenant_db_max_open {
            cfg.storage.tenant_db_max_open = v;
        }
        if let Some(v) = s.tenant_db_idle_close_s {
            cfg.storage.tenant_db_idle_close_s = v;
        }
    }

    if let Some(o) = &t.output {
        if let Some(v) = &o.sink {
            cfg.output.sink = v.clone();
        }
        if let Some(v) = o.jsonl_rotate_mb {
            cfg.output.jsonl_rotate_mb = v;
        }
        if let Some(v) = o.jsonl_flush_interval_s {
            cfg.output.jsonl_flush_interval_s = v;
        }
        if let Some(v) = o.include_debug_fields {
            cfg.output.include_debug_fields = v;
        }
        if let Some(v) = o.automated_replay_max_files_per_pass {
            cfg.output.automated_replay_max_files_per_pass = v;
        }
        if let Some(v) = o.automated_replay_interval_s {
            cfg.output.automated_replay_interval_s = v;
        }
        if let Some(v) = o.spool_max_mb {
            cfg.output.spool_max_mb = v;
        }
    }

    if let Some(m) = &t.metrics {
        if let Some(v) = m.prometheus_enabled {
            cfg.metrics.prometheus_enabled = v;
        }
        if let Some(v) = &m.prometheus_bind {
            cfg.metrics.prometheus_bind = v.clone();
        }
        if let Some(v) = m.health_enabled {
            cfg.metrics.health_enabled = v;
        }
        if let Some(v) = &m.health_bind {
            cfg.metrics.health_bind = v.clone();
        }
    }

    if let Some(vdrop) = &t.vdrop {
        if let Some(v) = vdrop.enabled {
            cfg.vdrop.enabled = v;
        }
        if let Some(v) = vdrop.device_enabled {
            cfg.vdrop.device_enabled = v;
        }
        if let Some(v) = vdrop.tenant_enabled {
            cfg.vdrop.tenant_enabled = v;
        }
        if let Some(v) = vdrop.source_stream_enabled {
            cfg.vdrop.source_stream_enabled = v;
        }
        if let Some(v) = vdrop.min_expected_windows_missed {
            cfg.vdrop.min_expected_windows_missed = v;
        }
        if vdrop.min_mature_windows.is_some() {
            cfg.vdrop.min_mature_windows = vdrop.min_mature_windows;
        }
        if vdrop.min_expected_lines.is_some() {
            cfg.vdrop.min_expected_lines = vdrop.min_expected_lines;
        }
    }
}

fn apply_env(cfg: &mut ConfigV1) {
    // sparx
    if let Some(v) = env_str("SPARX_DATA_ROOT") {
        cfg.sparx.data_root = v;
        cfg.sparx.global_db_path = format!("{}/global.db", cfg.sparx.data_root);
        cfg.sparx.tenant_db_root = format!("{}/tenants", cfg.sparx.data_root);
        cfg.sparx.alert_out_root = format!("{}/alerts", cfg.sparx.data_root);
    }
    if let Some(v) = env_str("SPARX_TENANT_ROOT") {
        cfg.sparx.tenant_root = v;
    }
    if let Some(v) = env_str("SPARX_GLOBAL_DB_PATH") {
        cfg.sparx.global_db_path = v;
    }
    if let Some(v) = env_str("SPARX_TENANT_DB_ROOT") {
        cfg.sparx.tenant_db_root = v;
    }
    if let Some(v) = env_str("SPARX_ALERT_OUT_ROOT") {
        cfg.sparx.alert_out_root = v;
    }
    if let Some(v) = env_str("SPARX_PID_FILE") {
        cfg.sparx.pid_file = Some(v);
    }
    if let Some(v) = env_str("SPARX_MODE") {
        cfg.sparx.mode = v;
    }
    if let Some(v) = env_str("SPARX_LOG_LEVEL") {
        cfg.sparx.log_level = v;
    }
    if let Some(v) = env_str("SPARX_LOG_FORMAT") {
        cfg.sparx.log_format = v;
    }

    // ingest
    if let Some(v) = env_u32("SPARX_WINDOW_SIZE_S") {
        cfg.ingest.window_size_s = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_EMIT_LATENCY_S") {
        cfg.ingest.max_emit_latency_s = v;
    }
    if let Some(v) = env_u32("SPARX_POLL_INTERVAL_MS") {
        cfg.ingest.poll_interval_ms = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_OPEN_FILES") {
        cfg.ingest.max_open_files = v;
    }
    if let Some(v) = env_bool("SPARX_FOLLOW_SYMLINKS") {
        cfg.ingest.follow_symlinks = v;
    }
    if let Some(v) = env_u32("SPARX_READ_CHUNK_BYTES") {
        cfg.ingest.read_chunk_bytes = v;
    }
    if let Some(v) = env_bool("SPARX_GZIP_ENABLED") {
        cfg.ingest.gzip_enabled = v;
    }
    if let Some(v) = env_str("SPARX_GZIP_SUFFIXES") {
        cfg.ingest.gzip_suffixes = split_csv(&v);
    }
    if let Some(v) = env_bool("SPARX_PREFER_PLAIN_WHEN_BOTH") {
        cfg.ingest.prefer_plain_when_both = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_LINE_LEN") {
        cfg.ingest.max_line_len = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_TOKENS_PER_LINE") {
        cfg.ingest.max_tokens_per_line = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_KV_PER_LINE") {
        cfg.ingest.max_kv_per_line = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_WORDS_FROM_QUOTED_VALUE") {
        cfg.ingest.max_words_from_quoted_value = v;
    }

    // features
    if let Some(v) = env_bool("SPARX_DICT_ENABLED") {
        cfg.features.dict_enabled = v;
    }
    if let Some(v) = env_u32("SPARX_DICT_MAX_ENTRIES") {
        cfg.features.dict_max_entries = v;
    }
    if let Some(v) = env_u8("SPARX_HASH_SPACE_BITS") {
        cfg.features.hash_space_bits = v;
    }
    if let Some(v) = env_u32("SPARX_DICT_GC_INTERVAL_S") {
        cfg.features.dict_gc_interval_s = v;
    }

    // baseline
    if let Some(v) = env_u32("SPARX_BASELINE_DAYS") {
        cfg.baseline.baseline_days = v;
    }
    if let Some(v) = env_u32("SPARX_BASELINE_MIN_DAYS") {
        cfg.baseline.baseline_min_days = v;
    }
    if let Some(v) = env_u32("SPARX_DF_RING_SLOTS") {
        cfg.baseline.df_ring_slots = v;
    }
    if let Some(v) = env_u32("SPARX_DF_BUCKETS_PER_SLOT") {
        cfg.baseline.df_buckets_per_slot = v;
    }

    // scoring
    if let Some(v) = env_f32("SPARX_OUTLIER_THRESHOLD") {
        cfg.scoring.outlier_threshold = v;
    }
    if let Some(v) = env_f32("SPARX_NOISE_THRESHOLD") {
        cfg.scoring.noise_threshold = v;
    }
    if let Some(v) = env_u32("SPARX_COLD_START_DAYS") {
        cfg.scoring.cold_start_days = v;
    }
    if let Some(v) = env_u32("SPARX_MIN_LINES_PER_WINDOW") {
        cfg.scoring.min_lines_per_window = v;
    }

    // caps
    if let Some(v) = env_u32("SPARX_MAX_FEATURES_PER_WINDOW") {
        cfg.caps.max_features_per_window = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_WORD_FEATURES_PER_WINDOW") {
        cfg.caps.max_word_features_per_window = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_SHAPE_FEATURES_PER_WINDOW") {
        cfg.caps.max_shape_features_per_window = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_SYSLOG_FEATURES_PER_WINDOW") {
        cfg.caps.max_syslog_features_per_window = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_SRCIPS") {
        cfg.caps.max_srcips = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_DSTIPS") {
        cfg.caps.max_dstips = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_USERIDS") {
        cfg.caps.max_userids = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_DOMAINS") {
        cfg.caps.max_domains = v;
    }
    if let Some(v) = env_u32("SPARX_MAX_HOSTS") {
        cfg.caps.max_hosts = v;
    }

    // storage
    if let Some(v) = env_u32("SPARX_TENANT_DB_MAX_OPEN") {
        cfg.storage.tenant_db_max_open = v;
    }
    if let Some(v) = env_u32("SPARX_TENANT_DB_IDLE_CLOSE_S") {
        cfg.storage.tenant_db_idle_close_s = v;
    }

    // output
    if let Some(v) = env_str("SPARX_OUTPUT_SINK") {
        cfg.output.sink = v;
    }
    if let Some(v) = env_u32("SPARX_JSONL_ROTATE_MB") {
        cfg.output.jsonl_rotate_mb = v;
    }
    if let Some(v) = env_u32("SPARX_JSONL_FLUSH_INTERVAL_S") {
        cfg.output.jsonl_flush_interval_s = v;
    }
    if let Some(v) = env_bool("SPARX_INCLUDE_DEBUG_FIELDS") {
        cfg.output.include_debug_fields = v;
    }
    if let Some(v) = env_u32("SPARX_AUTOMATED_REPLAY_MAX_FILES_PER_PASS") {
        cfg.output.automated_replay_max_files_per_pass = v;
    }
    if let Some(v) = env_u32("SPARX_AUTOMATED_REPLAY_INTERVAL_S") {
        cfg.output.automated_replay_interval_s = v;
    }
    if let Some(v) = env_u32("SPARX_SPOOL_MAX_MB") {
        cfg.output.spool_max_mb = v;
    }

    // metrics
    if let Some(v) = env_bool("SPARX_PROMETHEUS_ENABLED") {
        cfg.metrics.prometheus_enabled = v;
    }
    if let Some(v) = env_str("SPARX_PROMETHEUS_BIND") {
        cfg.metrics.prometheus_bind = v;
    }
    if let Some(v) = env_bool("SPARX_HEALTH_ENABLED") {
        cfg.metrics.health_enabled = v;
    }
    if let Some(v) = env_str("SPARX_HEALTH_BIND") {
        cfg.metrics.health_bind = v;
    }

    // vdrop
    if let Some(v) = env_bool("SPARX_VDROP_ENABLED") {
        cfg.vdrop.enabled = v;
    }
    if let Some(v) = env_bool("SPARX_VDROP_DEVICE_ENABLED") {
        cfg.vdrop.device_enabled = v;
    }
    if let Some(v) = env_bool("SPARX_VDROP_TENANT_ENABLED") {
        cfg.vdrop.tenant_enabled = v;
    }
    if let Some(v) = env_bool("SPARX_VDROP_SOURCE_STREAM_ENABLED") {
        cfg.vdrop.source_stream_enabled = v;
    }
    if let Some(v) = env_u32("SPARX_VDROP_MIN_EXPECTED_WINDOWS_MISSED") {
        cfg.vdrop.min_expected_windows_missed = v;
    }
    if let Some(v) = env_u64("SPARX_VDROP_MIN_MATURE_WINDOWS") {
        cfg.vdrop.min_mature_windows = Some(v);
    }
    if let Some(v) = env_u64("SPARX_VDROP_MIN_EXPECTED_LINES") {
        cfg.vdrop.min_expected_lines = Some(v);
    }
}

fn apply_cli(cfg: &mut ConfigV1, cli: &CliOverridesV1) {
    if let Some(v) = &cli.state_root {
        cfg.sparx.data_root = v.clone();
        cfg.sparx.global_db_path = format!("{}/global.db", cfg.sparx.data_root);
        cfg.sparx.tenant_db_root = format!("{}/tenants", cfg.sparx.data_root);
        cfg.sparx.alert_out_root = format!("{}/alerts", cfg.sparx.data_root);
    }
    if let Some(v) = &cli.watch_root {
        cfg.sparx.tenant_root = v.clone();
    }
    if let Some(v) = &cli.log_level {
        cfg.sparx.log_level = v.clone();
    }
    if let Some(v) = &cli.log_format {
        cfg.sparx.log_format = v.clone();
    }
}
