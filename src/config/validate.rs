// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Config validation.
// See: contracts/28_config_schema_v0_1.md
//
// Only fields classified as active and enumerated in the contract are
// validated here. Reserved/deferred continuity fields stay parseable but are
// not treated as active behavior gates in v0.1.

use super::load::ConfigErrorV1;
use super::ConfigV1;

pub const READ_CHUNK_BYTES_MAX_V1: u32 = 16 * 1024 * 1024;
pub const MAX_LINE_LEN_MAX_V1: u32 = 1024 * 1024;
pub const MAX_TOKENS_PER_LINE_MAX_V1: u32 = 4096;
pub const MAX_KV_PER_LINE_MAX_V1: u32 = 1024;
pub const MAX_WORDS_FROM_QUOTED_VALUE_MAX_V1: u32 = 1024;

fn validate_socket_addr_v1(field: &str, value: &str) -> Result<(), ConfigErrorV1> {
    value
        .parse::<std::net::SocketAddr>()
        .map(|_| ())
        .map_err(|_| ConfigErrorV1 {
            msg: format!("invalid {}: {}", field, value),
        })
}

fn one_of(value: &str, allowed: &[&str]) -> bool {
    allowed.iter().any(|v| *v == value)
}

pub fn validate_config_v1(cfg: &ConfigV1) -> Result<(), ConfigErrorV1> {
    // window_size whitelist: {60,120,300,600}
    let ws = cfg.ingest.window_size_s;
    if ws != 60 && ws != 120 && ws != 300 && ws != 600 {
        return Err(ConfigErrorV1 {
            msg: format!("invalid ingest.window_size_s: {}", ws),
        });
    }

    if cfg.ingest.max_emit_latency_s < ws {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid ingest.max_emit_latency_s: {} (must be >= window_size_s)",
                cfg.ingest.max_emit_latency_s
            ),
        });
    }

    if cfg.ingest.read_chunk_bytes == 0 || cfg.ingest.read_chunk_bytes > READ_CHUNK_BYTES_MAX_V1 {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid ingest.read_chunk_bytes: {} (must be 1..={})",
                cfg.ingest.read_chunk_bytes, READ_CHUNK_BYTES_MAX_V1
            ),
        });
    }

    if cfg.ingest.max_line_len == 0 || cfg.ingest.max_line_len > MAX_LINE_LEN_MAX_V1 {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid ingest.max_line_len: {} (must be 1..={})",
                cfg.ingest.max_line_len, MAX_LINE_LEN_MAX_V1
            ),
        });
    }

    if cfg.ingest.max_tokens_per_line == 0
        || cfg.ingest.max_tokens_per_line > MAX_TOKENS_PER_LINE_MAX_V1
    {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid ingest.max_tokens_per_line: {} (must be 1..={})",
                cfg.ingest.max_tokens_per_line, MAX_TOKENS_PER_LINE_MAX_V1
            ),
        });
    }

    if cfg.ingest.max_kv_per_line == 0 || cfg.ingest.max_kv_per_line > MAX_KV_PER_LINE_MAX_V1 {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid ingest.max_kv_per_line: {} (must be 1..={})",
                cfg.ingest.max_kv_per_line, MAX_KV_PER_LINE_MAX_V1
            ),
        });
    }

    if cfg.ingest.max_words_from_quoted_value == 0
        || cfg.ingest.max_words_from_quoted_value > MAX_WORDS_FROM_QUOTED_VALUE_MAX_V1
    {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid ingest.max_words_from_quoted_value: {} (must be 1..={})",
                cfg.ingest.max_words_from_quoted_value, MAX_WORDS_FROM_QUOTED_VALUE_MAX_V1
            ),
        });
    }

    // hash_space_bits remains a reserved continuity field in v0.1, but its
    // stable range is still enforced so the config surface stays deterministic.
    let hb = cfg.features.hash_space_bits;
    if hb < 20 || hb > 30 {
        return Err(ConfigErrorV1 {
            msg: format!("invalid features.hash_space_bits: {}", hb),
        });
    }

    if !cfg.scoring.outlier_threshold.is_finite()
        || cfg.scoring.outlier_threshold < 0.0
        || cfg.scoring.outlier_threshold > 1.0
    {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid scoring.outlier_threshold: {}",
                cfg.scoring.outlier_threshold
            ),
        });
    }

    if !cfg.scoring.noise_threshold.is_finite()
        || cfg.scoring.noise_threshold < 0.0
        || cfg.scoring.noise_threshold > 1.0
    {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid scoring.noise_threshold: {}",
                cfg.scoring.noise_threshold
            ),
        });
    }

    let sink = cfg.output.sink.as_str();
    if !one_of(sink, &["jsonl", "stdout"]) {
        return Err(ConfigErrorV1 {
            msg: format!("invalid output.sink: {}", cfg.output.sink),
        });
    }

    if cfg.output.automated_replay_max_files_per_pass == 0 {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid output.automated_replay_max_files_per_pass: {}",
                cfg.output.automated_replay_max_files_per_pass
            ),
        });
    }

    if cfg.output.automated_replay_interval_s == 0 {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid output.automated_replay_interval_s: {}",
                cfg.output.automated_replay_interval_s
            ),
        });
    }

    if cfg.output.spool_max_mb == 0 {
        return Err(ConfigErrorV1 {
            msg: format!("invalid output.spool_max_mb: {}", cfg.output.spool_max_mb),
        });
    }

    let mode = cfg.sparx.mode.as_str();
    if !one_of(mode, &["daemon", "oneshot"]) {
        return Err(ConfigErrorV1 {
            msg: format!("invalid sparx.mode: {}", cfg.sparx.mode),
        });
    }

    let log_level = cfg.sparx.log_level.as_str();
    if !one_of(log_level, &["error", "warn", "info", "debug", "trace"]) {
        return Err(ConfigErrorV1 {
            msg: format!("invalid sparx.log_level: {}", cfg.sparx.log_level),
        });
    }

    let log_format = cfg.sparx.log_format.as_str();
    if !one_of(log_format, &["text", "json"]) {
        return Err(ConfigErrorV1 {
            msg: format!("invalid sparx.log_format: {}", cfg.sparx.log_format),
        });
    }

    if cfg.metrics.prometheus_enabled {
        validate_socket_addr_v1("metrics.prometheus_bind", &cfg.metrics.prometheus_bind)?;
    }
    if cfg.metrics.health_enabled {
        validate_socket_addr_v1("metrics.health_bind", &cfg.metrics.health_bind)?;
    }
    if cfg.metrics.prometheus_enabled
        && cfg.metrics.health_enabled
        && cfg.metrics.prometheus_bind == cfg.metrics.health_bind
    {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid metrics binds: prometheus_bind and health_bind must differ when both endpoints are enabled ({})",
                cfg.metrics.prometheus_bind
            ),
        });
    }

    if cfg.vdrop.min_expected_windows_missed == 0 {
        return Err(ConfigErrorV1 {
            msg: format!(
                "invalid vdrop.min_expected_windows_missed: {}",
                cfg.vdrop.min_expected_windows_missed
            ),
        });
    }

    Ok(())
}
