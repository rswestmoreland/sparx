// Config validation.
// See: contracts/28_config_schema_v0_1.md
//
// Only fields classified as active and enumerated in the contract are
// validated here. Reserved/deferred continuity fields stay parseable but are
// not treated as active behavior gates in v0.1.

use super::load::ConfigErrorV1;
use super::ConfigV1;


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
            msg: format!("invalid scoring.outlier_threshold: {}", cfg.scoring.outlier_threshold),
        });
    }

    if !cfg.scoring.noise_threshold.is_finite()
        || cfg.scoring.noise_threshold < 0.0
        || cfg.scoring.noise_threshold > 1.0
    {
        return Err(ConfigErrorV1 {
            msg: format!("invalid scoring.noise_threshold: {}", cfg.scoring.noise_threshold),
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

    Ok(())
}
