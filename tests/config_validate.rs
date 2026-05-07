// Phase 12.5c tests for config validation and precedence.
// Uses a process-wide env lock because environment mutation is global.

use std::fs;
use std::sync::{Mutex, OnceLock};

use sparx::config::load::default_config_v1;
use sparx::config::load::load_config_v1;
use sparx::config::CliOverridesV1;
use sparx::config::validate::validate_config_v1;
use tempfile::tempdir;

fn env_lock_v1() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct EnvGuardV1 {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuardV1 {
    fn clear(keys: &[&'static str]) -> Self {
        let mut saved = Vec::new();
        for key in keys {
            saved.push((*key, std::env::var(key).ok()));
            std::env::remove_var(key);
        }
        Self { saved }
    }
}

impl Drop for EnvGuardV1 {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }
}

#[test]
fn config_defaults_validate() {
    let cfg = default_config_v1();
    let r = validate_config_v1(&cfg);
    assert!(r.is_ok());
}

#[test]
fn config_rejects_bad_window_size() {
    let mut cfg = default_config_v1();
    cfg.ingest.window_size_s = 61;
    let r = validate_config_v1(&cfg);
    assert!(r.is_err());
    let msg = r.err().unwrap().msg;
    assert!(msg.contains("ingest.window_size_s"));
}

#[test]
fn config_rejects_bad_log_level() {
    let mut cfg = default_config_v1();
    cfg.sparx.log_level = "verbose".to_string();
    let r = validate_config_v1(&cfg);
    assert!(r.is_err());
    let msg = r.err().unwrap().msg;
    assert!(msg.contains("sparx.log_level"));
}

#[test]
fn config_rejects_bad_log_format() {
    let mut cfg = default_config_v1();
    cfg.sparx.log_format = "yaml".to_string();
    let r = validate_config_v1(&cfg);
    assert!(r.is_err());
    let msg = r.err().unwrap().msg;
    assert!(msg.contains("sparx.log_format"));
}

#[test]
fn config_rejects_zero_automated_replay_max_files_per_pass_v1() {
    let mut cfg = default_config_v1();
    cfg.output.automated_replay_max_files_per_pass = 0;
    let r = validate_config_v1(&cfg);
    assert!(r.is_err());
    let msg = r.err().unwrap().msg;
    assert!(msg.contains("output.automated_replay_max_files_per_pass"));
}

#[test]
fn config_rejects_bad_mode() {
    let mut cfg = default_config_v1();
    cfg.sparx.mode = "service".to_string();
    let r = validate_config_v1(&cfg);
    assert!(r.is_err());
    let msg = r.err().unwrap().msg;
    assert!(msg.contains("sparx.mode"));
}

#[test]
fn config_rejects_bad_output_sink() {
    let mut cfg = default_config_v1();
    cfg.output.sink = "file".to_string();
    let r = validate_config_v1(&cfg);
    assert!(r.is_err());
    let msg = r.err().unwrap().msg;
    assert!(msg.contains("output.sink"));
}

#[test]
fn config_rejects_bad_scoring_outlier_threshold() {
    let mut cfg = default_config_v1();
    cfg.scoring.outlier_threshold = 1.5;
    let r = validate_config_v1(&cfg);
    assert!(r.is_err());
    let msg = r.err().unwrap().msg;
    assert!(msg.contains("scoring.outlier_threshold"));
}

#[test]
fn config_rejects_bad_scoring_noise_threshold() {
    let mut cfg = default_config_v1();
    cfg.scoring.noise_threshold = -0.1;
    let r = validate_config_v1(&cfg);
    assert!(r.is_err());
    let msg = r.err().unwrap().msg;
    assert!(msg.contains("scoring.noise_threshold"));
}

#[test]
fn config_file_env_cli_precedence_is_deterministic() {
    let _guard = env_lock_v1().lock().unwrap();
    let _env = EnvGuardV1::clear(&[
        "SPARX_LOG_LEVEL",
        "SPARX_LOG_FORMAT",
        "SPARX_OUTPUT_SINK",
    ]);

    let td = tempdir().unwrap();
    let cfg_path = td.path().join("sparx.toml");
    fs::write(
        &cfg_path,
        r#"
[sparx]
log_level = "debug"
log_format = "json"

[output]
sink = "stdout"
"#,
    )
    .unwrap();

    std::env::set_var("SPARX_LOG_LEVEL", "warn");
    std::env::set_var("SPARX_LOG_FORMAT", "text");
    std::env::set_var("SPARX_OUTPUT_SINK", "jsonl");

    let cli = CliOverridesV1 {
        config_path: Some(cfg_path.to_string_lossy().to_string()),
        watch_root: None,
        state_root: None,
        log_level: Some("error".to_string()),
        log_format: Some("json".to_string()),
    };

    let cfg = load_config_v1(&cli).unwrap();
    assert_eq!(cfg.sparx.log_level, "error");
    assert_eq!(cfg.sparx.log_format, "json");
    assert_eq!(cfg.output.sink, "jsonl");

}

#[test]
fn config_env_overrides_file_when_no_cli_override_exists() {
    let _guard = env_lock_v1().lock().unwrap();
    let _env = EnvGuardV1::clear(&[
        "SPARX_LOG_LEVEL",
        "SPARX_LOG_FORMAT",
        "SPARX_OUTPUT_SINK",
    ]);

    let td = tempdir().unwrap();
    let cfg_path = td.path().join("sparx.toml");
    fs::write(
        &cfg_path,
        r#"
[output]
sink = "stdout"
"#,
    )
    .unwrap();

    std::env::set_var("SPARX_OUTPUT_SINK", "jsonl");

    let cli = CliOverridesV1 {
        config_path: Some(cfg_path.to_string_lossy().to_string()),
        watch_root: None,
        state_root: None,
        log_level: None,
        log_format: None,
    };

    let cfg = load_config_v1(&cli).unwrap();
    assert_eq!(cfg.output.sink, "jsonl");

}

#[test]
fn validate_rejects_bad_metrics_prometheus_bind_v1() {
    let mut cfg = default_config_v1();
    cfg.metrics.prometheus_bind = "not-a-socket".to_string();
    let err = validate_config_v1(&cfg).unwrap_err();
    assert!(err.msg.contains("metrics.prometheus_bind"));
}

#[test]
fn validate_rejects_duplicate_metrics_binds_when_both_enabled_v1() {
    let mut cfg = default_config_v1();
    cfg.metrics.health_bind = cfg.metrics.prometheus_bind.clone();
    let err = validate_config_v1(&cfg).unwrap_err();
    assert!(err.msg.contains("must differ"));
}
