// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use tempfile::tempdir;

use sparx::alert::{
    AlertV1, CountedStringV1, EntitiesV1, FileSpanV1, ReasonV1, TopFeatureV1,
    ALERT_SCHEMA_VERSION_V1,
};
use sparx::cli::route::{
    clear_run_test_cycle_hook_v1, install_run_test_cycle_hook_v1, route_command_v1,
};
use sparx::cli::{CommandV1, MigrateModeV1};
use sparx::config::load::default_config_v1;
use sparx::db::GlobalTenantRecordV1;
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
    cfg.ingest.poll_interval_ms = 1;
    fs::create_dir_all(&cfg.sparx.tenant_root).unwrap();
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

fn copy_fixture_device_v1(
    cfg: &sparx::config::ConfigV1,
    tenant_id: &str,
    device: &str,
    files: &[&str],
) {
    let device_dir = Path::new(&cfg.sparx.tenant_root)
        .join(tenant_id)
        .join(device);
    fs::create_dir_all(&device_dir).unwrap();
    for file in files {
        fs::copy(fixture_source_v1(file), device_dir.join(file)).unwrap();
    }
}

fn write_tenant_policy_v1(cfg: &sparx::config::ConfigV1, tenant_id: &str, body: &str) {
    let policy_dir = Path::new(&cfg.sparx.tenant_root)
        .join(tenant_id)
        .join(".sparx");
    fs::create_dir_all(&policy_dir).unwrap();
    fs::write(policy_dir.join("policy.toml"), body).unwrap();
}

fn run_test_lock_v1() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

struct EnvVarGuardV1 {
    key: &'static str,
    old: Option<String>,
}

impl EnvVarGuardV1 {
    fn set_v1(key: &'static str, value: &str) -> Self {
        let old = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, old }
    }
}

impl Drop for EnvVarGuardV1 {
    fn drop(&mut self) {
        if let Some(old) = &self.old {
            std::env::set_var(self.key, old);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

struct RunHookGuardV1;

impl RunHookGuardV1 {
    fn install_v1<F>(hook: F) -> Self
    where
        F: Fn(u32, &mut SparxRuntimeV1, &sparx::config::ConfigV1) + Send + Sync + 'static,
    {
        install_run_test_cycle_hook_v1(hook);
        Self
    }
}

impl Drop for RunHookGuardV1 {
    fn drop(&mut self) {
        clear_run_test_cycle_hook_v1();
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

fn count_spool_files_v1(root: &Path) -> usize {
    if !root.exists() {
        return 0;
    }
    let mut count = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let mut entries: Vec<PathBuf> = fs::read_dir(&path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        entries.sort();
        for entry in entries {
            if entry.is_dir() {
                stack.push(entry);
            } else if entry.is_file() {
                count = count.saturating_add(1);
            }
        }
    }
    count
}

fn count_vdrop_alerts_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
) -> Result<usize, sparx::db::DbErrorV1> {
    runtime.with_tenant_db_v1(tenant_id, 0, |db| {
        let mut count = 0usize;
        for alert_id in db.list_primary_alert_ids_v1()? {
            if let Some(alert) = db.read_primary_alert_v1(&alert_id)? {
                if alert.reasons.iter().any(|reason| reason.code == "V_DROP") {
                    count = count.saturating_add(1);
                }
            }
        }
        Ok(count)
    })
}

fn count_source_stream_vdrop_alerts_v1(
    runtime: &mut SparxRuntimeV1,
    tenant_id: &str,
) -> Result<usize, sparx::db::DbErrorV1> {
    runtime.with_tenant_db_v1(tenant_id, 0, |db| {
        let mut count = 0usize;
        for alert_id in db.list_primary_alert_ids_v1()? {
            if let Some(alert) = db.read_primary_alert_v1(&alert_id)? {
                if alert.reasons.iter().any(|reason| {
                    reason.code == "V_DROP"
                        && reason
                            .details
                            .iter()
                            .any(|(key, value)| key == "subject_kind" && value == "source_stream")
                }) {
                    count = count.saturating_add(1);
                }
            }
        }
        Ok(count)
    })
}

#[test]
fn run_startup_and_shutdown_persist_process_state_and_flush_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log", "edge01.gz"]);

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let process = runtime.read_process_state_v1()?;
    assert!(process.last_run_start_ts.is_some());
    assert!(process.last_run_end_ts.is_some());
    assert_eq!(process.last_run_exit_code, Some(0));
    assert!(process.last_run_host.is_some());

    let device_key = device_key_v1("smoke", "edge01");
    let log_file_key = file_key_v1("edge01.log");
    let gz_file_key = file_key_v1("edge01.gz");
    let log_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.log"),
    )?
    .len();
    let gz_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.gz"),
    )?
    .len();

    let log_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.read_cursor_v1(&device_key, &log_file_key)
    })?;
    let gz_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.read_cursor_v1(&device_key, &gz_file_key)
    })?;
    let open_window =
        runtime.with_tenant_db_v1("smoke", 0, |db| db.read_open_window_state_v1(&device_key))?;

    assert_eq!(log_cursor.unwrap().offset, log_size);
    assert_eq!(gz_cursor.unwrap().offset, gz_size);
    assert!(open_window.is_none());
    Ok(())
}

#[test]
fn run_runtime_emits_vdrop_alerts_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 2);
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_evaluated_subjects_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_candidates_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("vdrop_alerts_emitted_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("vdrop_tracked_subjects__smoke")?,
        Some(2.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("vdrop_open_silence_subjects__smoke")?,
        Some(2.0)
    );
    Ok(())
}

#[test]
fn run_source_stream_gate_emits_runtime_hard_silence_alert_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    cfg.vdrop.source_stream_enabled = true;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 3);
    assert_eq!(
        count_source_stream_vdrop_alerts_v1(&mut runtime, "smoke")?,
        1
    );
    let catalogs = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.list_source_stream_catalogs_for_device_v1(&device_key)
    })?;
    assert_eq!(catalogs.len(), 1);
    Ok(())
}

#[test]
fn run_tenant_policy_can_disable_all_vdrop_subjects_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let mut cfg = temp_cfg_v1();
    cfg.scoring.cold_start_days = 0;
    cfg.scoring.min_lines_per_window = 1;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);
    write_tenant_policy_v1(&cfg, "smoke", "policy_version = 1\nvdrop_enabled = false\n");

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(count_vdrop_alerts_v1(&mut runtime, "smoke")?, 0);
    Ok(())
}

fn run_disabled_tenant_is_skipped_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let paths = runtime.tenant_paths_v1("smoke");
    runtime.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: "smoke".to_string(),
        created_ts: 1_700_000_000,
        last_seen_ts: 1_700_000_000,
        status: 1,
        tenant_root_rel: Some("smoke".to_string()),
        tenant_db_path: Some(paths.tenant_db_dir),
        alert_out_root: Some(paths.alert_out_dir),
    })?;
    runtime.set_tenant_active_index_v1("smoke", false)?;
    drop(runtime);

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    let file_key = file_key_v1("edge01.log");
    let cursor =
        runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&device_key, &file_key))?;
    assert!(cursor.is_none());
    assert!(runtime.list_active_tenants_v1()?.is_empty());
    Ok(())
}

#[test]
fn run_second_start_after_clean_shutdown_is_stable_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log", "edge01.gz"]);

    let first = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(first.exit_code, 0, "stderr={:?}", first.msg_stderr);

    let second = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(second.exit_code, 0, "stderr={:?}", second.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    let log_file_key = file_key_v1("edge01.log");
    let gz_file_key = file_key_v1("edge01.gz");
    let log_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.log"),
    )?
    .len();
    let gz_size = fs::metadata(
        Path::new(&cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.gz"),
    )?
    .len();
    let log_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.read_cursor_v1(&device_key, &log_file_key)
    })?;
    let gz_cursor = runtime.with_tenant_db_v1("smoke", 0, |db| {
        db.read_cursor_v1(&device_key, &gz_file_key)
    })?;
    assert_eq!(log_cursor.unwrap().offset, log_size);
    assert_eq!(gz_cursor.unwrap().offset, gz_size);
    Ok(())
}

#[test]
fn run_disable_without_restart_stops_ingest_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "2");

    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let log_path = Path::new(&cfg.sparx.tenant_root)
        .join("smoke")
        .join("edge01")
        .join("edge01.log");
    let initial_size = fs::metadata(&log_path)?.len();

    let _hook = RunHookGuardV1::install_v1(move |cycle_completed, runtime, hook_cfg| {
        if cycle_completed != 1 {
            return;
        }
        let log_path = Path::new(&hook_cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.log");
        let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
        writeln!(f, "<34>Jan  5 10:05:05 edge01 sshd[101]: Accepted publickey for root from 10.0.0.2 port 22 ssh2").unwrap();
        runtime.set_tenant_status_v1("smoke", 1).unwrap();
    });

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    let file_key = file_key_v1("edge01.log");
    let cursor =
        runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&device_key, &file_key))?;
    assert_eq!(cursor.unwrap().offset, initial_size);
    assert!(runtime.list_active_tenants_v1()?.is_empty());
    assert_eq!(runtime.read_tenant_record_v1("smoke")?.unwrap().status, 1);
    Ok(())
}

#[test]
fn run_terminating_tenant_no_longer_processes_once_marked_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "2");

    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let log_path = Path::new(&cfg.sparx.tenant_root)
        .join("smoke")
        .join("edge01")
        .join("edge01.log");
    let initial_size = fs::metadata(&log_path)?.len();

    let _hook = RunHookGuardV1::install_v1(move |cycle_completed, runtime, hook_cfg| {
        if cycle_completed != 1 {
            return;
        }
        let log_path = Path::new(&hook_cfg.sparx.tenant_root)
            .join("smoke")
            .join("edge01")
            .join("edge01.log");
        let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
        writeln!(f, "<34>Jan  5 10:06:06 edge01 sshd[102]: Accepted publickey for admin from 10.0.0.3 port 22 ssh2").unwrap();
        runtime.set_tenant_status_v1("smoke", 2).unwrap();
    });

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let device_key = device_key_v1("smoke", "edge01");
    let file_key = file_key_v1("edge01.log");
    let cursor =
        runtime.with_tenant_db_v1("smoke", 0, |db| db.read_cursor_v1(&device_key, &file_key))?;
    assert_eq!(cursor.unwrap().offset, initial_size);
    assert!(runtime.list_active_tenants_v1()?.is_empty());
    assert_eq!(runtime.read_tenant_record_v1("smoke")?.unwrap().status, 2);
    Ok(())
}

#[test]
fn run_active_index_reconciles_deterministically_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "tenant-b", "edge01", &["edge01.log"]);
    copy_fixture_device_v1(&cfg, "tenant-a", "edge01", &["edge01.log"]);

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let stale_paths = runtime.tenant_paths_v1("tenant-z");
    runtime.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: "tenant-z".to_string(),
        created_ts: 1,
        last_seen_ts: 1,
        status: 0,
        tenant_root_rel: Some("tenant-z".to_string()),
        tenant_db_path: Some(stale_paths.tenant_db_dir),
        alert_out_root: Some(stale_paths.alert_out_dir),
    })?;
    runtime.set_tenant_active_index_v1("tenant-z", true)?;
    let disabled_paths = runtime.tenant_paths_v1("tenant-disabled");
    runtime.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: "tenant-disabled".to_string(),
        created_ts: 1,
        last_seen_ts: 1,
        status: 1,
        tenant_root_rel: Some("tenant-disabled".to_string()),
        tenant_db_path: Some(disabled_paths.tenant_db_dir),
        alert_out_root: Some(disabled_paths.alert_out_dir),
    })?;
    runtime.set_tenant_active_index_v1("tenant-disabled", true)?;
    drop(runtime);

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(
        vec!["tenant-a".to_string(), "tenant-b".to_string()],
        runtime.list_active_tenants_v1()?
    );
    Ok(())
}

#[test]
fn run_updates_tenant_last_seen_ts_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let cfg = temp_cfg_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    let paths = runtime.tenant_paths_v1("smoke");
    runtime.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: "smoke".to_string(),
        created_ts: 1,
        last_seen_ts: 7,
        status: 0,
        tenant_root_rel: Some("smoke".to_string()),
        tenant_db_path: Some(paths.tenant_db_dir),
        alert_out_root: Some(paths.alert_out_dir),
    })?;
    drop(runtime);

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert!(
        runtime
            .read_tenant_record_v1("smoke")?
            .unwrap()
            .last_seen_ts
            > 7
    );
    Ok(())
}

#[test]
fn run_shutdown_best_effort_replays_spool_v1() {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let cfg = temp_cfg_v1();
    let alert = sample_spooled_alert_v1("tenant-a", "device-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let spool_path = write_spool_alert_v1(&cfg.sparx.data_root, &alert).unwrap();

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);
    assert!(!spool_path.exists());

    let out_path = jsonl_alert_path_v1(
        &cfg.sparx.alert_out_root,
        "tenant-a",
        "device-a",
        alert.window_start_ts,
        0,
    )
    .unwrap();
    assert!(out_path.is_file());
}

fn reserve_loopback_bind_v1() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback bind");
    let addr = listener.local_addr().expect("local addr").to_string();
    drop(listener);
    addr
}

fn http_request_v1(
    addr: &str,
    method: &str,
    path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr)?;
    let request = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        method, path, addr
    );
    stream.write_all(request.as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn http_get_v1(addr: &str, path: &str) -> Result<String, Box<dyn std::error::Error>> {
    http_request_v1(addr, "GET", path)
}

#[test]
fn run_exposes_metrics_and_health_endpoints_when_enabled_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let mut cfg = temp_cfg_v1();
    cfg.metrics.prometheus_bind = reserve_loopback_bind_v1();
    cfg.metrics.health_bind = reserve_loopback_bind_v1();
    cfg.output.sink = "stdout".to_string();
    cfg.output.automated_replay_interval_s = 3600;
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log", "edge01.gz"]);

    let tenant_a_alert = sample_spooled_alert_v1("tenant-a", "device-a", "run-metrics-tenant-a");
    let tenant_b_alert = sample_spooled_alert_v1("tenant-b", "device-b", "run-metrics-tenant-b");
    write_spool_alert_v1(&cfg.sparx.data_root, &tenant_a_alert)
        .map_err(|e| format!("failed to write tenant-a spool alert: {:?}", e))?;
    let tenant_b_path = write_spool_alert_v1(&cfg.sparx.data_root, &tenant_b_alert)
        .map_err(|e| format!("failed to write tenant-b spool alert: {:?}", e))?;
    let tenant_b_bytes = std::fs::metadata(&tenant_b_path)?.len();

    let metrics_resp: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let health_resp: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let metrics_resp_hook = Arc::clone(&metrics_resp);
    let health_resp_hook = Arc::clone(&health_resp);
    let metrics_addr = cfg.metrics.prometheus_bind.clone();
    let health_addr = cfg.metrics.health_bind.clone();

    let _hook = RunHookGuardV1::install_v1(move |cycle_completed, _runtime, _cfg| {
        if cycle_completed != 1 {
            return;
        }
        *metrics_resp_hook.lock().unwrap() =
            Some(http_get_v1(&metrics_addr, "/metrics").expect("metrics response"));
        *health_resp_hook.lock().unwrap() =
            Some(http_get_v1(&health_addr, "/healthz").expect("health response"));
    });

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let metrics_resp = metrics_resp
        .lock()
        .unwrap()
        .clone()
        .expect("captured metrics response");
    let health_resp = health_resp
        .lock()
        .unwrap()
        .clone()
        .expect("captured health response");
    assert!(metrics_resp.starts_with("HTTP/1.1 200 OK"));
    assert!(metrics_resp.contains("sparx_run_cycles_completed_total 1"));
    assert!(metrics_resp.contains("sparx_run_devices_processed_total"));
    assert!(metrics_resp.contains("sparx_recovery_automated_replay_max_files_per_pass 128"));
    assert!(metrics_resp.contains("sparx_recovery_automated_replay_interval_seconds 3600"));
    assert!(metrics_resp.contains("sparx_recovery_spool_max_megabytes 2048"));
    assert!(metrics_resp.contains("sparx_recovery_spool_backlog_files 2"));
    assert!(metrics_resp.contains("sparx_recovery_spool_backlog_tenants 2"));
    assert!(metrics_resp.contains("sparx_recovery_spool_oldest_file_ts "));
    assert!(metrics_resp.contains("sparx_recovery_spool_oldest_age_seconds "));
    assert!(metrics_resp.contains("sparx_recovery_stale_backlog 0"));
    assert!(metrics_resp.contains("sparx_recovery_stale_backlog_tenants 0"));
    assert!(metrics_resp
        .contains("sparx_recovery_spool_backlog_files_by_tenant{tenant_id=\"tenant-a\"} 1"));
    assert!(metrics_resp.contains(&format!(
        "sparx_recovery_spool_backlog_bytes_by_tenant{{tenant_id=\"tenant-b\"}} {}",
        tenant_b_bytes
    )));
    assert!(metrics_resp
        .contains("sparx_recovery_spool_oldest_age_seconds_by_tenant{tenant_id=\"tenant-a\"} "));
    assert!(
        metrics_resp.contains("sparx_recovery_stale_backlog_by_tenant{tenant_id=\"tenant-a\"} 0")
    );
    assert!(metrics_resp.contains("sparx_recovery_spool_writes_total 0"));
    assert!(metrics_resp.contains("sparx_recovery_spool_replayed_total 0"));
    assert!(metrics_resp.contains("sparx_recovery_spool_replay_fail_total 0"));
    assert!(metrics_resp.contains("sparx_recovery_spool_drop_total 0"));
    assert!(metrics_resp.contains("sparx_recovery_automated_replay_attempts_total 0"));
    assert!(metrics_resp.contains("sparx_recovery_backlog_trend_direction 0"));
    assert!(metrics_resp
        .contains("sparx_recovery_backlog_trend_direction_by_tenant{tenant_id=\"tenant-a\"} 0"));
    assert!(metrics_resp
        .contains("sparx_recovery_backlog_trend_direction_by_tenant{tenant_id=\"tenant-b\"} 0"));
    assert!(metrics_resp.contains("sparx_recovery_history_start_counter_snapshot_ts "));
    assert!(metrics_resp.contains(
        "sparx_recovery_history_start_counter_snapshot_ts_by_tenant{tenant_id=\"tenant-a\"}"
    ));
    assert!(metrics_resp.contains("sparx_recovery_history_counter_snapshot_interval_seconds_by_tenant{tenant_id=\"tenant-b\"}"));
    assert!(metrics_resp
        .contains("sparx_recovery_previous_counter_snapshot_ts_by_tenant{tenant_id=\"tenant-a\"}"));
    assert!(metrics_resp
        .contains("sparx_recovery_last_counter_snapshot_ts_by_tenant{tenant_id=\"tenant-b\"}"));
    assert!(metrics_resp
        .contains("sparx_recovery_spool_write_rate_per_second_by_tenant{tenant_id=\"tenant-a\"}"));
    assert!(metrics_resp.contains(
        "sparx_recovery_automated_replay_attempt_rate_per_second_by_tenant{tenant_id=\"tenant-b\"}"
    ));
    assert!(metrics_resp.contains("sparx_vdrop_enabled 1"));
    assert!(metrics_resp.contains("sparx_vdrop_device_enabled 1"));
    assert!(metrics_resp.contains("sparx_vdrop_tenant_enabled 1"));
    assert!(metrics_resp.contains("sparx_vdrop_source_stream_enabled 0"));
    assert!(metrics_resp.contains("sparx_vdrop_evaluated_subjects_total"));
    assert!(metrics_resp.contains("sparx_vdrop_alerts_emitted_total"));
    assert!(metrics_resp.contains("sparx_vdrop_open_drop_subjects"));
    assert!(metrics_resp.contains("sparx_vdrop_source_stream_evaluated_subjects_total"));
    assert!(metrics_resp.contains("sparx_vdrop_source_stream_alerts_emitted_total"));
    assert!(health_resp.starts_with("HTTP/1.1 200 OK"));
    assert!(health_resp.contains("status: ok"));
    assert!(health_resp.contains("spool_backlog_files: 2"));
    assert!(health_resp.contains("spool_backlog_tenants: 2"));
    assert!(health_resp.contains("spool_oldest_file_ts: "));
    assert!(health_resp.contains("spool_oldest_age_s: "));
    assert!(health_resp.contains("stale_backlog: false"));
    assert!(health_resp.contains("stale_backlog_tenants: 0"));
    assert!(health_resp.contains("spool_backlog_tenant[0].tenant_id: tenant-a"));
    assert!(health_resp.contains("spool_backlog_tenant[0].oldest_file_ts: "));
    assert!(health_resp.contains("spool_backlog_tenant[0].oldest_age_s: "));
    assert!(health_resp.contains("spool_backlog_tenant[0].stale: false"));
    assert!(health_resp.contains("spool_backlog_tenant[0].backlog_trend_direction: unknown"));
    assert!(health_resp.contains("spool_backlog_tenant[0].previous_counter_snapshot_ts: null"));
    assert!(health_resp.contains("spool_backlog_tenant[0].history_start_counter_snapshot_ts: "));
    assert!(health_resp.contains("spool_backlog_tenant[0].history_counter_snapshot_interval_s: "));
    assert!(health_resp.contains("spool_backlog_tenant[0].history_spool_write_rate_per_s: "));
    assert!(health_resp.contains("spool_backlog_tenant[0].spool_replayed_rate_per_s: null"));
    assert!(health_resp.contains("spool_backlog_tenant[0].spool_write_rate_per_s: null"));
    assert!(
        health_resp.contains("spool_backlog_tenant[0].automated_replay_attempt_rate_per_s: null")
    );
    assert!(health_resp.contains("spool_backlog_tenant[1].tenant_id: tenant-b"));
    assert!(health_resp.contains("spool_backlog_tenant[1].backlog_trend_direction: unknown"));
    assert!(health_resp.contains("spool_writes_total: 0"));
    assert!(health_resp.contains("automated_replay_attempts_total: 0"));
    assert!(health_resp.contains("automated_replay_max_files_per_pass: 128"));
    assert!(health_resp.contains("automated_replay_interval_s: 3600"));
    assert!(health_resp.contains("spool_max_mb: 2048"));
    assert!(health_resp.contains("backlog_trend_direction: unknown"));
    assert!(health_resp.contains("previous_counter_snapshot_ts: "));
    assert!(health_resp.contains("last_counter_snapshot_ts: "));
    assert!(health_resp.contains("counter_snapshot_interval_s: "));
    assert!(health_resp.contains("history_start_counter_snapshot_ts: "));
    assert!(health_resp.contains("history_counter_snapshot_interval_s: "));
    assert!(health_resp.contains("history_spool_write_rate_per_s: "));
    assert!(health_resp.contains("spool_write_rate_per_s: "));
    assert!(health_resp.contains("spool_replayed_rate_per_s: "));
    assert!(health_resp.contains("spool_replay_fail_rate_per_s: "));
    assert!(health_resp.contains("automated_replay_attempt_rate_per_s: "));
    assert!(health_resp.contains("vdrop_enabled: true"));
    assert!(health_resp.contains("vdrop_device_enabled: true"));
    assert!(health_resp.contains("vdrop_tenant_enabled: true"));
    assert!(health_resp.contains("vdrop_source_stream_enabled: false"));
    assert!(health_resp.contains("vdrop_evaluated_subjects_total: "));
    assert!(health_resp.contains("vdrop_alerts_emitted_total: "));
    assert!(health_resp.contains("vdrop_source_stream_evaluated_subjects_total: "));
    assert!(health_resp.contains("vdrop_source_stream_alerts_emitted_total: "));

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("run_cycles_completed_total")?,
        Some(1)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("run_last_cycle_completed_ts")?
        .is_some());
    Ok(())
}

#[test]
fn run_does_not_bind_endpoints_when_disabled_v1() {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let mut cfg = temp_cfg_v1();
    cfg.metrics.prometheus_enabled = false;
    cfg.metrics.health_enabled = false;
    cfg.metrics.prometheus_bind = reserve_loopback_bind_v1();
    cfg.metrics.health_bind = reserve_loopback_bind_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log", "edge01.gz"]);

    let prometheus_addr = cfg.metrics.prometheus_bind.clone();
    let health_addr = cfg.metrics.health_bind.clone();
    let connect_results: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(Vec::new()));
    let connect_results_hook = Arc::clone(&connect_results);
    let _hook = RunHookGuardV1::install_v1(move |cycle_completed, _runtime, _cfg| {
        if cycle_completed != 1 {
            return;
        }
        let mut guard = connect_results_hook.lock().unwrap();
        guard.push(TcpStream::connect(&prometheus_addr).is_ok());
        guard.push(TcpStream::connect(&health_addr).is_ok());
    });

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);
    assert_eq!(*connect_results.lock().unwrap(), vec![false, false]);
}

#[test]
fn run_observability_endpoints_reject_wrong_path_and_method_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let mut cfg = temp_cfg_v1();
    cfg.metrics.prometheus_bind = reserve_loopback_bind_v1();
    cfg.metrics.health_bind = reserve_loopback_bind_v1();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let wrong_path_resp: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let wrong_path_resp_hook = Arc::clone(&wrong_path_resp);
    let method_resp: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let method_resp_hook = Arc::clone(&method_resp);
    let metrics_addr = cfg.metrics.prometheus_bind.clone();

    let _hook = RunHookGuardV1::install_v1(move |cycle_completed, _runtime, _cfg| {
        if cycle_completed != 1 {
            return;
        }
        *wrong_path_resp_hook.lock().unwrap() =
            Some(http_get_v1(&metrics_addr, "/wrong").expect("wrong-path response"));
        *method_resp_hook.lock().unwrap() =
            Some(http_request_v1(&metrics_addr, "POST", "/metrics").expect("method response"));
    });

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let wrong_path_resp = wrong_path_resp
        .lock()
        .unwrap()
        .clone()
        .expect("captured wrong-path response");
    let method_resp = method_resp
        .lock()
        .unwrap()
        .clone()
        .expect("captured method response");
    assert!(wrong_path_resp.starts_with("HTTP/1.1 404 Not Found"));
    assert!(wrong_path_resp.contains("not found\n"));
    assert!(method_resp.starts_with("HTTP/1.1 405 Method Not Allowed"));
    assert!(method_resp.contains("method not allowed\n"));
    Ok(())
}

#[test]
fn run_partial_observability_startup_failure_releases_started_listener_v1() {
    let _lock = run_test_lock_v1();

    let mut cfg = temp_cfg_v1();
    cfg.metrics.prometheus_bind = reserve_loopback_bind_v1();
    let blocker = TcpListener::bind("127.0.0.1:0").expect("health blocker");
    cfg.metrics.health_bind = blocker
        .local_addr()
        .expect("health blocker addr")
        .to_string();
    copy_fixture_device_v1(&cfg, "smoke", "edge01", &["edge01.log"]);

    let prometheus_addr = cfg.metrics.prometheus_bind.clone();
    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 1);
    let stderr = result.msg_stderr.expect("startup stderr");
    assert!(stderr.contains("run observability startup error"));
    assert!(stderr.contains("health endpoint"));

    drop(blocker);
    let rebound = TcpListener::bind(&prometheus_addr);
    assert!(
        rebound.is_ok(),
        "prometheus listener should have been released after partial startup failure"
    );
}

#[test]
fn run_replays_spooled_alerts_automatically_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let cfg = temp_cfg_v1();
    let alert = sample_spooled_alert_v1("tenant-a", "device-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let spool_path = write_spool_alert_v1(&cfg.sparx.data_root, &alert).unwrap();

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);
    assert!(!spool_path.exists());

    let out_path = jsonl_alert_path_v1(
        &cfg.sparx.alert_out_root,
        "tenant-a",
        "device-a",
        alert.window_start_ts,
        0,
    )
    .unwrap();
    let line = fs::read_to_string(out_path).unwrap();
    let value: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
    assert_eq!(
        value["alert_id"].as_str(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    Ok(())
}

#[test]
fn run_automated_replay_is_bounded_per_cycle_v1() -> Result<(), Box<dyn std::error::Error>> {
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "1");

    let mut cfg = temp_cfg_v1();
    cfg.output.automated_replay_max_files_per_pass = 1;
    for i in 0..3u32 {
        let alert_id = format!("{:032x}", i);
        let alert = sample_spooled_alert_v1("tenant-a", "device-a", &alert_id);
        write_spool_alert_v1(&cfg.sparx.data_root, &alert).unwrap();
    }

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let spool_root = Path::new(&cfg.sparx.data_root).join("spool").join("alerts");
    assert_eq!(count_spool_files_v1(&spool_root), 1);

    let remaining = Path::new(&cfg.sparx.data_root)
        .join("spool")
        .join("alerts")
        .join("tenant=tenant-a")
        .join(format!("spool_{}.json", format!("{:032x}", 2u32)));
    assert!(remaining.exists());

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_automated_replay_attempts_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_spool_replayed_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_spool_replay_fail_total")?,
        Some(0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_spool_writes_total")?,
        Some(0)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_last_automated_replay_attempt_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_last_automated_replay_replayed")?,
        Some(1.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_last_automated_replay_failed")?,
        Some(0.0)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_previous_snapshot_ts")?
        .is_some());
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_last_snapshot_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_previous_snapshot_backlog_files")?,
        Some(2.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_last_snapshot_backlog_files")?,
        Some(1.0)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_previous_counter_snapshot_ts")?
        .is_some());
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_last_counter_snapshot_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_previous_counter_snapshot_spool_replayed_total")?,
        Some(1)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_last_counter_snapshot_spool_replayed_total")?,
        Some(2)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_previous_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_last_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(2)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_history_start_counter_snapshot_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_history_start_counter_snapshot_spool_writes_total")?,
        Some(0)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_history_start_counter_snapshot_spool_replayed_total"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_history_start_counter_snapshot_spool_replay_fail_total"
        )?,
        Some(0)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_history_start_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(1)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_tenant_history_start_counter_snapshot_ts__tenant-a")?
        .is_some());
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_writes_total__tenant-a"
        )?,
        Some(0)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_replayed_total__tenant-a"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total__tenant-a"
        )?,
        Some(0)
    );
    assert_eq!(runtime.global_db_v1().read_metric_counter_v1("recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total__tenant-a")?, Some(1));
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_tenant_previous_snapshot_ts__tenant-a")?
        .is_some());
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_tenant_last_snapshot_ts__tenant-a")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_tenant_previous_snapshot_backlog_files__tenant-a")?,
        Some(2.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_tenant_last_snapshot_backlog_files__tenant-a")?,
        Some(1.0)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_tenant_previous_counter_snapshot_ts__tenant-a")?
        .is_some());
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_tenant_last_counter_snapshot_ts__tenant-a")?
        .is_some());
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_previous_counter_snapshot_spool_replayed_total__tenant-a"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_last_counter_snapshot_spool_replayed_total__tenant-a"
        )?,
        Some(2)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total__tenant-a"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_last_counter_snapshot_automated_replay_attempts_total__tenant-a"
        )?,
        Some(2)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_tenant_previous_counter_snapshot_ts__tenant-a")?
        .is_some());
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_tenant_last_counter_snapshot_ts__tenant-a")?
        .is_some());
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_previous_counter_snapshot_spool_replayed_total__tenant-a"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_last_counter_snapshot_spool_replayed_total__tenant-a"
        )?,
        Some(2)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total__tenant-a"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_last_counter_snapshot_automated_replay_attempts_total__tenant-a"
        )?,
        Some(2)
    );
    Ok(())
}

#[test]
fn run_automated_replay_interval_limits_cycle_attempts_v1() -> Result<(), Box<dyn std::error::Error>>
{
    let _lock = run_test_lock_v1();
    let _guard = EnvVarGuardV1::set_v1("SPARX_TEST_RUN_MAX_CYCLES", "2");

    let mut cfg = temp_cfg_v1();
    cfg.output.automated_replay_max_files_per_pass = 1;
    cfg.output.automated_replay_interval_s = 3600;
    for i in 0..3u32 {
        let alert_id = format!("{:032x}", i);
        let alert = sample_spooled_alert_v1("tenant-a", "device-a", &alert_id);
        write_spool_alert_v1(&cfg.sparx.data_root, &alert).unwrap();
    }

    let result = route_command_v1(
        &CommandV1::Run {
            migrate: MigrateModeV1::Auto,
        },
        &cfg,
    );
    assert_eq!(result.exit_code, 0, "stderr={:?}", result.msg_stderr);

    let spool_root = Path::new(&cfg.sparx.data_root).join("spool").join("alerts");
    assert_eq!(count_spool_files_v1(&spool_root), 1);

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_automated_replay_attempts_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_spool_replayed_total")?,
        Some(2)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_last_automated_replay_replayed")?,
        Some(1.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_previous_snapshot_backlog_files")?,
        Some(2.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_last_snapshot_backlog_files")?,
        Some(1.0)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_previous_counter_snapshot_ts")?
        .is_some());
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_last_counter_snapshot_ts")?
        .is_some());
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_previous_counter_snapshot_spool_replayed_total")?,
        Some(1)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_counter_v1("recovery_last_counter_snapshot_spool_replayed_total")?,
        Some(2)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_previous_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_last_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(2)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_history_start_counter_snapshot_ts")?
        .is_some());
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_history_start_counter_snapshot_spool_replayed_total"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_history_start_counter_snapshot_automated_replay_attempts_total"
        )?,
        Some(1)
    );
    assert!(runtime
        .global_db_v1()
        .read_metric_counter_v1("recovery_tenant_history_start_counter_snapshot_ts__tenant-a")?
        .is_some());
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_writes_total__tenant-a"
        )?,
        Some(0)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_replayed_total__tenant-a"
        )?,
        Some(1)
    );
    assert_eq!(
        runtime.global_db_v1().read_metric_counter_v1(
            "recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total__tenant-a"
        )?,
        Some(0)
    );
    assert_eq!(runtime.global_db_v1().read_metric_counter_v1("recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total__tenant-a")?, Some(1));
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_tenant_previous_snapshot_backlog_files__tenant-a")?,
        Some(2.0)
    );
    assert_eq!(
        runtime
            .global_db_v1()
            .read_metric_gauge_v1("recovery_tenant_last_snapshot_backlog_files__tenant-a")?,
        Some(1.0)
    );
    Ok(())
}
