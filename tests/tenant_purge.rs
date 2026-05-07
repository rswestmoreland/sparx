use std::fs;

use tempfile::tempdir;

use sparx::cli::CommandV1;
use sparx::cli::route::route_command_v1;
use sparx::config::load::default_config_v1;
use sparx::db::{GlobalTenantPurgeEntryV1, GlobalTenantRecordV1, TenantSchemaStateV1};
use sparx::runtime::{SparxRuntimeV1, TenantPurgeOutcomeKindV1};

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
    cfg.storage.tenant_db_max_open = 2;
    cfg.storage.tenant_db_idle_close_s = 30;
    cfg
}

fn seed_tenant_record_v1(
    runtime: &SparxRuntimeV1,
    tenant_id: &str,
    status: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let paths = runtime.tenant_paths_v1(tenant_id);
    runtime.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: tenant_id.to_string(),
        created_ts: 1_700_010_000,
        last_seen_ts: 1_700_010_100,
        status,
        tenant_root_rel: Some(tenant_id.to_string()),
        tenant_db_path: Some(paths.tenant_db_dir),
        alert_out_root: Some(paths.alert_out_dir),
    })?;
    runtime.set_tenant_active_index_v1(tenant_id, true)?;
    Ok(())
}

#[test]
fn tenant_purge_empty_tenant_success_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    seed_tenant_record_v1(&runtime, "tenant-a", 2)?;

    let result = runtime.purge_tenant_v1("tenant-a", false, 1000)?;
    assert_eq!(TenantPurgeOutcomeKindV1::Success, result.outcome);
    assert_eq!(Some(2), result.status_before);
    assert_eq!(Some(3), result.status_after);
    assert!(result.deleted_db_dir);
    assert!(result.deleted_alert_dir);
    assert!(result.deleted_spool_dir);
    assert_eq!(
        vec![
            "requested".to_string(),
            "db_deleted".to_string(),
            "alerts_deleted".to_string(),
            "spool_deleted".to_string(),
            "complete".to_string(),
        ],
        result.journal_entries
    );
    assert!(runtime.list_active_tenants_v1()?.is_empty());
    assert_eq!(Some(3), runtime.read_tenant_record_v1("tenant-a")?.map(|r| r.status));
    assert_eq!(
        vec![
            GlobalTenantPurgeEntryV1 {
                tenant_id: "tenant-a".to_string(),
                ts: 1000,
                status: "requested".to_string(),
            },
            GlobalTenantPurgeEntryV1 {
                tenant_id: "tenant-a".to_string(),
                ts: 1001,
                status: "db_deleted".to_string(),
            },
            GlobalTenantPurgeEntryV1 {
                tenant_id: "tenant-a".to_string(),
                ts: 1002,
                status: "alerts_deleted".to_string(),
            },
            GlobalTenantPurgeEntryV1 {
                tenant_id: "tenant-a".to_string(),
                ts: 1003,
                status: "spool_deleted".to_string(),
            },
            GlobalTenantPurgeEntryV1 {
                tenant_id: "tenant-a".to_string(),
                ts: 1004,
                status: "complete".to_string(),
            },
        ],
        runtime.scan_tenant_purge_entries_v1("tenant-a")?
    );
    Ok(())
}

#[test]
fn tenant_purge_populated_closes_handle_and_preserves_other_tenants_v1(
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    seed_tenant_record_v1(&runtime, "tenant-a", 2)?;
    seed_tenant_record_v1(&runtime, "tenant-b", 2)?;

    runtime.with_tenant_db_v1("tenant-a", 1100, |db| {
        db.write_schema_state_v1(&TenantSchemaStateV1 {
            version: 1,
            created_ts: 1100,
            last_migrate_ts: 1100,
        })?;
        db.persist_sync_all_v1()
    })?;
    runtime.with_tenant_db_v1("tenant-b", 1101, |db| {
        db.write_schema_state_v1(&TenantSchemaStateV1 {
            version: 2,
            created_ts: 1101,
            last_migrate_ts: 1101,
        })?;
        db.persist_sync_all_v1()
    })?;

    let paths_a = runtime.tenant_paths_v1("tenant-a");
    let paths_b = runtime.tenant_paths_v1("tenant-b");
    fs::create_dir_all(&paths_a.alert_out_dir)?;
    fs::create_dir_all(&paths_a.spool_dir)?;
    fs::write(format!("{}/alert.jsonl", paths_a.alert_out_dir), b"{}\n")?;
    fs::write(format!("{}/spool.json", paths_a.spool_dir), b"{}\n")?;
    fs::create_dir_all(&paths_b.alert_out_dir)?;
    fs::create_dir_all(&paths_b.spool_dir)?;
    fs::write(format!("{}/keep.jsonl", paths_b.alert_out_dir), b"{}\n")?;

    let result = runtime.purge_tenant_v1("tenant-a", false, 1200)?;
    assert_eq!(TenantPurgeOutcomeKindV1::Success, result.outcome);
    assert!(result.closed_tenant_handle);
    assert!(!std::path::Path::new(&paths_a.tenant_db_dir).exists());
    assert!(!std::path::Path::new(&paths_a.alert_out_dir).exists());
    assert!(!std::path::Path::new(&paths_a.spool_dir).exists());
    assert!(std::path::Path::new(&paths_b.tenant_db_dir).exists());
    assert!(std::path::Path::new(&paths_b.alert_out_dir).exists());
    assert!(std::path::Path::new(&paths_b.spool_dir).exists());
    assert_eq!(Some(2), runtime.read_tenant_record_v1("tenant-b")?.map(|r| r.status));
    Ok(())
}

#[test]
fn tenant_purge_nonexistent_and_rejected_status_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;

    let missing = runtime.purge_tenant_v1("missing", false, 1300)?;
    assert_eq!(TenantPurgeOutcomeKindV1::TenantNotFound, missing.outcome);

    seed_tenant_record_v1(&runtime, "tenant-a", 0)?;
    let rejected = runtime.purge_tenant_v1("tenant-a", false, 1301)?;
    assert_eq!(TenantPurgeOutcomeKindV1::RejectedStatus, rejected.outcome);
    assert_eq!(Some(0), rejected.status_before);
    assert_eq!(Some(0), rejected.status_after);
    assert!(rejected.failure_details[0].contains("terminating (2) is required"));
    Ok(())
}

#[test]
fn tenant_purge_partial_and_route_exit_codes_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    seed_tenant_record_v1(&runtime, "tenant-a", 2)?;
    let paths = runtime.tenant_paths_v1("tenant-a");
    drop(runtime);

    fs::create_dir_all(&paths.tenant_db_dir)?;
    fs::create_dir_all(&paths.alert_out_dir)?;
    let spool_parent = std::path::Path::new(&paths.spool_dir)
        .parent()
        .expect("spool parent")
        .to_path_buf();
    fs::create_dir_all(&spool_parent)?;
    fs::write(&paths.spool_dir, b"not a directory")?;

    let r = route_command_v1(
        &CommandV1::TenantPurge {
            tenant_id: "tenant-a".to_string(),
            force: false,
        },
        &cfg,
    );
    assert_eq!(6, r.exit_code);
    assert!(r.msg_stdout.unwrap().contains("tenant purge partial"));
    assert!(r.msg_stderr.unwrap().contains("exists but is not a directory"));

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(Some(2), runtime.read_tenant_record_v1("tenant-a")?.map(|r| r.status));
    assert!(!std::path::Path::new(&paths.tenant_db_dir).exists());
    assert!(!std::path::Path::new(&paths.alert_out_dir).exists());
    assert!(std::path::Path::new(&paths.spool_dir).exists());
    Ok(())
}

#[test]
fn tenant_purge_all_delete_failures_return_io_error_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    seed_tenant_record_v1(&runtime, "tenant-a", 2)?;
    let paths = runtime.tenant_paths_v1("tenant-a");
    drop(runtime);

    fs::create_dir_all(
        std::path::Path::new(&paths.tenant_db_dir)
            .parent()
            .expect("tenant db parent"),
    )?;
    fs::create_dir_all(
        std::path::Path::new(&paths.alert_out_dir)
            .parent()
            .expect("alert parent"),
    )?;
    fs::create_dir_all(
        std::path::Path::new(&paths.spool_dir)
            .parent()
            .expect("spool parent"),
    )?;
    fs::write(&paths.tenant_db_dir, b"not a directory")?;
    fs::write(&paths.alert_out_dir, b"not a directory")?;
    fs::write(&paths.spool_dir, b"not a directory")?;

    let r = route_command_v1(
        &CommandV1::TenantPurge {
            tenant_id: "tenant-a".to_string(),
            force: false,
        },
        &cfg,
    );
    assert_eq!(3, r.exit_code);
    assert!(r.msg_stderr.unwrap().contains("tenant purge incomplete"));
    Ok(())
}
