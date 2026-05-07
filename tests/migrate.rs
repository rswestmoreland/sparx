use tempfile::tempdir;

use sparx::cli::route::route_command_v1;
use sparx::cli::CommandV1;
use sparx::config::load::default_config_v1;
use sparx::db::{GlobalSchemaStateV1, GlobalTenantRecordV1, TenantMigrateJournalEntryV1, TenantSchemaStateV1};
use sparx::runtime::{
    SchemaMigrateOutcomeKindV1, SparxRuntimeV1, GLOBAL_SCHEMA_VERSION_CURRENT_V1,
    TENANT_SCHEMA_VERSION_CURRENT_V1,
};

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
    cfg.storage.tenant_db_max_open = 4;
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
        created_ts: 1_700_020_000,
        last_seen_ts: 1_700_020_100,
        status,
        tenant_root_rel: Some(tenant_id.to_string()),
        tenant_db_path: Some(paths.tenant_db_dir),
        alert_out_root: Some(paths.alert_out_dir),
    })?;
    runtime.set_tenant_active_index_v1(tenant_id, status == 0)?;
    Ok(())
}

#[test]
fn migrate_current_schema_is_noop_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;

    runtime.write_global_schema_state_v1(&GlobalSchemaStateV1 {
        version: GLOBAL_SCHEMA_VERSION_CURRENT_V1,
        created_ts: 100,
        last_migrate_ts: 100,
    })?;
    seed_tenant_record_v1(&runtime, "tenant-a", 0)?;
    runtime.with_tenant_db_v1("tenant-a", 101, |db| {
        db.write_schema_state_v1(&TenantSchemaStateV1 {
            version: TENANT_SCHEMA_VERSION_CURRENT_V1,
            created_ts: 101,
            last_migrate_ts: 101,
        })
    })?;

    let global = runtime.migrate_global_schema_v1(200)?;
    let tenant = runtime.migrate_tenant_schema_v1("tenant-a", 300)?;

    assert_eq!(SchemaMigrateOutcomeKindV1::NoopCurrent, global.outcome);
    assert_eq!(SchemaMigrateOutcomeKindV1::NoopCurrent, tenant.outcome);
    assert!(global.journal_entries.is_empty());
    assert!(tenant.journal_entries.is_empty());
    assert!(runtime.scan_global_migrate_journal_entries_v1()?.is_empty());
    let tenant_entries = runtime.with_tenant_db_v1("tenant-a", 301, |db| db.scan_migrate_journal_entries_v1())?;
    assert!(tenant_entries.is_empty());
    Ok(())
}

#[test]
fn migrate_missing_schema_initializes_global_and_tenant_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    seed_tenant_record_v1(&runtime, "tenant-a", 0)?;

    let global = runtime.migrate_global_schema_v1(1000)?;
    let tenant = runtime.migrate_tenant_schema_v1("tenant-a", 2000)?;

    assert_eq!(SchemaMigrateOutcomeKindV1::Initialized, global.outcome);
    assert_eq!(SchemaMigrateOutcomeKindV1::Initialized, tenant.outcome);
    assert_eq!(Some(GLOBAL_SCHEMA_VERSION_CURRENT_V1), runtime.read_global_schema_state_v1()?.map(|s| s.version));
    let tenant_state = runtime.with_tenant_db_v1("tenant-a", 2001, |db| db.read_schema_state_v1())?;
    assert_eq!(Some(TENANT_SCHEMA_VERSION_CURRENT_V1), tenant_state.map(|s| s.version));

    let global_entries = runtime.scan_global_migrate_journal_entries_v1()?;
    assert_eq!(2, global_entries.len());
    assert_eq!("global_init_schema", global_entries[0].name);
    assert_eq!("tenant_schema_init", global_entries[1].name);

    let tenant_entries = runtime.with_tenant_db_v1("tenant-a", 2002, |db| db.scan_migrate_journal_entries_v1())?;
    assert_eq!(
        vec![TenantMigrateJournalEntryV1 {
            ts: 2000,
            name: "init_schema".to_string(),
            payload: b"ok from=none to=1".to_vec(),
        }],
        tenant_entries
    );
    Ok(())
}

#[test]
fn migrate_tenant_downgrade_refusal_disables_tenant_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    runtime.write_global_schema_state_v1(&GlobalSchemaStateV1 {
        version: GLOBAL_SCHEMA_VERSION_CURRENT_V1,
        created_ts: 100,
        last_migrate_ts: 100,
    })?;
    seed_tenant_record_v1(&runtime, "tenant-a", 0)?;
    runtime.with_tenant_db_v1("tenant-a", 101, |db| {
        db.write_schema_state_v1(&TenantSchemaStateV1 {
            version: TENANT_SCHEMA_VERSION_CURRENT_V1 + 1,
            created_ts: 101,
            last_migrate_ts: 101,
        })
    })?;

    let tenant = runtime.migrate_tenant_schema_v1("tenant-a", 3000)?;
    assert_eq!(SchemaMigrateOutcomeKindV1::RefusedDowngrade, tenant.outcome);
    assert_eq!(Some(1), runtime.read_tenant_record_v1("tenant-a")?.map(|r| r.status));
    assert!(runtime.list_active_tenants_v1()?.is_empty());
    let tenant_entries = runtime.with_tenant_db_v1("tenant-a", 3001, |db| db.scan_migrate_journal_entries_v1())?;
    assert!(tenant_entries.is_empty());
    let global_entries = runtime.scan_global_migrate_journal_entries_v1()?;
    assert_eq!(1, global_entries.len());
    assert_eq!("tenant_schema_refused_downgrade", global_entries[0].name);
    Ok(())
}

#[test]
fn migrate_global_downgrade_refusal_route_exits_one_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    runtime.write_global_schema_state_v1(&GlobalSchemaStateV1 {
        version: GLOBAL_SCHEMA_VERSION_CURRENT_V1 + 1,
        created_ts: 100,
        last_migrate_ts: 100,
    })?;
    drop(runtime);

    let r = route_command_v1(&CommandV1::MigrateAll, &cfg);
    assert_eq!(1, r.exit_code);
    assert!(r.msg_stdout.unwrap().contains("outcome: refused_downgrade"));
    assert!(r.msg_stderr.unwrap().contains("global schema version 2 is newer than binary schema 1"));
    Ok(())
}

#[test]
fn migrate_all_is_deterministic_and_skips_terminated_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    seed_tenant_record_v1(&runtime, "tenant-b", 0)?;
    seed_tenant_record_v1(&runtime, "tenant-a", 0)?;
    seed_tenant_record_v1(&runtime, "tenant-c", 3)?;

    let result = runtime.migrate_all_schemas_v1(4000)?;
    assert_eq!(SchemaMigrateOutcomeKindV1::Initialized, result.global.outcome);
    assert_eq!(vec!["tenant-a", "tenant-b", "tenant-c"], result.tenants.iter().map(|t| t.tenant_id.as_str()).collect::<Vec<_>>());
    assert_eq!(SchemaMigrateOutcomeKindV1::Initialized, result.tenants[0].outcome);
    assert_eq!(SchemaMigrateOutcomeKindV1::Initialized, result.tenants[1].outcome);
    assert_eq!(SchemaMigrateOutcomeKindV1::SkippedTerminated, result.tenants[2].outcome);
    Ok(())
}

#[test]
fn migrate_all_partial_route_returns_exit_six_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    seed_tenant_record_v1(&runtime, "tenant-a", 0)?;
    seed_tenant_record_v1(&runtime, "tenant-b", 0)?;
    runtime.with_tenant_db_v1("tenant-b", 100, |db| {
        db.write_schema_state_v1(&TenantSchemaStateV1 {
            version: TENANT_SCHEMA_VERSION_CURRENT_V1 + 1,
            created_ts: 100,
            last_migrate_ts: 100,
        })
    })?;
    drop(runtime);

    let r = route_command_v1(&CommandV1::MigrateAll, &cfg);
    assert_eq!(6, r.exit_code);
    let stdout = r.msg_stdout.unwrap();
    let stderr = r.msg_stderr.unwrap();
    assert!(stdout.contains("tenant_id: tenant-a outcome: initialized"));
    assert!(stdout.contains("tenant_id: tenant-b outcome: refused_downgrade"));
    assert!(stderr.contains("tenant-b: tenant tenant-b schema version 2 is newer than binary schema 1"));

    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;
    assert_eq!(Some(1), runtime.read_tenant_record_v1("tenant-b")?.map(|r| r.status));
    Ok(())
}

#[test]
fn migrate_tenant_missing_route_returns_exit_one_v1() {
    let cfg = temp_cfg_v1();
    let r = route_command_v1(
        &CommandV1::MigrateTenant {
            tenant_id: "missing".to_string(),
        },
        &cfg,
    );
    assert_eq!(1, r.exit_code);
    assert!(r.msg_stdout.unwrap().contains("tenant_outcome: tenant_not_found"));
    assert!(r.msg_stderr.unwrap().contains("tenant not found: missing"));
}
