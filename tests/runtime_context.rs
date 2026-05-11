// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use tempfile::tempdir;

use sparx::config::load::default_config_v1;
use sparx::db::{GlobalSchemaStateV1, GlobalTenantRecordV1, TenantSchemaStateV1};
use sparx::runtime::SparxRuntimeV1;

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

#[test]
fn runtime_bootstraps_storage_roots_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;

    assert!(runtime.layout_v1().data_root_v1().is_dir());
    assert!(runtime.layout_v1().global_db_path_v1().is_dir());
    assert!(runtime.layout_v1().tenant_db_root_v1().is_dir());
    assert!(runtime.layout_v1().alert_out_root_v1().is_dir());
    assert!(runtime.layout_v1().spool_root_v1().is_dir());
    assert_eq!(runtime.config_v1().sparx.data_root, cfg.sparx.data_root);
    Ok(())
}

#[test]
fn process_bookkeeping_roundtrip_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;

    let started = runtime.mark_process_start_v1(1_700_001_000, "edge-lab-01")?;
    assert_eq!(started.last_run_start_ts, Some(1_700_001_000));
    assert_eq!(started.last_run_end_ts, None);
    assert_eq!(started.last_run_exit_code, None);
    assert_eq!(started.last_run_host.as_deref(), Some("edge-lab-01"));

    let ended = runtime.mark_process_end_v1(1_700_001_120, 0)?;
    assert_eq!(ended.last_run_start_ts, Some(1_700_001_000));
    assert_eq!(ended.last_run_end_ts, Some(1_700_001_120));
    assert_eq!(ended.last_run_exit_code, Some(0));
    assert_eq!(ended.last_run_host.as_deref(), Some("edge-lab-01"));
    assert_eq!(ended, runtime.read_process_state_v1()?);
    Ok(())
}

#[test]
fn runtime_repo_helpers_and_tenant_access_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut runtime = SparxRuntimeV1::open_from_config_v1(&cfg)?;

    runtime.write_global_schema_state_v1(&GlobalSchemaStateV1 {
        version: 1,
        created_ts: 1_700_002_000,
        last_migrate_ts: 1_700_002_000,
    })?;
    assert_eq!(
        runtime.read_global_schema_state_v1()?,
        Some(GlobalSchemaStateV1 {
            version: 1,
            created_ts: 1_700_002_000,
            last_migrate_ts: 1_700_002_000,
        })
    );

    let paths = runtime.tenant_paths_v1("tenant-a");
    let record = GlobalTenantRecordV1 {
        tenant_id: "tenant-a".to_string(),
        created_ts: 1_700_002_100,
        last_seen_ts: 1_700_002_150,
        status: 1,
        tenant_root_rel: Some("tenant-a".to_string()),
        tenant_db_path: Some(paths.tenant_db_dir.clone()),
        alert_out_root: Some(paths.alert_out_dir.clone()),
    };
    runtime.upsert_tenant_record_v1(&record)?;
    runtime.set_tenant_active_index_v1("tenant-a", true)?;
    assert_eq!(Some(record), runtime.read_tenant_record_v1("tenant-a")?);
    assert_eq!(vec!["tenant-a".to_string()], runtime.list_active_tenants_v1()?);

    runtime.with_tenant_db_v1("tenant-a", 1_700_002_200, |db| {
        db.write_schema_state_v1(&TenantSchemaStateV1 {
            version: 7,
            created_ts: 1_700_002_200,
            last_migrate_ts: 1_700_002_200,
        })?;
        db.persist_sync_all_v1()
    })?;

    let tenant_version = runtime.with_tenant_db_v1("tenant-a", 1_700_002_201, |db| {
        let state = db.read_schema_state_v1()?;
        state
            .map(|s| Ok(s.version))
            .unwrap_or_else(|| Err(sparx::db::DbErrorV1::new_v1("missing tenant schema state")))
    })?;
    assert_eq!(7, tenant_version);
    assert_eq!(vec!["tenant-a".to_string()], runtime.list_open_tenant_ids_v1());

    let closed = runtime.close_idle_tenants_v1(1_700_002_240);
    assert_eq!(vec!["tenant-a".to_string()], closed);
    assert!(runtime.list_open_tenant_ids_v1().is_empty());
    Ok(())
}
