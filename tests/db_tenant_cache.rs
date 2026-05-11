// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use tempfile::tempdir;

use sparx::config::load::default_config_v1;
use sparx::db::layout::filesystem_layout_v1;
use sparx::db::{TenantDbCacheConfigV1, TenantDbCacheV1};

fn temp_cfg_v1() -> sparx::config::ConfigV1 {
    let tmp = tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let _leaked = Box::leak(Box::new(tmp));

    let mut cfg = default_config_v1();
    cfg.sparx.data_root = root.join("state").display().to_string();
    cfg.sparx.tenant_root = root.join("tenants").display().to_string();
    cfg.sparx.global_db_path = root.join("state/global.db").display().to_string();
    cfg.sparx.tenant_db_root = root.join("state/tenants").display().to_string();
    cfg.sparx.alert_out_root = root.join("state/alerts").display().to_string();
    cfg.storage.tenant_db_max_open = 2;
    cfg.storage.tenant_db_idle_close_s = 60;
    cfg
}

#[test]
fn open_under_cap_and_reuse_handle_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let layout = filesystem_layout_v1(&cfg);
    let mut cache = TenantDbCacheV1::new_v1(
        layout,
        TenantDbCacheConfigV1 {
            max_open: 2,
            idle_close_s: 60,
        },
    );

    cache.with_tenant_db_v1("tenant-b", 100, |db| {
        db.write_schema_state_v1(&sparx::db::TenantSchemaStateV1 {
            version: 1,
            created_ts: 100,
            last_migrate_ts: 100,
        })
    })?;
    cache.with_tenant_db_v1("tenant-a", 101, |db| {
        db.write_schema_state_v1(&sparx::db::TenantSchemaStateV1 {
            version: 1,
            created_ts: 101,
            last_migrate_ts: 101,
        })
    })?;
    assert_eq!(2, cache.open_count_v1());
    assert_eq!(vec!["tenant-a".to_string(), "tenant-b".to_string()], cache.list_open_tenant_ids_v1());

    let seq_before = cache
        .snapshot_v1()
        .into_iter()
        .find(|e| e.tenant_id == "tenant-a")
        .map(|e| e.last_touch_seq)
        .ok_or("missing tenant-a")?;
    cache.with_tenant_db_v1("tenant-a", 102, |db| {
        let state = db.read_schema_state_v1()?;
        if state.is_none() {
            return Err(sparx::db::DbErrorV1::new_v1("expected schema state"));
        }
        Ok(())
    })?;
    let seq_after = cache
        .snapshot_v1()
        .into_iter()
        .find(|e| e.tenant_id == "tenant-a")
        .map(|e| e.last_touch_seq)
        .ok_or("missing tenant-a after reuse")?;
    assert!(seq_after > seq_before);
    Ok(())
}

#[test]
fn lru_eviction_is_deterministic_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let layout = filesystem_layout_v1(&cfg);
    let mut cache = TenantDbCacheV1::new_v1(
        layout,
        TenantDbCacheConfigV1 {
            max_open: 2,
            idle_close_s: 60,
        },
    );

    cache.with_tenant_db_v1("tenant-a", 100, |_db| Ok(()))?;
    cache.with_tenant_db_v1("tenant-b", 101, |_db| Ok(()))?;
    cache.with_tenant_db_v1("tenant-a", 102, |_db| Ok(()))?;
    cache.with_tenant_db_v1("tenant-c", 103, |_db| Ok(()))?;

    assert_eq!(2, cache.open_count_v1());
    assert_eq!(vec!["tenant-a".to_string(), "tenant-c".to_string()], cache.list_open_tenant_ids_v1());
    assert!(!cache.contains_v1("tenant-b"));
    Ok(())
}

#[test]
fn idle_close_and_safe_reopen_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let layout = filesystem_layout_v1(&cfg);
    let mut cache = TenantDbCacheV1::new_v1(
        layout,
        TenantDbCacheConfigV1 {
            max_open: 3,
            idle_close_s: 10,
        },
    );

    cache.with_tenant_db_v1("tenant-a", 100, |db| {
        db.write_schema_state_v1(&sparx::db::TenantSchemaStateV1 {
            version: 9,
            created_ts: 100,
            last_migrate_ts: 100,
        })?;
        db.persist_sync_all_v1()
    })?;
    cache.with_tenant_db_v1("tenant-b", 105, |_db| Ok(()))?;

    let closed = cache.close_idle_v1(110);
    assert_eq!(vec!["tenant-a".to_string()], closed);
    assert!(!cache.contains_v1("tenant-a"));
    assert!(cache.contains_v1("tenant-b"));

    let reopened_version = cache.with_tenant_db_v1("tenant-a", 111, |db| {
        let state = db.read_schema_state_v1()?;
        state
            .map(|s| Ok(s.version))
            .unwrap_or_else(|| Err(sparx::db::DbErrorV1::new_v1("missing schema state after reopen")))
    })?;
    assert_eq!(9, reopened_version);
    Ok(())
}

#[test]
fn explicit_close_for_purge_and_close_all_v1() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = temp_cfg_v1();
    let mut cache = TenantDbCacheV1::from_config_v1(&cfg);

    cache.with_tenant_db_v1("tenant-a", 100, |_db| Ok(()))?;
    cache.with_tenant_db_v1("tenant-b", 100, |_db| Ok(()))?;
    assert!(cache.close_tenant_v1("tenant-a"));
    assert!(!cache.contains_v1("tenant-a"));
    assert!(!cache.close_tenant_v1("tenant-a"));

    let closed_all = cache.close_all_v1();
    assert_eq!(vec!["tenant-b".to_string()], closed_all);
    assert_eq!(0, cache.open_count_v1());
    Ok(())
}

#[test]
fn max_open_zero_fails_closed_v1() {
    let cfg = temp_cfg_v1();
    let layout = filesystem_layout_v1(&cfg);
    let mut cache = TenantDbCacheV1::new_v1(
        layout,
        TenantDbCacheConfigV1 {
            max_open: 0,
            idle_close_s: 60,
        },
    );

    let err = cache
        .with_tenant_db_v1("tenant-a", 100, |_db| Ok(()))
        .expect_err("expected max_open=0 failure");
    assert!(err.msg.contains("max_open is 0"));
}
