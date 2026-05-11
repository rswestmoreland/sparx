// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use tempfile::tempdir;

use sparx::db::{
    GlobalDbV1, GlobalMigrateJournalEntryV1, GlobalProcessStateV1, GlobalSchemaStateV1,
    GlobalTenantPurgeEntryV1, GlobalTenantRecordV1,
};

fn open_temp_global_db_v1() -> Result<GlobalDbV1, Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let path = tmp.path().join("global.db");
    // Keep the tempdir alive by leaking it for the duration of the test process.
    let _leaked = Box::leak(Box::new(tmp));
    Ok(GlobalDbV1::open_at_v1(path)?)
}

#[test]
fn schema_state_roundtrip_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_global_db_v1()?;

    let state = GlobalSchemaStateV1 {
        version: 1,
        created_ts: 1_700_000_100,
        last_migrate_ts: 1_700_000_200,
    };
    db.write_schema_state_v1(&state)?;

    assert_eq!(Some(state), db.read_schema_state_v1()?);
    Ok(())
}

#[test]
fn process_state_roundtrip_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_global_db_v1()?;

    let state = GlobalProcessStateV1 {
        last_run_start_ts: Some(1_700_000_100),
        last_run_end_ts: Some(1_700_000_200),
        last_run_exit_code: Some(6),
        last_run_host: Some("edge-lab-01".to_string()),
    };
    db.write_process_state_v1(&state)?;

    assert_eq!(state, db.read_process_state_v1()?);

    let cleared = GlobalProcessStateV1 {
        last_run_start_ts: None,
        last_run_end_ts: Some(1_700_000_300),
        last_run_exit_code: None,
        last_run_host: None,
    };
    db.write_process_state_v1(&cleared)?;
    assert_eq!(cleared, db.read_process_state_v1()?);
    Ok(())
}

#[test]
fn tenant_record_upsert_and_updates_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_global_db_v1()?;

    let tenant = GlobalTenantRecordV1 {
        tenant_id: "tenant-a".to_string(),
        created_ts: 1_700_000_100,
        last_seen_ts: 1_700_000_150,
        status: 0,
        tenant_root_rel: Some("tenant-a".to_string()),
        tenant_db_path: Some("/var/lib/sparx/tenants/tenant=tenant-a/tenant.db".to_string()),
        alert_out_root: Some("/var/lib/sparx/alerts/tenant=tenant-a".to_string()),
    };
    db.upsert_tenant_record_v1(&tenant)?;
    assert_eq!(Some(tenant.clone()), db.read_tenant_record_v1("tenant-a")?);

    db.set_tenant_status_v1("tenant-a", 1)?;
    db.set_tenant_last_seen_ts_v1("tenant-a", 1_700_000_250)?;

    let expected = GlobalTenantRecordV1 {
        status: 1,
        last_seen_ts: 1_700_000_250,
        ..tenant
    };
    assert_eq!(Some(expected), db.read_tenant_record_v1("tenant-a")?);
    Ok(())
}

#[test]
fn active_tenant_index_is_deterministic_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_global_db_v1()?;

    db.set_tenant_active_index_v1("tenant-b", true)?;
    db.set_tenant_active_index_v1("tenant-a", true)?;
    db.set_tenant_active_index_v1("tenant-c", true)?;

    assert_eq!(
        vec![
            "tenant-a".to_string(),
            "tenant-b".to_string(),
            "tenant-c".to_string(),
        ],
        db.list_active_tenants_v1()?
    );

    db.set_tenant_active_index_v1("tenant-b", false)?;
    assert_eq!(
        vec!["tenant-a".to_string(), "tenant-c".to_string()],
        db.list_active_tenants_v1()?
    );
    Ok(())
}

#[test]
fn tenant_purge_journal_scan_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_global_db_v1()?;

    db.append_tenant_purge_entry_v1("tenant-a", 100, "requested")?;
    db.append_tenant_purge_entry_v1("tenant-a", 200, "complete")?;

    let expected = vec![
        GlobalTenantPurgeEntryV1 {
            tenant_id: "tenant-a".to_string(),
            ts: 100,
            status: "requested".to_string(),
        },
        GlobalTenantPurgeEntryV1 {
            tenant_id: "tenant-a".to_string(),
            ts: 200,
            status: "complete".to_string(),
        },
    ];
    assert_eq!(expected, db.scan_tenant_purge_entries_v1("tenant-a")?);
    Ok(())
}

#[test]
fn known_tenant_ids_are_deterministic_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_global_db_v1()?;

    db.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: "tenant-b".to_string(),
        created_ts: 1,
        last_seen_ts: 2,
        status: 0,
        tenant_root_rel: None,
        tenant_db_path: None,
        alert_out_root: None,
    })?;
    db.upsert_tenant_record_v1(&GlobalTenantRecordV1 {
        tenant_id: "tenant-a".to_string(),
        created_ts: 1,
        last_seen_ts: 2,
        status: 0,
        tenant_root_rel: None,
        tenant_db_path: None,
        alert_out_root: None,
    })?;

    assert_eq!(vec!["tenant-a".to_string(), "tenant-b".to_string()], db.list_known_tenant_ids_v1()?);
    Ok(())
}

#[test]
fn migration_journal_scan_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_global_db_v1()?;

    db.append_migrate_journal_entry_v1(100, "create_schema", b"ok")?;
    db.append_migrate_journal_entry_v1(100, "seed_registry", b"ok-seed")?;
    db.append_migrate_journal_entry_v1(200, "upgrade_alerts", b"noop")?;

    let expected = vec![
        GlobalMigrateJournalEntryV1 {
            ts: 100,
            name: "create_schema".to_string(),
            payload: b"ok".to_vec(),
        },
        GlobalMigrateJournalEntryV1 {
            ts: 100,
            name: "seed_registry".to_string(),
            payload: b"ok-seed".to_vec(),
        },
        GlobalMigrateJournalEntryV1 {
            ts: 200,
            name: "upgrade_alerts".to_string(),
            payload: b"noop".to_vec(),
        },
    ];
    assert_eq!(expected, db.scan_migrate_journal_entries_v1()?);
    Ok(())
}

#[test]
fn global_metrics_roundtrip_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_global_db_v1()?;

    db.write_metric_counter_v1("run_cycles_completed_total", 9)?;
    db.write_metric_gauge_v1("run_last_cycle_devices_failed", 2.0)?;

    assert_eq!(Some(9), db.read_metric_counter_v1("run_cycles_completed_total")?);
    assert_eq!(Some(2.0), db.read_metric_gauge_v1("run_last_cycle_devices_failed")?);

    db.delete_metric_counter_v1("run_cycles_completed_total")?;
    db.delete_metric_gauge_v1("run_last_cycle_devices_failed")?;

    assert_eq!(None, db.read_metric_counter_v1("run_cycles_completed_total")?);
    assert_eq!(None, db.read_metric_gauge_v1("run_last_cycle_devices_failed")?);
    Ok(())
}
