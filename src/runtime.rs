// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Runtime context and repository helpers.
//
// Holds effective config, filesystem layout, global DB, tenant DB cache, global
// and tenant repository passthroughs, and process metadata helpers.

use std::fs;
use std::path::Path;

use crate::config::ConfigV1;
use crate::db::layout::{filesystem_layout_v1, FilesystemLayoutV1};
use crate::db::{
    DbErrorV1, GlobalDbV1, GlobalMigrateJournalEntryV1, GlobalProcessStateV1, GlobalSchemaStateV1,
    GlobalTenantPurgeEntryV1, GlobalTenantRecordV1, TenantDbCacheV1, TenantDbV1,
    TenantSchemaStateV1,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeTenantPathsV1 {
    pub tenant_id: String,
    pub tenant_db_dir: String,
    pub alert_out_dir: String,
    pub spool_dir: String,
    pub policy_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantPurgeOutcomeKindV1 {
    Success,
    Partial,
    TenantNotFound,
    RejectedStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantPurgeResultV1 {
    pub tenant_id: String,
    pub outcome: TenantPurgeOutcomeKindV1,
    pub status_before: Option<u8>,
    pub status_after: Option<u8>,
    pub closed_tenant_handle: bool,
    pub deleted_db_dir: bool,
    pub deleted_alert_dir: bool,
    pub deleted_spool_dir: bool,
    pub journal_entries: Vec<String>,
    pub failure_details: Vec<String>,
}

pub const GLOBAL_SCHEMA_VERSION_CURRENT_V1: u32 = 1;
pub const TENANT_SCHEMA_VERSION_CURRENT_V1: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaMigrateOutcomeKindV1 {
    NoopCurrent,
    Initialized,
    Upgraded,
    RefusedDowngrade,
    TenantNotFound,
    SkippedTerminated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalSchemaMigrateResultV1 {
    pub outcome: SchemaMigrateOutcomeKindV1,
    pub version_before: Option<u32>,
    pub version_after: Option<u32>,
    pub journal_entries: Vec<String>,
    pub failure_details: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantSchemaMigrateResultV1 {
    pub tenant_id: String,
    pub outcome: SchemaMigrateOutcomeKindV1,
    pub status_before: Option<u8>,
    pub status_after: Option<u8>,
    pub version_before: Option<u32>,
    pub version_after: Option<u32>,
    pub journal_entries: Vec<String>,
    pub failure_details: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrateAllResultV1 {
    pub global: GlobalSchemaMigrateResultV1,
    pub tenants: Vec<TenantSchemaMigrateResultV1>,
}

pub struct SparxRuntimeV1 {
    cfg: ConfigV1,
    layout: FilesystemLayoutV1,
    global_db: GlobalDbV1,
    tenant_cache: TenantDbCacheV1,
}

impl SparxRuntimeV1 {
    pub fn open_from_config_v1(cfg: &ConfigV1) -> Result<Self, DbErrorV1> {
        let cfg_owned = cfg.clone();
        let layout = filesystem_layout_v1(&cfg_owned);
        Self::ensure_storage_roots_from_layout_v1(&layout)?;
        let global_db = GlobalDbV1::open_from_layout_v1(&layout)?;
        let tenant_cache = TenantDbCacheV1::from_config_v1(&cfg_owned);
        Ok(Self {
            cfg: cfg_owned,
            layout,
            global_db,
            tenant_cache,
        })
    }

    pub fn ensure_storage_roots_v1(&self) -> Result<(), DbErrorV1> {
        Self::ensure_storage_roots_from_layout_v1(&self.layout)
    }

    fn ensure_storage_roots_from_layout_v1(layout: &FilesystemLayoutV1) -> Result<(), DbErrorV1> {
        let roots = vec![
            layout.data_root_v1(),
            layout.global_db_path_v1(),
            layout.tenant_db_root_v1(),
            layout.alert_out_root_v1(),
            layout.spool_root_v1(),
        ];
        for root in roots {
            fs::create_dir_all(&root).map_err(|e| {
                DbErrorV1::new_v1(format!(
                    "failed to create runtime storage root {}: {}",
                    root.display(),
                    e
                ))
            })?;
        }
        Ok(())
    }

    pub fn config_v1(&self) -> &ConfigV1 {
        &self.cfg
    }

    pub fn layout_v1(&self) -> &FilesystemLayoutV1 {
        &self.layout
    }

    pub fn global_db_v1(&self) -> &GlobalDbV1 {
        &self.global_db
    }

    pub fn tenant_cache_v1(&self) -> &TenantDbCacheV1 {
        &self.tenant_cache
    }

    pub fn tenant_cache_mut_v1(&mut self) -> &mut TenantDbCacheV1 {
        &mut self.tenant_cache
    }

    pub fn tenant_paths_v1(&self, tenant_id: &str) -> RuntimeTenantPathsV1 {
        RuntimeTenantPathsV1 {
            tenant_id: tenant_id.to_string(),
            tenant_db_dir: self
                .layout
                .tenant_db_dir_v1(tenant_id)
                .display()
                .to_string(),
            alert_out_dir: self
                .layout
                .tenant_alert_dir_v1(tenant_id)
                .display()
                .to_string(),
            spool_dir: self
                .layout
                .tenant_spool_dir_v1(tenant_id)
                .display()
                .to_string(),
            policy_path: self
                .layout
                .tenant_policy_path_v1(tenant_id)
                .display()
                .to_string(),
        }
    }

    pub fn read_global_schema_state_v1(&self) -> Result<Option<GlobalSchemaStateV1>, DbErrorV1> {
        self.global_db.read_schema_state_v1()
    }

    pub fn write_global_schema_state_v1(
        &self,
        state: &GlobalSchemaStateV1,
    ) -> Result<(), DbErrorV1> {
        self.global_db.write_schema_state_v1(state)
    }

    pub fn read_process_state_v1(&self) -> Result<GlobalProcessStateV1, DbErrorV1> {
        self.global_db.read_process_state_v1()
    }

    pub fn write_process_state_v1(&self, state: &GlobalProcessStateV1) -> Result<(), DbErrorV1> {
        self.global_db.write_process_state_v1(state)
    }

    pub fn mark_process_start_v1(
        &self,
        now_ts: i64,
        host: &str,
    ) -> Result<GlobalProcessStateV1, DbErrorV1> {
        let mut state = self.global_db.read_process_state_v1()?;
        state.last_run_start_ts = Some(now_ts);
        state.last_run_end_ts = None;
        state.last_run_exit_code = None;
        state.last_run_host = Some(host.to_string());
        self.global_db.write_process_state_v1(&state)?;
        Ok(state)
    }

    pub fn mark_process_end_v1(
        &self,
        now_ts: i64,
        exit_code: i32,
    ) -> Result<GlobalProcessStateV1, DbErrorV1> {
        let mut state = self.global_db.read_process_state_v1()?;
        state.last_run_end_ts = Some(now_ts);
        state.last_run_exit_code = Some(exit_code);
        self.global_db.write_process_state_v1(&state)?;
        Ok(state)
    }

    pub fn upsert_tenant_record_v1(&self, record: &GlobalTenantRecordV1) -> Result<(), DbErrorV1> {
        self.global_db.upsert_tenant_record_v1(record)
    }

    pub fn read_tenant_record_v1(
        &self,
        tenant_id: &str,
    ) -> Result<Option<GlobalTenantRecordV1>, DbErrorV1> {
        self.global_db.read_tenant_record_v1(tenant_id)
    }

    pub fn set_tenant_status_v1(&self, tenant_id: &str, status: u8) -> Result<(), DbErrorV1> {
        self.global_db.set_tenant_status_v1(tenant_id, status)
    }

    pub fn set_tenant_last_seen_ts_v1(
        &self,
        tenant_id: &str,
        last_seen_ts: i64,
    ) -> Result<(), DbErrorV1> {
        self.global_db
            .set_tenant_last_seen_ts_v1(tenant_id, last_seen_ts)
    }

    pub fn set_tenant_active_index_v1(
        &self,
        tenant_id: &str,
        is_active: bool,
    ) -> Result<(), DbErrorV1> {
        self.global_db
            .set_tenant_active_index_v1(tenant_id, is_active)
    }

    pub fn list_active_tenants_v1(&self) -> Result<Vec<String>, DbErrorV1> {
        self.global_db.list_active_tenants_v1()
    }

    pub fn list_known_tenant_ids_v1(&self) -> Result<Vec<String>, DbErrorV1> {
        self.global_db.list_known_tenant_ids_v1()
    }

    pub fn append_tenant_purge_entry_v1(
        &self,
        tenant_id: &str,
        ts: i64,
        status: &str,
    ) -> Result<(), DbErrorV1> {
        self.global_db
            .append_tenant_purge_entry_v1(tenant_id, ts, status)
    }

    pub fn scan_tenant_purge_entries_v1(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<GlobalTenantPurgeEntryV1>, DbErrorV1> {
        self.global_db.scan_tenant_purge_entries_v1(tenant_id)
    }

    pub fn append_global_migrate_journal_entry_v1(
        &self,
        ts: i64,
        name: &str,
        payload: &[u8],
    ) -> Result<(), DbErrorV1> {
        self.global_db
            .append_migrate_journal_entry_v1(ts, name, payload)
    }

    pub fn scan_global_migrate_journal_entries_v1(
        &self,
    ) -> Result<Vec<GlobalMigrateJournalEntryV1>, DbErrorV1> {
        self.global_db.scan_migrate_journal_entries_v1()
    }

    pub fn with_tenant_db_v1<T, F>(
        &mut self,
        tenant_id: &str,
        now_ts: i64,
        f: F,
    ) -> Result<T, DbErrorV1>
    where
        F: FnOnce(&TenantDbV1) -> Result<T, DbErrorV1>,
    {
        self.tenant_cache.with_tenant_db_v1(tenant_id, now_ts, f)
    }

    pub fn close_tenant_v1(&mut self, tenant_id: &str) -> bool {
        self.tenant_cache.close_tenant_v1(tenant_id)
    }

    pub fn close_all_tenants_v1(&mut self) -> Vec<String> {
        self.tenant_cache.close_all_v1()
    }

    pub fn close_idle_tenants_v1(&mut self, now_ts: i64) -> Vec<String> {
        self.tenant_cache.close_idle_v1(now_ts)
    }

    pub fn list_open_tenant_ids_v1(&self) -> Vec<String> {
        self.tenant_cache.list_open_tenant_ids_v1()
    }

    pub fn migrate_global_schema_v1(
        &self,
        now_ts: i64,
    ) -> Result<GlobalSchemaMigrateResultV1, DbErrorV1> {
        let current_version = GLOBAL_SCHEMA_VERSION_CURRENT_V1;
        match self.read_global_schema_state_v1()? {
            None => {
                self.write_global_schema_state_v1(&GlobalSchemaStateV1 {
                    version: current_version,
                    created_ts: now_ts,
                    last_migrate_ts: now_ts,
                })?;
                self.append_global_migrate_journal_entry_v1(
                    now_ts,
                    "global_init_schema",
                    format!("ok from=none to={}", current_version).as_bytes(),
                )?;
                self.global_db.persist_sync_all_v1()?;
                Ok(GlobalSchemaMigrateResultV1 {
                    outcome: SchemaMigrateOutcomeKindV1::Initialized,
                    version_before: None,
                    version_after: Some(current_version),
                    journal_entries: vec!["global_init_schema".to_string()],
                    failure_details: Vec::new(),
                })
            }
            Some(state) if state.version == current_version => Ok(GlobalSchemaMigrateResultV1 {
                outcome: SchemaMigrateOutcomeKindV1::NoopCurrent,
                version_before: Some(state.version),
                version_after: Some(state.version),
                journal_entries: Vec::new(),
                failure_details: Vec::new(),
            }),
            Some(state) if state.version < current_version => {
                self.write_global_schema_state_v1(&GlobalSchemaStateV1 {
                    version: current_version,
                    created_ts: state.created_ts,
                    last_migrate_ts: now_ts,
                })?;
                self.append_global_migrate_journal_entry_v1(
                    now_ts,
                    "global_upgrade_schema",
                    format!("ok from={} to={}", state.version, current_version).as_bytes(),
                )?;
                self.global_db.persist_sync_all_v1()?;
                Ok(GlobalSchemaMigrateResultV1 {
                    outcome: SchemaMigrateOutcomeKindV1::Upgraded,
                    version_before: Some(state.version),
                    version_after: Some(current_version),
                    journal_entries: vec!["global_upgrade_schema".to_string()],
                    failure_details: Vec::new(),
                })
            }
            Some(state) => Ok(GlobalSchemaMigrateResultV1 {
                outcome: SchemaMigrateOutcomeKindV1::RefusedDowngrade,
                version_before: Some(state.version),
                version_after: Some(state.version),
                journal_entries: Vec::new(),
                failure_details: vec![format!(
                    "global schema version {} is newer than binary schema {}",
                    state.version, current_version
                )],
            }),
        }
    }

    pub fn migrate_tenant_schema_v1(
        &mut self,
        tenant_id: &str,
        now_ts: i64,
    ) -> Result<TenantSchemaMigrateResultV1, DbErrorV1> {
        let record = match self.read_tenant_record_v1(tenant_id)? {
            Some(record) => record,
            None => {
                return Ok(TenantSchemaMigrateResultV1 {
                    tenant_id: tenant_id.to_string(),
                    outcome: SchemaMigrateOutcomeKindV1::TenantNotFound,
                    status_before: None,
                    status_after: None,
                    version_before: None,
                    version_after: None,
                    journal_entries: Vec::new(),
                    failure_details: vec![format!("tenant not found: {}", tenant_id)],
                });
            }
        };

        if record.status == 3 {
            return Ok(TenantSchemaMigrateResultV1 {
                tenant_id: tenant_id.to_string(),
                outcome: SchemaMigrateOutcomeKindV1::SkippedTerminated,
                status_before: Some(record.status),
                status_after: Some(record.status),
                version_before: None,
                version_after: None,
                journal_entries: Vec::new(),
                failure_details: vec![format!("tenant {} is terminated", tenant_id)],
            });
        }

        let current_version = TENANT_SCHEMA_VERSION_CURRENT_V1;
        let schema_before =
            self.with_tenant_db_v1(tenant_id, now_ts, |db| db.read_schema_state_v1())?;

        match schema_before {
            None => {
                self.with_tenant_db_v1(tenant_id, now_ts, |db| {
                    db.write_schema_state_v1(&TenantSchemaStateV1 {
                        version: current_version,
                        created_ts: now_ts,
                        last_migrate_ts: now_ts,
                    })?;
                    db.append_migrate_journal_entry_v1(
                        now_ts,
                        "init_schema",
                        format!("ok from=none to={}", current_version).as_bytes(),
                    )?;
                    db.persist_sync_all_v1()
                })?;
                self.append_global_migrate_journal_entry_v1(
                    now_ts + 1,
                    "tenant_schema_init",
                    format!("tenant_id={} from=none to={}", tenant_id, current_version).as_bytes(),
                )?;
                self.global_db.persist_sync_all_v1()?;
                Ok(TenantSchemaMigrateResultV1 {
                    tenant_id: tenant_id.to_string(),
                    outcome: SchemaMigrateOutcomeKindV1::Initialized,
                    status_before: Some(record.status),
                    status_after: Some(record.status),
                    version_before: None,
                    version_after: Some(current_version),
                    journal_entries: vec![
                        "init_schema".to_string(),
                        "tenant_schema_init".to_string(),
                    ],
                    failure_details: Vec::new(),
                })
            }
            Some(state) if state.version == current_version => Ok(TenantSchemaMigrateResultV1 {
                tenant_id: tenant_id.to_string(),
                outcome: SchemaMigrateOutcomeKindV1::NoopCurrent,
                status_before: Some(record.status),
                status_after: Some(record.status),
                version_before: Some(state.version),
                version_after: Some(state.version),
                journal_entries: Vec::new(),
                failure_details: Vec::new(),
            }),
            Some(state) if state.version < current_version => {
                self.with_tenant_db_v1(tenant_id, now_ts, |db| {
                    db.write_schema_state_v1(&TenantSchemaStateV1 {
                        version: current_version,
                        created_ts: state.created_ts,
                        last_migrate_ts: now_ts,
                    })?;
                    db.append_migrate_journal_entry_v1(
                        now_ts,
                        "upgrade_schema",
                        format!("ok from={} to={}", state.version, current_version).as_bytes(),
                    )?;
                    db.persist_sync_all_v1()
                })?;
                self.append_global_migrate_journal_entry_v1(
                    now_ts + 1,
                    "tenant_schema_upgrade",
                    format!(
                        "tenant_id={} from={} to={}",
                        tenant_id, state.version, current_version
                    )
                    .as_bytes(),
                )?;
                self.global_db.persist_sync_all_v1()?;
                Ok(TenantSchemaMigrateResultV1 {
                    tenant_id: tenant_id.to_string(),
                    outcome: SchemaMigrateOutcomeKindV1::Upgraded,
                    status_before: Some(record.status),
                    status_after: Some(record.status),
                    version_before: Some(state.version),
                    version_after: Some(current_version),
                    journal_entries: vec![
                        "upgrade_schema".to_string(),
                        "tenant_schema_upgrade".to_string(),
                    ],
                    failure_details: Vec::new(),
                })
            }
            Some(state) => {
                self.set_tenant_status_v1(tenant_id, 1)?;
                self.set_tenant_active_index_v1(tenant_id, false)?;
                self.append_global_migrate_journal_entry_v1(
                    now_ts,
                    "tenant_schema_refused_downgrade",
                    format!(
                        "tenant_id={} tenant_version={} current_version={}",
                        tenant_id, state.version, current_version
                    )
                    .as_bytes(),
                )?;
                self.global_db.persist_sync_all_v1()?;
                Ok(TenantSchemaMigrateResultV1 {
                    tenant_id: tenant_id.to_string(),
                    outcome: SchemaMigrateOutcomeKindV1::RefusedDowngrade,
                    status_before: Some(record.status),
                    status_after: Some(1),
                    version_before: Some(state.version),
                    version_after: Some(state.version),
                    journal_entries: vec!["tenant_schema_refused_downgrade".to_string()],
                    failure_details: vec![format!(
                        "tenant {} schema version {} is newer than binary schema {}",
                        tenant_id, state.version, current_version
                    )],
                })
            }
        }
    }

    pub fn migrate_all_schemas_v1(&mut self, now_ts: i64) -> Result<MigrateAllResultV1, DbErrorV1> {
        let global = self.migrate_global_schema_v1(now_ts)?;
        if global.outcome == SchemaMigrateOutcomeKindV1::RefusedDowngrade {
            return Ok(MigrateAllResultV1 {
                global,
                tenants: Vec::new(),
            });
        }

        let tenant_ids = self.list_known_tenant_ids_v1()?;
        let mut tenants = Vec::with_capacity(tenant_ids.len());
        for (idx, tenant_id) in tenant_ids.iter().enumerate() {
            let ts = now_ts + 1000 + (idx as i64) * 10;
            tenants.push(self.migrate_tenant_schema_v1(tenant_id, ts)?);
        }
        Ok(MigrateAllResultV1 { global, tenants })
    }

    pub fn purge_tenant_v1(
        &mut self,
        tenant_id: &str,
        force: bool,
        now_ts: i64,
    ) -> Result<TenantPurgeResultV1, DbErrorV1> {
        let record = match self.read_tenant_record_v1(tenant_id)? {
            Some(record) => record,
            None => {
                return Ok(TenantPurgeResultV1 {
                    tenant_id: tenant_id.to_string(),
                    outcome: TenantPurgeOutcomeKindV1::TenantNotFound,
                    status_before: None,
                    status_after: None,
                    closed_tenant_handle: false,
                    deleted_db_dir: false,
                    deleted_alert_dir: false,
                    deleted_spool_dir: false,
                    journal_entries: Vec::new(),
                    failure_details: Vec::new(),
                });
            }
        };

        if !force && record.status != 2 {
            return Ok(TenantPurgeResultV1 {
                tenant_id: tenant_id.to_string(),
                outcome: TenantPurgeOutcomeKindV1::RejectedStatus,
                status_before: Some(record.status),
                status_after: Some(record.status),
                closed_tenant_handle: false,
                deleted_db_dir: false,
                deleted_alert_dir: false,
                deleted_spool_dir: false,
                journal_entries: Vec::new(),
                failure_details: vec![format!(
                    "tenant {} status is {} but terminating (2) is required unless --force",
                    tenant_id, record.status
                )],
            });
        }

        let mut result = TenantPurgeResultV1 {
            tenant_id: tenant_id.to_string(),
            outcome: TenantPurgeOutcomeKindV1::Success,
            status_before: Some(record.status),
            status_after: None,
            closed_tenant_handle: self.close_tenant_v1(tenant_id),
            deleted_db_dir: false,
            deleted_alert_dir: false,
            deleted_spool_dir: false,
            journal_entries: Vec::new(),
            failure_details: Vec::new(),
        };

        let paths = self.tenant_paths_v1(tenant_id);
        self.append_tenant_purge_entry_v1(tenant_id, now_ts, "requested")?;
        result.journal_entries.push("requested".to_string());

        let delete_steps = [
            ("db_deleted", Path::new(&paths.tenant_db_dir), 1_i64),
            ("alerts_deleted", Path::new(&paths.alert_out_dir), 2_i64),
            ("spool_deleted", Path::new(&paths.spool_dir), 3_i64),
        ];

        for (status_name, path, offset) in delete_steps {
            match remove_dir_step_v1(path) {
                Ok(true) => {
                    match status_name {
                        "db_deleted" => result.deleted_db_dir = true,
                        "alerts_deleted" => result.deleted_alert_dir = true,
                        "spool_deleted" => result.deleted_spool_dir = true,
                        _ => {}
                    }
                    self.append_tenant_purge_entry_v1(tenant_id, now_ts + offset, status_name)?;
                    result.journal_entries.push(status_name.to_string());
                }
                Ok(false) => {
                    match status_name {
                        "db_deleted" => result.deleted_db_dir = true,
                        "alerts_deleted" => result.deleted_alert_dir = true,
                        "spool_deleted" => result.deleted_spool_dir = true,
                        _ => {}
                    }
                    self.append_tenant_purge_entry_v1(tenant_id, now_ts + offset, status_name)?;
                    result.journal_entries.push(status_name.to_string());
                }
                Err(msg) => {
                    result.failure_details.push(format!(
                        "{}: {}: {}",
                        status_name,
                        path.display(),
                        msg
                    ));
                }
            }
        }

        if result.failure_details.is_empty() {
            self.set_tenant_status_v1(tenant_id, 3)?;
            self.set_tenant_active_index_v1(tenant_id, false)?;
            self.append_tenant_purge_entry_v1(tenant_id, now_ts + 4, "complete")?;
            result.journal_entries.push("complete".to_string());
            result.status_after = Some(3);
        } else {
            let deleted_count = [
                result.deleted_db_dir,
                result.deleted_alert_dir,
                result.deleted_spool_dir,
            ]
            .into_iter()
            .filter(|v| *v)
            .count();
            result.outcome = if deleted_count > 0 {
                TenantPurgeOutcomeKindV1::Partial
            } else {
                TenantPurgeOutcomeKindV1::Partial
            };
            result.status_after = Some(record.status);
        }

        self.global_db.persist_sync_all_v1()?;
        Ok(result)
    }
}

fn remove_dir_step_v1(path: &Path) -> Result<bool, String> {
    match fs::metadata(path) {
        Ok(md) => {
            if !md.is_dir() {
                return Err("exists but is not a directory".to_string());
            }
            fs::remove_dir_all(path).map_err(|e| e.to_string())?;
            Ok(true)
        }
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(e.to_string())
            }
        }
    }
}
