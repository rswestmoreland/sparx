// Fjall-backed global DB wrapper and repository helpers.
//
// See:
// - contracts/06_rocksdb_topology_v0_1.md
// - contracts/30_global_db_key_prefix_map_v0_1.md

use std::collections::BTreeSet;
use std::path::Path;

use crate::config::ConfigV1;
use crate::db::fjall::{FjallKvDbV1, KvWriteOpV1};
use crate::db::keys::{
    key_global_alert_out_root_v1, key_global_metrics_counter_v1, key_global_metrics_gauge_v1, key_global_migrate_journal_v1, key_global_process_last_run_end_ts_v1,
    key_global_process_last_run_exit_code_v1, key_global_process_last_run_host_v1,
    key_global_process_last_run_start_ts_v1, key_global_schema_created_ts_v1,
    key_global_schema_last_migrate_ts_v1, key_global_schema_version_v1,
    key_global_tenant_created_ts_v1, key_global_tenant_db_path_v1,
    key_global_tenant_idx_active_v1, key_global_tenant_last_seen_ts_v1,
    key_global_tenant_purge_v1, key_global_tenant_root_rel_v1, key_global_tenant_status_v1,
    key_prefix_global_migrate_journal_v1, key_prefix_global_tenant_idx_active_v1,
    key_prefix_global_tenant_purge_v1, key_prefix_global_tenant_v1,
};
use crate::db::layout::{filesystem_layout_v1, FilesystemLayoutV1};
use crate::db::tenant_values::{decode_metrics_counter_v1, decode_metrics_gauge_v1, encode_metrics_counter_v1, encode_metrics_gauge_v1};
use crate::db::DbErrorV1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalSchemaStateV1 {
    pub version: u32,
    pub created_ts: i64,
    pub last_migrate_ts: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct GlobalProcessStateV1 {
    pub last_run_start_ts: Option<i64>,
    pub last_run_end_ts: Option<i64>,
    pub last_run_exit_code: Option<i32>,
    pub last_run_host: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalTenantRecordV1 {
    pub tenant_id: String,
    pub created_ts: i64,
    pub last_seen_ts: i64,
    pub status: u8,
    pub tenant_root_rel: Option<String>,
    pub tenant_db_path: Option<String>,
    pub alert_out_root: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalTenantPurgeEntryV1 {
    pub tenant_id: String,
    pub ts: i64,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalMigrateJournalEntryV1 {
    pub ts: i64,
    pub name: String,
    pub payload: Vec<u8>,
}

#[derive(Clone)]
pub struct GlobalDbV1 {
    inner: FjallKvDbV1,
}

impl GlobalDbV1 {
    pub fn open_at_v1(path: impl AsRef<Path>) -> Result<Self, DbErrorV1> {
        Ok(Self {
            inner: FjallKvDbV1::open_at_v1(path)?,
        })
    }

    pub fn open_from_layout_v1(layout: &FilesystemLayoutV1) -> Result<Self, DbErrorV1> {
        Self::open_at_v1(layout.global_db_path_v1())
    }

    pub fn open_from_config_v1(cfg: &ConfigV1) -> Result<Self, DbErrorV1> {
        let layout = filesystem_layout_v1(cfg);
        Self::open_from_layout_v1(&layout)
    }

    pub fn path_v1(&self) -> &Path {
        self.inner.path_v1()
    }

    pub fn get_raw_v1(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DbErrorV1> {
        self.inner.get_raw_v1(key)
    }

    pub fn put_raw_v1(&self, key: &[u8], value: &[u8]) -> Result<(), DbErrorV1> {
        self.inner.put_raw_v1(key, value)
    }

    pub fn delete_raw_v1(&self, key: &[u8]) -> Result<(), DbErrorV1> {
        self.inner.delete_raw_v1(key)
    }

    pub fn write_batch_raw_v1(&self, ops: &[KvWriteOpV1]) -> Result<(), DbErrorV1> {
        self.inner.write_batch_raw_v1(ops)
    }

    pub fn scan_prefix_raw_v1(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbErrorV1> {
        self.inner.scan_prefix_raw_v1(prefix)
    }

    pub fn scan_range_raw_v1(
        &self,
        start_inclusive: &[u8],
        end_inclusive: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbErrorV1> {
        self.inner.scan_range_raw_v1(start_inclusive, end_inclusive)
    }

    pub fn persist_sync_all_v1(&self) -> Result<(), DbErrorV1> {
        self.inner.persist_sync_all_v1()
    }

    pub fn write_metric_counter_v1(&self, name: &str, value: u64) -> Result<(), DbErrorV1> {
        self.put_raw_v1(key_global_metrics_counter_v1(name).as_bytes(), &encode_metrics_counter_v1(value))
    }

    pub fn read_metric_counter_v1(&self, name: &str) -> Result<Option<u64>, DbErrorV1> {
        let bytes = match self.get_raw_v1(key_global_metrics_counter_v1(name).as_bytes())? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let value = decode_metrics_counter_v1(&bytes)
            .map_err(|e| DbErrorV1::new_v1(format!("failed to decode global metric counter {}: {}", name, e.msg)))?;
        Ok(Some(value))
    }

    pub fn delete_metric_counter_v1(&self, name: &str) -> Result<(), DbErrorV1> {
        self.delete_raw_v1(key_global_metrics_counter_v1(name).as_bytes())
    }

    pub fn write_metric_gauge_v1(&self, name: &str, value: f64) -> Result<(), DbErrorV1> {
        self.put_raw_v1(key_global_metrics_gauge_v1(name).as_bytes(), &encode_metrics_gauge_v1(value))
    }

    pub fn read_metric_gauge_v1(&self, name: &str) -> Result<Option<f64>, DbErrorV1> {
        let bytes = match self.get_raw_v1(key_global_metrics_gauge_v1(name).as_bytes())? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let value = decode_metrics_gauge_v1(&bytes)
            .map_err(|e| DbErrorV1::new_v1(format!("failed to decode global metric gauge {}: {}", name, e.msg)))?;
        Ok(Some(value))
    }

    pub fn delete_metric_gauge_v1(&self, name: &str) -> Result<(), DbErrorV1> {
        self.delete_raw_v1(key_global_metrics_gauge_v1(name).as_bytes())
    }

    pub fn write_schema_state_v1(&self, state: &GlobalSchemaStateV1) -> Result<(), DbErrorV1> {
        let ops = vec![
            KvWriteOpV1::Put {
                key: key_global_schema_version_v1().bytes,
                value: encode_u32_v1(state.version),
            },
            KvWriteOpV1::Put {
                key: key_global_schema_created_ts_v1().bytes,
                value: encode_i64_v1(state.created_ts),
            },
            KvWriteOpV1::Put {
                key: key_global_schema_last_migrate_ts_v1().bytes,
                value: encode_i64_v1(state.last_migrate_ts),
            },
        ];
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_schema_state_v1(&self) -> Result<Option<GlobalSchemaStateV1>, DbErrorV1> {
        let version = self.get_raw_v1(key_global_schema_version_v1().as_bytes())?;
        let created_ts = self.get_raw_v1(key_global_schema_created_ts_v1().as_bytes())?;
        let last_migrate_ts = self.get_raw_v1(key_global_schema_last_migrate_ts_v1().as_bytes())?;

        let has_any = version.is_some() || created_ts.is_some() || last_migrate_ts.is_some();
        if !has_any {
            return Ok(None);
        }

        let version = version.ok_or_else(|| DbErrorV1::new_v1("global schema state missing version"))?;
        let created_ts =
            created_ts.ok_or_else(|| DbErrorV1::new_v1("global schema state missing created_ts"))?;
        let last_migrate_ts = last_migrate_ts
            .ok_or_else(|| DbErrorV1::new_v1("global schema state missing last_migrate_ts"))?;

        Ok(Some(GlobalSchemaStateV1 {
            version: decode_u32_v1(&version, "global schema version")?,
            created_ts: decode_i64_v1(&created_ts, "global schema created_ts")?,
            last_migrate_ts: decode_i64_v1(&last_migrate_ts, "global schema last_migrate_ts")?,
        }))
    }

    pub fn write_process_state_v1(&self, state: &GlobalProcessStateV1) -> Result<(), DbErrorV1> {
        let mut ops = Vec::new();
        push_optional_fixed_i64_v1(
            &mut ops,
            key_global_process_last_run_start_ts_v1().bytes,
            state.last_run_start_ts,
        );
        push_optional_fixed_i64_v1(
            &mut ops,
            key_global_process_last_run_end_ts_v1().bytes,
            state.last_run_end_ts,
        );
        push_optional_fixed_i32_v1(
            &mut ops,
            key_global_process_last_run_exit_code_v1().bytes,
            state.last_run_exit_code,
        );
        push_optional_string_v1(
            &mut ops,
            key_global_process_last_run_host_v1().bytes,
            state.last_run_host.as_deref(),
        );
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_process_state_v1(&self) -> Result<GlobalProcessStateV1, DbErrorV1> {
        Ok(GlobalProcessStateV1 {
            last_run_start_ts: match self.get_raw_v1(key_global_process_last_run_start_ts_v1().as_bytes())? {
                Some(bytes) => Some(decode_i64_v1(&bytes, "global process last_run_start_ts")?),
                None => None,
            },
            last_run_end_ts: match self.get_raw_v1(key_global_process_last_run_end_ts_v1().as_bytes())? {
                Some(bytes) => Some(decode_i64_v1(&bytes, "global process last_run_end_ts")?),
                None => None,
            },
            last_run_exit_code: match self
                .get_raw_v1(key_global_process_last_run_exit_code_v1().as_bytes())?
            {
                Some(bytes) => Some(decode_i32_v1(&bytes, "global process last_run_exit_code")?),
                None => None,
            },
            last_run_host: match self.get_raw_v1(key_global_process_last_run_host_v1().as_bytes())? {
                Some(bytes) => Some(decode_string_v1(&bytes, "global process last_run_host")?),
                None => None,
            },
        })
    }

    pub fn upsert_tenant_record_v1(&self, record: &GlobalTenantRecordV1) -> Result<(), DbErrorV1> {
        let mut ops = vec![
            KvWriteOpV1::Put {
                key: key_global_tenant_created_ts_v1(&record.tenant_id).bytes,
                value: encode_i64_v1(record.created_ts),
            },
            KvWriteOpV1::Put {
                key: key_global_tenant_last_seen_ts_v1(&record.tenant_id).bytes,
                value: encode_i64_v1(record.last_seen_ts),
            },
            KvWriteOpV1::Put {
                key: key_global_tenant_status_v1(&record.tenant_id).bytes,
                value: vec![record.status],
            },
        ];
        push_optional_string_v1(
            &mut ops,
            key_global_tenant_root_rel_v1(&record.tenant_id).bytes,
            record.tenant_root_rel.as_deref(),
        );
        push_optional_string_v1(
            &mut ops,
            key_global_tenant_db_path_v1(&record.tenant_id).bytes,
            record.tenant_db_path.as_deref(),
        );
        push_optional_string_v1(
            &mut ops,
            key_global_alert_out_root_v1(&record.tenant_id).bytes,
            record.alert_out_root.as_deref(),
        );
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_tenant_record_v1(
        &self,
        tenant_id: &str,
    ) -> Result<Option<GlobalTenantRecordV1>, DbErrorV1> {
        let created_ts = self.get_raw_v1(key_global_tenant_created_ts_v1(tenant_id).as_bytes())?;
        let last_seen_ts = self.get_raw_v1(key_global_tenant_last_seen_ts_v1(tenant_id).as_bytes())?;
        let status = self.get_raw_v1(key_global_tenant_status_v1(tenant_id).as_bytes())?;
        let has_any = created_ts.is_some() || last_seen_ts.is_some() || status.is_some();
        if !has_any {
            return Ok(None);
        }

        let created_ts = created_ts
            .ok_or_else(|| DbErrorV1::new_v1(format!("tenant {} record missing created_ts", tenant_id)))?;
        let last_seen_ts = last_seen_ts
            .ok_or_else(|| DbErrorV1::new_v1(format!("tenant {} record missing last_seen_ts", tenant_id)))?;
        let status =
            status.ok_or_else(|| DbErrorV1::new_v1(format!("tenant {} record missing status", tenant_id)))?;

        Ok(Some(GlobalTenantRecordV1 {
            tenant_id: tenant_id.to_string(),
            created_ts: decode_i64_v1(&created_ts, "tenant created_ts")?,
            last_seen_ts: decode_i64_v1(&last_seen_ts, "tenant last_seen_ts")?,
            status: decode_u8_v1(&status, "tenant status")?,
            tenant_root_rel: read_optional_string_v1(self, key_global_tenant_root_rel_v1(tenant_id).as_bytes(), "tenant tenant_root_rel")?,
            tenant_db_path: read_optional_string_v1(self, key_global_tenant_db_path_v1(tenant_id).as_bytes(), "tenant tenant_db_path")?,
            alert_out_root: read_optional_string_v1(self, key_global_alert_out_root_v1(tenant_id).as_bytes(), "tenant alert_out_root")?,
        }))
    }

    pub fn set_tenant_status_v1(&self, tenant_id: &str, status: u8) -> Result<(), DbErrorV1> {
        self.put_raw_v1(key_global_tenant_status_v1(tenant_id).as_bytes(), &[status])
    }

    pub fn set_tenant_last_seen_ts_v1(&self, tenant_id: &str, last_seen_ts: i64) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_global_tenant_last_seen_ts_v1(tenant_id).as_bytes(),
            &encode_i64_v1(last_seen_ts),
        )
    }

    pub fn set_tenant_active_index_v1(&self, tenant_id: &str, is_active: bool) -> Result<(), DbErrorV1> {
        if is_active {
            self.put_raw_v1(key_global_tenant_idx_active_v1(tenant_id).as_bytes(), &[])
        } else {
            self.delete_raw_v1(key_global_tenant_idx_active_v1(tenant_id).as_bytes())
        }
    }

    pub fn list_active_tenants_v1(&self) -> Result<Vec<String>, DbErrorV1> {
        let prefix = key_prefix_global_tenant_idx_active_v1();
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            out.push(parse_suffix_after_prefix_v1(&key, prefix.as_bytes(), "active tenant index")?);
        }
        Ok(out)
    }

    pub fn list_known_tenant_ids_v1(&self) -> Result<Vec<String>, DbErrorV1> {
        let prefix = b"tenant/v1";
        let entries = self.scan_prefix_raw_v1(prefix)?;
        let mut out: BTreeSet<String> = BTreeSet::new();
        for (key, _) in entries {
            let suffix = parse_suffix_after_prefix_v1(&key, prefix, "tenant record key")?;
            let (tenant_id, _) = split_once_v1(&suffix, '/', "tenant record key")?;
            out.insert(tenant_id.to_string());
        }
        Ok(out.into_iter().collect())
    }

    pub fn append_tenant_purge_entry_v1(
        &self,
        tenant_id: &str,
        ts: i64,
        status: &str,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(key_global_tenant_purge_v1(tenant_id, ts).as_bytes(), status.as_bytes())
    }

    pub fn scan_tenant_purge_entries_v1(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<GlobalTenantPurgeEntryV1>, DbErrorV1> {
        let prefix = key_prefix_global_tenant_purge_v1(tenant_id);
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let ts_text = parse_suffix_after_prefix_v1(&key, prefix.as_bytes(), "tenant purge journal")?;
            out.push(GlobalTenantPurgeEntryV1 {
                tenant_id: tenant_id.to_string(),
                ts: parse_i64_ascii_v1(&ts_text, "tenant purge ts")?,
                status: decode_string_v1(&value, "tenant purge status")?,
            });
        }
        Ok(out)
    }

    pub fn append_migrate_journal_entry_v1(
        &self,
        ts: i64,
        name: &str,
        payload: &[u8],
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(key_global_migrate_journal_v1(ts, name).as_bytes(), payload)
    }

    pub fn scan_migrate_journal_entries_v1(
        &self,
    ) -> Result<Vec<GlobalMigrateJournalEntryV1>, DbErrorV1> {
        let prefix = key_prefix_global_migrate_journal_v1();
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, payload) in entries {
            let suffix = parse_suffix_after_prefix_v1(&key, prefix.as_bytes(), "migration journal")?;
            let (ts_text, name) = split_once_v1(&suffix, '/', "migration journal key")?;
            out.push(GlobalMigrateJournalEntryV1 {
                ts: parse_i64_ascii_v1(ts_text, "migration journal ts")?,
                name: name.to_string(),
                payload,
            });
        }
        Ok(out)
    }

    pub fn list_tenant_record_keys_v1(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbErrorV1> {
        let prefix = key_prefix_global_tenant_v1(tenant_id);
        self.scan_prefix_raw_v1(prefix.as_bytes())
    }
}

fn read_optional_string_v1(
    db: &GlobalDbV1,
    key: &[u8],
    label: &str,
) -> Result<Option<String>, DbErrorV1> {
    match db.get_raw_v1(key)? {
        Some(bytes) => Ok(Some(decode_string_v1(&bytes, label)?)),
        None => Ok(None),
    }
}

fn push_optional_fixed_i64_v1(ops: &mut Vec<KvWriteOpV1>, key: Vec<u8>, value: Option<i64>) {
    match value {
        Some(v) => ops.push(KvWriteOpV1::Put {
            key,
            value: encode_i64_v1(v),
        }),
        None => ops.push(KvWriteOpV1::Delete { key }),
    }
}

fn push_optional_fixed_i32_v1(ops: &mut Vec<KvWriteOpV1>, key: Vec<u8>, value: Option<i32>) {
    match value {
        Some(v) => ops.push(KvWriteOpV1::Put {
            key,
            value: encode_i32_v1(v),
        }),
        None => ops.push(KvWriteOpV1::Delete { key }),
    }
}

fn push_optional_string_v1(ops: &mut Vec<KvWriteOpV1>, key: Vec<u8>, value: Option<&str>) {
    match value {
        Some(v) => ops.push(KvWriteOpV1::Put {
            key,
            value: v.as_bytes().to_vec(),
        }),
        None => ops.push(KvWriteOpV1::Delete { key }),
    }
}

fn encode_u32_v1(value: u32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn encode_i64_v1(value: i64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn encode_i32_v1(value: i32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn decode_u32_v1(bytes: &[u8], label: &str) -> Result<u32, DbErrorV1> {
    if bytes.len() != 4 {
        return Err(DbErrorV1::new_v1(format!(
            "{} has invalid length {}; expected 4",
            label,
            bytes.len()
        )));
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(arr))
}

fn decode_i64_v1(bytes: &[u8], label: &str) -> Result<i64, DbErrorV1> {
    if bytes.len() != 8 {
        return Err(DbErrorV1::new_v1(format!(
            "{} has invalid length {}; expected 8",
            label,
            bytes.len()
        )));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(i64::from_le_bytes(arr))
}

fn decode_i32_v1(bytes: &[u8], label: &str) -> Result<i32, DbErrorV1> {
    if bytes.len() != 4 {
        return Err(DbErrorV1::new_v1(format!(
            "{} has invalid length {}; expected 4",
            label,
            bytes.len()
        )));
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(bytes);
    Ok(i32::from_le_bytes(arr))
}

fn decode_u8_v1(bytes: &[u8], label: &str) -> Result<u8, DbErrorV1> {
    if bytes.len() != 1 {
        return Err(DbErrorV1::new_v1(format!(
            "{} has invalid length {}; expected 1",
            label,
            bytes.len()
        )));
    }
    Ok(bytes[0])
}

fn decode_string_v1(bytes: &[u8], label: &str) -> Result<String, DbErrorV1> {
    String::from_utf8(bytes.to_vec()).map_err(|e| {
        DbErrorV1::new_v1(format!("{} is not valid UTF-8: {}", label, e))
    })
}

fn parse_suffix_after_prefix_v1(key: &[u8], prefix: &[u8], label: &str) -> Result<String, DbErrorV1> {
    if !key.starts_with(prefix) {
        return Err(DbErrorV1::new_v1(format!(
            "{} key does not match expected prefix",
            label
        )));
    }
    let suffix = if key.len() == prefix.len() {
        &[][..]
    } else if key.get(prefix.len()) == Some(&b'/') {
        &key[prefix.len() + 1..]
    } else {
        return Err(DbErrorV1::new_v1(format!(
            "{} key does not contain separator after prefix",
            label
        )));
    };
    decode_string_v1(suffix, label)
}

fn split_once_v1<'a>(text: &'a str, delim: char, label: &str) -> Result<(&'a str, &'a str), DbErrorV1> {
    text.split_once(delim)
        .ok_or_else(|| DbErrorV1::new_v1(format!("{} is missing separator '{}'", label, delim)))
}

fn parse_i64_ascii_v1(text: &str, label: &str) -> Result<i64, DbErrorV1> {
    text.parse::<i64>()
        .map_err(|e| DbErrorV1::new_v1(format!("{} is not a valid i64: {}", label, e)))
}
