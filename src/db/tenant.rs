// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Fjall-backed tenant DB wrapper and repository helpers.
//
// See:
// - contracts/06_rocksdb_topology_v0_1.md
// - contracts/25_tenant_db_key_prefix_map_v0_1.md
// - contracts/26_open_window_checkpoint_encoding_v0_1.md
// - contracts/31_tenant_db_simple_value_encodings_v0_1.md

use std::collections::BTreeSet;
use std::path::Path;

use crate::alert::{decode_alert_v1, encode_alert_v1, AlertV1};
use crate::config::ConfigV1;
use crate::db::baseline_sketch::{
    decode_centroid_v1, decode_dfm_v1, decode_dfn_v1, decode_stats_v1, encode_centroid_v1,
    encode_dfm_v1, encode_dfn_v1, encode_stats_v1, CentroidValuePairV1, DeviceStatsV1,
    DfCountPairV1,
};
use crate::db::fjall::{FjallKvDbV1, KvWriteOpV1};
use crate::db::keys::{
    key_prefix_tenant_alert_v1, key_prefix_tenant_drop_open_device_v1,
    key_prefix_tenant_drop_open_source_stream_v1, key_prefix_tenant_migrate_journal_v1,
    key_prefix_tenant_silence_open_source_stream_v1, key_prefix_tenant_silence_open_v1,
    key_prefix_tenant_silence_subject_device_v1,
    key_prefix_tenant_silence_subject_source_stream_v1, key_prefix_tenant_source_stats_v1,
    key_prefix_tenant_source_stream_device_v1, key_tenant_active_window_v1,
    key_tenant_alert_idx_cat_v1, key_tenant_alert_idx_ent_v1, key_tenant_alert_idx_time_v1,
    key_tenant_alert_v1, key_tenant_centroid_v1, key_tenant_cursor_inode_v1,
    key_tenant_cursor_is_gzip_v1, key_tenant_cursor_last_read_ts_v1, key_tenant_cursor_mtime_v1,
    key_tenant_cursor_offset_v1, key_tenant_cursor_size_v1, key_tenant_dfm_v1, key_tenant_dfn_v1,
    key_tenant_drop_open_device_v1, key_tenant_drop_open_source_stream_v1,
    key_tenant_drop_open_tenant_v1, key_tenant_migrate_journal_v1, key_tenant_schema_created_ts_v1,
    key_tenant_schema_last_migrate_ts_v1, key_tenant_schema_version_v1,
    key_tenant_silence_open_device_v1, key_tenant_silence_open_source_stream_v1,
    key_tenant_silence_open_tenant_v1, key_tenant_silence_subject_device_state_v1,
    key_tenant_silence_subject_source_stream_state_v1, key_tenant_silence_subject_tenant_state_v1,
    key_tenant_source_stats_v1, key_tenant_source_stream_catalog_v1, key_tenant_stats_v1,
    key_tenant_window_row_ent_domain_v1, key_tenant_window_row_ent_dstip_v1,
    key_tenant_window_row_ent_host_v1, key_tenant_window_row_ent_srcip_v1,
    key_tenant_window_row_ent_userid_v1, key_tenant_window_row_feat_v1,
    key_tenant_window_row_meta_v1,
};
use crate::db::layout::{filesystem_layout_v1, FilesystemLayoutV1};
use crate::db::open_window::{
    decode_win_active_v1, decode_win_row_ent_domain_v1, decode_win_row_ent_dstip_v1,
    decode_win_row_ent_host_v1, decode_win_row_ent_srcip_v1, decode_win_row_ent_userid_v1,
    decode_win_row_feat_v1, decode_win_row_meta_v1, encode_win_active_v1,
    encode_win_row_ent_domain_v1, encode_win_row_ent_dstip_v1, encode_win_row_ent_host_v1,
    encode_win_row_ent_srcip_v1, encode_win_row_ent_userid_v1, encode_win_row_feat_v1,
    encode_win_row_meta_v1, SparseCountPairV1, WinActiveV1, WinMetaV1,
};
use crate::db::silence::{
    decode_expected_source_state_v1, decode_open_drop_state_v1, decode_open_silence_state_v1,
    encode_expected_source_state_v1, encode_open_drop_state_v1, encode_open_silence_state_v1,
    update_expected_source_state_from_window_v1, ExpectedSourceStateUpdateV1,
    ExpectedSourceStateV1, OpenDropStateV1, OpenSilenceStateV1,
};
use crate::db::source_stream::{
    decode_source_stream_catalog_v1, decode_source_stream_stats_v1,
    encode_source_stream_catalog_v1, encode_source_stream_stats_v1, SourceStreamCatalogV1,
    SourceStreamStatsV1,
};
use crate::db::tenant_values::{
    decode_cursor_inode_v1, decode_cursor_is_gzip_v1, decode_cursor_last_read_ts_v1,
    decode_cursor_mtime_v1, decode_cursor_offset_v1, decode_cursor_size_v1,
    decode_meta_schema_created_ts_v1, decode_meta_schema_last_migrate_ts_v1,
    decode_meta_schema_version_v1, encode_cursor_inode_v1, encode_cursor_is_gzip_v1,
    encode_cursor_last_read_ts_v1, encode_cursor_mtime_v1, encode_cursor_offset_v1,
    encode_cursor_size_v1, encode_meta_schema_created_ts_v1, encode_meta_schema_last_migrate_ts_v1,
    encode_meta_schema_version_v1,
};
use crate::db::DbErrorV1;
use crate::features::EntitySketchSnapshotV1;
use crate::ingest::FileCursorV1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantSchemaStateV1 {
    pub version: u32,
    pub created_ts: i64,
    pub last_migrate_ts: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantOpenWindowStateV1 {
    pub device_key: String,
    pub active: WinActiveV1,
    pub sparse_counts: Vec<SparseCountPairV1>,
    pub meta: WinMetaV1,
    pub entity_snapshot: EntitySketchSnapshotV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantDfSlotBucketStateV1 {
    pub slot: u8,
    pub bucket: u8,
    pub window_count: u32,
    pub df_pairs: Vec<DfCountPairV1>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TenantDeviceBaselineStateV1 {
    pub device_key: String,
    pub bucket: u8,
    pub centroid: Vec<CentroidValuePairV1>,
    pub stats: Option<DeviceStatsV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantMigrateJournalEntryV1 {
    pub ts: i64,
    pub name: String,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantAlertTimeIndexEntryV1 {
    pub device_key: String,
    pub window_start_ts: i64,
    pub alert_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantAlertCategoryIndexEntryV1 {
    pub category: String,
    pub window_start_ts: i64,
    pub alert_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantAlertEntityIndexEntryV1 {
    pub entity_kind: String,
    pub entity_value: String,
    pub window_start_ts: i64,
    pub alert_id: String,
}

#[derive(Clone)]
pub struct TenantDbV1 {
    inner: FjallKvDbV1,
}

impl TenantDbV1 {
    pub fn open_at_v1(path: impl AsRef<Path>) -> Result<Self, DbErrorV1> {
        Ok(Self {
            inner: FjallKvDbV1::open_at_v1(path)?,
        })
    }

    pub fn open_from_layout_v1(
        layout: &FilesystemLayoutV1,
        tenant_id: &str,
    ) -> Result<Self, DbErrorV1> {
        Self::open_at_v1(layout.tenant_db_dir_v1(tenant_id))
    }

    pub fn open_from_config_v1(cfg: &ConfigV1, tenant_id: &str) -> Result<Self, DbErrorV1> {
        let layout = filesystem_layout_v1(cfg);
        Self::open_from_layout_v1(&layout, tenant_id)
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

    pub fn write_schema_state_v1(&self, state: &TenantSchemaStateV1) -> Result<(), DbErrorV1> {
        let ops = vec![
            KvWriteOpV1::Put {
                key: key_tenant_schema_version_v1().bytes,
                value: encode_meta_schema_version_v1(state.version),
            },
            KvWriteOpV1::Put {
                key: key_tenant_schema_created_ts_v1().bytes,
                value: encode_meta_schema_created_ts_v1(state.created_ts),
            },
            KvWriteOpV1::Put {
                key: key_tenant_schema_last_migrate_ts_v1().bytes,
                value: encode_meta_schema_last_migrate_ts_v1(state.last_migrate_ts),
            },
        ];
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_schema_state_v1(&self) -> Result<Option<TenantSchemaStateV1>, DbErrorV1> {
        let version = self.get_raw_v1(key_tenant_schema_version_v1().as_bytes())?;
        let created_ts = self.get_raw_v1(key_tenant_schema_created_ts_v1().as_bytes())?;
        let last_migrate_ts = self.get_raw_v1(key_tenant_schema_last_migrate_ts_v1().as_bytes())?;

        let has_any = version.is_some() || created_ts.is_some() || last_migrate_ts.is_some();
        if !has_any {
            return Ok(None);
        }

        let version =
            version.ok_or_else(|| DbErrorV1::new_v1("tenant schema state missing version"))?;
        let created_ts = created_ts
            .ok_or_else(|| DbErrorV1::new_v1("tenant schema state missing created_ts"))?;
        let last_migrate_ts = last_migrate_ts
            .ok_or_else(|| DbErrorV1::new_v1("tenant schema state missing last_migrate_ts"))?;

        Ok(Some(TenantSchemaStateV1 {
            version: decode_meta_schema_version_v1(&version).map_err(|e| {
                DbErrorV1::new_v1(format!("tenant schema version decode failed: {:?}", e))
            })?,
            created_ts: decode_meta_schema_created_ts_v1(&created_ts).map_err(|e| {
                DbErrorV1::new_v1(format!("tenant schema created_ts decode failed: {:?}", e))
            })?,
            last_migrate_ts: decode_meta_schema_last_migrate_ts_v1(&last_migrate_ts).map_err(
                |e| {
                    DbErrorV1::new_v1(format!(
                        "tenant schema last_migrate_ts decode failed: {:?}",
                        e
                    ))
                },
            )?,
        }))
    }

    pub fn write_cursor_v1(
        &self,
        device_key: &str,
        file_key: &str,
        cursor: &FileCursorV1,
    ) -> Result<(), DbErrorV1> {
        let ops = vec![
            KvWriteOpV1::Put {
                key: key_tenant_cursor_inode_v1(device_key, file_key).bytes,
                value: encode_cursor_inode_v1(cursor.inode),
            },
            KvWriteOpV1::Put {
                key: key_tenant_cursor_mtime_v1(device_key, file_key).bytes,
                value: encode_cursor_mtime_v1(cursor.mtime),
            },
            KvWriteOpV1::Put {
                key: key_tenant_cursor_size_v1(device_key, file_key).bytes,
                value: encode_cursor_size_v1(cursor.size),
            },
            KvWriteOpV1::Put {
                key: key_tenant_cursor_offset_v1(device_key, file_key).bytes,
                value: encode_cursor_offset_v1(cursor.offset),
            },
            KvWriteOpV1::Put {
                key: key_tenant_cursor_is_gzip_v1(device_key, file_key).bytes,
                value: encode_cursor_is_gzip_v1(cursor.is_gzip),
            },
            KvWriteOpV1::Put {
                key: key_tenant_cursor_last_read_ts_v1(device_key, file_key).bytes,
                value: encode_cursor_last_read_ts_v1(cursor.last_read_ts),
            },
        ];
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_cursor_v1(
        &self,
        device_key: &str,
        file_key: &str,
    ) -> Result<Option<FileCursorV1>, DbErrorV1> {
        let inode = self.get_raw_v1(key_tenant_cursor_inode_v1(device_key, file_key).as_bytes())?;
        let mtime = self.get_raw_v1(key_tenant_cursor_mtime_v1(device_key, file_key).as_bytes())?;
        let size = self.get_raw_v1(key_tenant_cursor_size_v1(device_key, file_key).as_bytes())?;
        let offset =
            self.get_raw_v1(key_tenant_cursor_offset_v1(device_key, file_key).as_bytes())?;
        let is_gzip =
            self.get_raw_v1(key_tenant_cursor_is_gzip_v1(device_key, file_key).as_bytes())?;
        let last_read_ts =
            self.get_raw_v1(key_tenant_cursor_last_read_ts_v1(device_key, file_key).as_bytes())?;

        let has_any = inode.is_some()
            || mtime.is_some()
            || size.is_some()
            || offset.is_some()
            || is_gzip.is_some()
            || last_read_ts.is_some();
        if !has_any {
            return Ok(None);
        }

        Ok(Some(FileCursorV1 {
            inode: decode_cursor_inode_v1(
                &inode.ok_or_else(|| DbErrorV1::new_v1("cursor missing inode"))?,
            )
            .map_err(|e| DbErrorV1::new_v1(format!("cursor inode decode failed: {:?}", e)))?,
            mtime: decode_cursor_mtime_v1(
                &mtime.ok_or_else(|| DbErrorV1::new_v1("cursor missing mtime"))?,
            )
            .map_err(|e| DbErrorV1::new_v1(format!("cursor mtime decode failed: {:?}", e)))?,
            size: decode_cursor_size_v1(
                &size.ok_or_else(|| DbErrorV1::new_v1("cursor missing size"))?,
            )
            .map_err(|e| DbErrorV1::new_v1(format!("cursor size decode failed: {:?}", e)))?,
            offset: decode_cursor_offset_v1(
                &offset.ok_or_else(|| DbErrorV1::new_v1("cursor missing offset"))?,
            )
            .map_err(|e| DbErrorV1::new_v1(format!("cursor offset decode failed: {:?}", e)))?,
            is_gzip: decode_cursor_is_gzip_v1(
                &is_gzip.ok_or_else(|| DbErrorV1::new_v1("cursor missing is_gzip"))?,
            )
            .map_err(|e| DbErrorV1::new_v1(format!("cursor is_gzip decode failed: {:?}", e)))?,
            last_read_ts: decode_cursor_last_read_ts_v1(
                &last_read_ts.ok_or_else(|| DbErrorV1::new_v1("cursor missing last_read_ts"))?,
            )
            .map_err(|e| {
                DbErrorV1::new_v1(format!("cursor last_read_ts decode failed: {:?}", e))
            })?,
        }))
    }

    pub fn write_open_window_state_v1(
        &self,
        state: &TenantOpenWindowStateV1,
    ) -> Result<(), DbErrorV1> {
        let ops = vec![
            KvWriteOpV1::Put {
                key: key_tenant_active_window_v1(&state.device_key).bytes,
                value: encode_win_active_v1(&state.active),
            },
            KvWriteOpV1::Put {
                key: key_tenant_window_row_feat_v1(
                    &state.device_key,
                    state.active.active_window_id,
                )
                .bytes,
                value: encode_win_row_feat_v1(&state.sparse_counts).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window feat encode failed: {:?}", e))
                })?,
            },
            KvWriteOpV1::Put {
                key: key_tenant_window_row_meta_v1(
                    &state.device_key,
                    state.active.active_window_id,
                )
                .bytes,
                value: encode_win_row_meta_v1(&state.meta),
            },
            KvWriteOpV1::Put {
                key: key_tenant_window_row_ent_srcip_v1(
                    &state.device_key,
                    state.active.active_window_id,
                )
                .bytes,
                value: encode_win_row_ent_srcip_v1(&state.entity_snapshot.srcips).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window srcip encode failed: {:?}", e))
                })?,
            },
            KvWriteOpV1::Put {
                key: key_tenant_window_row_ent_dstip_v1(
                    &state.device_key,
                    state.active.active_window_id,
                )
                .bytes,
                value: encode_win_row_ent_dstip_v1(&state.entity_snapshot.dstips).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window dstip encode failed: {:?}", e))
                })?,
            },
            KvWriteOpV1::Put {
                key: key_tenant_window_row_ent_userid_v1(
                    &state.device_key,
                    state.active.active_window_id,
                )
                .bytes,
                value: encode_win_row_ent_userid_v1(&state.entity_snapshot.userids).map_err(
                    |e| DbErrorV1::new_v1(format!("open-window userid encode failed: {:?}", e)),
                )?,
            },
            KvWriteOpV1::Put {
                key: key_tenant_window_row_ent_domain_v1(
                    &state.device_key,
                    state.active.active_window_id,
                )
                .bytes,
                value: encode_win_row_ent_domain_v1(&state.entity_snapshot.domains).map_err(
                    |e| DbErrorV1::new_v1(format!("open-window domain encode failed: {:?}", e)),
                )?,
            },
            KvWriteOpV1::Put {
                key: key_tenant_window_row_ent_host_v1(
                    &state.device_key,
                    state.active.active_window_id,
                )
                .bytes,
                value: encode_win_row_ent_host_v1(&state.entity_snapshot.hosts).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window host encode failed: {:?}", e))
                })?,
            },
        ];
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_open_window_state_v1(
        &self,
        device_key: &str,
    ) -> Result<Option<TenantOpenWindowStateV1>, DbErrorV1> {
        let active_raw = self.get_raw_v1(key_tenant_active_window_v1(device_key).as_bytes())?;
        let Some(active_raw) = active_raw else {
            return Ok(None);
        };
        let active = decode_win_active_v1(&active_raw)
            .map_err(|e| DbErrorV1::new_v1(format!("open-window active decode failed: {:?}", e)))?;
        let window_id = active.active_window_id;

        let feat = self.get_required_v1(
            key_tenant_window_row_feat_v1(device_key, window_id).as_bytes(),
            "open-window feat",
        )?;
        let meta = self.get_required_v1(
            key_tenant_window_row_meta_v1(device_key, window_id).as_bytes(),
            "open-window meta",
        )?;
        let srcips = self.get_required_v1(
            key_tenant_window_row_ent_srcip_v1(device_key, window_id).as_bytes(),
            "open-window srcip",
        )?;
        let dstips = self.get_required_v1(
            key_tenant_window_row_ent_dstip_v1(device_key, window_id).as_bytes(),
            "open-window dstip",
        )?;
        let userids = self.get_required_v1(
            key_tenant_window_row_ent_userid_v1(device_key, window_id).as_bytes(),
            "open-window userid",
        )?;
        let domains = self.get_required_v1(
            key_tenant_window_row_ent_domain_v1(device_key, window_id).as_bytes(),
            "open-window domain",
        )?;
        let hosts = self.get_required_v1(
            key_tenant_window_row_ent_host_v1(device_key, window_id).as_bytes(),
            "open-window host",
        )?;

        Ok(Some(TenantOpenWindowStateV1 {
            device_key: device_key.to_string(),
            active,
            sparse_counts: decode_win_row_feat_v1(&feat).map_err(|e| {
                DbErrorV1::new_v1(format!("open-window feat decode failed: {:?}", e))
            })?,
            meta: decode_win_row_meta_v1(&meta).map_err(|e| {
                DbErrorV1::new_v1(format!("open-window meta decode failed: {:?}", e))
            })?,
            entity_snapshot: EntitySketchSnapshotV1 {
                srcips: decode_win_row_ent_srcip_v1(&srcips).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window srcip decode failed: {:?}", e))
                })?,
                dstips: decode_win_row_ent_dstip_v1(&dstips).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window dstip decode failed: {:?}", e))
                })?,
                userids: decode_win_row_ent_userid_v1(&userids).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window userid decode failed: {:?}", e))
                })?,
                domains: decode_win_row_ent_domain_v1(&domains).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window domain decode failed: {:?}", e))
                })?,
                hosts: decode_win_row_ent_host_v1(&hosts).map_err(|e| {
                    DbErrorV1::new_v1(format!("open-window host decode failed: {:?}", e))
                })?,
            },
        }))
    }

    pub fn delete_open_window_state_v1(
        &self,
        device_key: &str,
        window_id: u64,
    ) -> Result<(), DbErrorV1> {
        let ops = vec![
            KvWriteOpV1::Delete {
                key: key_tenant_active_window_v1(device_key).bytes,
            },
            KvWriteOpV1::Delete {
                key: key_tenant_window_row_feat_v1(device_key, window_id).bytes,
            },
            KvWriteOpV1::Delete {
                key: key_tenant_window_row_meta_v1(device_key, window_id).bytes,
            },
            KvWriteOpV1::Delete {
                key: key_tenant_window_row_ent_srcip_v1(device_key, window_id).bytes,
            },
            KvWriteOpV1::Delete {
                key: key_tenant_window_row_ent_dstip_v1(device_key, window_id).bytes,
            },
            KvWriteOpV1::Delete {
                key: key_tenant_window_row_ent_userid_v1(device_key, window_id).bytes,
            },
            KvWriteOpV1::Delete {
                key: key_tenant_window_row_ent_domain_v1(device_key, window_id).bytes,
            },
            KvWriteOpV1::Delete {
                key: key_tenant_window_row_ent_host_v1(device_key, window_id).bytes,
            },
        ];
        self.write_batch_raw_v1(&ops)
    }

    pub fn write_df_slot_bucket_state_v1(
        &self,
        state: &TenantDfSlotBucketStateV1,
    ) -> Result<(), DbErrorV1> {
        let ops = vec![
            KvWriteOpV1::Put {
                key: key_tenant_dfn_v1(state.slot, state.bucket).bytes,
                value: encode_dfn_v1(state.window_count),
            },
            KvWriteOpV1::Put {
                key: key_tenant_dfm_v1(state.slot, state.bucket).bytes,
                value: encode_dfm_v1(&state.df_pairs).map_err(|e| {
                    DbErrorV1::new_v1(format!("df slot bucket encode failed: {:?}", e))
                })?,
            },
        ];
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_df_slot_bucket_state_v1(
        &self,
        slot: u8,
        bucket: u8,
    ) -> Result<Option<TenantDfSlotBucketStateV1>, DbErrorV1> {
        let dfn = self.get_raw_v1(key_tenant_dfn_v1(slot, bucket).as_bytes())?;
        let dfm = self.get_raw_v1(key_tenant_dfm_v1(slot, bucket).as_bytes())?;
        let has_any = dfn.is_some() || dfm.is_some();
        if !has_any {
            return Ok(None);
        }

        let dfn = dfn.ok_or_else(|| DbErrorV1::new_v1("df slot bucket missing dfn"))?;
        let dfm = dfm.ok_or_else(|| DbErrorV1::new_v1("df slot bucket missing dfm"))?;
        Ok(Some(TenantDfSlotBucketStateV1 {
            slot,
            bucket,
            window_count: decode_dfn_v1(&dfn).map_err(|e| {
                DbErrorV1::new_v1(format!("df slot bucket dfn decode failed: {:?}", e))
            })?,
            df_pairs: decode_dfm_v1(&dfm).map_err(|e| {
                DbErrorV1::new_v1(format!("df slot bucket dfm decode failed: {:?}", e))
            })?,
        }))
    }

    pub fn read_device_expected_source_state_v1(
        &self,
        device_key: &str,
    ) -> Result<Option<ExpectedSourceStateV1>, DbErrorV1> {
        match self.get_raw_v1(key_tenant_silence_subject_device_state_v1(device_key).as_bytes())? {
            Some(bytes) => Ok(Some(decode_expected_source_state_v1(&bytes).map_err(
                |e| {
                    DbErrorV1::new_v1(format!(
                        "device expected-source state decode failed: {:?}",
                        e
                    ))
                },
            )?)),
            None => Ok(None),
        }
    }

    pub fn write_device_expected_source_state_v1(
        &self,
        device_key: &str,
        state: &ExpectedSourceStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_silence_subject_device_state_v1(device_key).as_bytes(),
            &encode_expected_source_state_v1(state),
        )
    }

    pub fn update_device_expected_source_state_v1(
        &self,
        device_key: &str,
        update: &ExpectedSourceStateUpdateV1,
    ) -> Result<ExpectedSourceStateV1, DbErrorV1> {
        let previous = self.read_device_expected_source_state_v1(device_key)?;
        let next = update_expected_source_state_from_window_v1(previous.as_ref(), update).map_err(
            |e| {
                DbErrorV1::new_v1(format!(
                    "device expected-source state update failed: {:?}",
                    e
                ))
            },
        )?;
        self.write_device_expected_source_state_v1(device_key, &next)?;
        Ok(next)
    }

    pub fn list_device_expected_source_states_v1(
        &self,
    ) -> Result<Vec<(String, ExpectedSourceStateV1)>, DbErrorV1> {
        let prefix = key_prefix_tenant_silence_subject_device_v1("");
        let prefix_bytes = prefix.as_bytes();
        let prefix_text = std::str::from_utf8(prefix_bytes).map_err(|e| {
            DbErrorV1::new_v1(format!("device expected-source prefix utf8 failed: {}", e))
        })?;
        let entries = self.scan_prefix_raw_v1(prefix_bytes)?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_text = String::from_utf8(key).map_err(|e| {
                DbErrorV1::new_v1(format!("device expected-source key utf8 failed: {}", e))
            })?;
            let suffix = key_text
                .strip_prefix(prefix_text)
                .ok_or_else(|| DbErrorV1::new_v1("device expected-source key missing prefix"))?;
            let device_key = suffix.strip_suffix("/state").ok_or_else(|| {
                DbErrorV1::new_v1("device expected-source key missing state suffix")
            })?;
            out.push((
                device_key.to_string(),
                decode_expected_source_state_v1(&value).map_err(|e| {
                    DbErrorV1::new_v1(format!(
                        "device expected-source state decode failed: {:?}",
                        e
                    ))
                })?,
            ));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    pub fn read_tenant_expected_source_state_v1(
        &self,
    ) -> Result<Option<ExpectedSourceStateV1>, DbErrorV1> {
        match self.get_raw_v1(key_tenant_silence_subject_tenant_state_v1().as_bytes())? {
            Some(bytes) => Ok(Some(decode_expected_source_state_v1(&bytes).map_err(
                |e| {
                    DbErrorV1::new_v1(format!(
                        "tenant expected-source state decode failed: {:?}",
                        e
                    ))
                },
            )?)),
            None => Ok(None),
        }
    }

    pub fn write_tenant_expected_source_state_v1(
        &self,
        state: &ExpectedSourceStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_silence_subject_tenant_state_v1().as_bytes(),
            &encode_expected_source_state_v1(state),
        )
    }

    pub fn update_tenant_expected_source_state_v1(
        &self,
        update: &ExpectedSourceStateUpdateV1,
    ) -> Result<ExpectedSourceStateV1, DbErrorV1> {
        let previous = self.read_tenant_expected_source_state_v1()?;
        let next = update_expected_source_state_from_window_v1(previous.as_ref(), update).map_err(
            |e| {
                DbErrorV1::new_v1(format!(
                    "tenant expected-source state update failed: {:?}",
                    e
                ))
            },
        )?;
        self.write_tenant_expected_source_state_v1(&next)?;
        Ok(next)
    }

    pub fn read_device_open_silence_state_v1(
        &self,
        device_key: &str,
    ) -> Result<Option<OpenSilenceStateV1>, DbErrorV1> {
        match self.get_raw_v1(key_tenant_silence_open_device_v1(device_key).as_bytes())? {
            Some(bytes) => Ok(Some(decode_open_silence_state_v1(&bytes).map_err(|e| {
                DbErrorV1::new_v1(format!("device open-silence state decode failed: {:?}", e))
            })?)),
            None => Ok(None),
        }
    }

    pub fn write_device_open_silence_state_v1(
        &self,
        device_key: &str,
        state: &OpenSilenceStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_silence_open_device_v1(device_key).as_bytes(),
            &encode_open_silence_state_v1(state),
        )
    }

    pub fn list_device_open_silence_states_v1(
        &self,
    ) -> Result<Vec<(String, OpenSilenceStateV1)>, DbErrorV1> {
        let base = key_prefix_tenant_silence_open_v1();
        let prefix_text = format!(
            "{}/device/",
            std::str::from_utf8(base.as_bytes()).map_err(|e| DbErrorV1::new_v1(format!(
                "device open-silence prefix utf8 failed: {}",
                e
            )))?
        );
        let entries = self.scan_prefix_raw_v1(prefix_text.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_text = String::from_utf8(key).map_err(|e| {
                DbErrorV1::new_v1(format!("device open-silence key utf8 failed: {}", e))
            })?;
            let device_key = key_text
                .strip_prefix(&prefix_text)
                .ok_or_else(|| DbErrorV1::new_v1("device open-silence key missing prefix"))?;
            out.push((
                device_key.to_string(),
                decode_open_silence_state_v1(&value).map_err(|e| {
                    DbErrorV1::new_v1(format!("device open-silence state decode failed: {:?}", e))
                })?,
            ));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    pub fn read_tenant_open_silence_state_v1(
        &self,
    ) -> Result<Option<OpenSilenceStateV1>, DbErrorV1> {
        match self.get_raw_v1(key_tenant_silence_open_tenant_v1().as_bytes())? {
            Some(bytes) => Ok(Some(decode_open_silence_state_v1(&bytes).map_err(|e| {
                DbErrorV1::new_v1(format!("tenant open-silence state decode failed: {:?}", e))
            })?)),
            None => Ok(None),
        }
    }

    pub fn write_tenant_open_silence_state_v1(
        &self,
        state: &OpenSilenceStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_silence_open_tenant_v1().as_bytes(),
            &encode_open_silence_state_v1(state),
        )
    }

    pub fn read_device_open_drop_state_v1(
        &self,
        device_key: &str,
    ) -> Result<Option<OpenDropStateV1>, DbErrorV1> {
        match self.get_raw_v1(key_tenant_drop_open_device_v1(device_key).as_bytes())? {
            Some(bytes) => Ok(Some(decode_open_drop_state_v1(&bytes).map_err(|e| {
                DbErrorV1::new_v1(format!("device open-drop state decode failed: {:?}", e))
            })?)),
            None => Ok(None),
        }
    }

    pub fn write_device_open_drop_state_v1(
        &self,
        device_key: &str,
        state: &OpenDropStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_drop_open_device_v1(device_key).as_bytes(),
            &encode_open_drop_state_v1(state),
        )
    }

    pub fn list_device_open_drop_states_v1(
        &self,
    ) -> Result<Vec<(String, OpenDropStateV1)>, DbErrorV1> {
        let prefix = key_prefix_tenant_drop_open_device_v1("");
        let prefix_text = std::str::from_utf8(prefix.as_bytes()).map_err(|e| {
            DbErrorV1::new_v1(format!("device open-drop prefix utf8 failed: {}", e))
        })?;
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_text = String::from_utf8(key).map_err(|e| {
                DbErrorV1::new_v1(format!("device open-drop key utf8 failed: {}", e))
            })?;
            let device_key = key_text
                .strip_prefix(prefix_text)
                .ok_or_else(|| DbErrorV1::new_v1("device open-drop key missing prefix"))?;
            out.push((
                device_key.to_string(),
                decode_open_drop_state_v1(&value).map_err(|e| {
                    DbErrorV1::new_v1(format!("device open-drop state decode failed: {:?}", e))
                })?,
            ));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    pub fn read_tenant_open_drop_state_v1(&self) -> Result<Option<OpenDropStateV1>, DbErrorV1> {
        match self.get_raw_v1(key_tenant_drop_open_tenant_v1().as_bytes())? {
            Some(bytes) => Ok(Some(decode_open_drop_state_v1(&bytes).map_err(|e| {
                DbErrorV1::new_v1(format!("tenant open-drop state decode failed: {:?}", e))
            })?)),
            None => Ok(None),
        }
    }

    pub fn write_tenant_open_drop_state_v1(
        &self,
        state: &OpenDropStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_drop_open_tenant_v1().as_bytes(),
            &encode_open_drop_state_v1(state),
        )
    }

    pub fn read_source_stream_open_silence_state_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
    ) -> Result<Option<OpenSilenceStateV1>, DbErrorV1> {
        match self.get_raw_v1(
            key_tenant_silence_open_source_stream_v1(device_key, source_stream_id).as_bytes(),
        )? {
            Some(bytes) => Ok(Some(decode_open_silence_state_v1(&bytes).map_err(|e| {
                DbErrorV1::new_v1(format!(
                    "source-stream open-silence state decode failed: {:?}",
                    e
                ))
            })?)),
            None => Ok(None),
        }
    }

    pub fn write_source_stream_open_silence_state_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
        state: &OpenSilenceStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_silence_open_source_stream_v1(device_key, source_stream_id).as_bytes(),
            &encode_open_silence_state_v1(state),
        )
    }

    pub fn list_source_stream_open_silence_states_for_device_v1(
        &self,
        device_key: &str,
    ) -> Result<Vec<(String, OpenSilenceStateV1)>, DbErrorV1> {
        let prefix = key_prefix_tenant_silence_open_source_stream_v1(device_key, "");
        let prefix_text = std::str::from_utf8(prefix.as_bytes()).map_err(|e| {
            DbErrorV1::new_v1(format!(
                "source-stream open-silence prefix utf8 failed: {}",
                e
            ))
        })?;
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_text = String::from_utf8(key).map_err(|e| {
                DbErrorV1::new_v1(format!("source-stream open-silence key utf8 failed: {}", e))
            })?;
            let source_stream_id = key_text.strip_prefix(prefix_text).ok_or_else(|| {
                DbErrorV1::new_v1("source-stream open-silence key missing prefix")
            })?;
            out.push((
                source_stream_id.to_string(),
                decode_open_silence_state_v1(&value).map_err(|e| {
                    DbErrorV1::new_v1(format!(
                        "source-stream open-silence state decode failed: {:?}",
                        e
                    ))
                })?,
            ));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    pub fn read_source_stream_open_drop_state_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
    ) -> Result<Option<OpenDropStateV1>, DbErrorV1> {
        match self.get_raw_v1(
            key_tenant_drop_open_source_stream_v1(device_key, source_stream_id).as_bytes(),
        )? {
            Some(bytes) => Ok(Some(decode_open_drop_state_v1(&bytes).map_err(|e| {
                DbErrorV1::new_v1(format!(
                    "source-stream open-drop state decode failed: {:?}",
                    e
                ))
            })?)),
            None => Ok(None),
        }
    }

    pub fn write_source_stream_open_drop_state_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
        state: &OpenDropStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_drop_open_source_stream_v1(device_key, source_stream_id).as_bytes(),
            &encode_open_drop_state_v1(state),
        )
    }

    pub fn list_source_stream_open_drop_states_for_device_v1(
        &self,
        device_key: &str,
    ) -> Result<Vec<(String, OpenDropStateV1)>, DbErrorV1> {
        let prefix = key_prefix_tenant_drop_open_source_stream_v1(device_key, "");
        let prefix_text = std::str::from_utf8(prefix.as_bytes()).map_err(|e| {
            DbErrorV1::new_v1(format!("source-stream open-drop prefix utf8 failed: {}", e))
        })?;
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_text = String::from_utf8(key).map_err(|e| {
                DbErrorV1::new_v1(format!("source-stream open-drop key utf8 failed: {}", e))
            })?;
            let source_stream_id = key_text
                .strip_prefix(prefix_text)
                .ok_or_else(|| DbErrorV1::new_v1("source-stream open-drop key missing prefix"))?;
            out.push((
                source_stream_id.to_string(),
                decode_open_drop_state_v1(&value).map_err(|e| {
                    DbErrorV1::new_v1(format!(
                        "source-stream open-drop state decode failed: {:?}",
                        e
                    ))
                })?,
            ));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    pub fn read_source_stream_catalog_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
    ) -> Result<Option<SourceStreamCatalogV1>, DbErrorV1> {
        match self.get_raw_v1(
            key_tenant_source_stream_catalog_v1(device_key, source_stream_id).as_bytes(),
        )? {
            Some(bytes) => Ok(Some(decode_source_stream_catalog_v1(&bytes).map_err(
                |e| DbErrorV1::new_v1(format!("source-stream catalog decode failed: {:?}", e)),
            )?)),
            None => Ok(None),
        }
    }

    pub fn write_source_stream_catalog_v1(
        &self,
        catalog: &SourceStreamCatalogV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_source_stream_catalog_v1(&catalog.device_key, &catalog.source_stream_id)
                .as_bytes(),
            &encode_source_stream_catalog_v1(catalog).map_err(|e| {
                DbErrorV1::new_v1(format!("source-stream catalog encode failed: {:?}", e))
            })?,
        )
    }

    pub fn list_source_stream_catalogs_for_device_v1(
        &self,
        device_key: &str,
    ) -> Result<Vec<SourceStreamCatalogV1>, DbErrorV1> {
        let base = key_prefix_tenant_source_stream_device_v1(device_key);
        let prefix_text = format!(
            "{}/",
            std::str::from_utf8(base.as_bytes()).map_err(|e| DbErrorV1::new_v1(format!(
                "source-stream catalog prefix utf8 failed: {}",
                e
            )))?
        );
        let entries = self.scan_prefix_raw_v1(prefix_text.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_text = String::from_utf8(key).map_err(|e| {
                DbErrorV1::new_v1(format!("source-stream catalog key utf8 failed: {}", e))
            })?;
            let suffix = key_text
                .strip_prefix(&prefix_text)
                .ok_or_else(|| DbErrorV1::new_v1("source-stream catalog key missing prefix"))?;
            let source_stream_id = suffix.strip_suffix("/catalog").ok_or_else(|| {
                DbErrorV1::new_v1("source-stream catalog key missing catalog suffix")
            })?;
            let catalog = decode_source_stream_catalog_v1(&value).map_err(|e| {
                DbErrorV1::new_v1(format!("source-stream catalog decode failed: {:?}", e))
            })?;
            if catalog.device_key != device_key || catalog.source_stream_id != source_stream_id {
                return Err(DbErrorV1::new_v1(
                    "source-stream catalog key/value mismatch",
                ));
            }
            out.push(catalog);
        }
        out.sort_by(|a, b| {
            a.device_key
                .cmp(&b.device_key)
                .then(a.source_stream_id.cmp(&b.source_stream_id))
                .then(a.canonical_source_path.cmp(&b.canonical_source_path))
        });
        Ok(out)
    }

    pub fn read_source_stream_stats_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
        bucket: u8,
    ) -> Result<Option<SourceStreamStatsV1>, DbErrorV1> {
        match self.get_raw_v1(
            key_tenant_source_stats_v1(device_key, source_stream_id, bucket).as_bytes(),
        )? {
            Some(bytes) => Ok(Some(decode_source_stream_stats_v1(&bytes).map_err(
                |e| DbErrorV1::new_v1(format!("source-stream stats decode failed: {:?}", e)),
            )?)),
            None => Ok(None),
        }
    }

    pub fn write_source_stream_stats_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
        bucket: u8,
        stats: &SourceStreamStatsV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_source_stats_v1(device_key, source_stream_id, bucket).as_bytes(),
            &encode_source_stream_stats_v1(stats).map_err(|e| {
                DbErrorV1::new_v1(format!("source-stream stats encode failed: {:?}", e))
            })?,
        )
    }

    pub fn list_source_stream_stats_for_device_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
    ) -> Result<Vec<(u8, SourceStreamStatsV1)>, DbErrorV1> {
        let prefix = key_prefix_tenant_source_stats_v1(device_key, source_stream_id);
        let prefix_text = format!(
            "{}/",
            std::str::from_utf8(prefix.as_bytes()).map_err(|e| DbErrorV1::new_v1(format!(
                "source-stream stats prefix utf8 failed: {}",
                e
            )))?
        );
        let entries = self.scan_prefix_raw_v1(prefix_text.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_text = String::from_utf8(key).map_err(|e| {
                DbErrorV1::new_v1(format!("source-stream stats key utf8 failed: {}", e))
            })?;
            let bucket_text = key_text
                .strip_prefix(&prefix_text)
                .ok_or_else(|| DbErrorV1::new_v1("source-stream stats key missing prefix"))?;
            let bucket = bucket_text.parse::<u8>().map_err(|e| {
                DbErrorV1::new_v1(format!("source-stream stats bucket parse failed: {}", e))
            })?;
            out.push((
                bucket,
                decode_source_stream_stats_v1(&value).map_err(|e| {
                    DbErrorV1::new_v1(format!("source-stream stats decode failed: {:?}", e))
                })?,
            ));
        }
        out.sort_by_key(|(bucket, _)| *bucket);
        Ok(out)
    }

    pub fn read_source_stream_expected_source_state_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
    ) -> Result<Option<ExpectedSourceStateV1>, DbErrorV1> {
        match self.get_raw_v1(
            key_tenant_silence_subject_source_stream_state_v1(device_key, source_stream_id)
                .as_bytes(),
        )? {
            Some(bytes) => Ok(Some(decode_expected_source_state_v1(&bytes).map_err(
                |e| {
                    DbErrorV1::new_v1(format!(
                        "source-stream expected-source state decode failed: {:?}",
                        e
                    ))
                },
            )?)),
            None => Ok(None),
        }
    }

    pub fn write_source_stream_expected_source_state_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
        state: &ExpectedSourceStateV1,
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(
            key_tenant_silence_subject_source_stream_state_v1(device_key, source_stream_id)
                .as_bytes(),
            &encode_expected_source_state_v1(state),
        )
    }

    pub fn update_source_stream_expected_source_state_v1(
        &self,
        device_key: &str,
        source_stream_id: &str,
        update: &ExpectedSourceStateUpdateV1,
    ) -> Result<ExpectedSourceStateV1, DbErrorV1> {
        let previous =
            self.read_source_stream_expected_source_state_v1(device_key, source_stream_id)?;
        let next = update_expected_source_state_from_window_v1(previous.as_ref(), update).map_err(
            |e| {
                DbErrorV1::new_v1(format!(
                    "source-stream expected-source state update failed: {:?}",
                    e
                ))
            },
        )?;
        self.write_source_stream_expected_source_state_v1(device_key, source_stream_id, &next)?;
        Ok(next)
    }

    pub fn list_source_stream_expected_source_states_for_device_v1(
        &self,
        device_key: &str,
    ) -> Result<Vec<(String, ExpectedSourceStateV1)>, DbErrorV1> {
        let prefix = key_prefix_tenant_silence_subject_source_stream_v1(device_key, "");
        let prefix_text = std::str::from_utf8(prefix.as_bytes()).map_err(|e| {
            DbErrorV1::new_v1(format!(
                "source-stream expected-source prefix utf8 failed: {}",
                e
            ))
        })?;
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let key_text = String::from_utf8(key).map_err(|e| {
                DbErrorV1::new_v1(format!(
                    "source-stream expected-source key utf8 failed: {}",
                    e
                ))
            })?;
            let suffix = key_text.strip_prefix(prefix_text).ok_or_else(|| {
                DbErrorV1::new_v1("source-stream expected-source key missing prefix")
            })?;
            let source_stream_id = suffix.strip_suffix("/state").ok_or_else(|| {
                DbErrorV1::new_v1("source-stream expected-source key missing state suffix")
            })?;
            out.push((
                source_stream_id.to_string(),
                decode_expected_source_state_v1(&value).map_err(|e| {
                    DbErrorV1::new_v1(format!(
                        "source-stream expected-source state decode failed: {:?}",
                        e
                    ))
                })?,
            ));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    pub fn write_device_baseline_state_v1(
        &self,
        state: &TenantDeviceBaselineStateV1,
    ) -> Result<(), DbErrorV1> {
        let mut ops = vec![KvWriteOpV1::Put {
            key: key_tenant_centroid_v1(&state.device_key, state.bucket).bytes,
            value: encode_centroid_v1(&state.centroid).map_err(|e| {
                DbErrorV1::new_v1(format!("device baseline centroid encode failed: {:?}", e))
            })?,
        }];
        match &state.stats {
            Some(stats) => ops.push(KvWriteOpV1::Put {
                key: key_tenant_stats_v1(&state.device_key, state.bucket).bytes,
                value: encode_stats_v1(stats),
            }),
            None => ops.push(KvWriteOpV1::Delete {
                key: key_tenant_stats_v1(&state.device_key, state.bucket).bytes,
            }),
        }
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_device_baseline_state_v1(
        &self,
        device_key: &str,
        bucket: u8,
    ) -> Result<Option<TenantDeviceBaselineStateV1>, DbErrorV1> {
        let centroid = self.get_raw_v1(key_tenant_centroid_v1(device_key, bucket).as_bytes())?;
        let stats = self.get_raw_v1(key_tenant_stats_v1(device_key, bucket).as_bytes())?;
        let has_any = centroid.is_some() || stats.is_some();
        if !has_any {
            return Ok(None);
        }

        let centroid =
            centroid.ok_or_else(|| DbErrorV1::new_v1("device baseline missing centroid"))?;
        let stats = match stats {
            Some(bytes) => Some(decode_stats_v1(&bytes).map_err(|e| {
                DbErrorV1::new_v1(format!("device baseline stats decode failed: {:?}", e))
            })?),
            None => None,
        };
        Ok(Some(TenantDeviceBaselineStateV1 {
            device_key: device_key.to_string(),
            bucket,
            centroid: decode_centroid_v1(&centroid).map_err(|e| {
                DbErrorV1::new_v1(format!("device baseline centroid decode failed: {:?}", e))
            })?,
            stats,
        }))
    }

    pub fn write_primary_alert_v1(&self, alert: &AlertV1) -> Result<(), DbErrorV1> {
        let encoded = encode_alert_v1(alert)
            .map_err(|e| DbErrorV1::new_v1(format!("alert encode failed: {:?}", e)))?;
        let existing = self.read_primary_alert_v1(&alert.alert_id)?;
        let mut ops = Vec::new();
        if let Some(previous) = existing.as_ref() {
            append_alert_secondary_index_delete_ops_v1(&mut ops, previous);
        }
        ops.push(KvWriteOpV1::Put {
            key: key_tenant_alert_v1(&alert.alert_id).bytes,
            value: encoded,
        });
        append_alert_secondary_index_put_ops_v1(&mut ops, alert);
        self.write_batch_raw_v1(&ops)
    }

    pub fn read_primary_alert_v1(&self, alert_id: &str) -> Result<Option<AlertV1>, DbErrorV1> {
        match self.get_raw_v1(key_tenant_alert_v1(alert_id).as_bytes())? {
            Some(bytes) => Ok(Some(decode_alert_v1(&bytes).map_err(|e| {
                DbErrorV1::new_v1(format!("alert decode failed: {:?}", e))
            })?)),
            None => Ok(None),
        }
    }

    pub fn list_primary_alert_ids_v1(&self) -> Result<Vec<String>, DbErrorV1> {
        let prefix = key_prefix_tenant_alert_v1();
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            out.push(parse_suffix_after_prefix_v1(
                &key,
                prefix.as_bytes(),
                "tenant alert key",
            )?);
        }
        Ok(out)
    }

    pub fn list_time_index_entries_v1(
        &self,
    ) -> Result<Vec<TenantAlertTimeIndexEntryV1>, DbErrorV1> {
        let entries = self.scan_prefix_raw_v1(b"alert_idx_time/v1")?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            out.push(parse_alert_time_index_entry_v1(&key)?);
        }
        Ok(out)
    }

    pub fn list_category_index_entries_v1(
        &self,
    ) -> Result<Vec<TenantAlertCategoryIndexEntryV1>, DbErrorV1> {
        let entries = self.scan_prefix_raw_v1(b"alert_idx_cat/v1")?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            out.push(parse_alert_category_index_entry_v1(&key)?);
        }
        Ok(out)
    }

    pub fn list_entity_index_entries_v1(
        &self,
    ) -> Result<Vec<TenantAlertEntityIndexEntryV1>, DbErrorV1> {
        let entries = self.scan_prefix_raw_v1(b"alert_idx_ent/v1")?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            out.push(parse_alert_entity_index_entry_v1(&key)?);
        }
        Ok(out)
    }

    pub fn select_alert_ids_via_time_index_if_complete_v1(
        &self,
        since: Option<i64>,
        until: Option<i64>,
    ) -> Result<Option<Vec<String>>, DbErrorV1> {
        let rows = self.list_time_index_entries_v1()?;
        let primary_ids = self.list_primary_alert_ids_v1()?;
        if rows.len() != primary_ids.len() {
            return Ok(None);
        }

        let primary_set: BTreeSet<String> = primary_ids.into_iter().collect();
        let indexed_set: BTreeSet<String> = rows.iter().map(|row| row.alert_id.clone()).collect();
        if primary_set != indexed_set {
            return Ok(None);
        }

        let mut out = Vec::new();
        for row in rows {
            if let Some(since_ts) = since {
                if row.window_start_ts < since_ts {
                    continue;
                }
            }
            if let Some(until_ts) = until {
                if row.window_start_ts >= until_ts {
                    continue;
                }
            }
            out.push(row.alert_id);
        }
        Ok(Some(out))
    }

    pub fn select_alert_ids_via_category_index_if_complete_v1(
        &self,
        category: &str,
        since: Option<i64>,
        until: Option<i64>,
    ) -> Result<Option<Vec<String>>, DbErrorV1> {
        let rows = self.list_category_index_entries_v1()?;
        let primary_ids = self.list_primary_alert_ids_v1()?;
        if rows.len() != primary_ids.len() {
            return Ok(None);
        }

        let primary_set: BTreeSet<String> = primary_ids.into_iter().collect();
        let indexed_set: BTreeSet<String> = rows.iter().map(|row| row.alert_id.clone()).collect();
        if primary_set != indexed_set {
            return Ok(None);
        }

        let mut out = Vec::new();
        for row in rows {
            if row.category != category {
                continue;
            }
            if let Some(since_ts) = since {
                if row.window_start_ts < since_ts {
                    continue;
                }
            }
            if let Some(until_ts) = until {
                if row.window_start_ts >= until_ts {
                    continue;
                }
            }
            out.push(row.alert_id);
        }
        Ok(Some(out))
    }

    pub fn select_alert_ids_via_entity_index_if_complete_v1(
        &self,
        entity_kind: &str,
        entity_value: &str,
        since: Option<i64>,
        until: Option<i64>,
    ) -> Result<Option<Vec<String>>, DbErrorV1> {
        let rows = self.list_entity_index_entries_v1()?;
        let mut indexed_matches = Vec::new();
        for row in rows {
            if row.entity_kind != entity_kind || row.entity_value != entity_value {
                continue;
            }
            if let Some(since_ts) = since {
                if row.window_start_ts < since_ts {
                    continue;
                }
            }
            if let Some(until_ts) = until {
                if row.window_start_ts >= until_ts {
                    continue;
                }
            }
            indexed_matches.push(row.alert_id);
        }

        let mut primary_matches = Vec::new();
        for alert_id in self.list_primary_alert_ids_v1()? {
            let Some(alert) = self.read_primary_alert_v1(&alert_id)? else {
                continue;
            };
            if let Some(since_ts) = since {
                if alert.window_start_ts < since_ts {
                    continue;
                }
            }
            if let Some(until_ts) = until {
                if alert.window_start_ts >= until_ts {
                    continue;
                }
            }
            let matches = collect_alert_entity_index_parts_v1(&alert)
                .into_iter()
                .any(|(kind, value)| kind == entity_kind && value == entity_value);
            if matches {
                primary_matches.push(alert.alert_id);
            }
        }

        let indexed_set: BTreeSet<String> = indexed_matches.iter().cloned().collect();
        let primary_set: BTreeSet<String> = primary_matches.into_iter().collect();
        if indexed_set != primary_set {
            return Ok(None);
        }
        Ok(Some(indexed_matches))
    }

    pub fn append_migrate_journal_entry_v1(
        &self,
        ts: i64,
        name: &str,
        payload: &[u8],
    ) -> Result<(), DbErrorV1> {
        self.put_raw_v1(key_tenant_migrate_journal_v1(ts, name).as_bytes(), payload)
    }

    pub fn scan_migrate_journal_entries_v1(
        &self,
    ) -> Result<Vec<TenantMigrateJournalEntryV1>, DbErrorV1> {
        let prefix = key_prefix_tenant_migrate_journal_v1();
        let entries = self.scan_prefix_raw_v1(prefix.as_bytes())?;
        let mut out = Vec::with_capacity(entries.len());
        for (key, payload) in entries {
            let suffix =
                parse_suffix_after_prefix_v1(&key, prefix.as_bytes(), "tenant migration journal")?;
            let (ts_text, name) = split_once_v1(&suffix, '/', "tenant migration journal key")?;
            out.push(TenantMigrateJournalEntryV1 {
                ts: parse_i64_ascii_v1(ts_text, "tenant migration journal ts")?,
                name: name.to_string(),
                payload,
            });
        }
        Ok(out)
    }

    pub fn list_alert_record_keys_v1(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbErrorV1> {
        let prefix = key_prefix_tenant_alert_v1();
        self.scan_prefix_raw_v1(prefix.as_bytes())
    }

    pub fn list_secondary_alert_index_keys_v1(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbErrorV1> {
        let mut out = Vec::new();
        out.extend(self.scan_prefix_raw_v1(b"alert_idx_time/v1")?);
        out.extend(self.scan_prefix_raw_v1(b"alert_idx_cat/v1")?);
        out.extend(self.scan_prefix_raw_v1(b"alert_idx_ent/v1")?);
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    fn get_required_v1(&self, key: &[u8], label: &str) -> Result<Vec<u8>, DbErrorV1> {
        self.get_raw_v1(key)?
            .ok_or_else(|| DbErrorV1::new_v1(format!("{} missing", label)))
    }
}

fn append_alert_secondary_index_put_ops_v1(ops: &mut Vec<KvWriteOpV1>, alert: &AlertV1) {
    ops.push(KvWriteOpV1::Put {
        key: key_tenant_alert_idx_time_v1(
            &alert.device_key,
            alert.window_start_ts,
            &alert.alert_id,
        )
        .bytes,
        value: Vec::new(),
    });
    ops.push(KvWriteOpV1::Put {
        key: key_tenant_alert_idx_cat_v1(
            alert_label_category_v1(alert),
            alert.window_start_ts,
            &alert.alert_id,
        )
        .bytes,
        value: Vec::new(),
    });
    for (entity_kind, entity_value) in collect_alert_entity_index_parts_v1(alert) {
        ops.push(KvWriteOpV1::Put {
            key: key_tenant_alert_idx_ent_v1(
                &entity_kind,
                &entity_value,
                alert.window_start_ts,
                &alert.alert_id,
            )
            .bytes,
            value: Vec::new(),
        });
    }
}

fn append_alert_secondary_index_delete_ops_v1(ops: &mut Vec<KvWriteOpV1>, alert: &AlertV1) {
    ops.push(KvWriteOpV1::Delete {
        key: key_tenant_alert_idx_time_v1(
            &alert.device_key,
            alert.window_start_ts,
            &alert.alert_id,
        )
        .bytes,
    });
    ops.push(KvWriteOpV1::Delete {
        key: key_tenant_alert_idx_cat_v1(
            alert_label_category_v1(alert),
            alert.window_start_ts,
            &alert.alert_id,
        )
        .bytes,
    });
    for (entity_kind, entity_value) in collect_alert_entity_index_parts_v1(alert) {
        ops.push(KvWriteOpV1::Delete {
            key: key_tenant_alert_idx_ent_v1(
                &entity_kind,
                &entity_value,
                alert.window_start_ts,
                &alert.alert_id,
            )
            .bytes,
        });
    }
}

fn alert_label_category_v1(alert: &AlertV1) -> &'static str {
    match alert.label {
        crate::types::LabelV1::Outlier => "outlier",
        crate::types::LabelV1::NoiseSuspect => "noise_suspect",
        crate::types::LabelV1::Info => "info",
    }
}

fn collect_alert_entity_index_parts_v1(alert: &AlertV1) -> Vec<(String, String)> {
    let mut out = BTreeSet::new();
    for entry in &alert.entities.src_ips {
        if !entry.value.is_empty() {
            out.insert(("srcip".to_string(), entry.value.clone()));
        }
    }
    for entry in &alert.entities.dst_ips {
        if !entry.value.is_empty() {
            out.insert(("dstip".to_string(), entry.value.clone()));
        }
    }
    for entry in &alert.entities.user_ids {
        if !entry.value.is_empty() {
            out.insert(("userid".to_string(), entry.value.clone()));
        }
    }
    for entry in &alert.entities.domains {
        if !entry.value.is_empty() {
            out.insert(("domain".to_string(), entry.value.clone()));
        }
    }
    for entry in &alert.entities.hosts {
        if !entry.value.is_empty() {
            out.insert(("host".to_string(), entry.value.clone()));
        }
    }
    out.into_iter().collect()
}

fn decode_string_v1(bytes: &[u8], label: &str) -> Result<String, DbErrorV1> {
    String::from_utf8(bytes.to_vec())
        .map_err(|e| DbErrorV1::new_v1(format!("{} is not valid UTF-8: {}", label, e)))
}

fn parse_alert_time_index_entry_v1(key: &[u8]) -> Result<TenantAlertTimeIndexEntryV1, DbErrorV1> {
    let suffix =
        parse_suffix_after_prefix_v1(key, b"alert_idx_time/v1", "tenant alert time index key")?;
    let (device_key, rest) = split_once_v1(&suffix, '/', "tenant alert time index key")?;
    let (window_start_ts_text, alert_id) = split_once_v1(rest, '/', "tenant alert time index key")?;
    Ok(TenantAlertTimeIndexEntryV1 {
        device_key: device_key.to_string(),
        window_start_ts: parse_i64_ascii_v1(
            window_start_ts_text,
            "tenant alert time index window_start_ts",
        )?,
        alert_id: alert_id.to_string(),
    })
}

fn parse_alert_category_index_entry_v1(
    key: &[u8],
) -> Result<TenantAlertCategoryIndexEntryV1, DbErrorV1> {
    let suffix =
        parse_suffix_after_prefix_v1(key, b"alert_idx_cat/v1", "tenant alert category index key")?;
    let (category, rest) = split_once_v1(&suffix, '/', "tenant alert category index key")?;
    let (window_start_ts_text, alert_id) =
        split_once_v1(rest, '/', "tenant alert category index key")?;
    Ok(TenantAlertCategoryIndexEntryV1 {
        category: category.to_string(),
        window_start_ts: parse_i64_ascii_v1(
            window_start_ts_text,
            "tenant alert category index window_start_ts",
        )?,
        alert_id: alert_id.to_string(),
    })
}

fn parse_alert_entity_index_entry_v1(
    key: &[u8],
) -> Result<TenantAlertEntityIndexEntryV1, DbErrorV1> {
    let suffix =
        parse_suffix_after_prefix_v1(key, b"alert_idx_ent/v1", "tenant alert entity index key")?;
    let (entity_kind, rest) = split_once_v1(&suffix, '/', "tenant alert entity index key")?;
    let (entity_value, rest) = split_once_v1(rest, '/', "tenant alert entity index key")?;
    let (window_start_ts_text, alert_id) =
        split_once_v1(rest, '/', "tenant alert entity index key")?;
    Ok(TenantAlertEntityIndexEntryV1 {
        entity_kind: entity_kind.to_string(),
        entity_value: entity_value.to_string(),
        window_start_ts: parse_i64_ascii_v1(
            window_start_ts_text,
            "tenant alert entity index window_start_ts",
        )?,
        alert_id: alert_id.to_string(),
    })
}

fn parse_suffix_after_prefix_v1(
    key: &[u8],
    prefix: &[u8],
    label: &str,
) -> Result<String, DbErrorV1> {
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

fn split_once_v1<'a>(
    text: &'a str,
    delim: char,
    label: &str,
) -> Result<(&'a str, &'a str), DbErrorV1> {
    text.split_once(delim)
        .ok_or_else(|| DbErrorV1::new_v1(format!("{} is missing separator '{}'", label, delim)))
}

fn parse_i64_ascii_v1(text: &str, label: &str) -> Result<i64, DbErrorV1> {
    text.parse::<i64>()
        .map_err(|e| DbErrorV1::new_v1(format!("{} is not a valid i64: {}", label, e)))
}
