// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Key builders for global and tenant DBs.
// See: contracts/25_tenant_db_key_prefix_map_v0_1.md
//   and contracts/30_global_db_key_prefix_map_v0_1.md
// This module only defines canonical ASCII UTF-8 key formatting. Engine adapters
// and value encodings live in separate DB modules.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyBytes {
    pub bytes: Vec<u8>,
}

impl KeyBytes {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

fn build_key(parts: &[&str]) -> KeyBytes {
    let mut s = String::new();
    for (idx, part) in parts.iter().enumerate() {
        if idx != 0 {
            s.push('/');
        }
        s.push_str(part);
    }
    KeyBytes {
        bytes: s.into_bytes(),
    }
}

fn u8_part(v: u8) -> String {
    v.to_string()
}

fn u32_part(v: u32) -> String {
    v.to_string()
}

fn u64_part(v: u64) -> String {
    v.to_string()
}

fn i64_part(v: i64) -> String {
    v.to_string()
}

// -----------------------------------------------------------------------------
// Global DB prefixes and keys.
// -----------------------------------------------------------------------------

pub fn key_prefix_global_schema_v1() -> KeyBytes {
    build_key(&["meta", "schema", "v1"])
}

pub fn key_prefix_global_process_v1() -> KeyBytes {
    build_key(&["meta", "process", "v1"])
}

pub fn key_prefix_global_tenant_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant", "v1", tenant_id])
}

pub fn key_prefix_global_tenant_purge_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant_purge", "v1", tenant_id])
}

pub fn key_prefix_global_tenant_idx_active_v1() -> KeyBytes {
    build_key(&["tenant_idx_active", "v1"])
}

pub fn key_prefix_global_tenant_idx_seen_v1() -> KeyBytes {
    build_key(&["tenant_idx_seen", "v1"])
}

pub fn key_prefix_global_metrics_counter_v1() -> KeyBytes {
    build_key(&["metrics", "v1", "counter"])
}

pub fn key_prefix_global_metrics_gauge_v1() -> KeyBytes {
    build_key(&["metrics", "v1", "gauge"])
}

pub fn key_prefix_global_migrate_journal_v1() -> KeyBytes {
    build_key(&["migrate", "v1", "journal"])
}

pub fn key_global_schema_version_v1() -> KeyBytes {
    build_key(&["meta", "schema", "v1", "version"])
}

pub fn key_global_schema_created_ts_v1() -> KeyBytes {
    build_key(&["meta", "schema", "v1", "created_ts"])
}

pub fn key_global_schema_last_migrate_ts_v1() -> KeyBytes {
    build_key(&["meta", "schema", "v1", "last_migrate_ts"])
}

pub fn key_global_process_last_run_start_ts_v1() -> KeyBytes {
    build_key(&["meta", "process", "v1", "last_run_start_ts"])
}

pub fn key_global_process_last_run_end_ts_v1() -> KeyBytes {
    build_key(&["meta", "process", "v1", "last_run_end_ts"])
}

pub fn key_global_process_last_run_exit_code_v1() -> KeyBytes {
    build_key(&["meta", "process", "v1", "last_run_exit_code"])
}

pub fn key_global_process_last_run_host_v1() -> KeyBytes {
    build_key(&["meta", "process", "v1", "last_run_host"])
}

pub fn key_global_tenant_created_ts_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant", "v1", tenant_id, "created_ts"])
}

pub fn key_global_tenant_last_seen_ts_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant", "v1", tenant_id, "last_seen_ts"])
}

pub fn key_global_tenant_status_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant", "v1", tenant_id, "status"])
}

pub fn key_global_tenant_root_rel_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant", "v1", tenant_id, "tenant_root_rel"])
}

pub fn key_global_tenant_db_path_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant", "v1", tenant_id, "tenant_db_path"])
}

pub fn key_global_alert_out_root_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant", "v1", tenant_id, "alert_out_root"])
}

pub fn key_global_tenant_purge_v1(tenant_id: &str, ts: i64) -> KeyBytes {
    let ts = i64_part(ts);
    build_key(&["tenant_purge", "v1", tenant_id, &ts])
}

pub fn key_global_tenant_idx_active_v1(tenant_id: &str) -> KeyBytes {
    build_key(&["tenant_idx_active", "v1", tenant_id])
}

pub fn key_global_tenant_idx_seen_v1(last_seen_ts: i64, tenant_id: &str) -> KeyBytes {
    let ts = i64_part(last_seen_ts);
    build_key(&["tenant_idx_seen", "v1", &ts, tenant_id])
}

pub fn key_global_metrics_counter_v1(name: &str) -> KeyBytes {
    build_key(&["metrics", "v1", "counter", name])
}

pub fn key_global_metrics_gauge_v1(name: &str) -> KeyBytes {
    build_key(&["metrics", "v1", "gauge", name])
}

pub fn key_global_migrate_journal_v1(ts: i64, name: &str) -> KeyBytes {
    let ts = i64_part(ts);
    build_key(&["migrate", "v1", "journal", &ts, name])
}

// -----------------------------------------------------------------------------
// Tenant DB prefixes and keys.
// -----------------------------------------------------------------------------

pub fn key_prefix_tenant_schema_v1() -> KeyBytes {
    build_key(&["meta", "schema", "v1"])
}

pub fn key_prefix_tenant_ingest_v1() -> KeyBytes {
    build_key(&["meta", "ingest", "v1"])
}

pub fn key_prefix_tenant_device_v1(device_key: &str) -> KeyBytes {
    build_key(&["dev", "v1", device_key])
}

pub fn key_prefix_tenant_cursor_v1(device_key: &str, file_key: &str) -> KeyBytes {
    build_key(&["cursor", "v1", device_key, file_key])
}

pub fn key_prefix_tenant_win_active_v1() -> KeyBytes {
    build_key(&["win_active", "v1"])
}

pub fn key_prefix_tenant_win_row_v1(device_key: &str, window_id: u64) -> KeyBytes {
    let window_id = u64_part(window_id);
    build_key(&["win_row", "v1", device_key, &window_id])
}

pub fn key_prefix_tenant_feat_dict_str_v1() -> KeyBytes {
    build_key(&["feat_dict", "v1", "str"])
}

pub fn key_prefix_tenant_feat_dict_id_v1() -> KeyBytes {
    build_key(&["feat_dict", "v1", "id"])
}

pub fn key_prefix_tenant_feat_dict_meta_v1() -> KeyBytes {
    build_key(&["feat_dict", "v1", "meta"])
}

pub fn key_prefix_tenant_df_ring_v1() -> KeyBytes {
    build_key(&["meta", "df_ring", "v1"])
}

pub fn key_prefix_tenant_dfn_v1() -> KeyBytes {
    build_key(&["dfN", "v1"])
}

pub fn key_prefix_tenant_dfn_slot_v1(slot: u8) -> KeyBytes {
    let slot = u8_part(slot);
    build_key(&["dfN", "v1", &slot])
}

pub fn key_prefix_tenant_dfm_v1() -> KeyBytes {
    build_key(&["dfM", "v1"])
}

pub fn key_prefix_tenant_dfm_slot_v1(slot: u8) -> KeyBytes {
    let slot = u8_part(slot);
    build_key(&["dfM", "v1", &slot])
}

pub fn key_prefix_tenant_centroid_v1(device_key: &str) -> KeyBytes {
    build_key(&["centroid", "v1", device_key])
}

pub fn key_prefix_tenant_stats_v1(device_key: &str) -> KeyBytes {
    build_key(&["stats", "v1", device_key])
}

pub fn key_prefix_tenant_source_stream_v1() -> KeyBytes {
    build_key(&["source_stream", "v1"])
}

pub fn key_prefix_tenant_source_stream_device_v1(device_key: &str) -> KeyBytes {
    build_key(&["source_stream", "v1", device_key])
}

pub fn key_prefix_tenant_source_stats_v1(device_key: &str, source_stream_id: &str) -> KeyBytes {
    build_key(&["source_stats", "v1", device_key, source_stream_id])
}

pub fn key_prefix_tenant_silence_subject_source_stream_v1(
    device_key: &str,
    source_stream_id: &str,
) -> KeyBytes {
    build_key(&[
        "silence_subject",
        "v1",
        "source_stream",
        device_key,
        source_stream_id,
    ])
}

pub fn key_prefix_tenant_alert_v1() -> KeyBytes {
    build_key(&["alert", "v1"])
}

pub fn key_prefix_tenant_alert_idx_time_v1(device_key: &str) -> KeyBytes {
    build_key(&["alert_idx_time", "v1", device_key])
}

pub fn key_prefix_tenant_alert_idx_cat_v1(category: &str) -> KeyBytes {
    build_key(&["alert_idx_cat", "v1", category])
}

pub fn key_prefix_tenant_alert_idx_ent_v1(entity_kind: &str, entity_value: &str) -> KeyBytes {
    build_key(&["alert_idx_ent", "v1", entity_kind, entity_value])
}

pub fn key_prefix_tenant_metrics_counter_v1() -> KeyBytes {
    build_key(&["metrics", "v1", "counter"])
}

pub fn key_prefix_tenant_metrics_gauge_v1() -> KeyBytes {
    build_key(&["metrics", "v1", "gauge"])
}

pub fn key_prefix_tenant_silence_subject_v1() -> KeyBytes {
    build_key(&["silence_subject", "v1"])
}

pub fn key_prefix_tenant_silence_subject_device_v1(device_key: &str) -> KeyBytes {
    build_key(&["silence_subject", "v1", "device", device_key])
}

pub fn key_prefix_tenant_silence_open_v1() -> KeyBytes {
    build_key(&["silence_open", "v1"])
}

pub fn key_prefix_tenant_silence_open_source_stream_v1(
    device_key: &str,
    source_stream_id: &str,
) -> KeyBytes {
    build_key(&[
        "silence_open",
        "v1",
        "source_stream",
        device_key,
        source_stream_id,
    ])
}

pub fn key_prefix_tenant_drop_open_v1() -> KeyBytes {
    build_key(&["drop_open", "v1"])
}

pub fn key_prefix_tenant_drop_open_device_v1(device_key: &str) -> KeyBytes {
    build_key(&["drop_open", "v1", "device", device_key])
}

pub fn key_prefix_tenant_drop_open_source_stream_v1(
    device_key: &str,
    source_stream_id: &str,
) -> KeyBytes {
    build_key(&[
        "drop_open",
        "v1",
        "source_stream",
        device_key,
        source_stream_id,
    ])
}

pub fn key_prefix_tenant_migrate_journal_v1() -> KeyBytes {
    build_key(&["migrate", "v1", "journal"])
}

pub fn key_tenant_schema_version_v1() -> KeyBytes {
    build_key(&["meta", "schema", "v1", "version"])
}

pub fn key_tenant_schema_created_ts_v1() -> KeyBytes {
    build_key(&["meta", "schema", "v1", "created_ts"])
}

pub fn key_tenant_schema_last_migrate_ts_v1() -> KeyBytes {
    build_key(&["meta", "schema", "v1", "last_migrate_ts"])
}

pub fn key_tenant_ingest_last_flush_ts_v1() -> KeyBytes {
    build_key(&["meta", "ingest", "v1", "last_flush_ts"])
}

pub fn key_tenant_ingest_worker_epoch_v1() -> KeyBytes {
    build_key(&["meta", "ingest", "v1", "worker_epoch"])
}

pub fn key_tenant_device_path_v1(device_key: &str) -> KeyBytes {
    build_key(&["dev", "v1", device_key, "path"])
}

pub fn key_tenant_device_created_ts_v1(device_key: &str) -> KeyBytes {
    build_key(&["dev", "v1", device_key, "created_ts"])
}

pub fn key_tenant_device_last_seen_ts_v1(device_key: &str) -> KeyBytes {
    build_key(&["dev", "v1", device_key, "last_seen_ts"])
}

pub fn key_tenant_cursor_inode_v1(device_key: &str, file_key: &str) -> KeyBytes {
    build_key(&["cursor", "v1", device_key, file_key, "inode"])
}

pub fn key_tenant_cursor_mtime_v1(device_key: &str, file_key: &str) -> KeyBytes {
    build_key(&["cursor", "v1", device_key, file_key, "mtime"])
}

pub fn key_tenant_cursor_size_v1(device_key: &str, file_key: &str) -> KeyBytes {
    build_key(&["cursor", "v1", device_key, file_key, "size"])
}

pub fn key_tenant_cursor_offset_v1(device_key: &str, file_key: &str) -> KeyBytes {
    build_key(&["cursor", "v1", device_key, file_key, "offset"])
}

pub fn key_tenant_cursor_is_gzip_v1(device_key: &str, file_key: &str) -> KeyBytes {
    build_key(&["cursor", "v1", device_key, file_key, "is_gzip"])
}

pub fn key_tenant_cursor_last_read_ts_v1(device_key: &str, file_key: &str) -> KeyBytes {
    build_key(&["cursor", "v1", device_key, file_key, "last_read_ts"])
}

pub fn key_tenant_active_window_v1(device_key: &str) -> KeyBytes {
    build_key(&["win_active", "v1", device_key])
}

pub fn key_tenant_window_row_feat_v1(device_key: &str, window_id: u64) -> KeyBytes {
    let window_id = u64_part(window_id);
    build_key(&["win_row", "v1", device_key, &window_id, "feat"])
}

pub fn key_tenant_window_row_meta_v1(device_key: &str, window_id: u64) -> KeyBytes {
    let window_id = u64_part(window_id);
    build_key(&["win_row", "v1", device_key, &window_id, "meta"])
}

pub fn key_tenant_window_row_ent_srcip_v1(device_key: &str, window_id: u64) -> KeyBytes {
    let window_id = u64_part(window_id);
    build_key(&["win_row", "v1", device_key, &window_id, "ent", "srcip"])
}

pub fn key_tenant_window_row_ent_dstip_v1(device_key: &str, window_id: u64) -> KeyBytes {
    let window_id = u64_part(window_id);
    build_key(&["win_row", "v1", device_key, &window_id, "ent", "dstip"])
}

pub fn key_tenant_window_row_ent_userid_v1(device_key: &str, window_id: u64) -> KeyBytes {
    let window_id = u64_part(window_id);
    build_key(&["win_row", "v1", device_key, &window_id, "ent", "userid"])
}

pub fn key_tenant_window_row_ent_domain_v1(device_key: &str, window_id: u64) -> KeyBytes {
    let window_id = u64_part(window_id);
    build_key(&["win_row", "v1", device_key, &window_id, "ent", "domain"])
}

pub fn key_tenant_window_row_ent_host_v1(device_key: &str, window_id: u64) -> KeyBytes {
    let window_id = u64_part(window_id);
    build_key(&["win_row", "v1", device_key, &window_id, "ent", "host"])
}

pub fn key_tenant_feature_dict_str_v1(feature_string: &str) -> KeyBytes {
    build_key(&["feat_dict", "v1", "str", feature_string])
}

pub fn key_tenant_feature_dict_id_v1(feature_id: u32) -> KeyBytes {
    let feature_id = u32_part(feature_id);
    build_key(&["feat_dict", "v1", "id", &feature_id])
}

pub fn key_tenant_feature_dict_next_id_v1() -> KeyBytes {
    build_key(&["feat_dict", "v1", "meta", "next_id"])
}

pub fn key_tenant_feature_dict_entries_v1() -> KeyBytes {
    build_key(&["feat_dict", "v1", "meta", "entries"])
}

pub fn key_tenant_feature_dict_last_gc_ts_v1() -> KeyBytes {
    build_key(&["feat_dict", "v1", "meta", "last_gc_ts"])
}

pub fn key_tenant_df_ring_current_day_epoch_v1() -> KeyBytes {
    build_key(&["meta", "df_ring", "v1", "current_day_epoch"])
}

pub fn key_tenant_df_ring_day_slot_epoch_v1(slot: u8) -> KeyBytes {
    let slot = u8_part(slot);
    build_key(&["meta", "df_ring", "v1", "day_slot_epoch", &slot])
}

pub fn key_tenant_df_ring_last_roll_epoch_v1() -> KeyBytes {
    build_key(&["meta", "df_ring", "v1", "last_roll_epoch"])
}

pub fn key_tenant_dfn_v1(slot: u8, bucket: u8) -> KeyBytes {
    let slot = u8_part(slot);
    let bucket = u8_part(bucket);
    build_key(&["dfN", "v1", &slot, &bucket])
}

pub fn key_tenant_dfm_v1(slot: u8, bucket: u8) -> KeyBytes {
    let slot = u8_part(slot);
    let bucket = u8_part(bucket);
    build_key(&["dfM", "v1", &slot, &bucket])
}

pub fn key_tenant_centroid_v1(device_key: &str, bucket: u8) -> KeyBytes {
    let bucket = u8_part(bucket);
    build_key(&["centroid", "v1", device_key, &bucket])
}

pub fn key_tenant_stats_v1(device_key: &str, bucket: u8) -> KeyBytes {
    let bucket = u8_part(bucket);
    build_key(&["stats", "v1", device_key, &bucket])
}

pub fn key_tenant_source_stream_catalog_v1(device_key: &str, source_stream_id: &str) -> KeyBytes {
    build_key(&[
        "source_stream",
        "v1",
        device_key,
        source_stream_id,
        "catalog",
    ])
}

pub fn key_tenant_source_stats_v1(
    device_key: &str,
    source_stream_id: &str,
    bucket: u8,
) -> KeyBytes {
    let bucket = u8_part(bucket);
    build_key(&["source_stats", "v1", device_key, source_stream_id, &bucket])
}

pub fn key_tenant_alert_v1(alert_id: &str) -> KeyBytes {
    build_key(&["alert", "v1", alert_id])
}

pub fn key_tenant_alert_idx_time_v1(
    device_key: &str,
    window_start_ts: i64,
    alert_id: &str,
) -> KeyBytes {
    let window_start_ts = i64_part(window_start_ts);
    build_key(&[
        "alert_idx_time",
        "v1",
        device_key,
        &window_start_ts,
        alert_id,
    ])
}

pub fn key_tenant_alert_idx_cat_v1(
    category: &str,
    window_start_ts: i64,
    alert_id: &str,
) -> KeyBytes {
    let window_start_ts = i64_part(window_start_ts);
    build_key(&["alert_idx_cat", "v1", category, &window_start_ts, alert_id])
}

pub fn key_tenant_alert_idx_ent_v1(
    entity_kind: &str,
    entity_value: &str,
    window_start_ts: i64,
    alert_id: &str,
) -> KeyBytes {
    let window_start_ts = i64_part(window_start_ts);
    build_key(&[
        "alert_idx_ent",
        "v1",
        entity_kind,
        entity_value,
        &window_start_ts,
        alert_id,
    ])
}

pub fn key_tenant_metrics_counter_v1(name: &str) -> KeyBytes {
    build_key(&["metrics", "v1", "counter", name])
}

pub fn key_tenant_metrics_gauge_v1(name: &str) -> KeyBytes {
    build_key(&["metrics", "v1", "gauge", name])
}

pub fn key_tenant_silence_subject_device_state_v1(device_key: &str) -> KeyBytes {
    build_key(&["silence_subject", "v1", "device", device_key, "state"])
}

pub fn key_tenant_silence_subject_tenant_state_v1() -> KeyBytes {
    build_key(&["silence_subject", "v1", "tenant", "state"])
}

pub fn key_tenant_silence_subject_source_stream_state_v1(
    device_key: &str,
    source_stream_id: &str,
) -> KeyBytes {
    build_key(&[
        "silence_subject",
        "v1",
        "source_stream",
        device_key,
        source_stream_id,
        "state",
    ])
}

pub fn key_tenant_silence_open_device_v1(device_key: &str) -> KeyBytes {
    build_key(&["silence_open", "v1", "device", device_key])
}

pub fn key_tenant_silence_open_tenant_v1() -> KeyBytes {
    build_key(&["silence_open", "v1", "tenant"])
}

pub fn key_tenant_silence_open_source_stream_v1(
    device_key: &str,
    source_stream_id: &str,
) -> KeyBytes {
    build_key(&[
        "silence_open",
        "v1",
        "source_stream",
        device_key,
        source_stream_id,
    ])
}

pub fn key_tenant_drop_open_device_v1(device_key: &str) -> KeyBytes {
    build_key(&["drop_open", "v1", "device", device_key])
}

pub fn key_tenant_drop_open_tenant_v1() -> KeyBytes {
    build_key(&["drop_open", "v1", "tenant"])
}

pub fn key_tenant_drop_open_source_stream_v1(device_key: &str, source_stream_id: &str) -> KeyBytes {
    build_key(&[
        "drop_open",
        "v1",
        "source_stream",
        device_key,
        source_stream_id,
    ])
}

pub fn key_tenant_migrate_journal_v1(ts: i64, name: &str) -> KeyBytes {
    let ts = i64_part(ts);
    build_key(&["migrate", "v1", "journal", &ts, name])
}
