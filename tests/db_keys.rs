use sparx::db::keys::*;

fn s(k: KeyBytes) -> String {
    String::from_utf8(k.bytes).unwrap()
}

#[test]
fn global_key_builders_match_contract_paths() {
    assert_eq!(s(key_prefix_global_schema_v1()), "meta/schema/v1");
    assert_eq!(s(key_global_schema_version_v1()), "meta/schema/v1/version");
    assert_eq!(s(key_global_schema_created_ts_v1()), "meta/schema/v1/created_ts");
    assert_eq!(s(key_global_schema_last_migrate_ts_v1()), "meta/schema/v1/last_migrate_ts");
    assert_eq!(s(key_prefix_global_process_v1()), "meta/process/v1");
    assert_eq!(s(key_global_process_last_run_start_ts_v1()), "meta/process/v1/last_run_start_ts");
    assert_eq!(s(key_global_process_last_run_end_ts_v1()), "meta/process/v1/last_run_end_ts");
    assert_eq!(s(key_global_process_last_run_exit_code_v1()), "meta/process/v1/last_run_exit_code");
    assert_eq!(s(key_global_process_last_run_host_v1()), "meta/process/v1/last_run_host");
    assert_eq!(s(key_prefix_global_tenant_v1("acme")), "tenant/v1/acme");
    assert_eq!(s(key_global_tenant_created_ts_v1("acme")), "tenant/v1/acme/created_ts");
    assert_eq!(s(key_global_tenant_last_seen_ts_v1("acme")), "tenant/v1/acme/last_seen_ts");
    assert_eq!(s(key_global_tenant_status_v1("acme")), "tenant/v1/acme/status");
    assert_eq!(s(key_global_tenant_root_rel_v1("acme")), "tenant/v1/acme/tenant_root_rel");
    assert_eq!(s(key_global_tenant_db_path_v1("acme")), "tenant/v1/acme/tenant_db_path");
    assert_eq!(s(key_global_alert_out_root_v1("acme")), "tenant/v1/acme/alert_out_root");
    assert_eq!(s(key_prefix_global_tenant_purge_v1("acme")), "tenant_purge/v1/acme");
    assert_eq!(s(key_global_tenant_purge_v1("acme", 1700000000)), "tenant_purge/v1/acme/1700000000");
    assert_eq!(s(key_prefix_global_tenant_idx_active_v1()), "tenant_idx_active/v1");
    assert_eq!(s(key_global_tenant_idx_active_v1("acme")), "tenant_idx_active/v1/acme");
    assert_eq!(s(key_prefix_global_tenant_idx_seen_v1()), "tenant_idx_seen/v1");
    assert_eq!(s(key_global_tenant_idx_seen_v1(1700000000, "acme")), "tenant_idx_seen/v1/1700000000/acme");
    assert_eq!(s(key_prefix_global_metrics_counter_v1()), "metrics/v1/counter");
    assert_eq!(s(key_global_metrics_counter_v1("tenants_known_total")), "metrics/v1/counter/tenants_known_total");
    assert_eq!(s(key_prefix_global_metrics_gauge_v1()), "metrics/v1/gauge");
    assert_eq!(s(key_global_metrics_gauge_v1("tenants_active_total")), "metrics/v1/gauge/tenants_active_total");
    assert_eq!(s(key_prefix_global_migrate_journal_v1()), "migrate/v1/journal");
    assert_eq!(s(key_global_migrate_journal_v1(1700000000, "init")), "migrate/v1/journal/1700000000/init");
}

#[test]
fn tenant_key_builders_match_contract_paths() {
    assert_eq!(s(key_prefix_tenant_schema_v1()), "meta/schema/v1");
    assert_eq!(s(key_tenant_schema_version_v1()), "meta/schema/v1/version");
    assert_eq!(s(key_tenant_schema_created_ts_v1()), "meta/schema/v1/created_ts");
    assert_eq!(s(key_tenant_schema_last_migrate_ts_v1()), "meta/schema/v1/last_migrate_ts");
    assert_eq!(s(key_prefix_tenant_ingest_v1()), "meta/ingest/v1");
    assert_eq!(s(key_tenant_ingest_last_flush_ts_v1()), "meta/ingest/v1/last_flush_ts");
    assert_eq!(s(key_tenant_ingest_worker_epoch_v1()), "meta/ingest/v1/worker_epoch");
    assert_eq!(s(key_prefix_tenant_device_v1("dev01")), "dev/v1/dev01");
    assert_eq!(s(key_tenant_device_path_v1("dev01")), "dev/v1/dev01/path");
    assert_eq!(s(key_tenant_device_created_ts_v1("dev01")), "dev/v1/dev01/created_ts");
    assert_eq!(s(key_tenant_device_last_seen_ts_v1("dev01")), "dev/v1/dev01/last_seen_ts");
    assert_eq!(s(key_prefix_tenant_cursor_v1("dev01", "file01")), "cursor/v1/dev01/file01");
    assert_eq!(s(key_tenant_cursor_inode_v1("dev01", "file01")), "cursor/v1/dev01/file01/inode");
    assert_eq!(s(key_tenant_cursor_mtime_v1("dev01", "file01")), "cursor/v1/dev01/file01/mtime");
    assert_eq!(s(key_tenant_cursor_size_v1("dev01", "file01")), "cursor/v1/dev01/file01/size");
    assert_eq!(s(key_tenant_cursor_offset_v1("dev01", "file01")), "cursor/v1/dev01/file01/offset");
    assert_eq!(s(key_tenant_cursor_is_gzip_v1("dev01", "file01")), "cursor/v1/dev01/file01/is_gzip");
    assert_eq!(s(key_tenant_cursor_last_read_ts_v1("dev01", "file01")), "cursor/v1/dev01/file01/last_read_ts");
}

#[test]
fn tenant_window_baseline_and_alert_keys_match_contract_paths() {
    assert_eq!(s(key_prefix_tenant_win_active_v1()), "win_active/v1");
    assert_eq!(s(key_tenant_active_window_v1("dev01")), "win_active/v1/dev01");
    assert_eq!(s(key_prefix_tenant_win_row_v1("dev01", 42)), "win_row/v1/dev01/42");
    assert_eq!(s(key_tenant_window_row_feat_v1("dev01", 42)), "win_row/v1/dev01/42/feat");
    assert_eq!(s(key_tenant_window_row_meta_v1("dev01", 42)), "win_row/v1/dev01/42/meta");
    assert_eq!(s(key_tenant_window_row_ent_srcip_v1("dev01", 42)), "win_row/v1/dev01/42/ent/srcip");
    assert_eq!(s(key_tenant_window_row_ent_dstip_v1("dev01", 42)), "win_row/v1/dev01/42/ent/dstip");
    assert_eq!(s(key_tenant_window_row_ent_userid_v1("dev01", 42)), "win_row/v1/dev01/42/ent/userid");
    assert_eq!(s(key_tenant_window_row_ent_domain_v1("dev01", 42)), "win_row/v1/dev01/42/ent/domain");
    assert_eq!(s(key_tenant_window_row_ent_host_v1("dev01", 42)), "win_row/v1/dev01/42/ent/host");
    assert_eq!(s(key_prefix_tenant_df_ring_v1()), "meta/df_ring/v1");
    assert_eq!(s(key_tenant_df_ring_current_day_epoch_v1()), "meta/df_ring/v1/current_day_epoch");
    assert_eq!(s(key_tenant_df_ring_day_slot_epoch_v1(6)), "meta/df_ring/v1/day_slot_epoch/6");
    assert_eq!(s(key_tenant_df_ring_last_roll_epoch_v1()), "meta/df_ring/v1/last_roll_epoch");
    assert_eq!(s(key_prefix_tenant_dfn_v1()), "dfN/v1");
    assert_eq!(s(key_prefix_tenant_dfn_slot_v1(6)), "dfN/v1/6");
    assert_eq!(s(key_tenant_dfn_v1(6, 17)), "dfN/v1/6/17");
    assert_eq!(s(key_prefix_tenant_dfm_v1()), "dfM/v1");
    assert_eq!(s(key_prefix_tenant_dfm_slot_v1(6)), "dfM/v1/6");
    assert_eq!(s(key_tenant_dfm_v1(6, 17)), "dfM/v1/6/17");
    assert_eq!(s(key_prefix_tenant_centroid_v1("dev01")), "centroid/v1/dev01");
    assert_eq!(s(key_tenant_centroid_v1("dev01", 17)), "centroid/v1/dev01/17");
    assert_eq!(s(key_prefix_tenant_stats_v1("dev01")), "stats/v1/dev01");
    assert_eq!(s(key_tenant_stats_v1("dev01", 17)), "stats/v1/dev01/17");
    assert_eq!(s(key_prefix_tenant_alert_v1()), "alert/v1");
    assert_eq!(s(key_tenant_alert_v1("alert01")), "alert/v1/alert01");
    assert_eq!(s(key_prefix_tenant_alert_idx_time_v1("dev01")), "alert_idx_time/v1/dev01");
    assert_eq!(s(key_tenant_alert_idx_time_v1("dev01", 1700000000, "alert01")), "alert_idx_time/v1/dev01/1700000000/alert01");
    assert_eq!(s(key_prefix_tenant_alert_idx_cat_v1("outlier")), "alert_idx_cat/v1/outlier");
    assert_eq!(s(key_tenant_alert_idx_cat_v1("outlier", 1700000000, "alert01")), "alert_idx_cat/v1/outlier/1700000000/alert01");
    assert_eq!(s(key_prefix_tenant_alert_idx_ent_v1("userid", "alice")), "alert_idx_ent/v1/userid/alice");
    assert_eq!(s(key_tenant_alert_idx_ent_v1("userid", "alice", 1700000000, "alert01")), "alert_idx_ent/v1/userid/alice/1700000000/alert01");
}

#[test]
fn tenant_feature_metrics_and_migration_keys_match_contract_paths() {
    assert_eq!(s(key_prefix_tenant_feat_dict_str_v1()), "feat_dict/v1/str");
    assert_eq!(s(key_tenant_feature_dict_str_v1("canon=login_fail")), "feat_dict/v1/str/canon=login_fail");
    assert_eq!(s(key_tenant_feature_dict_str_v1("SourceIp_net@/24")), "feat_dict/v1/str/SourceIp_net@/24");
    assert_eq!(s(key_prefix_tenant_feat_dict_id_v1()), "feat_dict/v1/id");
    assert_eq!(s(key_tenant_feature_dict_id_v1(7)), "feat_dict/v1/id/7");
    assert_eq!(s(key_prefix_tenant_feat_dict_meta_v1()), "feat_dict/v1/meta");
    assert_eq!(s(key_tenant_feature_dict_next_id_v1()), "feat_dict/v1/meta/next_id");
    assert_eq!(s(key_tenant_feature_dict_entries_v1()), "feat_dict/v1/meta/entries");
    assert_eq!(s(key_tenant_feature_dict_last_gc_ts_v1()), "feat_dict/v1/meta/last_gc_ts");
    assert_eq!(s(key_prefix_tenant_metrics_counter_v1()), "metrics/v1/counter");
    assert_eq!(s(key_tenant_metrics_counter_v1("cursor_resets_total")), "metrics/v1/counter/cursor_resets_total");
    assert_eq!(s(key_prefix_tenant_metrics_gauge_v1()), "metrics/v1/gauge");
    assert_eq!(s(key_tenant_metrics_gauge_v1("ingest_lag_seconds")), "metrics/v1/gauge/ingest_lag_seconds");
    assert_eq!(s(key_prefix_tenant_migrate_journal_v1()), "migrate/v1/journal");
    assert_eq!(s(key_tenant_migrate_journal_v1(1700000000, "phase2a")), "migrate/v1/journal/1700000000/phase2a");
}

#[test]
fn sample_keys_are_ascii_utf8() {
    let samples = [
        key_global_schema_version_v1(),
        key_global_tenant_status_v1("acme"),
        key_tenant_cursor_offset_v1("dev01", "file01"),
        key_tenant_window_row_ent_domain_v1("dev01", 42),
        key_tenant_feature_dict_str_v1("SourceIp_net@/24"),
        key_tenant_alert_idx_ent_v1("userid", "alice", 1700000000, "alert01"),
    ];

    for key in samples {
        assert!(std::str::from_utf8(key.as_bytes()).is_ok());
        assert!(key.as_bytes().iter().all(|b| *b < 128));
    }
}
