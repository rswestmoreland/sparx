// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use tempfile::tempdir;

use sparx::alert::{
    encode_alert_v1, AlertV1, CountedStringV1, EntitiesV1, FileSpanV1, ReasonV1, TopFeatureV1,
    ALERT_SCHEMA_VERSION_V1,
};
use sparx::db::baseline_sketch::{CentroidValuePairV1, DeviceStatsV1, DfCountPairV1, WelfordF64V1};
use sparx::db::keys::{
    key_tenant_alert_v1, key_tenant_drop_open_device_v1, key_tenant_drop_open_source_stream_v1,
    key_tenant_drop_open_tenant_v1, key_tenant_silence_open_device_v1,
    key_tenant_silence_open_source_stream_v1, key_tenant_silence_open_tenant_v1,
    key_tenant_silence_subject_device_state_v1, key_tenant_silence_subject_tenant_state_v1,
    key_tenant_silence_subject_source_stream_state_v1, key_tenant_source_stats_v1,
    key_tenant_source_stream_catalog_v1,
};
use sparx::db::open_window::{SparseCountPairV1, TopKStringEntryV1, WinActiveV1, WinMetaV1};
use sparx::db::{
    ExpectedSourceStateUpdateV1, SourceStreamStatsV1, TenantDbV1, TenantDeviceBaselineStateV1,
    TenantDfSlotBucketStateV1, TenantMigrateJournalEntryV1, TenantOpenWindowStateV1,
    TenantSchemaStateV1,
};
use sparx::db::silence::{
    OpenDropStateV1, OpenSilenceStateV1, OPEN_DROP_FLAG_OPEN_V1, OPEN_SILENCE_FLAG_OPEN_V1,
    SILENCE_SCHEMA_VERSION_V1,
    SILENCE_SUBJECT_KIND_DEVICE_V1, SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
    SILENCE_SUBJECT_KIND_TENANT_V1,
};
use sparx::db::source_stream::{
    source_stream_catalog_from_identity_v1, source_stream_identity_from_path_v1,
    update_source_stream_stats_from_observation_v1,
};
use sparx::features::EntitySketchSnapshotV1;
use sparx::ingest::FileCursorV1;
use sparx::types::{ConfidenceV1, FeatureFamilyV1, LabelV1};

fn open_temp_tenant_db_v1() -> Result<TenantDbV1, Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let path = tmp.path().join("tenant.db");
    let _leaked = Box::leak(Box::new(tmp));
    Ok(TenantDbV1::open_at_v1(path)?)
}

fn key_strings_v1(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
    entries
        .iter()
        .map(|(key, _)| String::from_utf8(key.clone()).expect("utf8"))
        .collect()
}

fn sample_alert_v1(alert_id: &str) -> AlertV1 {
    AlertV1 {
        schema_version: ALERT_SCHEMA_VERSION_V1,
        alert_id: alert_id.to_string(),
        tenant_id: "tenant-a".to_string(),
        device_key: "device-001".to_string(),
        device_path: "tenant-a/device-a".to_string(),
        window_start_ts: 1_700_100_000,
        window_end_ts: 1_700_100_600,
        window_size_s: 600,
        bucket: 7,
        label: LabelV1::Outlier,
        confidence: ConfidenceV1::High,
        cold_start: false,
        score_total: 0.91,
        score_rarity: 0.85,
        score_drift: 0.90,
        score_volume: 0.77,
        baseline_n_bucket: Some(42),
        baseline_centroid_norm: Some(1.25),
        reasons: vec![ReasonV1 {
            code: "rare_feature_mix".to_string(),
            msg: "Rare weighted feature mix exceeded threshold".to_string(),
            details: vec![("threshold".to_string(), "0.85".to_string())],
        }],
        top_features: vec![TopFeatureV1 {
            feature: "CANON:user=alice".to_string(),
            feature_id: 11,
            count: 3,
            family: FeatureFamilyV1::Canon,
            tf_w: 0.5,
            idf: 1.1,
            contrib: 0.55,
        }],
        summary_analyst: "Analyst summary".to_string(),
        summary_customer: "Customer summary".to_string(),
        entities: EntitiesV1 {
            src_ips: vec![CountedStringV1 {
                value: "10.0.0.1".to_string(),
                count: 2,
            }],
            dst_ips: Vec::new(),
            user_ids: vec![CountedStringV1 {
                value: "alice".to_string(),
                count: 3,
            }],
            domains: Vec::new(),
            hosts: Vec::new(),
        },
        lines: 12,
        bytes: 4096,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
        provenance: vec![FileSpanV1 {
            file_rel: "messages.log".to_string(),
            file_key: "f-001".to_string(),
            inode: 777,
            offset_start: 120,
            offset_end: 240,
            is_gzip: false,
        }],
        signature: "sig-001".to_string(),
    }
}

#[test]
fn tenant_schema_state_survives_reopen_v1() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let path = tmp.path().join("tenant.db");

    {
        let db = TenantDbV1::open_at_v1(&path)?;
        let state = TenantSchemaStateV1 {
            version: 1,
            created_ts: 1_700_000_100,
            last_migrate_ts: 1_700_000_200,
        };
        db.write_schema_state_v1(&state)?;
        db.persist_sync_all_v1()?;
        assert_eq!(Some(state), db.read_schema_state_v1()?);
    }

    let reopened = TenantDbV1::open_at_v1(&path)?;
    assert_eq!(
        Some(TenantSchemaStateV1 {
            version: 1,
            created_ts: 1_700_000_100,
            last_migrate_ts: 1_700_000_200,
        }),
        reopened.read_schema_state_v1()?
    );
    Ok(())
}

#[test]
fn alert_primary_roundtrip_and_secondary_indexes_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;

    let alert_b = sample_alert_v1("alert-b");
    let alert_a = sample_alert_v1("alert-a");
    let alert_c = sample_alert_v1("alert-c");

    db.write_primary_alert_v1(&alert_b)?;
    db.write_primary_alert_v1(&alert_a)?;
    db.write_primary_alert_v1(&alert_c)?;

    assert_eq!(Some(alert_a.clone()), db.read_primary_alert_v1("alert-a")?);
    assert_eq!(
        vec![
            "alert-a".to_string(),
            "alert-b".to_string(),
            "alert-c".to_string(),
        ],
        db.list_primary_alert_ids_v1()?
    );
    assert_eq!(3, db.list_alert_record_keys_v1()?.len());

    let index_keys = key_strings_v1(&db.list_secondary_alert_index_keys_v1()?);
    assert_eq!(12, index_keys.len());
    assert_eq!(
        vec![
            "alert_idx_cat/v1/outlier/1700100000/alert-a".to_string(),
            "alert_idx_cat/v1/outlier/1700100000/alert-b".to_string(),
            "alert_idx_cat/v1/outlier/1700100000/alert-c".to_string(),
            "alert_idx_ent/v1/srcip/10.0.0.1/1700100000/alert-a".to_string(),
            "alert_idx_ent/v1/srcip/10.0.0.1/1700100000/alert-b".to_string(),
            "alert_idx_ent/v1/srcip/10.0.0.1/1700100000/alert-c".to_string(),
            "alert_idx_ent/v1/userid/alice/1700100000/alert-a".to_string(),
            "alert_idx_ent/v1/userid/alice/1700100000/alert-b".to_string(),
            "alert_idx_ent/v1/userid/alice/1700100000/alert-c".to_string(),
            "alert_idx_time/v1/device-001/1700100000/alert-a".to_string(),
            "alert_idx_time/v1/device-001/1700100000/alert-b".to_string(),
            "alert_idx_time/v1/device-001/1700100000/alert-c".to_string(),
        ],
        index_keys
    );
    Ok(())
}

#[test]
fn alert_rewrite_replaces_stale_secondary_indexes_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;
    let mut alert = sample_alert_v1("alert-a");
    db.write_primary_alert_v1(&alert)?;

    alert.window_start_ts = 1_700_100_600;
    alert.window_end_ts = 1_700_101_200;
    alert.label = LabelV1::Info;
    alert.entities.src_ips.clear();
    alert.entities.user_ids = vec![CountedStringV1 {
        value: "bob".to_string(),
        count: 1,
    }];
    db.write_primary_alert_v1(&alert)?;

    let index_keys = key_strings_v1(&db.list_secondary_alert_index_keys_v1()?);
    assert_eq!(3, index_keys.len());
    assert_eq!(
        vec![
            "alert_idx_cat/v1/info/1700100600/alert-a".to_string(),
            "alert_idx_ent/v1/userid/bob/1700100600/alert-a".to_string(),
            "alert_idx_time/v1/device-001/1700100600/alert-a".to_string(),
        ],
        index_keys
    );
    Ok(())
}

#[test]
fn complete_time_index_returns_filtered_alert_ids_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;

    let mut alert_a = sample_alert_v1("alert-a");
    alert_a.window_start_ts = 1_700_100_000;
    alert_a.window_end_ts = 1_700_100_600;
    let mut alert_b = sample_alert_v1("alert-b");
    alert_b.window_start_ts = 1_700_100_600;
    alert_b.window_end_ts = 1_700_101_200;
    let mut alert_c = sample_alert_v1("alert-c");
    alert_c.window_start_ts = 1_700_101_200;
    alert_c.window_end_ts = 1_700_101_800;

    db.write_primary_alert_v1(&alert_a)?;
    db.write_primary_alert_v1(&alert_b)?;
    db.write_primary_alert_v1(&alert_c)?;

    assert_eq!(
        Some(vec![
            "alert-a".to_string(),
            "alert-b".to_string(),
            "alert-c".to_string(),
        ]),
        db.select_alert_ids_via_time_index_if_complete_v1(None, None)?
    );
    assert_eq!(
        Some(vec!["alert-b".to_string(), "alert-c".to_string()]),
        db.select_alert_ids_via_time_index_if_complete_v1(Some(1_700_100_600), None)?
    );
    assert_eq!(
        Some(vec!["alert-b".to_string()]),
        db.select_alert_ids_via_time_index_if_complete_v1(Some(1_700_100_600), Some(1_700_101_200))?
    );
    Ok(())
}

#[test]
fn incomplete_time_index_forces_fallback_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;

    let indexed = sample_alert_v1("alert-indexed");
    db.write_primary_alert_v1(&indexed)?;

    let mut legacy = sample_alert_v1("alert-legacy");
    legacy.window_start_ts = 1_700_100_600;
    legacy.window_end_ts = 1_700_101_200;
    let encoded = encode_alert_v1(&legacy)
        .map_err(|e| format!("legacy alert encode failed: {:?}", e))?;
    db.put_raw_v1(key_tenant_alert_v1(&legacy.alert_id).as_bytes(), &encoded)?;

    assert_eq!(None, db.select_alert_ids_via_time_index_if_complete_v1(None, None)?);
    Ok(())
}

#[test]
fn complete_category_index_returns_filtered_alert_ids_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;

    let mut alert_a = sample_alert_v1("alert-a");
    alert_a.window_start_ts = 1_700_100_000;
    let mut alert_b = sample_alert_v1("alert-b");
    alert_b.window_start_ts = 1_700_100_600;
    alert_b.label = LabelV1::NoiseSuspect;
    let mut alert_c = sample_alert_v1("alert-c");
    alert_c.window_start_ts = 1_700_101_200;
    alert_c.label = LabelV1::Info;

    db.write_primary_alert_v1(&alert_a)?;
    db.write_primary_alert_v1(&alert_b)?;
    db.write_primary_alert_v1(&alert_c)?;

    assert_eq!(
        Some(vec!["alert-b".to_string()]),
        db.select_alert_ids_via_category_index_if_complete_v1("noise_suspect", None, None)?
    );
    assert_eq!(
        Some(vec!["alert-c".to_string()]),
        db.select_alert_ids_via_category_index_if_complete_v1("info", Some(1_700_101_200), None)?
    );
    Ok(())
}

#[test]
fn complete_entity_index_returns_filtered_alert_ids_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;

    let alert_a = sample_alert_v1("alert-a");
    let mut alert_b = sample_alert_v1("alert-b");
    alert_b.window_start_ts = 1_700_100_600;
    alert_b.entities.user_ids = vec![CountedStringV1 {
        value: "bob".to_string(),
        count: 1,
    }];
    let mut alert_c = sample_alert_v1("alert-c");
    alert_c.window_start_ts = 1_700_101_200;

    db.write_primary_alert_v1(&alert_a)?;
    db.write_primary_alert_v1(&alert_b)?;
    db.write_primary_alert_v1(&alert_c)?;

    assert_eq!(
        Some(vec!["alert-a".to_string(), "alert-c".to_string()]),
        db.select_alert_ids_via_entity_index_if_complete_v1("userid", "alice", None, None)?
    );
    assert_eq!(
        Some(vec!["alert-b".to_string()]),
        db.select_alert_ids_via_entity_index_if_complete_v1("userid", "bob", Some(1_700_100_600), None)?
    );
    Ok(())
}

#[test]
fn incomplete_entity_index_forces_fallback_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;

    let indexed = sample_alert_v1("alert-indexed");
    db.write_primary_alert_v1(&indexed)?;

    let mut legacy = sample_alert_v1("alert-legacy");
    legacy.window_start_ts = 1_700_100_600;
    legacy.window_end_ts = 1_700_101_200;
    let encoded = encode_alert_v1(&legacy)
        .map_err(|e| format!("legacy alert encode failed: {:?}", e))?;
    db.put_raw_v1(key_tenant_alert_v1(&legacy.alert_id).as_bytes(), &encoded)?;

    assert_eq!(
        None,
        db.select_alert_ids_via_entity_index_if_complete_v1("userid", "alice", None, None)?
    );
    Ok(())
}

#[test]
fn cursor_open_window_and_baseline_roundtrip_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;

    let cursor = FileCursorV1 {
        inode: 12345,
        mtime: 1_700_000_300,
        size: 900,
        offset: 640,
        is_gzip: false,
        last_read_ts: 1_700_000_305,
    };
    db.write_cursor_v1("device-001", "file-001", &cursor)?;
    assert_eq!(Some(cursor), db.read_cursor_v1("device-001", "file-001")?);

    let open_window = TenantOpenWindowStateV1 {
        device_key: "device-001".to_string(),
        active: WinActiveV1 {
            active_window_start_ts: 1_700_000_000,
            active_window_id: 7,
            last_update_ts: 1_700_000_360,
        },
        sparse_counts: vec![
            SparseCountPairV1 {
                feature_id: 3,
                count: 2,
            },
            SparseCountPairV1 {
                feature_id: 8,
                count: 9,
            },
        ],
        meta: WinMetaV1 {
            window_start_ts: 1_700_000_000,
            window_end_ts: 1_700_000_600,
            lines: 5,
            bytes: 1200,
            dropped_features: 0,
            dropped_words: 1,
            dropped_shapes: 0,
        },
        entity_snapshot: EntitySketchSnapshotV1 {
            srcips: vec![TopKStringEntryV1 {
                value: "10.1.1.1".to_string(),
                count: 2,
            }],
            dstips: vec![TopKStringEntryV1 {
                value: "10.2.2.2".to_string(),
                count: 1,
            }],
            userids: vec![TopKStringEntryV1 {
                value: "alice".to_string(),
                count: 3,
            }],
            domains: vec![TopKStringEntryV1 {
                value: "example.org".to_string(),
                count: 1,
            }],
            hosts: vec![TopKStringEntryV1 {
                value: "host-a".to_string(),
                count: 1,
            }],
        },
    };
    db.write_open_window_state_v1(&open_window)?;
    assert_eq!(Some(open_window.clone()), db.read_open_window_state_v1("device-001")?);
    db.delete_open_window_state_v1("device-001", open_window.active.active_window_id)?;
    assert_eq!(None, db.read_open_window_state_v1("device-001")?);

    let df_state = TenantDfSlotBucketStateV1 {
        slot: 2,
        bucket: 7,
        window_count: 12,
        df_pairs: vec![
            DfCountPairV1 {
                feature_id: 3,
                df_count: 4,
            },
            DfCountPairV1 {
                feature_id: 8,
                df_count: 7,
            },
        ],
    };
    db.write_df_slot_bucket_state_v1(&df_state)?;
    assert_eq!(Some(df_state), db.read_df_slot_bucket_state_v1(2, 7)?);

    let baseline_state = TenantDeviceBaselineStateV1 {
        device_key: "device-001".to_string(),
        bucket: 7,
        centroid: vec![
            CentroidValuePairV1 {
                feature_id: 3,
                value: 0.25,
            },
            CentroidValuePairV1 {
                feature_id: 8,
                value: 0.75,
            },
        ],
        stats: Some(DeviceStatsV1 {
            line_count: WelfordF64V1 {
                n: 10,
                mean: 5.5,
                m2: 3.25,
            },
            byte_count: WelfordF64V1 {
                n: 10,
                mean: 400.0,
                m2: 22.0,
            },
            score_total: WelfordF64V1 {
                n: 10,
                mean: 0.35,
                m2: 0.08,
            },
            last_update_ts: 1_700_000_700,
        }),
    };
    db.write_device_baseline_state_v1(&baseline_state)?;
    assert_eq!(
        Some(baseline_state),
        db.read_device_baseline_state_v1("device-001", 7)?
    );

    Ok(())
}

#[test]
fn tenant_migrate_journal_scan_is_deterministic_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;

    db.append_migrate_journal_entry_v1(200, "b-step", b"bb")?;
    db.append_migrate_journal_entry_v1(100, "c-step", b"cc")?;
    db.append_migrate_journal_entry_v1(100, "a-step", b"aa")?;

    assert_eq!(
        vec![
            TenantMigrateJournalEntryV1 {
                ts: 100,
                name: "a-step".to_string(),
                payload: b"aa".to_vec(),
            },
            TenantMigrateJournalEntryV1 {
                ts: 100,
                name: "c-step".to_string(),
                payload: b"cc".to_vec(),
            },
            TenantMigrateJournalEntryV1 {
                ts: 200,
                name: "b-step".to_string(),
                payload: b"bb".to_vec(),
            },
        ],
        db.scan_migrate_journal_entries_v1()?
    );
    Ok(())
}

#[test]
fn tenant_db_updates_expected_source_state_records_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;
    let device_update = ExpectedSourceStateUpdateV1 {
        subject_kind_u8: SILENCE_SUBJECT_KIND_DEVICE_V1,
        window_size_s_u32: 60,
        window_start_ts_i64: 1_700_000_000,
        window_end_ts_i64: 1_700_000_060,
        observed_lines_u64: 12,
        observed_bytes_u64: 2048,
        bucket_u8: 17,
        update_ts_i64: 1_700_000_060,
        min_lines_per_window_u32: 10,
    };
    let tenant_update = ExpectedSourceStateUpdateV1 {
        subject_kind_u8: SILENCE_SUBJECT_KIND_TENANT_V1,
        ..device_update.clone()
    };

    let device_state = db.update_device_expected_source_state_v1("device-001", &device_update)?;
    let tenant_state = db.update_tenant_expected_source_state_v1(&tenant_update)?;

    assert_eq!(device_state.observed_windows_total_u64, 1);
    assert_eq!(device_state.mature_windows_total_u64, 1);
    assert_eq!(tenant_state.subject_kind_u8, SILENCE_SUBJECT_KIND_TENANT_V1);
    assert_eq!(tenant_state.last_observed_lines_u64, 12);

    assert!(db
        .get_raw_v1(key_tenant_silence_subject_device_state_v1("device-001").as_bytes())?
        .is_some());
    assert!(db
        .get_raw_v1(key_tenant_silence_subject_tenant_state_v1().as_bytes())?
        .is_some());
    assert_eq!(db.read_device_expected_source_state_v1("device-001")?, Some(device_state.clone()));
    assert_eq!(db.read_tenant_expected_source_state_v1()?, Some(tenant_state));
    assert_eq!(
        db.list_device_expected_source_states_v1()?,
        vec![("device-001".to_string(), device_state)]
    );
    Ok(())
}


#[test]
fn tenant_db_roundtrips_source_stream_catalog_stats_and_state_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;
    let identity = source_stream_identity_from_path_v1("tenant-a", "device-001", "var/log/auth.log")?;
    let catalog = source_stream_catalog_from_identity_v1(&identity, 1_700_000_000, 1_700_000_060)?;

    db.write_source_stream_catalog_v1(&catalog)?;
    assert!(db
        .get_raw_v1(key_tenant_source_stream_catalog_v1("device-001", &identity.source_stream_id).as_bytes())?
        .is_some());
    assert_eq!(
        db.read_source_stream_catalog_v1("device-001", &identity.source_stream_id)?,
        Some(catalog.clone())
    );
    assert_eq!(db.list_source_stream_catalogs_for_device_v1("device-001")?, vec![catalog]);

    let stats = update_source_stream_stats_from_observation_v1(None, 12, 1200, 1_700_000_060)?;
    let stats = update_source_stream_stats_from_observation_v1(Some(&stats), 18, 2400, 1_700_000_120)?;
    db.write_source_stream_stats_v1("device-001", &identity.source_stream_id, 17, &stats)?;
    assert!(db
        .get_raw_v1(key_tenant_source_stats_v1("device-001", &identity.source_stream_id, 17).as_bytes())?
        .is_some());
    assert_eq!(
        db.read_source_stream_stats_v1("device-001", &identity.source_stream_id, 17)?,
        Some(SourceStreamStatsV1 {
            line_count: WelfordF64V1 { n: 2, mean: 15.0, m2: 18.0 },
            byte_count: WelfordF64V1 { n: 2, mean: 1800.0, m2: 720000.0 },
            score_total: WelfordF64V1 { n: 0, mean: 0.0, m2: 0.0 },
            last_update_ts: 1_700_000_120,
        })
    );
    assert_eq!(
        db.list_source_stream_stats_for_device_v1("device-001", &identity.source_stream_id)?,
        vec![(17, stats.clone())]
    );

    let update = ExpectedSourceStateUpdateV1 {
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        window_size_s_u32: 60,
        window_start_ts_i64: 1_700_000_000,
        window_end_ts_i64: 1_700_000_060,
        observed_lines_u64: 12,
        observed_bytes_u64: 1200,
        bucket_u8: 17,
        update_ts_i64: 1_700_000_060,
        min_lines_per_window_u32: 10,
    };
    let state = db.update_source_stream_expected_source_state_v1("device-001", &identity.source_stream_id, &update)?;
    assert_eq!(state.subject_kind_u8, SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1);
    assert!(db
        .get_raw_v1(key_tenant_silence_subject_source_stream_state_v1("device-001", &identity.source_stream_id).as_bytes())?
        .is_some());
    assert_eq!(
        db.read_source_stream_expected_source_state_v1("device-001", &identity.source_stream_id)?,
        Some(state.clone())
    );
    assert_eq!(
        db.list_source_stream_expected_source_states_for_device_v1("device-001")?,
        vec![(identity.source_stream_id, state)]
    );
    Ok(())
}

#[test]
fn tenant_db_roundtrips_open_silence_dedup_state_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;
    let device_open = OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_DEVICE_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_OPEN_V1,
        silence_start_ts_i64: 1_700_000_060,
        last_alert_window_start_ts_i64: 1_700_000_060,
        last_alert_window_end_ts_i64: 1_700_000_240,
        last_alert_id: "0123456789abcdef0123456789abcdef".to_string(),
    };
    let tenant_open = OpenSilenceStateV1 {
        subject_kind_u8: SILENCE_SUBJECT_KIND_TENANT_V1,
        last_alert_id: "abcdef0123456789abcdef0123456789".to_string(),
        ..device_open.clone()
    };

    db.write_device_open_silence_state_v1("device-001", &device_open)?;
    db.write_tenant_open_silence_state_v1(&tenant_open)?;

    assert!(db
        .get_raw_v1(key_tenant_silence_open_device_v1("device-001").as_bytes())?
        .is_some());
    assert!(db
        .get_raw_v1(key_tenant_silence_open_tenant_v1().as_bytes())?
        .is_some());
    assert_eq!(db.read_device_open_silence_state_v1("device-001")?, Some(device_open));
    assert_eq!(db.read_tenant_open_silence_state_v1()?, Some(tenant_open));
    Ok(())
}


#[test]
fn tenant_db_roundtrips_open_drop_state_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;
    let device_open = OpenDropStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_DEVICE_V1,
        state_flags_u8: OPEN_DROP_FLAG_OPEN_V1,
        drop_start_ts_i64: 1_700_000_060,
        last_alert_window_start_ts_i64: 1_700_000_060,
        last_alert_window_end_ts_i64: 1_700_000_120,
        last_alert_id: "0123456789abcdef0123456789abcdef".to_string(),
    };
    let tenant_open = OpenDropStateV1 {
        subject_kind_u8: SILENCE_SUBJECT_KIND_TENANT_V1,
        last_alert_id: "abcdef0123456789abcdef0123456789".to_string(),
        ..device_open.clone()
    };

    db.write_device_open_drop_state_v1("device-001", &device_open)?;
    db.write_tenant_open_drop_state_v1(&tenant_open)?;

    assert!(db
        .get_raw_v1(key_tenant_drop_open_device_v1("device-001").as_bytes())?
        .is_some());
    assert!(db
        .get_raw_v1(key_tenant_drop_open_tenant_v1().as_bytes())?
        .is_some());
    assert_eq!(db.read_device_open_drop_state_v1("device-001")?, Some(device_open.clone()));
    assert_eq!(db.read_tenant_open_drop_state_v1()?, Some(tenant_open));
    assert_eq!(db.list_device_open_drop_states_v1()?, vec![("device-001".to_string(), device_open)]);
    Ok(())
}

#[test]
fn tenant_db_roundtrips_source_stream_open_states_v1() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_temp_tenant_db_v1()?;
    let source_stream_id_a = "0123456789abcdef0123456789abcdef";
    let source_stream_id_b = "fedcba9876543210fedcba9876543210";
    let silence_a = OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_OPEN_V1,
        silence_start_ts_i64: 1_700_000_060,
        last_alert_window_start_ts_i64: 1_700_000_060,
        last_alert_window_end_ts_i64: 1_700_000_240,
        last_alert_id: "0123456789abcdef0123456789abcdef".to_string(),
    };
    let silence_b = OpenSilenceStateV1 {
        last_alert_id: "abcdef0123456789abcdef0123456789".to_string(),
        ..silence_a.clone()
    };
    let drop_a = OpenDropStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        state_flags_u8: OPEN_DROP_FLAG_OPEN_V1,
        drop_start_ts_i64: 1_700_000_060,
        last_alert_window_start_ts_i64: 1_700_000_060,
        last_alert_window_end_ts_i64: 1_700_000_120,
        last_alert_id: "11111111111111111111111111111111".to_string(),
    };
    let drop_b = OpenDropStateV1 {
        last_alert_id: "22222222222222222222222222222222".to_string(),
        ..drop_a.clone()
    };

    db.write_source_stream_open_silence_state_v1("device-001", source_stream_id_b, &silence_b)?;
    db.write_source_stream_open_silence_state_v1("device-001", source_stream_id_a, &silence_a)?;
    db.write_source_stream_open_drop_state_v1("device-001", source_stream_id_b, &drop_b)?;
    db.write_source_stream_open_drop_state_v1("device-001", source_stream_id_a, &drop_a)?;

    assert!(db
        .get_raw_v1(key_tenant_silence_open_source_stream_v1("device-001", source_stream_id_a).as_bytes())?
        .is_some());
    assert!(db
        .get_raw_v1(key_tenant_drop_open_source_stream_v1("device-001", source_stream_id_a).as_bytes())?
        .is_some());
    assert_eq!(
        db.read_source_stream_open_silence_state_v1("device-001", source_stream_id_a)?,
        Some(silence_a.clone())
    );
    assert_eq!(
        db.read_source_stream_open_drop_state_v1("device-001", source_stream_id_a)?,
        Some(drop_a.clone())
    );
    assert_eq!(
        db.list_source_stream_open_silence_states_for_device_v1("device-001")?,
        vec![
            (source_stream_id_a.to_string(), silence_a),
            (source_stream_id_b.to_string(), silence_b),
        ]
    );
    assert_eq!(
        db.list_source_stream_open_drop_states_for_device_v1("device-001")?,
        vec![
            (source_stream_id_a.to_string(), drop_a),
            (source_stream_id_b.to_string(), drop_b),
        ]
    );
    Ok(())
}
