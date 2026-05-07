use tempfile::tempdir;

use sparx::alert::{
    encode_alert_v1, AlertV1, CountedStringV1, EntitiesV1, FileSpanV1, ReasonV1, TopFeatureV1,
    ALERT_SCHEMA_VERSION_V1,
};
use sparx::db::baseline_sketch::{CentroidValuePairV1, DeviceStatsV1, DfCountPairV1, WelfordF64V1};
use sparx::db::keys::key_tenant_alert_v1;
use sparx::db::open_window::{SparseCountPairV1, TopKStringEntryV1, WinActiveV1, WinMetaV1};
use sparx::db::{
    TenantDbV1, TenantDeviceBaselineStateV1, TenantDfSlotBucketStateV1,
    TenantMigrateJournalEntryV1, TenantOpenWindowStateV1, TenantSchemaStateV1,
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
