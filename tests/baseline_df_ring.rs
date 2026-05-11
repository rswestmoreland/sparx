// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use chrono::{TimeZone, Utc};

use sparx::baseline::{
    day_epoch_for_ts_v1, plan_df_ring_update_v1, slot_for_day_epoch_v1, DfRingConfigV1,
    DfRingErrorV1, DfRingMetaStateV1, DfRingMutationV1, DfRingSlotBucketStateV1,
    DF_RING_SLOTS_DEFAULT_V1,
};
use sparx::db::baseline_sketch::{decode_dfm_v1, decode_dfn_v1, DfCountPairV1};
use sparx::db::keys::{
    key_tenant_df_ring_current_day_epoch_v1, key_tenant_df_ring_day_slot_epoch_v1,
    key_tenant_dfm_v1, key_tenant_dfn_v1, KeyBytes,
};
use sparx::db::open_window::{SparseCountPairV1, WinMetaV1};
use sparx::db::tenant_values::{
    decode_meta_df_ring_current_day_epoch_v1, decode_meta_df_ring_day_slot_epoch_v1,
};
use sparx::features::EntitySketchSnapshotV1;
use sparx::window::{bucket_for_window_start_ts_v1, FinalizedWindowRowV1, WindowKeyV1};

fn s(key: &KeyBytes) -> String {
    String::from_utf8(key.bytes.clone()).unwrap()
}

fn row(window_start_ts: i64, window_id: u64, sparse_counts: &[(u32, u32)]) -> FinalizedWindowRowV1 {
    FinalizedWindowRowV1 {
        key: WindowKeyV1 {
            device_key: "dev01".to_string(),
            window_start_ts,
            window_end_ts: window_start_ts + 60,
            bucket: bucket_for_window_start_ts_v1(window_start_ts).unwrap(),
        },
        window_id,
        meta: WinMetaV1 {
            window_start_ts,
            window_end_ts: window_start_ts + 60,
            lines: 1,
            bytes: 128,
            dropped_features: 0,
            dropped_words: 0,
            dropped_shapes: 0,
        },
        sparse_counts: sparse_counts
            .iter()
            .map(|(feature_id, count)| SparseCountPairV1 {
                feature_id: *feature_id,
                count: *count,
            })
            .collect(),
        entity_snapshot: EntitySketchSnapshotV1::default(),
    }
}

#[test]
fn day_epoch_and_slot_follow_utc_floor_rules() {
    let ts = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let day_epoch = day_epoch_for_ts_v1(ts);
    assert_eq!(day_epoch, ts.div_euclid(86_400));
    assert_eq!(
        slot_for_day_epoch_v1(day_epoch, DF_RING_SLOTS_DEFAULT_V1).unwrap(),
        day_epoch.rem_euclid(i64::from(DF_RING_SLOTS_DEFAULT_V1)) as u8
    );

    let negative_ts = -1;
    assert_eq!(day_epoch_for_ts_v1(negative_ts), -1);
    assert_eq!(
        slot_for_day_epoch_v1(-1, DF_RING_SLOTS_DEFAULT_V1).unwrap(),
        6
    );
}

#[test]
fn fresh_slot_update_writes_meta_dfn_and_dfm_with_presence_counts() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, 7, &[(7, 9), (5, 1)]);
    let day_epoch = day_epoch_for_ts_v1(start);
    let slot = slot_for_day_epoch_v1(day_epoch, 7).unwrap();
    let cfg = DfRingConfigV1::default();
    let meta = DfRingMetaStateV1 {
        current_day_epoch: None,
        day_slot_epochs: vec![None; 7],
    };
    let state = DfRingSlotBucketStateV1 {
        window_count: 0,
        df_pairs: Vec::new(),
    };

    let plan = plan_df_ring_update_v1(&row, &cfg, &meta, &state, &[]).unwrap();
    assert!(plan.cleared_stale_slot);
    assert_eq!(plan.day_epoch, day_epoch);
    assert_eq!(plan.slot, slot);
    assert_eq!(plan.next_window_count, 1);
    assert_eq!(
        plan.next_df_pairs,
        vec![
            DfCountPairV1 {
                feature_id: 5,
                df_count: 1
            },
            DfCountPairV1 {
                feature_id: 7,
                df_count: 1
            },
        ]
    );
    assert_eq!(plan.mutations.len(), 4);

    match &plan.mutations[0] {
        DfRingMutationV1::Put(kv) => {
            assert_eq!(kv.key, key_tenant_df_ring_day_slot_epoch_v1(slot));
            assert_eq!(
                decode_meta_df_ring_day_slot_epoch_v1(&kv.value).unwrap(),
                day_epoch
            );
        }
        other => panic!("unexpected first mutation: {:?}", other),
    }
    match &plan.mutations[1] {
        DfRingMutationV1::Put(kv) => {
            assert_eq!(kv.key, key_tenant_df_ring_current_day_epoch_v1());
            assert_eq!(
                decode_meta_df_ring_current_day_epoch_v1(&kv.value).unwrap(),
                day_epoch
            );
        }
        other => panic!("unexpected second mutation: {:?}", other),
    }
    match &plan.mutations[2] {
        DfRingMutationV1::Put(kv) => {
            assert_eq!(kv.key, key_tenant_dfn_v1(slot, row.key.bucket));
            assert_eq!(decode_dfn_v1(&kv.value).unwrap(), 1);
        }
        other => panic!("unexpected third mutation: {:?}", other),
    }
    match &plan.mutations[3] {
        DfRingMutationV1::Put(kv) => {
            assert_eq!(kv.key, key_tenant_dfm_v1(slot, row.key.bucket));
            assert_eq!(decode_dfm_v1(&kv.value).unwrap(), plan.next_df_pairs);
        }
        other => panic!("unexpected fourth mutation: {:?}", other),
    }
}

#[test]
fn same_day_update_accumulates_existing_slot_state_without_rollover() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 16, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, 8, &[(7, 12), (9, 1)]);
    let day_epoch = day_epoch_for_ts_v1(start);
    let slot = slot_for_day_epoch_v1(day_epoch, 7).unwrap();
    let mut day_slot_epochs = vec![None; 7];
    day_slot_epochs[usize::from(slot)] = Some(day_epoch);
    let meta = DfRingMetaStateV1 {
        current_day_epoch: Some(day_epoch),
        day_slot_epochs,
    };
    let state = DfRingSlotBucketStateV1 {
        window_count: 4,
        df_pairs: vec![
            DfCountPairV1 {
                feature_id: 5,
                df_count: 2,
            },
            DfCountPairV1 {
                feature_id: 7,
                df_count: 9,
            },
        ],
    };

    let plan =
        plan_df_ring_update_v1(&row, &DfRingConfigV1::default(), &meta, &state, &[]).unwrap();
    assert!(!plan.cleared_stale_slot);
    assert_eq!(plan.next_window_count, 5);
    assert_eq!(
        plan.next_df_pairs,
        vec![
            DfCountPairV1 {
                feature_id: 5,
                df_count: 2
            },
            DfCountPairV1 {
                feature_id: 7,
                df_count: 10
            },
            DfCountPairV1 {
                feature_id: 9,
                df_count: 1
            },
        ]
    );
    assert_eq!(plan.mutations.len(), 2);
}

#[test]
fn rollover_deletes_stale_slot_keys_sorted_and_resets_slot_bucket_state() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 8, 0, 5, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, 9, &[(44, 2)]);
    let day_epoch = day_epoch_for_ts_v1(start);
    let slot = slot_for_day_epoch_v1(day_epoch, 7).unwrap();
    let mut day_slot_epochs = vec![None; 7];
    day_slot_epochs[usize::from(slot)] = Some(day_epoch - 7);
    let meta = DfRingMetaStateV1 {
        current_day_epoch: Some(day_epoch - 1),
        day_slot_epochs,
    };
    let state = DfRingSlotBucketStateV1 {
        window_count: 99,
        df_pairs: vec![DfCountPairV1 {
            feature_id: 2,
            df_count: 99,
        }],
    };
    let stale_keys = vec![
        key_tenant_dfm_v1(slot, 17),
        key_tenant_dfn_v1(slot, 4),
        key_tenant_dfn_v1(slot, 17),
        key_tenant_dfm_v1(slot, 4),
        key_tenant_dfn_v1(slot, 4),
    ];

    let plan = plan_df_ring_update_v1(&row, &DfRingConfigV1::default(), &meta, &state, &stale_keys)
        .unwrap();
    assert!(plan.cleared_stale_slot);
    assert_eq!(plan.next_window_count, 1);
    assert_eq!(
        plan.next_df_pairs,
        vec![DfCountPairV1 {
            feature_id: 44,
            df_count: 1
        }]
    );

    let deleted: Vec<String> = plan
        .mutations
        .iter()
        .filter_map(|mutation| match mutation {
            DfRingMutationV1::Delete(key) => Some(s(key)),
            DfRingMutationV1::Put(_) => None,
        })
        .collect();
    assert_eq!(
        deleted,
        vec![
            s(&key_tenant_dfm_v1(slot, 4)),
            s(&key_tenant_dfm_v1(slot, 17)),
            s(&key_tenant_dfn_v1(slot, 4)),
            s(&key_tenant_dfn_v1(slot, 17)),
        ]
    );
}

#[test]
fn df_cap_keeps_top_counts_with_feature_id_tiebreak() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 17, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, 10, &[]);
    let day_epoch = day_epoch_for_ts_v1(start);
    let slot = slot_for_day_epoch_v1(day_epoch, 7).unwrap();
    let mut day_slot_epochs = vec![None; 7];
    day_slot_epochs[usize::from(slot)] = Some(day_epoch);
    let meta = DfRingMetaStateV1 {
        current_day_epoch: Some(day_epoch),
        day_slot_epochs,
    };
    let state = DfRingSlotBucketStateV1 {
        window_count: 2,
        df_pairs: vec![
            DfCountPairV1 {
                feature_id: 9,
                df_count: 2,
            },
            DfCountPairV1 {
                feature_id: 5,
                df_count: 2,
            },
            DfCountPairV1 {
                feature_id: 7,
                df_count: 2,
            },
        ],
    };
    let cfg = DfRingConfigV1 {
        df_ring_slots: 7,
        df_bucket_count: 48,
        df_map_cap: 2,
    };

    let plan = plan_df_ring_update_v1(&row, &cfg, &meta, &state, &[]).unwrap();
    assert_eq!(
        plan.next_df_pairs,
        vec![
            DfCountPairV1 {
                feature_id: 5,
                df_count: 2
            },
            DfCountPairV1 {
                feature_id: 7,
                df_count: 2
            },
        ]
    );
}

#[test]
fn rejects_invalid_bucket_and_stale_keys_outside_slot_prefixes() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 18, 0)
        .single()
        .unwrap()
        .timestamp();
    let mut bad_bucket_row = row(start, 11, &[(1, 1)]);
    bad_bucket_row.key.bucket = 48;
    let meta = DfRingMetaStateV1 {
        current_day_epoch: None,
        day_slot_epochs: vec![None; 7],
    };
    let state = DfRingSlotBucketStateV1 {
        window_count: 0,
        df_pairs: Vec::new(),
    };
    assert_eq!(
        plan_df_ring_update_v1(
            &bad_bucket_row,
            &DfRingConfigV1::default(),
            &meta,
            &state,
            &[]
        )
        .unwrap_err(),
        DfRingErrorV1::InvalidBucket {
            bucket: 48,
            df_bucket_count: 48,
        }
    );

    let day_epoch = day_epoch_for_ts_v1(start);
    let slot = slot_for_day_epoch_v1(day_epoch, 7).unwrap();
    let row = row(start, 11, &[(1, 1)]);
    let bad_key = key_tenant_df_ring_current_day_epoch_v1();
    assert_eq!(
        plan_df_ring_update_v1(
            &row,
            &DfRingConfigV1::default(),
            &meta,
            &state,
            &[bad_key.clone()]
        )
        .unwrap_err(),
        DfRingErrorV1::StaleSlotKeyOutsidePrefixes {
            key: s(&bad_key),
            slot,
        }
    );
}
