use std::collections::BTreeMap;

use sparx::db::baseline_sketch::*;
use sparx::db::keys::*;
use sparx::db::tenant_values::*;

fn s(k: KeyBytes) -> String {
    String::from_utf8(k.bytes).unwrap()
}

#[test]
fn dfn_roundtrip_uses_fixed_4_byte_layout() {
    let encoded = encode_dfn_v1(1234);
    assert_eq!(encoded.len(), 4);
    assert_eq!(decode_dfn_v1(&encoded).unwrap(), 1234);
}

#[test]
fn dfm_roundtrip_preserves_df_counts_and_canonical_order() {
    let pairs = vec![
        DfCountPairV1 {
            feature_id: 900,
            df_count: 7,
        },
        DfCountPairV1 {
            feature_id: 7,
            df_count: 1,
        },
        DfCountPairV1 {
            feature_id: 55,
            df_count: 3,
        },
    ];

    let encoded = encode_dfm_v1(&pairs).unwrap();
    let decoded = decode_dfm_v1(&encoded).unwrap();
    assert_eq!(
        decoded,
        vec![
            DfCountPairV1 {
                feature_id: 7,
                df_count: 1,
            },
            DfCountPairV1 {
                feature_id: 55,
                df_count: 3,
            },
            DfCountPairV1 {
                feature_id: 900,
                df_count: 7,
            },
        ]
    );
}

#[test]
fn dfm_rejects_zero_counts_and_non_increasing_feature_ids() {
    let zero_count = vec![DfCountPairV1 {
        feature_id: 7,
        df_count: 0,
    }];
    assert_eq!(encode_dfm_v1(&zero_count).unwrap_err(), BaselineSketchErrorV1::ZeroCount);

    let encoded = vec![2, 9, 1, 8, 2];
    assert_eq!(
        decode_dfm_v1(&encoded).unwrap_err(),
        BaselineSketchErrorV1::FeatureIdsNotStrictlyIncreasing { prev: 9, next: 8 }
    );
}

#[test]
fn centroid_roundtrip_preserves_values_and_canonical_order() {
    let pairs = vec![
        CentroidValuePairV1 {
            feature_id: 500,
            value: 1.5,
        },
        CentroidValuePairV1 {
            feature_id: 9,
            value: -0.25,
        },
        CentroidValuePairV1 {
            feature_id: 77,
            value: 42.0,
        },
    ];

    let encoded = encode_centroid_v1(&pairs).unwrap();
    let decoded = decode_centroid_v1(&encoded).unwrap();
    assert_eq!(
        decoded,
        vec![
            CentroidValuePairV1 {
                feature_id: 9,
                value: -0.25,
            },
            CentroidValuePairV1 {
                feature_id: 77,
                value: 42.0,
            },
            CentroidValuePairV1 {
                feature_id: 500,
                value: 1.5,
            },
        ]
    );
}

#[test]
fn centroid_rejects_non_increasing_feature_ids_and_trailing_bytes() {
    let bad_order = vec![2, 9, 0, 0, 128, 63, 8, 0, 0, 0, 64, 64];
    assert_eq!(
        decode_centroid_v1(&bad_order).unwrap_err(),
        BaselineSketchErrorV1::FeatureIdsNotStrictlyIncreasing { prev: 9, next: 8 }
    );

    let mut trailing = encode_centroid_v1(&[CentroidValuePairV1 {
        feature_id: 1,
        value: 2.0,
    }])
    .unwrap();
    trailing.push(0xff);
    assert_eq!(
        decode_centroid_v1(&trailing).unwrap_err(),
        BaselineSketchErrorV1::TrailingBytes { remaining: 1 }
    );
}

#[test]
fn device_stats_roundtrip_uses_fixed_68_byte_layout() {
    let stats = DeviceStatsV1 {
        line_count: WelfordF64V1 {
            n: 10,
            mean: 25.5,
            m2: 12.25,
        },
        byte_count: WelfordF64V1 {
            n: 10,
            mean: 4096.0,
            m2: 128.0,
        },
        score_total: WelfordF64V1 {
            n: 10,
            mean: 0.42,
            m2: 0.08,
        },
        last_update_ts: 1700000000,
    };

    let encoded = encode_stats_v1(&stats);
    assert_eq!(encoded.len(), DEVICE_STATS_V1_LEN);
    assert_eq!(decode_stats_v1(&encoded).unwrap(), stats);
}

#[test]
fn device_stats_allow_score_total_empty_state_with_n_zero() {
    let stats = DeviceStatsV1 {
        line_count: WelfordF64V1 {
            n: 5,
            mean: 12.0,
            m2: 3.0,
        },
        byte_count: WelfordF64V1 {
            n: 5,
            mean: 512.0,
            m2: 64.0,
        },
        score_total: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        last_update_ts: 1700000123,
    };

    let decoded = decode_stats_v1(&encode_stats_v1(&stats)).unwrap();
    assert_eq!(decoded.score_total.n, 0);
    assert_eq!(decoded.score_total.mean, 0.0);
    assert_eq!(decoded.score_total.m2, 0.0);
}

#[test]
fn ring_rollover_clears_stale_slot_by_prefix_deterministically() {
    let slot = 3u8;
    let bucket = 17u8;
    let device_key = "dev01";
    let old_day_epoch = 20000i64;
    let new_day_epoch = 20007i64;

    let mut kv = BTreeMap::<String, Vec<u8>>::new();
    let dfn_key = s(key_tenant_dfn_v1(slot, bucket));
    let dfm_key = s(key_tenant_dfm_v1(slot, bucket));
    let centroid_key = s(key_tenant_centroid_v1(device_key, bucket));
    let slot_epoch_key = s(key_tenant_df_ring_day_slot_epoch_v1(slot));
    let current_day_key = s(key_tenant_df_ring_current_day_epoch_v1());

    kv.insert(dfn_key.clone(), encode_dfn_v1(9));
    kv.insert(
        dfm_key.clone(),
        encode_dfm_v1(&[DfCountPairV1 {
            feature_id: 7,
            df_count: 9,
        }])
        .unwrap(),
    );
    kv.insert(
        centroid_key.clone(),
        encode_centroid_v1(&[CentroidValuePairV1 {
            feature_id: 7,
            value: 1.0,
        }])
        .unwrap(),
    );
    kv.insert(
        slot_epoch_key.clone(),
        encode_meta_df_ring_day_slot_epoch_v1(old_day_epoch),
    );
    kv.insert(
        current_day_key.clone(),
        encode_meta_df_ring_current_day_epoch_v1(old_day_epoch),
    );

    let dfn_prefix = s(key_prefix_tenant_dfn_slot_v1(slot));
    let dfm_prefix = s(key_prefix_tenant_dfm_slot_v1(slot));
    kv.retain(|key, _| !key.starts_with(&dfn_prefix) && !key.starts_with(&dfm_prefix));
    kv.insert(
        slot_epoch_key.clone(),
        encode_meta_df_ring_day_slot_epoch_v1(new_day_epoch),
    );
    kv.insert(
        current_day_key.clone(),
        encode_meta_df_ring_current_day_epoch_v1(new_day_epoch),
    );

    assert!(!kv.contains_key(&dfn_key));
    assert!(!kv.contains_key(&dfm_key));
    assert!(kv.contains_key(&centroid_key));
    assert_eq!(
        decode_meta_df_ring_day_slot_epoch_v1(kv.get(&slot_epoch_key).unwrap()).unwrap(),
        new_day_epoch
    );
    assert_eq!(
        decode_meta_df_ring_current_day_epoch_v1(kv.get(&current_day_key).unwrap()).unwrap(),
        new_day_epoch
    );
}

#[test]
fn n_bucket_equals_sum_of_dfn_across_slots() {
    let bucket = 11u8;
    let mut kv = BTreeMap::<String, Vec<u8>>::new();

    for slot in 0u8..7u8 {
        kv.insert(s(key_tenant_dfn_v1(slot, bucket)), encode_dfn_v1(u32::from(slot) + 1));
    }

    let mut total = 0u32;
    for slot in 0u8..7u8 {
        let key = s(key_tenant_dfn_v1(slot, bucket));
        total += decode_dfn_v1(kv.get(&key).unwrap()).unwrap();
    }

    assert_eq!(total, 28);
}
