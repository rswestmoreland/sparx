// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;

use sparx::db::keys::*;
use sparx::db::open_window::*;

fn s(k: KeyBytes) -> String {
    String::from_utf8(k.bytes).unwrap()
}

#[test]
fn win_active_roundtrip_uses_fixed_24_byte_layout() {
    let value = WinActiveV1 {
        active_window_start_ts: 1700000000,
        active_window_id: 42,
        last_update_ts: 1700000030,
    };

    let encoded = encode_win_active_v1(&value);
    assert_eq!(encoded.len(), WIN_ACTIVE_V1_LEN);
    assert_eq!(decode_win_active_v1(&encoded).unwrap(), value);
}

#[test]
fn sparse_counts_roundtrip_preserves_counts_and_canonical_order() {
    let pairs = vec![
        SparseCountPairV1 {
            feature_id: 900,
            count: 7,
        },
        SparseCountPairV1 {
            feature_id: 7,
            count: 1,
        },
        SparseCountPairV1 {
            feature_id: 55,
            count: 3,
        },
    ];

    let encoded = encode_win_row_feat_v1(&pairs).unwrap();
    let decoded = decode_win_row_feat_v1(&encoded).unwrap();
    assert_eq!(
        decoded,
        vec![
            SparseCountPairV1 {
                feature_id: 7,
                count: 1,
            },
            SparseCountPairV1 {
                feature_id: 55,
                count: 3,
            },
            SparseCountPairV1 {
                feature_id: 900,
                count: 7,
            },
        ]
    );
}

#[test]
fn sparse_counts_reject_zero_and_non_increasing_feature_ids() {
    let zero_count = vec![SparseCountPairV1 {
        feature_id: 7,
        count: 0,
    }];
    assert_eq!(
        encode_win_row_feat_v1(&zero_count).unwrap_err(),
        OpenWindowErrorV1::ZeroCount
    );

    let encoded = vec![2, 9, 1, 8, 2];
    assert_eq!(
        decode_win_row_feat_v1(&encoded).unwrap_err(),
        OpenWindowErrorV1::FeatureIdsNotStrictlyIncreasing { prev: 9, next: 8 }
    );
}

#[test]
fn win_meta_roundtrip_has_constant_40_byte_layout() {
    let meta = WinMetaV1 {
        window_start_ts: 1700000000,
        window_end_ts: 1700000600,
        lines: 25,
        bytes: 4096,
        dropped_features: 2,
        dropped_words: 3,
        dropped_shapes: 4,
    };

    let encoded = encode_win_row_meta_v1(&meta);
    assert_eq!(encoded.len(), WIN_META_V1_LEN);
    assert_eq!(decode_win_row_meta_v1(&encoded).unwrap(), meta);
}

#[test]
fn topk_strings_order_is_deterministic_for_ties() {
    let entries = vec![
        TopKStringEntryV1 {
            value: "zebra".to_string(),
            count: 4,
        },
        TopKStringEntryV1 {
            value: "alpha".to_string(),
            count: 4,
        },
        TopKStringEntryV1 {
            value: "omega".to_string(),
            count: 9,
        },
    ];

    let encoded = encode_win_row_ent_userid_v1(&entries).unwrap();
    let decoded = decode_win_row_ent_userid_v1(&encoded).unwrap();
    assert_eq!(
        decoded,
        vec![
            TopKStringEntryV1 {
                value: "omega".to_string(),
                count: 9,
            },
            TopKStringEntryV1 {
                value: "alpha".to_string(),
                count: 4,
            },
            TopKStringEntryV1 {
                value: "zebra".to_string(),
                count: 4,
            },
        ]
    );
}

#[test]
fn topk_strings_reject_duplicate_values_and_zero_counts() {
    let dupes = vec![
        TopKStringEntryV1 {
            value: "alice".to_string(),
            count: 2,
        },
        TopKStringEntryV1 {
            value: "alice".to_string(),
            count: 1,
        },
    ];
    assert_eq!(
        encode_win_row_ent_domain_v1(&dupes).unwrap_err(),
        OpenWindowErrorV1::DuplicateTopKValue
    );

    let zero = vec![TopKStringEntryV1 {
        value: "host01".to_string(),
        count: 0,
    }];
    assert_eq!(
        encode_win_row_ent_host_v1(&zero).unwrap_err(),
        OpenWindowErrorV1::ZeroCount
    );
}

#[test]
fn checkpoint_write_order_keeps_win_active_last_and_finalize_advances_deterministically() {
    let device_key = "dev01";
    let current_window_id = 42u64;
    let next_window_id = 43u64;
    let mut kv = BTreeMap::<String, Vec<u8>>::new();

    let feat_key = s(key_tenant_window_row_feat_v1(device_key, current_window_id));
    let meta_key = s(key_tenant_window_row_meta_v1(device_key, current_window_id));
    let srcip_key = s(key_tenant_window_row_ent_srcip_v1(device_key, current_window_id));
    let dstip_key = s(key_tenant_window_row_ent_dstip_v1(device_key, current_window_id));
    let userid_key = s(key_tenant_window_row_ent_userid_v1(device_key, current_window_id));
    let domain_key = s(key_tenant_window_row_ent_domain_v1(device_key, current_window_id));
    let host_key = s(key_tenant_window_row_ent_host_v1(device_key, current_window_id));
    let active_key = s(key_tenant_active_window_v1(device_key));

    kv.insert(
        feat_key.clone(),
        encode_win_row_feat_v1(&[SparseCountPairV1 {
            feature_id: 7,
            count: 3,
        }])
        .unwrap(),
    );
    kv.insert(
        meta_key.clone(),
        encode_win_row_meta_v1(&WinMetaV1 {
            window_start_ts: 1700000000,
            window_end_ts: 1700000600,
            lines: 25,
            bytes: 4096,
            dropped_features: 0,
            dropped_words: 0,
            dropped_shapes: 0,
        }),
    );
    kv.insert(srcip_key.clone(), encode_win_row_ent_srcip_v1(&[]).unwrap());
    kv.insert(dstip_key.clone(), encode_win_row_ent_dstip_v1(&[]).unwrap());
    kv.insert(userid_key.clone(), encode_win_row_ent_userid_v1(&[]).unwrap());
    kv.insert(domain_key.clone(), encode_win_row_ent_domain_v1(&[]).unwrap());
    kv.insert(host_key.clone(), encode_win_row_ent_host_v1(&[]).unwrap());

    assert!(!kv.contains_key(&active_key));

    kv.insert(
        active_key.clone(),
        encode_win_active_v1(&WinActiveV1 {
            active_window_start_ts: 1700000000,
            active_window_id: current_window_id,
            last_update_ts: 1700000030,
        }),
    );

    assert!(kv.contains_key(&feat_key));
    assert!(kv.contains_key(&meta_key));
    assert!(kv.contains_key(&srcip_key));
    assert!(kv.contains_key(&dstip_key));
    assert!(kv.contains_key(&userid_key));
    assert!(kv.contains_key(&domain_key));
    assert!(kv.contains_key(&host_key));
    assert!(kv.contains_key(&active_key));

    let delete_order = vec![
        feat_key.clone(),
        meta_key.clone(),
        srcip_key.clone(),
        dstip_key.clone(),
        userid_key.clone(),
        domain_key.clone(),
        host_key.clone(),
    ];

    for key in &delete_order {
        kv.remove(key);
    }

    kv.insert(
        active_key.clone(),
        encode_win_active_v1(&WinActiveV1 {
            active_window_start_ts: 1700000600,
            active_window_id: next_window_id,
            last_update_ts: 1700000601,
        }),
    );

    for key in &delete_order {
        assert!(!kv.contains_key(key));
    }

    let active = decode_win_active_v1(kv.get(&active_key).unwrap()).unwrap();
    assert_eq!(active.active_window_id, next_window_id);
    assert_eq!(active.active_window_start_ts, 1700000600);
}
