// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use sparx::config::FeaturesSectionV1;
use sparx::db::keys::{
    key_tenant_feature_dict_entries_v1, key_tenant_feature_dict_id_v1,
    key_tenant_feature_dict_next_id_v1, key_tenant_feature_dict_str_v1,
};
use sparx::db::tenant_values::{
    decode_feat_dict_id_to_str_v1, decode_feat_dict_meta_entries_v1,
    decode_feat_dict_meta_next_id_v1, decode_feat_dict_str_to_id_v1,
};
use sparx::features::{
    FeatureDictionaryConfigV1, FeatureDictionaryErrorV1, FeatureDictionaryMetaV1,
    FeatureDictionaryV1,
};

fn base_cfg() -> FeatureDictionaryConfigV1 {
    FeatureDictionaryConfigV1 {
        dict_enabled: true,
        dict_max_entries: 3,
    }
}

#[test]
fn config_conversion_preserves_dict_controls() {
    let features = FeaturesSectionV1 {
        dict_enabled: false,
        dict_max_entries: 99,
        hash_space_bits: 26,
        dict_gc_interval_s: 3600,
    };
    let cfg = FeatureDictionaryConfigV1::from(&features);
    assert!(!cfg.dict_enabled);
    assert_eq!(cfg.dict_max_entries, 99);
}

#[test]
fn new_insert_emits_forward_reverse_and_meta_writes() {
    let mut dict = FeatureDictionaryV1::new_empty_v1(base_cfg(), 1, 0);
    let out = dict.resolve_or_insert_v1("k=src_ip").unwrap();
    assert_eq!(out.feature_id, 1);
    assert!(out.inserted);
    assert_eq!(out.writes.len(), 4);

    assert_eq!(
        out.writes[0].key,
        key_tenant_feature_dict_str_v1("k=src_ip")
    );
    assert_eq!(
        decode_feat_dict_str_to_id_v1(&out.writes[0].value).unwrap(),
        1
    );

    assert_eq!(out.writes[1].key, key_tenant_feature_dict_id_v1(1));
    assert_eq!(
        decode_feat_dict_id_to_str_v1(&out.writes[1].value).unwrap(),
        "k=src_ip"
    );

    assert_eq!(out.writes[2].key, key_tenant_feature_dict_next_id_v1());
    assert_eq!(
        decode_feat_dict_meta_next_id_v1(&out.writes[2].value).unwrap(),
        2
    );

    assert_eq!(out.writes[3].key, key_tenant_feature_dict_entries_v1());
    assert_eq!(
        decode_feat_dict_meta_entries_v1(&out.writes[3].value).unwrap(),
        1
    );
}

#[test]
fn inserts_are_stable_and_increment_meta_deterministically() {
    let mut dict = FeatureDictionaryV1::new_empty_v1(base_cfg(), 10, 77);
    let a = dict.resolve_or_insert_v1("k=src_ip").unwrap();
    let b = dict.resolve_or_insert_v1("canon=SourceIp").unwrap();
    let c = dict.resolve_or_insert_v1("k=src_ip").unwrap();

    assert_eq!(a.feature_id, 10);
    assert_eq!(b.feature_id, 11);
    assert_eq!(c.feature_id, 10);
    assert!(!c.inserted);
    assert_eq!(dict.meta_v1().next_id, 12);
    assert_eq!(dict.meta_v1().entries, 2);
    assert_eq!(dict.meta_v1().last_gc_ts, 77);
    assert_eq!(
        dict.forward_entries_v1(),
        vec![
            ("canon=SourceIp".to_string(), 11),
            ("k=src_ip".to_string(), 10),
        ]
    );
    assert_eq!(
        dict.reverse_entries_v1(),
        vec![
            (10, "k=src_ip".to_string()),
            (11, "canon=SourceIp".to_string()),
        ]
    );
}

#[test]
fn load_persisted_validates_forward_reverse_and_meta() {
    let dict = FeatureDictionaryV1::load_persisted_v1(
        base_cfg(),
        FeatureDictionaryMetaV1 {
            next_id: 3,
            entries: 2,
            last_gc_ts: 123,
        },
        vec![
            ("k=src_ip".to_string(), 1),
            ("canon=SourceIp".to_string(), 2),
        ],
        vec![
            (1, "k=src_ip".to_string()),
            (2, "canon=SourceIp".to_string()),
        ],
    )
    .unwrap();

    assert_eq!(dict.lookup_feature_id_v1("k=src_ip"), Some(1));
    assert_eq!(dict.lookup_feature_string_v1(2), Some("canon=SourceIp"));
    assert_eq!(dict.meta_v1().next_id, 3);
    assert_eq!(dict.meta_v1().entries, 2);
}

#[test]
fn load_persisted_rejects_meta_entry_mismatch() {
    let err = FeatureDictionaryV1::load_persisted_v1(
        base_cfg(),
        FeatureDictionaryMetaV1 {
            next_id: 3,
            entries: 1,
            last_gc_ts: 0,
        },
        vec![
            ("k=src_ip".to_string(), 1),
            ("canon=SourceIp".to_string(), 2),
        ],
        vec![
            (1, "k=src_ip".to_string()),
            (2, "canon=SourceIp".to_string()),
        ],
    )
    .unwrap_err();

    assert_eq!(
        err,
        FeatureDictionaryErrorV1::MetaEntriesMismatch {
            meta_entries: 1,
            actual_entries: 2,
        }
    );
}

#[test]
fn load_persisted_rejects_reverse_value_mismatch() {
    let err = FeatureDictionaryV1::load_persisted_v1(
        base_cfg(),
        FeatureDictionaryMetaV1 {
            next_id: 3,
            entries: 2,
            last_gc_ts: 0,
        },
        vec![
            ("k=src_ip".to_string(), 1),
            ("canon=SourceIp".to_string(), 2),
        ],
        vec![(1, "k=src_ip".to_string()), (2, "canon=DestIp".to_string())],
    )
    .unwrap_err();

    assert_eq!(
        err,
        FeatureDictionaryErrorV1::ReverseMapValueMismatch {
            feature_id: 2,
            forward_feature_string: "canon=SourceIp".to_string(),
            reverse_feature_string: "canon=DestIp".to_string(),
        }
    );
}

#[test]
fn dict_disabled_rejects_new_insert() {
    let mut dict = FeatureDictionaryV1::new_empty_v1(
        FeatureDictionaryConfigV1 {
            dict_enabled: false,
            dict_max_entries: 3,
        },
        1,
        0,
    );
    let err = dict.resolve_or_insert_v1("k=src_ip").unwrap_err();
    assert_eq!(err, FeatureDictionaryErrorV1::DictionaryDisabled);
}

#[test]
fn dict_cap_is_enforced_without_eviction() {
    let mut dict = FeatureDictionaryV1::new_empty_v1(base_cfg(), 1, 0);
    dict.resolve_or_insert_v1("a").unwrap();
    dict.resolve_or_insert_v1("b").unwrap();
    dict.resolve_or_insert_v1("c").unwrap();
    let err = dict.resolve_or_insert_v1("d").unwrap_err();
    assert_eq!(
        err,
        FeatureDictionaryErrorV1::DictionaryFull { max_entries: 3 }
    );
    assert_eq!(dict.lookup_feature_id_v1("a"), Some(1));
    assert_eq!(dict.lookup_feature_id_v1("b"), Some(2));
    assert_eq!(dict.lookup_feature_id_v1("c"), Some(3));
    assert_eq!(dict.meta_v1().entries, 3);
    assert_eq!(dict.meta_v1().next_id, 4);
}

#[test]
fn next_id_exhaustion_is_rejected_before_insert() {
    let mut dict = FeatureDictionaryV1::new_empty_v1(base_cfg(), u32::MAX, 0);
    let err = dict.resolve_or_insert_v1("k=src_ip").unwrap_err();
    assert_eq!(err, FeatureDictionaryErrorV1::NextIdExhausted);
    assert_eq!(dict.meta_v1().entries, 0);
    assert_eq!(dict.lookup_feature_id_v1("k=src_ip"), None);
}
