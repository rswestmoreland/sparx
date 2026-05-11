// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};

use sparx::db::keys::{
    key_tenant_active_window_v1, key_tenant_window_row_ent_domain_v1,
    key_tenant_window_row_ent_dstip_v1, key_tenant_window_row_ent_host_v1,
    key_tenant_window_row_ent_srcip_v1, key_tenant_window_row_ent_userid_v1,
    key_tenant_window_row_feat_v1, key_tenant_window_row_meta_v1,
};
use sparx::db::open_window::{
    decode_win_active_v1, decode_win_row_ent_domain_v1, decode_win_row_ent_dstip_v1,
    decode_win_row_ent_host_v1, decode_win_row_ent_srcip_v1, decode_win_row_ent_userid_v1,
    decode_win_row_feat_v1, decode_win_row_meta_v1, TopKStringEntryV1,
};
use sparx::features::{
    emit_line_features_v1, EmittedFeatureV1, FeatureDictionaryConfigV1, FeatureDictionaryV1,
    FeatureEmissionLineV1, FeatureStringV1,
};
use sparx::tokenize::{SyslogEnvelopeV1, TokenEventV1};
use sparx::types::FeatureFamilyV1;
use sparx::window::{
    align_window_start_ts_v1, bucket_for_window_start_ts_v1, compute_window_key_v1,
    WindowAccumulatorV1, WindowApplyLineResultV1, WindowCapsV1, WindowFinalizeMutationV1,
};

fn base_caps() -> WindowCapsV1 {
    WindowCapsV1 {
        max_features_per_window: 50_000,
        max_word_features_per_window: 20_000,
        max_shape_features_per_window: 20_000,
        max_syslog_features_per_window: 2_000,
        entity_sketch_caps: sparx::features::EntitySketchCapsV1 {
            max_srcips: 64,
            max_dstips: 64,
            max_userids: 128,
            max_domains: 128,
            max_hosts: 128,
        },
    }
}

fn tiny_caps() -> WindowCapsV1 {
    WindowCapsV1 {
        max_features_per_window: 4,
        max_word_features_per_window: 1,
        max_shape_features_per_window: 1,
        max_syslog_features_per_window: 1,
        entity_sketch_caps: sparx::features::EntitySketchCapsV1 {
            max_srcips: 2,
            max_dstips: 2,
            max_userids: 2,
            max_domains: 2,
            max_hosts: 2,
        },
    }
}

fn base_dict() -> FeatureDictionaryV1 {
    FeatureDictionaryV1::new_empty_v1(
        FeatureDictionaryConfigV1 {
            dict_enabled: true,
            dict_max_entries: 1_000,
        },
        100,
        0,
    )
}

fn counted(entries: &[TopKStringEntryV1]) -> Vec<(String, u32)> {
    entries
        .iter()
        .map(|entry| (entry.value.clone(), entry.count))
        .collect()
}

#[test]
fn window_alignment_and_bucket_follow_utc_epoch_rules() {
    let friday = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 42)
        .single()
        .unwrap()
        .timestamp();
    let aligned = align_window_start_ts_v1(friday, 60).unwrap();
    let expected = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    assert_eq!(aligned, expected);
    assert_eq!(bucket_for_window_start_ts_v1(aligned).unwrap(), 13);

    let saturday = Utc
        .with_ymd_and_hms(2024, 1, 6, 13, 0, 0)
        .single()
        .unwrap()
        .timestamp();
    assert_eq!(bucket_for_window_start_ts_v1(saturday).unwrap(), 37);

    let key = compute_window_key_v1("dev01", aligned, 60).unwrap();
    assert_eq!(key.window_start_ts, expected);
    assert_eq!(key.window_end_ts, expected + 60);
    assert_eq!(key.bucket, 13);
}

#[test]
fn apply_line_resolves_features_updates_meta_and_entities() {
    let mut dict = base_dict();
    let line_ts = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 42)
        .single()
        .unwrap()
        .timestamp();
    let window_start_ts = align_window_start_ts_v1(line_ts, 60).unwrap();
    let mut acc = WindowAccumulatorV1::new_v1("dev01", window_start_ts, 7, 60, line_ts, base_caps()).unwrap();

    let line = emit_line_features_v1(
        &SyslogEnvelopeV1::default(),
        &[
            TokenEventV1::Kv {
                key_norm: "src_ip".to_string(),
                value_raw: "10.2.3.4".to_string(),
            },
            TokenEventV1::Kv {
                key_norm: "user".to_string(),
                value_raw: "Alice@Example.com".to_string(),
            },
            TokenEventV1::Word {
                token_raw: "FAILED".to_string(),
            },
        ],
    );

    let result = acc
        .apply_line_v1(line_ts, line_ts + 1, 123, &line, &mut dict)
        .unwrap();
    let applied = match result {
        WindowApplyLineResultV1::Applied(v) => v,
        other => panic!("unexpected apply result: {:?}", other),
    };
    assert!(!applied.dict_writes.is_empty());
    assert_eq!(applied.dropped_features, 0);
    assert_eq!(applied.dropped_words, 0);
    assert_eq!(applied.dropped_shapes, 0);

    assert_eq!(acc.meta_v1().lines, 1);
    assert_eq!(acc.meta_v1().bytes, 123);
    assert_eq!(acc.active_v1().last_update_ts, line_ts + 1);

    let counts: BTreeMap<u32, u32> = acc
        .sparse_counts_v1()
        .into_iter()
        .map(|pair| (pair.feature_id, pair.count))
        .collect();

    let src_shape = dict.lookup_feature_id_v1("SourceIp=<IPV4>").unwrap();
    let src_bucket = dict.lookup_feature_id_v1("SourceIp_net@10.2.3.0/24").unwrap();
    let user_shape = dict.lookup_feature_id_v1("User=alice").unwrap();
    let word_failed = dict.lookup_feature_id_v1("w=failed").unwrap();
    assert_eq!(counts.get(&src_shape), Some(&1));
    assert_eq!(counts.get(&src_bucket), Some(&1));
    assert_eq!(counts.get(&user_shape), Some(&1));
    assert_eq!(counts.get(&word_failed), Some(&1));

    let snapshot = acc.entity_snapshot_v1();
    assert_eq!(counted(&snapshot.srcips), vec![("10.2.3.4".to_string(), 1)]);
    assert_eq!(counted(&snapshot.userids), vec![("alice".to_string(), 1)]);
    assert_eq!(counted(&snapshot.domains), vec![("example.com".to_string(), 1)]);
}

#[test]
fn apply_line_returns_different_window_without_mutation() {
    let mut dict = base_dict();
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let mut acc = WindowAccumulatorV1::new_v1("dev01", start, 9, 60, start, base_caps()).unwrap();
    let line = FeatureEmissionLineV1::default();

    let result = acc.apply_line_v1(start + 60, start + 61, 55, &line, &mut dict).unwrap();
    assert_eq!(
        result,
        WindowApplyLineResultV1::DifferentWindow {
            line_window_start_ts: start + 60,
        }
    );
    assert_eq!(acc.meta_v1().lines, 0);
    assert!(acc.sparse_counts_v1().is_empty());
}

#[test]
fn window_caps_drop_words_plaintext_shapes_and_general_features_deterministically() {
    let mut dict = base_dict();
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let mut acc = WindowAccumulatorV1::new_v1("dev01", start, 11, 60, start, tiny_caps()).unwrap();

    let line = FeatureEmissionLineV1 {
        features: vec![
            EmittedFeatureV1 {
                feature: FeatureStringV1 {
                    s: "SourceIp=<IPV4>".to_string(),
                },
                family: FeatureFamilyV1::Shape,
                count: 1,
            },
            EmittedFeatureV1 {
                feature: FeatureStringV1 {
                    s: "k=src_ip".to_string(),
                },
                family: FeatureFamilyV1::KeyPres,
                count: 1,
            },
            EmittedFeatureV1 {
                feature: FeatureStringV1 {
                    s: "canon=SourceIp".to_string(),
                },
                family: FeatureFamilyV1::Canon,
                count: 1,
            },
            EmittedFeatureV1 {
                feature: FeatureStringV1 {
                    s: "w=failed".to_string(),
                },
                family: FeatureFamilyV1::Word,
                count: 2,
            },
            EmittedFeatureV1 {
                feature: FeatureStringV1 {
                    s: "shape=<IPV4>".to_string(),
                },
                family: FeatureFamilyV1::Shape,
                count: 3,
            },
            EmittedFeatureV1 {
                feature: FeatureStringV1 {
                    s: "syslog_pri=134".to_string(),
                },
                family: FeatureFamilyV1::Syslog,
                count: 4,
            },
        ],
        metadata: Vec::new(),
        structured_pairs_found: false,
    };

    let result = acc.apply_line_v1(start, start + 1, 99, &line, &mut dict).unwrap();
    let applied = match result {
        WindowApplyLineResultV1::Applied(v) => v,
        other => panic!("unexpected apply result: {:?}", other),
    };

    assert_eq!(applied.dropped_features, 4);
    assert_eq!(applied.dropped_words, 0);
    assert_eq!(applied.dropped_shapes, 3);
    assert_eq!(acc.meta_v1().dropped_features, 4);
    assert_eq!(acc.meta_v1().dropped_words, 0);
    assert_eq!(acc.meta_v1().dropped_shapes, 3);
    assert_eq!(acc.sparse_counts_v1().len(), 4);
    assert!(dict.lookup_feature_id_v1("shape=<IPV4>").is_some());
    assert!(dict.lookup_feature_id_v1("syslog_pri=134").is_some());
}

#[test]
fn checkpoint_writes_are_ordered_and_restore_roundtrips_state() {
    let mut dict = base_dict();
    let line_ts = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 42)
        .single()
        .unwrap()
        .timestamp();
    let window_start_ts = align_window_start_ts_v1(line_ts, 60).unwrap();
    let mut acc = WindowAccumulatorV1::new_v1("dev01", window_start_ts, 42, 60, line_ts, base_caps()).unwrap();

    let line = emit_line_features_v1(
        &SyslogEnvelopeV1::default(),
        &[
            TokenEventV1::Kv {
                key_norm: "src_ip".to_string(),
                value_raw: "10.2.3.4".to_string(),
            },
            TokenEventV1::Kv {
                key_norm: "dst_ip".to_string(),
                value_raw: "10.9.8.7".to_string(),
            },
            TokenEventV1::Kv {
                key_norm: "user".to_string(),
                value_raw: "Alice@Example.com".to_string(),
            },
        ],
    );
    let _ = acc.apply_line_v1(line_ts, line_ts + 1, 321, &line, &mut dict).unwrap();

    let writes = acc.checkpoint_writes_v1().unwrap();
    assert_eq!(writes.len(), 8);
    assert_eq!(writes[0].key, key_tenant_window_row_feat_v1("dev01", 42));
    assert_eq!(writes[1].key, key_tenant_window_row_meta_v1("dev01", 42));
    assert_eq!(writes[2].key, key_tenant_window_row_ent_srcip_v1("dev01", 42));
    assert_eq!(writes[3].key, key_tenant_window_row_ent_dstip_v1("dev01", 42));
    assert_eq!(writes[4].key, key_tenant_window_row_ent_userid_v1("dev01", 42));
    assert_eq!(writes[5].key, key_tenant_window_row_ent_domain_v1("dev01", 42));
    assert_eq!(writes[6].key, key_tenant_window_row_ent_host_v1("dev01", 42));
    assert_eq!(writes[7].key, key_tenant_active_window_v1("dev01"));

    let active = decode_win_active_v1(&writes[7].value).unwrap();
    let feat = decode_win_row_feat_v1(&writes[0].value).unwrap();
    let meta = decode_win_row_meta_v1(&writes[1].value).unwrap();
    let snapshot = sparx::features::EntitySketchSnapshotV1 {
        srcips: decode_win_row_ent_srcip_v1(&writes[2].value).unwrap(),
        dstips: decode_win_row_ent_dstip_v1(&writes[3].value).unwrap(),
        userids: decode_win_row_ent_userid_v1(&writes[4].value).unwrap(),
        domains: decode_win_row_ent_domain_v1(&writes[5].value).unwrap(),
        hosts: decode_win_row_ent_host_v1(&writes[6].value).unwrap(),
    };

    let restored = WindowAccumulatorV1::from_checkpoint_v1(
        "dev01",
        base_caps(),
        active,
        meta,
        &feat,
        &snapshot,
        &dict,
    )
    .unwrap();

    assert_eq!(restored.window_key_v1(), acc.window_key_v1());
    assert_eq!(restored.active_v1(), acc.active_v1());
    assert_eq!(restored.meta_v1(), acc.meta_v1());
    assert_eq!(restored.sparse_counts_v1(), acc.sparse_counts_v1());

    assert_eq!(restored.entity_snapshot_v1(), acc.entity_snapshot_v1());
}

#[test]
fn finalized_row_snapshot_matches_active_state() {
    let mut dict = base_dict();
    let line_ts = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 42)
        .single()
        .unwrap()
        .timestamp();
    let window_start_ts = align_window_start_ts_v1(line_ts, 60).unwrap();
    let mut acc = WindowAccumulatorV1::new_v1("dev01", window_start_ts, 42, 60, line_ts, base_caps()).unwrap();

    let line = emit_line_features_v1(
        &SyslogEnvelopeV1::default(),
        &[
            TokenEventV1::Kv {
                key_norm: "src_ip".to_string(),
                value_raw: "10.2.3.4".to_string(),
            },
            TokenEventV1::Kv {
                key_norm: "user".to_string(),
                value_raw: "Alice@Example.com".to_string(),
            },
        ],
    );
    let _ = acc.apply_line_v1(line_ts, line_ts + 1, 222, &line, &mut dict).unwrap();

    let row = acc.finalized_row_v1();
    assert_eq!(row.key, acc.window_key_v1());
    assert_eq!(row.window_id, 42);
    assert_eq!(row.meta, *acc.meta_v1());
    assert_eq!(row.sparse_counts, acc.sparse_counts_v1());
    assert_eq!(row.entity_snapshot, acc.entity_snapshot_v1());
}

#[test]
fn finalize_idle_deletes_open_window_keys_and_active_last() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let acc = WindowAccumulatorV1::new_v1("dev01", start, 42, 60, start + 5, base_caps()).unwrap();

    let plan = acc.finalize_idle_v1();
    assert_eq!(plan.finalized_row.window_id, 42);
    assert_eq!(plan.mutations.len(), 8);
    assert_eq!(
        plan.mutations[0],
        WindowFinalizeMutationV1::Delete(key_tenant_window_row_feat_v1("dev01", 42))
    );
    assert_eq!(
        plan.mutations[1],
        WindowFinalizeMutationV1::Delete(key_tenant_window_row_meta_v1("dev01", 42))
    );
    assert_eq!(
        plan.mutations[2],
        WindowFinalizeMutationV1::Delete(key_tenant_window_row_ent_srcip_v1("dev01", 42))
    );
    assert_eq!(
        plan.mutations[3],
        WindowFinalizeMutationV1::Delete(key_tenant_window_row_ent_dstip_v1("dev01", 42))
    );
    assert_eq!(
        plan.mutations[4],
        WindowFinalizeMutationV1::Delete(key_tenant_window_row_ent_userid_v1("dev01", 42))
    );
    assert_eq!(
        plan.mutations[5],
        WindowFinalizeMutationV1::Delete(key_tenant_window_row_ent_domain_v1("dev01", 42))
    );
    assert_eq!(
        plan.mutations[6],
        WindowFinalizeMutationV1::Delete(key_tenant_window_row_ent_host_v1("dev01", 42))
    );
    assert_eq!(
        plan.mutations[7],
        WindowFinalizeMutationV1::Delete(key_tenant_active_window_v1("dev01"))
    );
}

#[test]
fn finalize_and_advance_rolls_to_next_empty_window_and_updates_active_last() {
    let mut dict = base_dict();
    let line_ts = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 42)
        .single()
        .unwrap()
        .timestamp();
    let window_start_ts = align_window_start_ts_v1(line_ts, 60).unwrap();
    let next_window_start_ts = window_start_ts + 120;
    let mut acc = WindowAccumulatorV1::new_v1("dev01", window_start_ts, 42, 60, line_ts, base_caps()).unwrap();

    let line = emit_line_features_v1(
        &SyslogEnvelopeV1::default(),
        &[TokenEventV1::Kv {
            key_norm: "src_ip".to_string(),
            value_raw: "10.2.3.4".to_string(),
        }],
    );
    let _ = acc.apply_line_v1(line_ts, line_ts + 1, 111, &line, &mut dict).unwrap();

    let (plan, next) = acc
        .finalize_and_advance_v1(next_window_start_ts, next_window_start_ts + 1)
        .unwrap();

    assert_eq!(plan.finalized_row.window_id, 42);
    assert_eq!(plan.mutations.len(), 8);
    match &plan.mutations[7] {
        WindowFinalizeMutationV1::Put(write) => {
            assert_eq!(write.key, key_tenant_active_window_v1("dev01"));
            let active = decode_win_active_v1(&write.value).unwrap();
            assert_eq!(active.active_window_start_ts, next_window_start_ts);
            assert_eq!(active.active_window_id, 43);
            assert_eq!(active.last_update_ts, next_window_start_ts + 1);
        }
        other => panic!("unexpected final mutation: {:?}", other),
    }

    assert_eq!(next.active_v1().active_window_start_ts, next_window_start_ts);
    assert_eq!(next.active_v1().active_window_id, 43);
    assert_eq!(next.meta_v1().window_start_ts, next_window_start_ts);
    assert_eq!(next.meta_v1().window_end_ts, next_window_start_ts + 60);
    assert_eq!(next.meta_v1().lines, 0);
    assert_eq!(next.meta_v1().bytes, 0);
    assert!(next.sparse_counts_v1().is_empty());
    assert_eq!(next.entity_snapshot_v1(), sparx::features::EntitySketchSnapshotV1::default());
}

#[test]
fn finalize_and_advance_rejects_overlapping_or_misaligned_next_window_start() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let acc = WindowAccumulatorV1::new_v1("dev01", start, 42, 60, start, base_caps()).unwrap();

    let overlap = acc.finalize_and_advance_v1(start + 30, start + 31).unwrap_err();
    assert_eq!(
        overlap,
        sparx::window::WindowErrorV1::InvalidNextWindowStart {
            current_window_start_ts: start,
            current_window_end_ts: start + 60,
            next_window_start_ts: start + 30,
        }
    );

    let misaligned = acc.finalize_and_advance_v1(start + 61, start + 62).unwrap_err();
    assert_eq!(
        misaligned,
        sparx::window::WindowErrorV1::InvalidNextWindowStart {
            current_window_start_ts: start,
            current_window_end_ts: start + 60,
            next_window_start_ts: start + 61,
        }
    );
}

#[test]
fn finalize_and_advance_rejects_window_id_overflow() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let acc = WindowAccumulatorV1::new_v1("dev01", start, u64::MAX, 60, start, base_caps()).unwrap();

    let err = acc.finalize_and_advance_v1(start + 60, start + 61).unwrap_err();
    assert_eq!(
        err,
        sparx::window::WindowErrorV1::WindowIdOverflow {
            current_window_id: u64::MAX,
        }
    );
}

#[test]
fn restored_state_needs_feature_strings_for_family_counts() {
    let dict = base_dict();
    let active = sparx::db::open_window::WinActiveV1 {
        active_window_start_ts: 1700000000,
        active_window_id: 5,
        last_update_ts: 1700000001,
    };
    let meta = sparx::db::open_window::WinMetaV1 {
        window_start_ts: 1700000000,
        window_end_ts: 1700000060,
        lines: 1,
        bytes: 1,
        dropped_features: 0,
        dropped_words: 0,
        dropped_shapes: 0,
    };
    let err = WindowAccumulatorV1::from_checkpoint_v1(
        "dev01",
        base_caps(),
        active,
        meta,
        &[sparx::db::open_window::SparseCountPairV1 {
            feature_id: 999,
            count: 1,
        }],
        &sparx::features::EntitySketchSnapshotV1::default(),
        &dict,
    )
    .unwrap_err();
    assert_eq!(
        err,
        sparx::window::WindowErrorV1::MissingFeatureString { feature_id: 999 }
    );
}


#[test]
fn apply_line_is_atomic_when_dictionary_insert_fails() {
    let mut dict = FeatureDictionaryV1::new_empty_v1(
        FeatureDictionaryConfigV1 {
            dict_enabled: true,
            dict_max_entries: 1,
        },
        10,
        0,
    );
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let mut acc = WindowAccumulatorV1::new_v1("dev01", start, 13, 60, start, base_caps()).unwrap();

    let line = FeatureEmissionLineV1 {
        features: vec![
            EmittedFeatureV1 {
                feature: FeatureStringV1 {
                    s: "k=src_ip".to_string(),
                },
                family: FeatureFamilyV1::KeyPres,
                count: 1,
            },
            EmittedFeatureV1 {
                feature: FeatureStringV1 {
                    s: "canon=SourceIp".to_string(),
                },
                family: FeatureFamilyV1::Canon,
                count: 1,
            },
        ],
        metadata: vec![sparx::features::MetadataIdentityV1 {
            kind: sparx::features::MetadataIdentityKindV1::SourceIp,
            value: "10.2.3.4".to_string(),
        }],
        structured_pairs_found: true,
    };

    let err = acc.apply_line_v1(start, start + 1, 88, &line, &mut dict).unwrap_err();
    assert_eq!(
        err,
        sparx::window::WindowErrorV1::FeatureDictionary(
            sparx::features::FeatureDictionaryErrorV1::DictionaryFull { max_entries: 1 }
        )
    );
    assert_eq!(acc.meta_v1().lines, 0);
    assert_eq!(acc.meta_v1().bytes, 0);
    assert!(acc.sparse_counts_v1().is_empty());
    assert_eq!(acc.entity_snapshot_v1(), sparx::features::EntitySketchSnapshotV1::default());
    assert!(dict.lookup_feature_id_v1("k=src_ip").is_none());
    assert!(dict.lookup_feature_id_v1("canon=SourceIp").is_none());
}

#[test]
fn compute_window_key_rejects_zero_and_misaligned_starts() {
    let aligned = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let zero = compute_window_key_v1("dev01", aligned, 0).unwrap_err();
    assert_eq!(
        zero,
        sparx::window::WindowErrorV1::InvalidWindowSize { window_size_s: 0 }
    );

    let misaligned = compute_window_key_v1("dev01", aligned + 1, 60).unwrap_err();
    assert_eq!(
        misaligned,
        sparx::window::WindowErrorV1::MisalignedWindowStart {
            window_start_ts: aligned + 1,
            aligned_window_start_ts: aligned,
            window_size_s: 60,
        }
    );
}
