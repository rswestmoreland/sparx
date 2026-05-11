// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use chrono::{TimeZone, Utc};

use sparx::alert::{
    alert_primary_put_v1, build_alert_v1, decode_alert_v1, encode_alert_v1, AlertScoringConfigV1,
    FileSpanV1,
};
use sparx::baseline::{BucketBaselineV1, CentroidPairV1, DfPairV1};
use sparx::db::baseline_sketch::{DeviceStatsV1, WelfordF64V1};
use sparx::db::keys::{key_tenant_alert_v1, KeyBytes};
use sparx::db::open_window::{SparseCountPairV1, TopKStringEntryV1, WinMetaV1};
use sparx::features::{
    EntitySketchSnapshotV1, FeatureDictionaryConfigV1, FeatureDictionaryMetaV1, FeatureDictionaryV1,
};
use sparx::types::{ConfidenceV1, FeatureFamilyV1, LabelV1};
use sparx::window::{bucket_for_window_start_ts_v1, FinalizedWindowRowV1, WindowKeyV1};

fn s(key: &KeyBytes) -> String {
    String::from_utf8(key.bytes.clone()).unwrap()
}

fn dict() -> FeatureDictionaryV1 {
    FeatureDictionaryV1::load_persisted_v1(
        FeatureDictionaryConfigV1 {
            dict_enabled: true,
            dict_max_entries: 100,
        },
        FeatureDictionaryMetaV1 {
            next_id: 20,
            entries: 8,
            last_gc_ts: 0,
        },
        vec![
            ("SourceIp=<IPV4>".to_string(), 1),
            ("SourceIp_net@10.2.3.0/24".to_string(), 2),
            ("k=src_ip".to_string(), 3),
            ("canon=SourceIp".to_string(), 4),
            ("User=alice".to_string(), 5),
            ("w=failed".to_string(), 6),
            ("shape=<UUID>".to_string(), 7),
            ("k=a".to_string(), 8),
        ],
        vec![
            (1, "SourceIp=<IPV4>".to_string()),
            (2, "SourceIp_net@10.2.3.0/24".to_string()),
            (3, "k=src_ip".to_string()),
            (4, "canon=SourceIp".to_string()),
            (5, "User=alice".to_string()),
            (6, "w=failed".to_string()),
            (7, "shape=<UUID>".to_string()),
            (8, "k=a".to_string()),
        ],
    )
    .unwrap()
}

fn row(
    window_start_ts: i64,
    sparse_counts: &[(u32, u32)],
    lines: u32,
    bytes: u64,
    entity_snapshot: EntitySketchSnapshotV1,
) -> FinalizedWindowRowV1 {
    FinalizedWindowRowV1 {
        key: WindowKeyV1 {
            device_key: "dev01".to_string(),
            window_start_ts,
            window_end_ts: window_start_ts + 60,
            bucket: bucket_for_window_start_ts_v1(window_start_ts).unwrap(),
        },
        window_id: 11,
        meta: WinMetaV1 {
            window_start_ts,
            window_end_ts: window_start_ts + 60,
            lines,
            bytes,
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
        entity_snapshot,
    }
}

fn baseline(
    window_start_ts: i64,
    n_bucket: u32,
    df: &[(u32, u32)],
    centroid: &[(u32, f32)],
) -> BucketBaselineV1 {
    BucketBaselineV1 {
        bucket: bucket_for_window_start_ts_v1(window_start_ts).unwrap(),
        n_bucket,
        df: df
            .iter()
            .map(|(feature_id, df_count)| DfPairV1 {
                feature_id: *feature_id,
                df_count: *df_count,
            })
            .collect(),
        centroid: centroid
            .iter()
            .map(|(feature_id, value)| CentroidPairV1 {
                feature_id: *feature_id,
                value: *value,
            })
            .collect(),
    }
}

fn stats(line_mean: f64, line_m2: f64, byte_mean: f64, byte_m2: f64, n: u32) -> DeviceStatsV1 {
    DeviceStatsV1 {
        line_count: WelfordF64V1 {
            n,
            mean: line_mean,
            m2: line_m2,
        },
        byte_count: WelfordF64V1 {
            n,
            mean: byte_mean,
            m2: byte_m2,
        },
        score_total: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        last_update_ts: 0,
    }
}

fn cfg() -> AlertScoringConfigV1 {
    AlertScoringConfigV1::default()
}

fn approx_eq_f32(left: f32, right: f32) {
    let diff = (left - right).abs();
    assert!(diff <= 0.0001, "left={left} right={right} diff={diff}");
}

#[test]
fn rarity_normalization_is_invariant_to_uniform_row_scaling() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 0, 0)
        .single()
        .unwrap()
        .timestamp();
    let row_small = row(
        start,
        &[(1, 1), (6, 1)],
        5,
        100,
        EntitySketchSnapshotV1::default(),
    );
    let row_large = row(
        start,
        &[(1, 10), (6, 10)],
        5,
        100,
        EntitySketchSnapshotV1::default(),
    );
    let base = baseline(start, 100, &[(1, 1), (6, 1)], &[]);

    let small = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row_small,
        &dict(),
        &base,
        None,
        &cfg(),
        &[],
    )
    .unwrap();
    let large = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row_large,
        &dict(),
        &base,
        None,
        &cfg(),
        &[],
    )
    .unwrap();

    approx_eq_f32(small.rarity, large.rarity);
}

#[test]
fn drift_is_near_zero_when_weighted_row_matches_centroid() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 1, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(
        start,
        &[(1, 1), (6, 2)],
        5,
        100,
        EntitySketchSnapshotV1::default(),
    );
    let tf_shape = 1.0_f64.ln_1p() as f32;
    let tf_word = (0.3_f64 * 2.0_f64.ln_1p()) as f32;
    let base = baseline(
        start,
        100,
        &[(1, 10), (6, 10)],
        &[(1, tf_shape), (6, tf_word)],
    );

    let result = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &base,
        None,
        &cfg(),
        &[],
    )
    .unwrap();

    assert!(result.drift <= 0.0001, "drift={}", result.drift);
}

#[test]
fn volume_extreme_in_cold_start_emits_info() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 2, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(
        start,
        &[(6, 1)],
        500,
        50000,
        EntitySketchSnapshotV1::default(),
    );
    let base = baseline(start, 0, &[], &[]);
    let current_stats = stats(10.0, 25.0, 1000.0, 250000.0, 10);

    let result = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &base,
        Some(&current_stats),
        &cfg(),
        &[],
    )
    .unwrap();

    assert!(result.cold_start);
    assert!(result.volume >= 0.90, "volume={}", result.volume);
    let alert = result.alert.unwrap();
    assert_eq!(alert.label, LabelV1::Info);
    assert_eq!(alert.confidence, ConfidenceV1::Low);
}

#[test]
fn cold_start_suppresses_outlier_and_keeps_info_only() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 3, 0)
        .single()
        .unwrap()
        .timestamp();
    let snapshot = EntitySketchSnapshotV1 {
        userids: vec![TopKStringEntryV1 {
            value: "alice".to_string(),
            count: 2,
        }],
        ..EntitySketchSnapshotV1::default()
    };
    let row = row(start, &[(5, 3)], 20, 1000, snapshot);
    let base = baseline(start, 10, &[(5, 0)], &[(5, -0.1)]);

    let result = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &base,
        None,
        &cfg(),
        &[],
    )
    .unwrap();

    assert!(result.cold_start);
    let alert = result.alert.unwrap();
    assert_eq!(alert.label, LabelV1::Info);
}

#[test]
fn blob_ratio_can_drive_noise_suspect_without_entity_focus() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 4, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[(7, 3)], 10, 100, EntitySketchSnapshotV1::default());
    let base = baseline(start, 120, &[(7, 0)], &[(7, -0.01)]);

    let result = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &base,
        None,
        &cfg(),
        &[],
    )
    .unwrap();

    assert!(
        result.blob_ratio >= 0.60,
        "blob_ratio={}",
        result.blob_ratio
    );
    let alert = result.alert.unwrap();
    assert_eq!(alert.label, LabelV1::NoiseSuspect);
    assert!(alert
        .reasons
        .iter()
        .any(|reason| reason.code == "N_HIGH_CARDINALITY"));
}

#[test]
fn top_features_alert_id_entities_and_postcard_roundtrip_are_deterministic() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 5, 0)
        .single()
        .unwrap()
        .timestamp();
    let snapshot = EntitySketchSnapshotV1 {
        srcips: vec![
            TopKStringEntryV1 {
                value: "10.2.3.9".to_string(),
                count: 2,
            },
            TopKStringEntryV1 {
                value: "10.2.3.8".to_string(),
                count: 2,
            },
        ],
        userids: vec![
            TopKStringEntryV1 {
                value: "bob".to_string(),
                count: 1,
            },
            TopKStringEntryV1 {
                value: "alice".to_string(),
                count: 1,
            },
        ],
        ..EntitySketchSnapshotV1::default()
    };
    let row = row(start, &[(3, 1), (8, 1)], 10, 100, snapshot);
    let base = baseline(start, 200, &[(3, 0), (8, 0)], &[(3, -0.1), (8, -0.1)]);
    let provenance = vec![
        FileSpanV1 {
            file_rel: "b.log".to_string(),
            file_key: "fb".to_string(),
            inode: 2,
            offset_start: 20,
            offset_end: 30,
            is_gzip: false,
        },
        FileSpanV1 {
            file_rel: "a.log".to_string(),
            file_key: "fa".to_string(),
            inode: 1,
            offset_start: 10,
            offset_end: 20,
            is_gzip: false,
        },
    ];

    let first = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &base,
        None,
        &cfg(),
        &provenance,
    )
    .unwrap();
    let second = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &base,
        None,
        &cfg(),
        &provenance,
    )
    .unwrap();
    let first_alert = first.alert.unwrap();
    let second_alert = second.alert.unwrap();

    assert_eq!(first_alert.alert_id, second_alert.alert_id);
    assert_eq!(first_alert.signature, second_alert.signature);
    assert_eq!(first_alert.top_features[0].feature, "k=a");
    assert_eq!(first_alert.top_features[1].feature, "k=src_ip");
    assert_eq!(first_alert.entities.src_ips[0].value, "10.2.3.8");
    assert_eq!(first_alert.entities.src_ips[1].value, "10.2.3.9");
    assert_eq!(first_alert.entities.user_ids[0].value, "alice");
    assert_eq!(first_alert.entities.user_ids[1].value, "bob");
    assert_eq!(first_alert.provenance[0].file_rel, "a.log");
    assert_eq!(first_alert.provenance[1].file_rel, "b.log");

    let encoded = encode_alert_v1(&first_alert).unwrap();
    let decoded = decode_alert_v1(&encoded).unwrap();
    assert_eq!(decoded, first_alert);

    let put = alert_primary_put_v1(&first_alert).unwrap();
    assert_eq!(s(&put.key), s(&key_tenant_alert_v1(&first_alert.alert_id)));
    assert_eq!(decode_alert_v1(&put.value).unwrap(), first_alert);
}

#[test]
fn outlier_reasons_include_new_feature_drift_and_entity_focus() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 6, 0)
        .single()
        .unwrap()
        .timestamp();
    let snapshot = EntitySketchSnapshotV1 {
        userids: vec![TopKStringEntryV1 {
            value: "alice".to_string(),
            count: 3,
        }],
        ..EntitySketchSnapshotV1::default()
    };
    let row = row(start, &[(5, 5)], 50, 2000, snapshot);
    let base = baseline(start, 200, &[(5, 0)], &[(5, -0.5)]);

    let outlier_stats = stats(10.0, 9.0, 100.0, 2500.0, 10);
    let result = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &base,
        Some(&outlier_stats),
        &cfg(),
        &[],
    )
    .unwrap();
    let alert = result.alert.unwrap();

    assert_eq!(alert.label, LabelV1::Outlier);
    assert_eq!(alert.confidence, ConfidenceV1::High);
    assert!(alert
        .reasons
        .iter()
        .any(|reason| reason.code == "R_NEW_FEATURE"));
    assert!(alert
        .reasons
        .iter()
        .any(|reason| reason.code == "D_HIGH_DRIFT"));
    assert!(alert
        .reasons
        .iter()
        .any(|reason| reason.code == "O_ENTITY_FOCUSED"));
    assert_eq!(alert.top_features[0].family, FeatureFamilyV1::Shape);
}

#[test]
fn cold_start_days_activate_day_based_bucket_floor() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 7, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[(6, 2)], 12, 120, EntitySketchSnapshotV1::default());
    let cfg = cfg();

    let immature = baseline(start, 119, &[(6, 2)], &[(6, 0.15)]);
    let mature = baseline(start, 120, &[(6, 2)], &[(6, 0.15)]);

    let immature_result = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &immature,
        None,
        &cfg,
        &[],
    )
    .unwrap();
    let mature_result = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &mature,
        None,
        &cfg,
        &[],
    )
    .unwrap();

    assert!(immature_result.cold_start);
    assert!(!mature_result.cold_start);
}

#[test]
fn min_lines_per_window_suppresses_alert_but_keeps_score_preview() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 8, 0)
        .single()
        .unwrap()
        .timestamp();
    let snapshot = EntitySketchSnapshotV1 {
        userids: vec![TopKStringEntryV1 {
            value: "alice".to_string(),
            count: 3,
        }],
        ..EntitySketchSnapshotV1::default()
    };
    let row = row(start, &[(5, 5)], 9, 900, snapshot);
    let base = baseline(start, 200, &[(5, 0)], &[(5, -0.5)]);
    let outlier_stats = stats(10.0, 9.0, 100.0, 2500.0, 10);

    let result = build_alert_v1(
        "acme",
        "tenant/acme/dev01",
        &row,
        &dict(),
        &base,
        Some(&outlier_stats),
        &cfg(),
        &[],
    )
    .unwrap();

    assert!(result.below_min_lines);
    assert!(
        result.score_total >= 0.60,
        "score_total={}",
        result.score_total
    );
    assert!(result.alert.is_none());
    assert!(result.primary_put.is_none());
}

fn sample_vdrop_candidate(
    subject_kind_u8: u8,
    subject_key: &str,
) -> sparx::db::silence::VDropCandidateV1 {
    let subject_kind_label =
        if subject_kind_u8 == sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1 {
            "device"
        } else {
            "tenant"
        };
    let mut reason_details = vec![
        ("subject_kind".to_string(), subject_kind_label.to_string()),
        ("tenant_id".to_string(), "tenant-a".to_string()),
    ];
    if subject_kind_u8 == sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1 {
        reason_details.push(("device_key".to_string(), subject_key.to_string()));
    }
    reason_details.extend(vec![
        ("window_start_ts".to_string(), "1700000060".to_string()),
        ("window_end_ts".to_string(), "1700000240".to_string()),
        ("last_seen_ts".to_string(), "1700000060".to_string()),
        ("expected_windows_missed".to_string(), "3".to_string()),
        ("expected_lines".to_string(), "90".to_string()),
        ("observed_lines".to_string(), "0".to_string()),
        ("drop_ratio".to_string(), "1.000000".to_string()),
        ("bucket".to_string(), "8".to_string()),
    ]);
    sparx::db::silence::VDropCandidateV1 {
        subject_kind_u8,
        subject_key: subject_key.to_string(),
        tenant_id: "tenant-a".to_string(),
        window_start_ts_i64: 1_700_000_060,
        window_end_ts_i64: 1_700_000_240,
        last_seen_ts_i64: 1_700_000_060,
        expected_windows_missed_u64: 3,
        expected_lines_u64: 90,
        observed_lines_u64: 0,
        drop_ratio_f32: 1.0,
        bucket_u8: 8,
        reason_details,
    }
}

fn is_lower_hex_128(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[test]
fn vdrop_alert_construction_is_deterministic_and_empty_provenance() {
    let candidate = sample_vdrop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1,
        "device-a",
    );
    let first = sparx::alert::build_vdrop_alert_v1(&candidate).unwrap();
    let second = sparx::alert::build_vdrop_alert_v1(&candidate).unwrap();

    assert_eq!(first, second);
    assert!(is_lower_hex_128(&first.alert.alert_id));
    assert!(is_lower_hex_128(&first.alert.signature));
    assert_eq!(
        first.alert.schema_version,
        sparx::alert::ALERT_SCHEMA_VERSION_V1
    );
    assert_eq!(first.alert.tenant_id, "tenant-a");
    assert_eq!(first.alert.device_key, "device-a");
    assert_eq!(first.alert.device_path, "device-a");
    assert_eq!(first.alert.window_start_ts, 1_700_000_060);
    assert_eq!(first.alert.window_end_ts, 1_700_000_240);
    assert_eq!(first.alert.window_size_s, 180);
    assert_eq!(first.alert.bucket, 8);
    assert_eq!(first.alert.label, LabelV1::Info);
    assert_eq!(first.alert.confidence, ConfidenceV1::Medium);
    assert!(!first.alert.cold_start);
    assert_eq!(first.alert.score_total, 1.0);
    assert_eq!(first.alert.score_rarity, 0.0);
    assert_eq!(first.alert.score_drift, 0.0);
    assert_eq!(first.alert.score_volume, 1.0);
    assert_eq!(first.alert.lines, 0);
    assert_eq!(first.alert.bytes, 0);
    assert!(first.alert.top_features.is_empty());
    assert!(first.alert.provenance.is_empty());
    assert_eq!(first.alert.reasons.len(), 1);
    assert_eq!(
        first.alert.reasons[0].code,
        sparx::alert::VDROP_REASON_CODE_V1
    );
    assert_eq!(first.alert.reasons[0].details, candidate.reason_details);

    assert_eq!(
        s(&first.primary_put.key),
        s(&key_tenant_alert_v1(&first.alert.alert_id))
    );
    assert_eq!(
        decode_alert_v1(&first.primary_put.value).unwrap(),
        first.alert
    );
    assert_eq!(
        first.open_silence.schema_version_u16,
        sparx::db::silence::SILENCE_SCHEMA_VERSION_V1
    );
    assert_eq!(
        first.open_silence.subject_kind_u8,
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1
    );
    assert_eq!(
        first.open_silence.state_flags_u8,
        sparx::db::silence::OPEN_SILENCE_FLAG_OPEN_V1
    );
    assert_eq!(
        first.open_silence.silence_start_ts_i64,
        candidate.window_start_ts_i64
    );
    assert_eq!(
        first.open_silence.last_alert_window_end_ts_i64,
        candidate.window_end_ts_i64
    );
    assert_eq!(first.open_silence.last_alert_id, first.alert.alert_id);
}

#[test]
fn vdrop_alert_construction_maps_tenant_aggregate_to_sentinel_device_key() {
    let candidate = sample_vdrop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_TENANT_V1,
        "tenant-a",
    );
    let result = sparx::alert::build_vdrop_alert_v1(&candidate).unwrap();

    assert_eq!(
        result.alert.device_key,
        sparx::alert::VDROP_TENANT_AGGREGATE_DEVICE_KEY_V1
    );
    assert_eq!(result.alert.device_path, "tenant:tenant-a");
    assert_eq!(
        result.alert.reasons[0].details[0],
        ("subject_kind".to_string(), "tenant".to_string())
    );
    assert!(!result.alert.reasons[0]
        .details
        .iter()
        .any(|(key, _)| key == "device_key"));
    assert_eq!(
        result.open_silence.subject_kind_u8,
        sparx::db::silence::SILENCE_SUBJECT_KIND_TENANT_V1
    );
}

#[test]
fn vdrop_alert_construction_rejects_invalid_candidates() {
    let mut candidate = sample_vdrop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1,
        "device-a",
    );
    candidate.subject_kind_u8 = 99;
    assert!(matches!(
        sparx::alert::build_vdrop_alert_v1(&candidate).unwrap_err(),
        sparx::alert::AlertErrorV1::InvalidVDropCandidate { .. }
    ));

    let mut candidate = sample_vdrop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1,
        "device-a",
    );
    candidate.drop_ratio_f32 = f32::NAN;
    assert!(matches!(
        sparx::alert::build_vdrop_alert_v1(&candidate).unwrap_err(),
        sparx::alert::AlertErrorV1::InvalidVDropCandidate { .. }
    ));
}

fn sample_sharp_drop_candidate(
    subject_kind_u8: u8,
    subject_key: &str,
) -> sparx::db::silence::SharpDropCandidateV1 {
    let mut reason_details = vec![
        ("drop_kind".to_string(), "sharp_drop".to_string()),
        (
            "subject_kind".to_string(),
            if subject_kind_u8 == sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1 {
                "device".to_string()
            } else {
                "tenant".to_string()
            },
        ),
        ("tenant_id".to_string(), "tenant-a".to_string()),
    ];
    if subject_kind_u8 == sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1 {
        reason_details.push(("device_key".to_string(), subject_key.to_string()));
    }
    reason_details.extend(vec![
        ("window_start_ts".to_string(), "1700000060".to_string()),
        ("window_end_ts".to_string(), "1700000120".to_string()),
        ("bucket".to_string(), "8".to_string()),
        ("expected_lines".to_string(), "100.000000".to_string()),
        ("observed_lines".to_string(), "20".to_string()),
        (
            "observed_expected_ratio".to_string(),
            "0.200000".to_string(),
        ),
        ("drop_ratio".to_string(), "0.800000".to_string()),
        ("baseline_n".to_string(), "12".to_string()),
        ("baseline_mean_lines".to_string(), "100.000000".to_string()),
        ("baseline_stddev_lines".to_string(), "10.000000".to_string()),
        ("z_drop".to_string(), "8.000000".to_string()),
        (
            "max_observed_expected_ratio".to_string(),
            "0.250000".to_string(),
        ),
        ("min_drop_ratio".to_string(), "0.750000".to_string()),
        (
            "min_absolute_drop_lines".to_string(),
            "25.000000".to_string(),
        ),
        ("expected_bytes".to_string(), "10000.000000".to_string()),
        ("observed_bytes".to_string(), "2000".to_string()),
        ("absolute_drop_lines".to_string(), "80.000000".to_string()),
    ]);

    sparx::db::silence::SharpDropCandidateV1 {
        subject_kind_u8,
        subject_key: subject_key.to_string(),
        tenant_id: "tenant-a".to_string(),
        window_start_ts_i64: 1_700_000_060,
        window_end_ts_i64: 1_700_000_120,
        expected_lines_f64: 100.0,
        observed_lines_u64: 20,
        expected_bytes_f64: 10_000.0,
        observed_bytes_u64: 2_000,
        observed_expected_ratio_f32: 0.2,
        drop_ratio_f32: 0.8,
        absolute_drop_lines_f64: 80.0,
        line_stddevs_below_mean_f32: Some(8.0),
        maturity_count_u32: 12,
        bucket_u8: 8,
        reason_details,
    }
}

fn sample_file_span(file_rel: &str, offset_start: u64) -> FileSpanV1 {
    FileSpanV1 {
        file_rel: file_rel.to_string(),
        file_key: format!("{}-key", file_rel),
        inode: 42,
        offset_start,
        offset_end: offset_start + 100,
        is_gzip: false,
    }
}

#[test]
fn sharp_drop_alert_construction_is_deterministic_and_preserves_device_provenance() {
    let candidate = sample_sharp_drop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1,
        "device-a",
    );
    let provenance = vec![
        sample_file_span("b.log", 200),
        sample_file_span("a.log", 100),
    ];
    let first = sparx::alert::build_sharp_drop_alert_v1(&candidate, &provenance).unwrap();
    let second = sparx::alert::build_sharp_drop_alert_v1(&candidate, &provenance).unwrap();

    assert_eq!(first, second);
    assert!(is_lower_hex_128(&first.alert.alert_id));
    assert!(is_lower_hex_128(&first.alert.signature));
    assert_eq!(
        first.alert.schema_version,
        sparx::alert::ALERT_SCHEMA_VERSION_V1
    );
    assert_eq!(first.alert.tenant_id, "tenant-a");
    assert_eq!(first.alert.device_key, "device-a");
    assert_eq!(first.alert.device_path, "device-a");
    assert_eq!(first.alert.window_start_ts, 1_700_000_060);
    assert_eq!(first.alert.window_end_ts, 1_700_000_120);
    assert_eq!(first.alert.window_size_s, 60);
    assert_eq!(first.alert.bucket, 8);
    assert_eq!(first.alert.label, LabelV1::Info);
    assert_eq!(first.alert.confidence, ConfidenceV1::Medium);
    assert!(!first.alert.cold_start);
    assert_eq!(first.alert.score_total, 0.8);
    assert_eq!(first.alert.score_rarity, 0.0);
    assert_eq!(first.alert.score_drift, 0.0);
    assert_eq!(first.alert.score_volume, 0.8);
    assert_eq!(first.alert.lines, 20);
    assert_eq!(first.alert.bytes, 2_000);
    assert!(first.alert.top_features.is_empty());
    assert_eq!(first.alert.provenance.len(), 2);
    assert_eq!(first.alert.provenance[0].file_rel, "a.log");
    assert_eq!(first.alert.provenance[1].file_rel, "b.log");
    assert_eq!(first.alert.reasons.len(), 1);
    assert_eq!(
        first.alert.reasons[0].code,
        sparx::alert::VDROP_REASON_CODE_V1
    );
    assert_eq!(
        first.alert.reasons[0].msg,
        "log volume dropped sharply but did not stop for this subject"
    );
    assert_eq!(first.alert.reasons[0].details, candidate.reason_details);
    assert_eq!(
        first.alert.reasons[0].details[0],
        ("drop_kind".to_string(), "sharp_drop".to_string())
    );
    assert!(first.alert.summary_analyst.contains("sharp_drop"));
    assert!(first.alert.summary_customer.contains("dropped sharply"));

    assert_eq!(
        s(&first.primary_put.key),
        s(&key_tenant_alert_v1(&first.alert.alert_id))
    );
    assert_eq!(
        decode_alert_v1(&first.primary_put.value).unwrap(),
        first.alert
    );
    assert_eq!(
        first.open_drop.schema_version_u16,
        sparx::db::silence::SILENCE_SCHEMA_VERSION_V1
    );
    assert_eq!(
        first.open_drop.subject_kind_u8,
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1
    );
    assert_eq!(
        first.open_drop.state_flags_u8,
        sparx::db::silence::OPEN_DROP_FLAG_OPEN_V1
    );
    assert_eq!(
        first.open_drop.drop_start_ts_i64,
        candidate.window_start_ts_i64
    );
    assert_eq!(
        first.open_drop.last_alert_window_end_ts_i64,
        candidate.window_end_ts_i64
    );
    assert_eq!(first.open_drop.last_alert_id, first.alert.alert_id);
}

#[test]
fn sharp_drop_alert_construction_maps_tenant_aggregate_to_sentinel_and_empty_provenance() {
    let candidate = sample_sharp_drop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_TENANT_V1,
        "tenant-a",
    );
    let provenance = vec![sample_file_span("tenant-source.log", 0)];
    let result = sparx::alert::build_sharp_drop_alert_v1(&candidate, &provenance).unwrap();

    assert_eq!(
        result.alert.device_key,
        sparx::alert::VDROP_TENANT_AGGREGATE_DEVICE_KEY_V1
    );
    assert_eq!(result.alert.device_path, "tenant:tenant-a");
    assert!(result.alert.provenance.is_empty());
    assert_eq!(
        result.open_drop.subject_kind_u8,
        sparx::db::silence::SILENCE_SUBJECT_KIND_TENANT_V1
    );
    assert!(!result.alert.reasons[0]
        .details
        .iter()
        .any(|(key, _)| key == "device_key"));
}

#[test]
fn sharp_drop_alert_id_does_not_collide_with_hard_silence_vdrop() {
    let sharp = sample_sharp_drop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1,
        "device-a",
    );
    let sharp_alert = sparx::alert::build_sharp_drop_alert_v1(&sharp, &[]).unwrap();

    let mut hard = sample_vdrop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1,
        "device-a",
    );
    hard.window_start_ts_i64 = sharp.window_start_ts_i64;
    hard.window_end_ts_i64 = sharp.window_end_ts_i64;
    let hard_alert = sparx::alert::build_vdrop_alert_v1(&hard).unwrap();

    assert_ne!(sharp_alert.alert.alert_id, hard_alert.alert.alert_id);
    assert_eq!(
        sharp_alert.alert.reasons[0].details[0],
        ("drop_kind".to_string(), "sharp_drop".to_string())
    );
    assert_ne!(
        hard_alert.alert.reasons[0].details.first().unwrap().0,
        "drop_kind"
    );
}

#[test]
fn sharp_drop_alert_construction_rejects_invalid_candidates() {
    let mut candidate = sample_sharp_drop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1,
        "device-a",
    );
    candidate.observed_lines_u64 = 0;
    assert!(matches!(
        sparx::alert::build_sharp_drop_alert_v1(&candidate, &[]).unwrap_err(),
        sparx::alert::AlertErrorV1::InvalidSharpDropCandidate { .. }
    ));

    let mut candidate = sample_sharp_drop_candidate(
        sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1,
        "device-a",
    );
    candidate.reason_details.remove(0);
    assert!(matches!(
        sparx::alert::build_sharp_drop_alert_v1(&candidate, &[]).unwrap_err(),
        sparx::alert::AlertErrorV1::InvalidSharpDropCandidate { .. }
    ));
}

fn sample_source_stream_subject_for_alert_v1() -> sparx::db::source_stream::SourceStreamSubjectV1 {
    sparx::db::source_stream::SourceStreamSubjectV1 {
        tenant_id: "tenant-a".to_string(),
        device_key: "device-a".to_string(),
        source_stream_id: "0123456789abcdef0123456789abcdef".to_string(),
        canonical_source_path: "var/log/auth.log".to_string(),
    }
}

fn sample_source_stream_vdrop_candidate_v1() -> sparx::db::silence::VDropCandidateV1 {
    let subject = sample_source_stream_subject_for_alert_v1();
    sparx::db::silence::VDropCandidateV1 {
        subject_kind_u8: sparx::db::silence::SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        subject_key: subject.source_stream_id.clone(),
        tenant_id: subject.tenant_id.clone(),
        window_start_ts_i64: 1_700_000_060,
        window_end_ts_i64: 1_700_000_240,
        last_seen_ts_i64: 1_700_000_060,
        expected_windows_missed_u64: 3,
        expected_lines_u64: 90,
        observed_lines_u64: 0,
        drop_ratio_f32: 1.0,
        bucket_u8: 8,
        reason_details: vec![
            ("subject_kind".to_string(), "source_stream".to_string()),
            ("tenant_id".to_string(), subject.tenant_id),
            ("device_key".to_string(), subject.device_key),
            ("source_stream_id".to_string(), subject.source_stream_id),
            ("source_path".to_string(), subject.canonical_source_path),
            ("window_start_ts".to_string(), "1700000060".to_string()),
            ("window_end_ts".to_string(), "1700000240".to_string()),
            ("last_seen_ts".to_string(), "1700000060".to_string()),
            ("expected_windows_missed".to_string(), "3".to_string()),
            ("expected_lines".to_string(), "90".to_string()),
            ("observed_lines".to_string(), "0".to_string()),
            ("drop_ratio".to_string(), "1.000000".to_string()),
            ("bucket".to_string(), "8".to_string()),
        ],
    }
}

fn sample_source_stream_sharp_drop_candidate_v1() -> sparx::db::silence::SharpDropCandidateV1 {
    let subject = sample_source_stream_subject_for_alert_v1();
    sparx::db::silence::SharpDropCandidateV1 {
        subject_kind_u8: sparx::db::silence::SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        subject_key: subject.source_stream_id.clone(),
        tenant_id: subject.tenant_id.clone(),
        window_start_ts_i64: 1_700_000_060,
        window_end_ts_i64: 1_700_000_120,
        expected_lines_f64: 100.0,
        observed_lines_u64: 20,
        expected_bytes_f64: 10_000.0,
        observed_bytes_u64: 2_000,
        observed_expected_ratio_f32: 0.2,
        drop_ratio_f32: 0.8,
        absolute_drop_lines_f64: 80.0,
        line_stddevs_below_mean_f32: Some(8.0),
        maturity_count_u32: 12,
        bucket_u8: 8,
        reason_details: vec![
            ("drop_kind".to_string(), "sharp_drop".to_string()),
            ("subject_kind".to_string(), "source_stream".to_string()),
            ("tenant_id".to_string(), subject.tenant_id),
            ("device_key".to_string(), subject.device_key),
            ("source_stream_id".to_string(), subject.source_stream_id),
            ("source_path".to_string(), subject.canonical_source_path),
            ("window_start_ts".to_string(), "1700000060".to_string()),
            ("window_end_ts".to_string(), "1700000120".to_string()),
            ("bucket".to_string(), "8".to_string()),
            ("expected_lines".to_string(), "100.000000".to_string()),
            ("observed_lines".to_string(), "20".to_string()),
            (
                "observed_expected_ratio".to_string(),
                "0.200000".to_string(),
            ),
            ("drop_ratio".to_string(), "0.800000".to_string()),
            ("baseline_n".to_string(), "12".to_string()),
            ("baseline_mean_lines".to_string(), "100.000000".to_string()),
            ("baseline_stddev_lines".to_string(), "10.000000".to_string()),
            ("z_drop".to_string(), "8.000000".to_string()),
            (
                "max_observed_expected_ratio".to_string(),
                "0.250000".to_string(),
            ),
            ("min_drop_ratio".to_string(), "0.750000".to_string()),
            (
                "min_absolute_drop_lines".to_string(),
                "25.000000".to_string(),
            ),
            ("expected_bytes".to_string(), "10000.000000".to_string()),
            ("observed_bytes".to_string(), "2000".to_string()),
            ("absolute_drop_lines".to_string(), "80.000000".to_string()),
        ],
    }
}

#[test]
fn source_stream_vdrop_alert_construction_is_deterministic_and_runtime_inactive_v1() {
    let subject = sample_source_stream_subject_for_alert_v1();
    let candidate = sample_source_stream_vdrop_candidate_v1();
    let first = sparx::alert::build_source_stream_vdrop_alert_v1(&subject, &candidate).unwrap();
    let second = sparx::alert::build_source_stream_vdrop_alert_v1(&subject, &candidate).unwrap();

    assert_eq!(first, second);
    assert!(is_lower_hex_128(&first.alert.alert_id));
    assert!(is_lower_hex_128(&first.alert.signature));
    assert_eq!(first.alert.device_key, "device-a");
    assert_eq!(
        first.alert.device_path,
        "source_stream:device-a/var/log/auth.log"
    );
    assert_eq!(first.alert.lines, 0);
    assert_eq!(first.alert.bytes, 0);
    assert!(first.alert.provenance.is_empty());
    assert_eq!(first.alert.reasons.len(), 1);
    assert_eq!(
        first.alert.reasons[0].details[0],
        ("drop_kind".to_string(), "hard_silence".to_string())
    );
    assert!(first.alert.reasons[0]
        .details
        .iter()
        .any(|(key, value)| key == "source_stream_id" && value == &subject.source_stream_id));
    assert!(first.alert.reasons[0]
        .details
        .iter()
        .any(|(key, value)| key == "source_path" && value == "var/log/auth.log"));
    assert_eq!(
        s(&first.primary_put.key),
        s(&key_tenant_alert_v1(&first.alert.alert_id))
    );
    assert_eq!(
        decode_alert_v1(&first.primary_put.value).unwrap(),
        first.alert
    );
    assert_eq!(
        first.open_silence.subject_kind_u8,
        sparx::db::silence::SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
    );
    assert_eq!(first.open_silence.last_alert_id, first.alert.alert_id);
}

#[test]
fn source_stream_sharp_drop_alert_construction_preserves_source_provenance_v1() {
    let subject = sample_source_stream_subject_for_alert_v1();
    let candidate = sample_source_stream_sharp_drop_candidate_v1();
    let spans = vec![
        sample_file_span("var/log/auth.log", 0),
        sample_file_span("var/log/auth.log", 100),
    ];
    let result =
        sparx::alert::build_source_stream_sharp_drop_alert_v1(&subject, &candidate, &spans)
            .unwrap();

    assert!(is_lower_hex_128(&result.alert.alert_id));
    assert_eq!(result.alert.device_key, "device-a");
    assert_eq!(
        result.alert.device_path,
        "source_stream:device-a/var/log/auth.log"
    );
    assert_eq!(result.alert.lines, 20);
    assert_eq!(result.alert.bytes, 2_000);
    assert_eq!(result.alert.provenance, spans);
    assert_eq!(
        result.alert.reasons[0].details[0],
        ("drop_kind".to_string(), "sharp_drop".to_string())
    );
    assert!(result.alert.reasons[0]
        .details
        .iter()
        .any(|(key, value)| key == "source_stream_id" && value == &subject.source_stream_id));
    assert_eq!(
        result.open_drop.subject_kind_u8,
        sparx::db::silence::SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
    );
    assert_eq!(result.open_drop.last_alert_id, result.alert.alert_id);
}

#[test]
fn source_stream_alert_builders_reject_mismatched_subjects_v1() {
    let subject = sample_source_stream_subject_for_alert_v1();
    let mut wrong_subject = subject.clone();
    wrong_subject.source_stream_id = "fedcba9876543210fedcba9876543210".to_string();
    let vdrop = sample_source_stream_vdrop_candidate_v1();
    assert!(matches!(
        sparx::alert::build_source_stream_vdrop_alert_v1(&wrong_subject, &vdrop).unwrap_err(),
        sparx::alert::AlertErrorV1::InvalidVDropCandidate { .. }
    ));

    let mut sharp = sample_source_stream_sharp_drop_candidate_v1();
    sharp.subject_kind_u8 = sparx::db::silence::SILENCE_SUBJECT_KIND_DEVICE_V1;
    assert!(matches!(
        sparx::alert::build_source_stream_sharp_drop_alert_v1(&subject, &sharp, &[]).unwrap_err(),
        sparx::alert::AlertErrorV1::InvalidSharpDropCandidate { .. }
    ));
}

#[test]
fn source_stream_hard_silence_and_sharp_drop_alert_ids_do_not_collide_v1() {
    let subject = sample_source_stream_subject_for_alert_v1();
    let hard = sparx::alert::build_source_stream_vdrop_alert_v1(
        &subject,
        &sample_source_stream_vdrop_candidate_v1(),
    )
    .unwrap();
    let sharp = sparx::alert::build_source_stream_sharp_drop_alert_v1(
        &subject,
        &sample_source_stream_sharp_drop_candidate_v1(),
        &[],
    )
    .unwrap();

    assert_ne!(hard.alert.alert_id, sharp.alert.alert_id);
}
