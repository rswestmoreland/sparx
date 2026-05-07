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

fn baseline(window_start_ts: i64, n_bucket: u32, df: &[(u32, u32)], centroid: &[(u32, f32)]) -> BucketBaselineV1 {
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
    let row_small = row(start, &[(1, 1), (6, 1)], 5, 100, EntitySketchSnapshotV1::default());
    let row_large = row(start, &[(1, 10), (6, 10)], 5, 100, EntitySketchSnapshotV1::default());
    let base = baseline(start, 100, &[(1, 1), (6, 1)], &[]);

    let small = build_alert_v1("acme", "tenant/acme/dev01", &row_small, &dict(), &base, None, &cfg(), &[]).unwrap();
    let large = build_alert_v1("acme", "tenant/acme/dev01", &row_large, &dict(), &base, None, &cfg(), &[]).unwrap();

    approx_eq_f32(small.rarity, large.rarity);
}

#[test]
fn drift_is_near_zero_when_weighted_row_matches_centroid() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 1, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[(1, 1), (6, 2)], 5, 100, EntitySketchSnapshotV1::default());
    let tf_shape = 1.0_f64.ln_1p() as f32;
    let tf_word = (0.3_f64 * 2.0_f64.ln_1p()) as f32;
    let base = baseline(start, 100, &[(1, 10), (6, 10)], &[(1, tf_shape), (6, tf_word)]);

    let result = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &base, None, &cfg(), &[]).unwrap();

    assert!(result.drift <= 0.0001, "drift={}", result.drift);
}

#[test]
fn volume_extreme_in_cold_start_emits_info() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 2, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[(6, 1)], 500, 50000, EntitySketchSnapshotV1::default());
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

    let result = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &base, None, &cfg(), &[]).unwrap();

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

    let result = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &base, None, &cfg(), &[]).unwrap();

    assert!(result.blob_ratio >= 0.60, "blob_ratio={}", result.blob_ratio);
    let alert = result.alert.unwrap();
    assert_eq!(alert.label, LabelV1::NoiseSuspect);
    assert!(alert.reasons.iter().any(|reason| reason.code == "N_HIGH_CARDINALITY"));
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

    let first = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &base, None, &cfg(), &provenance).unwrap();
    let second = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &base, None, &cfg(), &provenance).unwrap();
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
    let result = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &base, Some(&outlier_stats), &cfg(), &[]).unwrap();
    let alert = result.alert.unwrap();

    assert_eq!(alert.label, LabelV1::Outlier);
    assert_eq!(alert.confidence, ConfidenceV1::High);
    assert!(alert.reasons.iter().any(|reason| reason.code == "R_NEW_FEATURE"));
    assert!(alert.reasons.iter().any(|reason| reason.code == "D_HIGH_DRIFT"));
    assert!(alert.reasons.iter().any(|reason| reason.code == "O_ENTITY_FOCUSED"));
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

    let immature_result = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &immature, None, &cfg, &[]).unwrap();
    let mature_result = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &mature, None, &cfg, &[]).unwrap();

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

    let result = build_alert_v1("acme", "tenant/acme/dev01", &row, &dict(), &base, Some(&outlier_stats), &cfg(), &[]).unwrap();

    assert!(result.below_min_lines);
    assert!(result.score_total >= 0.60, "score_total={}", result.score_total);
    assert!(result.alert.is_none());
    assert!(result.primary_put.is_none());
}
