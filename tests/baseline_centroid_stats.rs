use chrono::{TimeZone, Utc};

use sparx::baseline::{
    plan_centroid_stats_update_v1, weighted_row_vector_v1, CentroidStatsConfigV1,
    CentroidStatsErrorV1, CentroidStatsMutationV1, CENTROID_CAP_DEFAULT_V1,
};
use sparx::db::baseline_sketch::{
    decode_centroid_v1, decode_stats_v1, CentroidValuePairV1, DeviceStatsV1, WelfordF64V1,
};
use sparx::db::keys::{key_tenant_centroid_v1, key_tenant_stats_v1, KeyBytes};
use sparx::db::open_window::{SparseCountPairV1, WinMetaV1};
use sparx::features::{
    EntitySketchSnapshotV1, FeatureDictionaryConfigV1, FeatureDictionaryMetaV1, FeatureDictionaryV1,
};
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
            next_id: 10,
            entries: 6,
            last_gc_ts: 0,
        },
        vec![
            ("SourceIp=<IPV4>".to_string(), 1),
            ("SourceIp_net@10.2.3.0/24".to_string(), 2),
            ("k=src_ip".to_string(), 3),
            ("canon=SourceIp".to_string(), 4),
            ("syslog_app=sshd".to_string(), 5),
            ("w=failed".to_string(), 6),
        ],
        vec![
            (1, "SourceIp=<IPV4>".to_string()),
            (2, "SourceIp_net@10.2.3.0/24".to_string()),
            (3, "k=src_ip".to_string()),
            (4, "canon=SourceIp".to_string()),
            (5, "syslog_app=sshd".to_string()),
            (6, "w=failed".to_string()),
        ],
    )
    .unwrap()
}

fn row(window_start_ts: i64, sparse_counts: &[(u32, u32)], lines: u32, bytes: u64) -> FinalizedWindowRowV1 {
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
        entity_snapshot: EntitySketchSnapshotV1::default(),
    }
}

fn cfg(alpha: f32, cap: usize) -> CentroidStatsConfigV1 {
    CentroidStatsConfigV1 {
        centroid_alpha: alpha,
        centroid_cap: cap,
    }
}

fn approx_eq_f32(left: f32, right: f32) {
    let diff = (left - right).abs();
    assert!(diff <= 0.0001, "left={left} right={right} diff={diff}");
}

fn approx_eq_f64(left: f64, right: f64) {
    let diff = (left - right).abs();
    assert!(diff <= 0.0000001, "left={left} right={right} diff={diff}");
}

#[test]
fn weighted_row_vector_uses_family_weights_and_log1p_counts() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 15, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)], 1, 10);
    let weighted = weighted_row_vector_v1(&row, &dict()).unwrap();

    assert_eq!(weighted.len(), 6);
    approx_eq_f32(weighted[0].value, (1.0_f64 * 1.0_f64.ln_1p()) as f32);
    approx_eq_f32(weighted[1].value, (0.8_f64 * 2.0_f64.ln_1p()) as f32);
    approx_eq_f32(weighted[2].value, (0.5_f64 * 3.0_f64.ln_1p()) as f32);
    approx_eq_f32(weighted[3].value, (0.5_f64 * 4.0_f64.ln_1p()) as f32);
    approx_eq_f32(weighted[4].value, (0.2_f64 * 5.0_f64.ln_1p()) as f32);
    approx_eq_f32(weighted[5].value, (0.3_f64 * 6.0_f64.ln_1p()) as f32);
}

#[test]
fn update_from_empty_state_writes_centroid_and_stats_without_score_total() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 16, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[(1, 1), (6, 2)], 3, 120);
    let update_ts = start + 60;

    let plan = plan_centroid_stats_update_v1(
        &row,
        &dict(),
        &cfg(1.0, CENTROID_CAP_DEFAULT_V1),
        &[],
        None,
        None,
        update_ts,
    )
    .unwrap();

    assert!(!plan.score_total_updated);
    assert_eq!(plan.weighted_row_pairs, plan.next_centroid_pairs);
    assert_eq!(plan.next_stats.line_count.n, 1);
    assert_eq!(plan.next_stats.byte_count.n, 1);
    assert_eq!(plan.next_stats.score_total.n, 0);
    approx_eq_f64(plan.next_stats.line_count.mean, 3.0);
    approx_eq_f64(plan.next_stats.byte_count.mean, 120.0);
    assert_eq!(plan.next_stats.last_update_ts, update_ts);
    assert_eq!(plan.mutations.len(), 2);

    match &plan.mutations[0] {
        CentroidStatsMutationV1::Put(kv) => {
            assert_eq!(s(&kv.key), s(&key_tenant_centroid_v1("dev01", row.key.bucket)));
            assert_eq!(decode_centroid_v1(&kv.value).unwrap(), plan.next_centroid_pairs);
        }
    }
    match &plan.mutations[1] {
        CentroidStatsMutationV1::Put(kv) => {
            assert_eq!(s(&kv.key), s(&key_tenant_stats_v1("dev01", row.key.bucket)));
            assert_eq!(decode_stats_v1(&kv.value).unwrap(), plan.next_stats);
        }
    }
}

#[test]
fn update_applies_ema_cap_and_score_stats_deterministically() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 17, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[(2, 1), (3, 3), (5, 1)], 5, 200);
    let current_centroid = vec![
        CentroidValuePairV1 {
            feature_id: 1,
            value: 4.0,
        },
        CentroidValuePairV1 {
            feature_id: 4,
            value: -4.0,
        },
    ];
    let current_stats = DeviceStatsV1 {
        line_count: WelfordF64V1 {
            n: 2,
            mean: 3.0,
            m2: 2.0,
        },
        byte_count: WelfordF64V1 {
            n: 2,
            mean: 100.0,
            m2: 50.0,
        },
        score_total: WelfordF64V1 {
            n: 1,
            mean: 0.3,
            m2: 0.0,
        },
        last_update_ts: start - 60,
    };

    let plan = plan_centroid_stats_update_v1(
        &row,
        &dict(),
        &cfg(0.25, 3),
        &current_centroid,
        Some(&current_stats),
        Some(0.9),
        start + 60,
    )
    .unwrap();

    assert!(plan.score_total_updated);
    assert_eq!(plan.next_centroid_pairs.len(), 3);
    assert_eq!(
        plan.next_centroid_pairs
            .iter()
            .map(|pair| pair.feature_id)
            .collect::<Vec<u32>>(),
        vec![1, 3, 4]
    );
    approx_eq_f32(plan.next_centroid_pairs[0].value, 3.0);
    approx_eq_f32(
        plan.next_centroid_pairs[1].value,
        (0.25_f64 * 0.5_f64 * 3.0_f64.ln_1p()) as f32
    );
    approx_eq_f32(plan.next_centroid_pairs[2].value, -3.0);

    assert_eq!(plan.next_stats.line_count.n, 3);
    approx_eq_f64(plan.next_stats.line_count.mean, 3.6666666666666665);
    assert_eq!(plan.next_stats.byte_count.n, 3);
    approx_eq_f64(plan.next_stats.byte_count.mean, 133.33333333333334);
    assert_eq!(plan.next_stats.score_total.n, 2);
    approx_eq_f64(plan.next_stats.score_total.mean, 0.6);
    assert_eq!(plan.next_stats.last_update_ts, start + 60);
}

#[test]
fn alpha_one_with_empty_row_drops_exact_zero_centroid_entries() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 18, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[], 1, 1);
    let current_centroid = vec![CentroidValuePairV1 {
        feature_id: 1,
        value: 2.0,
    }];

    let plan = plan_centroid_stats_update_v1(
        &row,
        &dict(),
        &cfg(1.0, CENTROID_CAP_DEFAULT_V1),
        &current_centroid,
        None,
        None,
        start + 60,
    )
    .unwrap();

    assert!(plan.next_centroid_pairs.is_empty());
}

#[test]
fn rejects_invalid_config_missing_features_and_bad_bucket() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 19, 0)
        .single()
        .unwrap()
        .timestamp();
    let missing_feature_row = row(start, &[(99, 1)], 1, 1);
    assert_eq!(
        weighted_row_vector_v1(&missing_feature_row, &dict()).unwrap_err(),
        CentroidStatsErrorV1::MissingFeatureString { feature_id: 99 }
    );

    let good_row = row(start, &[(1, 1)], 1, 1);
    assert_eq!(
        plan_centroid_stats_update_v1(&good_row, &dict(), &cfg(0.0, 1), &[], None, None, start + 60)
            .unwrap_err(),
        CentroidStatsErrorV1::InvalidCentroidAlpha { centroid_alpha: 0.0 }
    );
    assert_eq!(
        plan_centroid_stats_update_v1(&good_row, &dict(), &cfg(0.5, 0), &[], None, None, start + 60)
            .unwrap_err(),
        CentroidStatsErrorV1::InvalidCentroidCap { centroid_cap: 0 }
    );

    let mut bad_bucket_row = row(start, &[(1, 1)], 1, 1);
    bad_bucket_row.key.bucket = 48;
    assert_eq!(
        plan_centroid_stats_update_v1(&bad_bucket_row, &dict(), &cfg(0.5, 1), &[], None, None, start + 60)
            .unwrap_err(),
        CentroidStatsErrorV1::InvalidBucket { bucket: 48 }
    );
}

#[test]
fn rejects_stats_counter_overflow() {
    let start = Utc
        .with_ymd_and_hms(2024, 1, 5, 13, 20, 0)
        .single()
        .unwrap()
        .timestamp();
    let row = row(start, &[(1, 1)], 1, 1);
    let current_stats = DeviceStatsV1 {
        line_count: WelfordF64V1 {
            n: u32::MAX,
            mean: 1.0,
            m2: 0.0,
        },
        byte_count: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        score_total: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        last_update_ts: start,
    };

    assert_eq!(
        plan_centroid_stats_update_v1(
            &row,
            &dict(),
            &cfg(0.5, 10),
            &[],
            Some(&current_stats),
            None,
            start + 60,
        )
        .unwrap_err(),
        CentroidStatsErrorV1::StatsCountOverflow {
            field: "line_count",
            current_n: u32::MAX,
        }
    );
}
