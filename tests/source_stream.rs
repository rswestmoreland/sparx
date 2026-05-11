// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use sparx::db::baseline_sketch::WelfordF64V1;
use sparx::db::silence::*;
use sparx::db::source_stream::*;

#[test]
fn source_stream_path_canonicalization_accepts_safe_relative_paths_v1() {
    assert_eq!(
        canonicalize_source_stream_path_v1("var/log/auth.log").unwrap(),
        "var/log/auth.log"
    );
    assert_eq!(
        canonicalize_source_stream_path_v1("var\\log\\auth.log").unwrap(),
        "var/log/auth.log"
    );
}

#[test]
fn source_stream_path_canonicalization_rejects_unsafe_paths_v1() {
    assert_eq!(
        canonicalize_source_stream_path_v1("").unwrap_err(),
        SourceStreamErrorV1::EmptyPath
    );
    assert_eq!(
        canonicalize_source_stream_path_v1("/var/log/auth.log").unwrap_err(),
        SourceStreamErrorV1::AbsolutePath
    );
    assert_eq!(
        canonicalize_source_stream_path_v1("var//log").unwrap_err(),
        SourceStreamErrorV1::InvalidPathComponent {
            component: String::new(),
        }
    );
    assert_eq!(
        canonicalize_source_stream_path_v1("var/../log").unwrap_err(),
        SourceStreamErrorV1::InvalidPathComponent {
            component: "..".to_string(),
        }
    );
    assert_eq!(
        canonicalize_source_stream_path_v1("var/log\nauth.log").unwrap_err(),
        SourceStreamErrorV1::InvalidPathByte(b'\n')
    );
}

#[test]
fn source_stream_id_is_deterministic_and_not_feature_id_v1() {
    let a = source_stream_identity_from_path_v1("tenant-a", "device-key-001", "var/log/auth.log")
        .unwrap();
    let b = source_stream_identity_from_path_v1("tenant-a", "device-key-001", "var\\log\\auth.log")
        .unwrap();
    let c = source_stream_identity_from_path_v1("tenant-a", "device-key-001", "var/log/messages")
        .unwrap();

    assert_eq!(a.source_stream_id, b.source_stream_id);
    assert_ne!(a.source_stream_id, c.source_stream_id);
    assert_eq!(a.source_stream_id.len(), SOURCE_STREAM_ID_HEX_LEN_V1);
    assert!(a
        .source_stream_id
        .bytes()
        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')));
    assert_eq!(a.canonical_source_path, "var/log/auth.log");
}

#[test]
fn source_stream_catalog_roundtrips_variable_encoding_v1() {
    let identity =
        source_stream_identity_from_path_v1("tenant-a", "device-key-001", "var/log/auth.log")
            .unwrap();
    let catalog =
        source_stream_catalog_from_identity_v1(&identity, 1_700_000_000, 1_700_000_600).unwrap();
    let encoded = encode_source_stream_catalog_v1(&catalog).unwrap();

    assert_eq!(
        encoded.len(),
        SOURCE_STREAM_CATALOG_V1_FIXED_LEN + 32 + 14 + 16
    );
    assert_eq!(
        &encoded[0..2],
        &SOURCE_STREAM_SCHEMA_VERSION_V1.to_le_bytes()
    );
    assert_eq!(encoded[2], SOURCE_STREAM_FLAG_ACTIVE_V1);
    assert_eq!(encoded[3], 0);
    assert_eq!(&encoded[4..6], &0u16.to_le_bytes());
    assert_eq!(&encoded[6..14], &1_700_000_000i64.to_le_bytes());
    assert_eq!(&encoded[14..22], &1_700_000_600i64.to_le_bytes());
    assert_eq!(&encoded[22..24], &32u16.to_le_bytes());
    assert_eq!(&encoded[24..26], &14u16.to_le_bytes());
    assert_eq!(&encoded[26..28], &16u16.to_le_bytes());
    assert_eq!(decode_source_stream_catalog_v1(&encoded).unwrap(), catalog);
}

#[test]
fn source_stream_catalog_rejects_malformed_values_v1() {
    let identity =
        source_stream_identity_from_path_v1("tenant-a", "device-key-001", "var/log/auth.log")
            .unwrap();
    let catalog = source_stream_catalog_from_identity_v1(&identity, 10, 20).unwrap();

    let mut encoded = encode_source_stream_catalog_v1(&catalog).unwrap();
    encoded[0..2].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_source_stream_catalog_v1(&encoded).unwrap_err(),
        SourceStreamErrorV1::UnknownSchemaVersion(2)
    );

    let mut encoded = encode_source_stream_catalog_v1(&catalog).unwrap();
    encoded[3] = 1;
    assert_eq!(
        decode_source_stream_catalog_v1(&encoded).unwrap_err(),
        SourceStreamErrorV1::InvalidReservedField {
            field: "reserved_u8_0",
            value: 1,
        }
    );

    let mut encoded = encode_source_stream_catalog_v1(&catalog).unwrap();
    encoded[22..24].copy_from_slice(&33u16.to_le_bytes());
    assert!(matches!(
        decode_source_stream_catalog_v1(&encoded).unwrap_err(),
        SourceStreamErrorV1::InvalidSourceStreamId
            | SourceStreamErrorV1::InvalidStringLength {
                field: "canonical_source_path",
                ..
            }
    ));
}

#[test]
fn source_stream_catalog_update_preserves_first_and_latest_seen_v1() {
    let identity =
        source_stream_identity_from_path_v1("tenant-a", "device-key-001", "var/log/auth.log")
            .unwrap();
    let initial = update_source_stream_catalog_observed_v1(None, &identity, 100).unwrap();
    let newer = update_source_stream_catalog_observed_v1(Some(&initial), &identity, 200).unwrap();
    let older = update_source_stream_catalog_observed_v1(Some(&newer), &identity, 50).unwrap();

    assert_eq!(older.first_seen_ts_i64, 50);
    assert_eq!(older.last_seen_ts_i64, 200);
}

#[test]
fn source_stream_stats_roundtrip_and_update_v1() {
    let stats =
        update_source_stream_stats_from_observation_v1(None, 10, 1000, 1_700_000_000).unwrap();
    let stats =
        update_source_stream_stats_from_observation_v1(Some(&stats), 20, 3000, 1_700_000_060)
            .unwrap();

    assert_eq!(stats.line_count.n, 2);
    assert_eq!(stats.line_count.mean, 15.0);
    assert_eq!(stats.byte_count.n, 2);
    assert_eq!(stats.byte_count.mean, 2000.0);
    assert_eq!(
        stats.score_total,
        WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0
        }
    );
    assert_eq!(stats.last_update_ts, 1_700_000_060);

    let encoded = encode_source_stream_stats_v1(&stats).unwrap();
    assert_eq!(encoded.len(), SOURCE_STREAM_STATS_V1_LEN);
    assert_eq!(decode_source_stream_stats_v1(&encoded).unwrap(), stats);
}

#[test]
fn source_stream_stats_rejects_wrong_length_and_reserved_score_v1() {
    assert_eq!(
        decode_source_stream_stats_v1(&[0; SOURCE_STREAM_STATS_V1_LEN - 1]).unwrap_err(),
        SourceStreamErrorV1::InvalidLength {
            expected: SOURCE_STREAM_STATS_V1_LEN,
            actual: SOURCE_STREAM_STATS_V1_LEN - 1,
        }
    );

    let mut stats = empty_source_stream_stats_v1(1);
    stats.score_total = WelfordF64V1 {
        n: 1,
        mean: 1.0,
        m2: 0.0,
    };
    assert_eq!(
        encode_source_stream_stats_v1(&stats).unwrap_err(),
        SourceStreamErrorV1::InvalidReservedField {
            field: "score_total",
            value: 1,
        }
    );
}

fn sample_source_stream_subject_v1() -> SourceStreamSubjectV1 {
    let identity =
        source_stream_identity_from_path_v1("tenant-a", "device-key-001", "var/log/auth.log")
            .unwrap();
    source_stream_subject_from_identity_v1(&identity)
}

fn sample_source_stream_expected_state_v1(mature_windows_total_u64: u64) -> ExpectedSourceStateV1 {
    ExpectedSourceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        state_flags_u8: 0,
        window_size_s_u32: 60,
        observed_windows_total_u64: mature_windows_total_u64,
        mature_windows_total_u64,
        last_seen_window_start_ts_i64: 1_700_000_000,
        last_seen_window_end_ts_i64: 1_700_000_060,
        last_observed_lines_u64: 50,
        last_observed_bytes_u64: 5000,
        last_bucket_u8: 8,
        reserved_u8_0: 0,
        reserved_u16_0: 0,
        last_update_ts_i64: 1_700_000_060,
    }
}

fn sample_source_stream_hard_silence_config_v1() -> VDropEvaluationConfigV1 {
    VDropEvaluationConfigV1 {
        eval_ts_i64: 1_700_000_180,
        min_mature_windows_u64: 3,
        min_expected_windows_missed_u64: 2,
        min_expected_lines_u64: 75,
    }
}

fn sample_source_stream_stats_for_drop_v1(
    mean_lines: f64,
    line_stddev: f64,
    n: u32,
) -> SourceStreamStatsV1 {
    SourceStreamStatsV1 {
        line_count: WelfordF64V1 {
            n,
            mean: mean_lines,
            m2: line_stddev * line_stddev * f64::from(n.saturating_sub(1)),
        },
        byte_count: WelfordF64V1 {
            n,
            mean: mean_lines * 100.0,
            m2: line_stddev * line_stddev * 10_000.0 * f64::from(n.saturating_sub(1)),
        },
        score_total: WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        last_update_ts: 1_700_000_000,
    }
}

fn sample_source_stream_current_window_v1(observed_lines_u64: u64) -> SourceStreamCurrentWindowV1 {
    SourceStreamCurrentWindowV1 {
        subject: sample_source_stream_subject_v1(),
        window_start_ts_i64: 1_700_000_060,
        window_end_ts_i64: 1_700_000_120,
        observed_lines_u64,
        observed_bytes_u64: observed_lines_u64 * 100,
        bucket_u8: 8,
    }
}

fn sample_source_stream_sharp_drop_config_v1() -> SharpDropEvaluationConfigV1 {
    SharpDropEvaluationConfigV1 {
        min_maturity_count_u64: 3,
        min_expected_lines_f64: 25.0,
        min_absolute_drop_lines_f64: 25.0,
        max_observed_expected_ratio_f32: SHARP_DROP_DEFAULT_MAX_OBSERVED_EXPECTED_RATIO_V1,
        min_drop_ratio_f32: SHARP_DROP_DEFAULT_MIN_DROP_RATIO_V1,
        variance_gate_stddevs_f32: SHARP_DROP_DEFAULT_VARIANCE_GATE_STDDEVS_V1,
    }
}

fn assert_source_close_f32_v1(actual: f32, expected: f32) {
    assert!(
        (actual - expected).abs() < 0.000_001,
        "actual={actual} expected={expected}"
    );
}

fn assert_source_close_f64_v1(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_001,
        "actual={actual} expected={expected}"
    );
}

#[test]
fn source_stream_expected_volume_uses_source_stream_stats_v1() {
    let stats = sample_source_stream_stats_for_drop_v1(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_source_stream_stats_v1(&stats).unwrap();

    assert_eq!(expected.maturity_count_u32, 12);
    assert_source_close_f64_v1(expected.expected_lines_f64, 100.0);
    assert_source_close_f64_v1(expected.expected_bytes_f64, 10_000.0);
    assert_source_close_f64_v1(expected.line_stddev_f64, 10.0);
}

#[test]
fn source_stream_expected_volume_rejects_invalid_stats_without_panic_v1() {
    let mut stats = sample_source_stream_stats_for_drop_v1(100.0, 10.0, 12);
    stats.line_count.mean = f64::NAN;

    assert_eq!(
        sharp_drop_expected_volume_from_source_stream_stats_v1(&stats).unwrap_err(),
        SourceStreamErrorV1::InvalidStatsField {
            field: "line_count"
        }
    );
}

#[test]
fn source_stream_hard_silence_evaluator_emits_full_drop_candidate_v1() {
    let subject = sample_source_stream_subject_v1();
    let state = sample_source_stream_expected_state_v1(12);
    let cfg = sample_source_stream_hard_silence_config_v1();
    let eval = evaluate_source_stream_hard_silence_candidate_v1(&subject, Some(&state), None, &cfg)
        .unwrap();

    let candidate = match eval {
        VDropEvaluationV1::Candidate(candidate) => candidate,
        other => panic!(
            "expected source-stream hard-silence candidate, got {:?}",
            other
        ),
    };

    assert_eq!(
        candidate.subject_kind_u8,
        SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
    );
    assert_eq!(candidate.subject_key, subject.source_stream_id.clone());
    assert_eq!(candidate.tenant_id, "tenant-a");
    assert_eq!(candidate.window_start_ts_i64, 1_700_000_060);
    assert_eq!(candidate.window_end_ts_i64, 1_700_000_180);
    assert_eq!(candidate.expected_windows_missed_u64, 2);
    assert_eq!(candidate.expected_lines_u64, 100);
    assert_eq!(candidate.observed_lines_u64, 0);
    assert_source_close_f32_v1(candidate.drop_ratio_f32, 1.0);
    assert_eq!(candidate.bucket_u8, 8);
    assert_eq!(
        candidate.reason_details,
        vec![
            ("subject_kind".to_string(), "source_stream".to_string()),
            ("tenant_id".to_string(), "tenant-a".to_string()),
            ("device_key".to_string(), "device-key-001".to_string()),
            (
                "source_stream_id".to_string(),
                candidate.subject_key.clone()
            ),
            ("source_path".to_string(), "var/log/auth.log".to_string()),
            ("window_start_ts".to_string(), "1700000060".to_string()),
            ("window_end_ts".to_string(), "1700000180".to_string()),
            ("last_seen_ts".to_string(), "1700000060".to_string()),
            ("expected_windows_missed".to_string(), "2".to_string()),
            ("expected_lines".to_string(), "100".to_string()),
            ("observed_lines".to_string(), "0".to_string()),
            ("drop_ratio".to_string(), "1.000000".to_string()),
            ("bucket".to_string(), "8".to_string()),
        ]
    );
}

#[test]
fn source_stream_hard_silence_evaluator_suppresses_immature_or_wrong_subject_v1() {
    let subject = sample_source_stream_subject_v1();
    let cfg = sample_source_stream_hard_silence_config_v1();
    let immature = sample_source_stream_expected_state_v1(2);

    assert_eq!(
        evaluate_source_stream_hard_silence_candidate_v1(&subject, Some(&immature), None, &cfg)
            .unwrap(),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::NotMature {
            mature_windows_total: 2,
            min_mature_windows: 3,
        })
    );

    let mut wrong_subject = sample_source_stream_expected_state_v1(12);
    wrong_subject.subject_kind_u8 = SILENCE_SUBJECT_KIND_DEVICE_V1;
    assert_eq!(
        evaluate_source_stream_hard_silence_candidate_v1(
            &subject,
            Some(&wrong_subject),
            None,
            &cfg
        )
        .unwrap(),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::InvalidSubjectKind(
            SILENCE_SUBJECT_KIND_DEVICE_V1
        ))
    );
}

#[test]
fn source_stream_sharp_drop_evaluator_emits_reduced_nonzero_candidate_v1() {
    let stats = sample_source_stream_stats_for_drop_v1(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_source_stream_stats_v1(&stats).unwrap();
    let current = sample_source_stream_current_window_v1(20);
    let cfg = sample_source_stream_sharp_drop_config_v1();
    let eval = evaluate_source_stream_sharp_drop_candidate_v1(&current, &expected, &cfg).unwrap();

    let candidate = match eval {
        SharpDropEvaluationV1::Candidate(candidate) => candidate,
        other => panic!(
            "expected source-stream sharp-drop candidate, got {:?}",
            other
        ),
    };

    assert_eq!(
        candidate.subject_kind_u8,
        SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
    );
    assert_eq!(
        candidate.subject_key,
        current.subject.source_stream_id.clone()
    );
    assert_eq!(candidate.tenant_id, "tenant-a");
    assert_source_close_f64_v1(candidate.expected_lines_f64, 100.0);
    assert_eq!(candidate.observed_lines_u64, 20);
    assert_source_close_f32_v1(candidate.observed_expected_ratio_f32, 0.2);
    assert_source_close_f32_v1(candidate.drop_ratio_f32, 0.8);
    assert_source_close_f64_v1(candidate.absolute_drop_lines_f64, 80.0);
    assert_source_close_f32_v1(candidate.line_stddevs_below_mean_f32.unwrap(), 8.0);
    assert_eq!(candidate.maturity_count_u32, 12);
    assert_eq!(candidate.bucket_u8, 8);
    assert_eq!(
        candidate.reason_details[0],
        ("drop_kind".to_string(), "sharp_drop".to_string())
    );
    assert_eq!(
        candidate.reason_details[1],
        ("subject_kind".to_string(), "source_stream".to_string())
    );
    assert_eq!(
        candidate.reason_details[2],
        ("tenant_id".to_string(), "tenant-a".to_string())
    );
    assert_eq!(
        candidate.reason_details[3],
        ("device_key".to_string(), "device-key-001".to_string())
    );
    assert_eq!(
        candidate.reason_details[4],
        (
            "source_stream_id".to_string(),
            candidate.subject_key.clone()
        )
    );
    assert_eq!(
        candidate.reason_details[5],
        ("source_path".to_string(), "var/log/auth.log".to_string())
    );
}

#[test]
fn source_stream_sharp_drop_evaluator_preserves_suppression_rules_v1() {
    let stats = sample_source_stream_stats_for_drop_v1(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_source_stream_stats_v1(&stats).unwrap();
    let cfg = sample_source_stream_sharp_drop_config_v1();

    assert_eq!(
        evaluate_source_stream_sharp_drop_candidate_v1(
            &sample_source_stream_current_window_v1(0),
            &expected,
            &cfg
        )
        .unwrap(),
        SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::HardSilencePriority)
    );

    let low_expected = sharp_drop_expected_volume_from_source_stream_stats_v1(
        &sample_source_stream_stats_for_drop_v1(20.0, 10.0, 12),
    )
    .unwrap();
    assert_eq!(
        evaluate_source_stream_sharp_drop_candidate_v1(
            &sample_source_stream_current_window_v1(5),
            &low_expected,
            &cfg
        )
        .unwrap(),
        SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::BelowExpectedLineFloor {
            expected_lines: 20.0,
            min_expected_lines: 25.0,
        })
    );
}

fn sample_source_stream_vdrop_candidate_for_state_v1() -> VDropCandidateV1 {
    let subject = sample_source_stream_subject_v1();
    VDropCandidateV1 {
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        subject_key: subject.source_stream_id,
        tenant_id: subject.tenant_id,
        window_start_ts_i64: 1_700_000_060,
        window_end_ts_i64: 1_700_000_240,
        last_seen_ts_i64: 1_700_000_060,
        expected_windows_missed_u64: 3,
        expected_lines_u64: 90,
        observed_lines_u64: 0,
        drop_ratio_f32: 1.0,
        bucket_u8: 8,
        reason_details: Vec::new(),
    }
}

fn sample_source_stream_sharp_drop_candidate_for_state_v1() -> SharpDropCandidateV1 {
    let subject = sample_source_stream_subject_v1();
    SharpDropCandidateV1 {
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        subject_key: subject.source_stream_id,
        tenant_id: subject.tenant_id,
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
        reason_details: Vec::new(),
    }
}

#[test]
fn source_stream_open_state_helpers_create_and_suppress_matching_subject_v1() {
    let subject = sample_source_stream_subject_v1();
    let hard_candidate = sample_source_stream_vdrop_candidate_for_state_v1();
    let hard_state = source_stream_open_silence_state_from_candidate_v1(
        &subject,
        &hard_candidate,
        "0123456789abcdef0123456789abcdef",
    )
    .unwrap();
    assert_eq!(hard_state.schema_version_u16, SILENCE_SCHEMA_VERSION_V1);
    assert_eq!(
        hard_state.subject_kind_u8,
        SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
    );
    assert_eq!(hard_state.state_flags_u8, OPEN_SILENCE_FLAG_OPEN_V1);
    assert!(source_stream_open_silence_state_suppresses_candidate_v1(
        &subject,
        &hard_candidate,
        Some(&hard_state),
    )
    .unwrap());

    let sharp_candidate = sample_source_stream_sharp_drop_candidate_for_state_v1();
    let drop_state = source_stream_open_drop_state_from_candidate_v1(
        &subject,
        &sharp_candidate,
        "fedcba9876543210fedcba9876543210",
    )
    .unwrap();
    assert_eq!(drop_state.schema_version_u16, SILENCE_SCHEMA_VERSION_V1);
    assert_eq!(
        drop_state.subject_kind_u8,
        SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
    );
    assert_eq!(drop_state.state_flags_u8, OPEN_DROP_FLAG_OPEN_V1);
    assert!(source_stream_open_drop_state_suppresses_candidate_v1(
        &subject,
        &sharp_candidate,
        Some(&drop_state),
    )
    .unwrap());
}

#[test]
fn source_stream_open_state_helpers_reject_mismatched_subject_v1() {
    let subject = sample_source_stream_subject_v1();
    let mut wrong_candidate = sample_source_stream_vdrop_candidate_for_state_v1();
    wrong_candidate.subject_key = "fedcba9876543210fedcba9876543210".to_string();
    assert!(source_stream_open_silence_state_from_candidate_v1(
        &subject,
        &wrong_candidate,
        "0123456789abcdef0123456789abcdef",
    )
    .is_err());

    let mut wrong_drop = sample_source_stream_sharp_drop_candidate_for_state_v1();
    wrong_drop.subject_kind_u8 = SILENCE_SUBJECT_KIND_DEVICE_V1;
    assert!(source_stream_open_drop_state_from_candidate_v1(
        &subject,
        &wrong_drop,
        "0123456789abcdef0123456789abcdef",
    )
    .is_err());
}
