// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use sparx::db::silence::*;

fn sample_expected_state(subject_kind: u8) -> ExpectedSourceStateV1 {
    ExpectedSourceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: subject_kind,
        state_flags_u8: 0,
        window_size_s_u32: 60,
        observed_windows_total_u64: 12,
        mature_windows_total_u64: 9,
        last_seen_window_start_ts_i64: 1_700_000_000,
        last_seen_window_end_ts_i64: 1_700_000_060,
        last_observed_lines_u64: 44,
        last_observed_bytes_u64: 4096,
        last_bucket_u8: 17,
        reserved_u8_0: 0,
        reserved_u16_0: 0,
        last_update_ts_i64: 1_700_000_061,
    }
}

#[test]
fn expected_source_state_roundtrips_fixed_68_byte_encoding() {
    let state = sample_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    let encoded = encode_expected_source_state_v1(&state);

    assert_eq!(encoded.len(), EXPECTED_SOURCE_STATE_V1_LEN);
    assert_eq!(&encoded[0..2], &1u16.to_le_bytes());
    assert_eq!(encoded[2], SILENCE_SUBJECT_KIND_DEVICE_V1);
    assert_eq!(encoded[3], 0);
    assert_eq!(&encoded[4..8], &60u32.to_le_bytes());
    assert_eq!(&encoded[8..16], &12u64.to_le_bytes());
    assert_eq!(&encoded[16..24], &9u64.to_le_bytes());
    assert_eq!(&encoded[24..32], &1_700_000_000i64.to_le_bytes());
    assert_eq!(&encoded[32..40], &1_700_000_060i64.to_le_bytes());
    assert_eq!(&encoded[40..48], &44u64.to_le_bytes());
    assert_eq!(&encoded[48..56], &4096u64.to_le_bytes());
    assert_eq!(encoded[56], 17);
    assert_eq!(encoded[57], 0);
    assert_eq!(&encoded[58..60], &0u16.to_le_bytes());
    assert_eq!(&encoded[60..68], &1_700_000_061i64.to_le_bytes());

    assert_eq!(decode_expected_source_state_v1(&encoded).unwrap(), state);
}

#[test]
fn expected_source_state_allows_tenant_subject_kind() {
    let state = sample_expected_state(SILENCE_SUBJECT_KIND_TENANT_V1);
    let encoded = encode_expected_source_state_v1(&state);
    assert_eq!(decode_expected_source_state_v1(&encoded).unwrap(), state);
}

#[test]
fn expected_source_state_allows_source_stream_subject_kind() {
    let state = sample_expected_state(SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1);
    let encoded = encode_expected_source_state_v1(&state);
    assert_eq!(decode_expected_source_state_v1(&encoded).unwrap(), state);
}

#[test]
fn expected_source_state_rejects_invalid_contract_fields() {
    let mut encoded =
        encode_expected_source_state_v1(&sample_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1));
    encoded[0..2].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_expected_source_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::UnknownSchemaVersion(2)
    );

    let mut encoded =
        encode_expected_source_state_v1(&sample_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1));
    encoded[2] = 9;
    assert_eq!(
        decode_expected_source_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::InvalidSubjectKind(9)
    );

    let mut encoded =
        encode_expected_source_state_v1(&sample_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1));
    encoded[57] = 1;
    assert_eq!(
        decode_expected_source_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::InvalidReservedField {
            field: "reserved_u8_0",
            value: 1,
        }
    );

    let mut encoded =
        encode_expected_source_state_v1(&sample_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1));
    encoded[58..60].copy_from_slice(&1u16.to_le_bytes());
    assert_eq!(
        decode_expected_source_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::InvalidReservedField {
            field: "reserved_u16_0",
            value: 1,
        }
    );
}

#[test]
fn expected_source_state_rejects_wrong_length() {
    assert_eq!(
        decode_expected_source_state_v1(&[0; EXPECTED_SOURCE_STATE_V1_LEN - 1]).unwrap_err(),
        SilenceStateErrorV1::InvalidLength {
            expected: EXPECTED_SOURCE_STATE_V1_LEN,
            actual: EXPECTED_SOURCE_STATE_V1_LEN - 1,
        }
    );
}

#[test]
fn open_silence_state_roundtrips_variable_alert_id_encoding() {
    let state = OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_DEVICE_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_OPEN_V1,
        silence_start_ts_i64: 1_700_000_120,
        last_alert_window_start_ts_i64: 1_700_000_180,
        last_alert_window_end_ts_i64: 1_700_000_240,
        last_alert_id: "0123456789abcdef".to_string(),
    };

    let encoded = encode_open_silence_state_v1(&state);
    assert_eq!(encoded.len(), OPEN_SILENCE_STATE_V1_FIXED_LEN + 16);
    assert_eq!(&encoded[0..2], &1u16.to_le_bytes());
    assert_eq!(encoded[2], SILENCE_SUBJECT_KIND_DEVICE_V1);
    assert_eq!(encoded[3], OPEN_SILENCE_FLAG_OPEN_V1);
    assert_eq!(&encoded[4..12], &1_700_000_120i64.to_le_bytes());
    assert_eq!(&encoded[12..20], &1_700_000_180i64.to_le_bytes());
    assert_eq!(&encoded[20..28], &1_700_000_240i64.to_le_bytes());
    assert_eq!(&encoded[28..30], &16u16.to_le_bytes());
    assert_eq!(&encoded[30..], b"0123456789abcdef");

    assert_eq!(decode_open_silence_state_v1(&encoded).unwrap(), state);
}

#[test]
fn open_silence_state_allows_closed_tenant_subject_without_alert_id() {
    let state = OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_TENANT_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_CLOSED_V1,
        silence_start_ts_i64: 1_700_000_120,
        last_alert_window_start_ts_i64: 0,
        last_alert_window_end_ts_i64: 0,
        last_alert_id: String::new(),
    };

    let encoded = encode_open_silence_state_v1(&state);
    assert_eq!(encoded.len(), OPEN_SILENCE_STATE_V1_FIXED_LEN);
    assert_eq!(decode_open_silence_state_v1(&encoded).unwrap(), state);
}

#[test]
fn open_silence_state_rejects_invalid_contract_fields() {
    let state = OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_DEVICE_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_OPEN_V1,
        silence_start_ts_i64: 1,
        last_alert_window_start_ts_i64: 2,
        last_alert_window_end_ts_i64: 3,
        last_alert_id: "abc123".to_string(),
    };

    let mut encoded = encode_open_silence_state_v1(&state);
    encoded[0..2].copy_from_slice(&3u16.to_le_bytes());
    assert_eq!(
        decode_open_silence_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::UnknownSchemaVersion(3)
    );

    let mut encoded = encode_open_silence_state_v1(&state);
    encoded[2] = 7;
    assert_eq!(
        decode_open_silence_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::InvalidSubjectKind(7)
    );

    let mut encoded = encode_open_silence_state_v1(&state);
    encoded[30] = b'G';
    assert_eq!(
        decode_open_silence_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::InvalidAlertIdByte(b'G')
    );
}

#[test]
fn open_silence_state_rejects_length_mismatch() {
    assert_eq!(
        decode_open_silence_state_v1(&[0; OPEN_SILENCE_STATE_V1_FIXED_LEN - 1]).unwrap_err(),
        SilenceStateErrorV1::MinimumLength {
            minimum: OPEN_SILENCE_STATE_V1_FIXED_LEN,
            actual: OPEN_SILENCE_STATE_V1_FIXED_LEN - 1,
        }
    );

    let mut encoded = vec![0u8; OPEN_SILENCE_STATE_V1_FIXED_LEN];
    encoded[0..2].copy_from_slice(&1u16.to_le_bytes());
    encoded[2] = SILENCE_SUBJECT_KIND_DEVICE_V1;
    encoded[28..30].copy_from_slice(&1u16.to_le_bytes());
    assert_eq!(
        decode_open_silence_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::InvalidAlertIdLength {
            declared: 1,
            remaining: 0,
        }
    );

    let mut encoded = encode_open_silence_state_v1(&OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_DEVICE_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_OPEN_V1,
        silence_start_ts_i64: 1,
        last_alert_window_start_ts_i64: 2,
        last_alert_window_end_ts_i64: 3,
        last_alert_id: "abc".to_string(),
    });
    encoded[28..30].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        decode_open_silence_state_v1(&encoded).unwrap_err(),
        SilenceStateErrorV1::TrailingBytes { remaining: 1 }
    );
}

fn sample_expected_update(
    subject_kind: u8,
    window_start_ts: i64,
    window_end_ts: i64,
    lines: u64,
) -> ExpectedSourceStateUpdateV1 {
    ExpectedSourceStateUpdateV1 {
        subject_kind_u8: subject_kind,
        window_size_s_u32: 60,
        window_start_ts_i64: window_start_ts,
        window_end_ts_i64: window_end_ts,
        observed_lines_u64: lines,
        observed_bytes_u64: lines.saturating_mul(100),
        bucket_u8: 7,
        update_ts_i64: window_end_ts,
        min_lines_per_window_u32: 10,
    }
}

#[test]
fn expected_source_update_initializes_and_tracks_maturity_floor() {
    let cold = sample_expected_update(
        SILENCE_SUBJECT_KIND_DEVICE_V1,
        1_700_000_000,
        1_700_000_060,
        4,
    );
    let state = update_expected_source_state_from_window_v1(None, &cold).unwrap();

    assert_eq!(state.schema_version_u16, SILENCE_SCHEMA_VERSION_V1);
    assert_eq!(state.subject_kind_u8, SILENCE_SUBJECT_KIND_DEVICE_V1);
    assert_eq!(state.window_size_s_u32, 60);
    assert_eq!(state.observed_windows_total_u64, 1);
    assert_eq!(state.mature_windows_total_u64, 0);
    assert_eq!(state.last_seen_window_start_ts_i64, 1_700_000_000);
    assert_eq!(state.last_seen_window_end_ts_i64, 1_700_000_060);
    assert_eq!(state.last_observed_lines_u64, 4);
    assert_eq!(state.last_observed_bytes_u64, 400);
    assert_eq!(state.last_bucket_u8, 7);
    assert_eq!(state.last_update_ts_i64, 1_700_000_060);

    let mature = sample_expected_update(
        SILENCE_SUBJECT_KIND_DEVICE_V1,
        1_700_000_060,
        1_700_000_120,
        10,
    );
    let state = update_expected_source_state_from_window_v1(Some(&state), &mature).unwrap();
    assert_eq!(state.observed_windows_total_u64, 2);
    assert_eq!(state.mature_windows_total_u64, 1);
    assert_eq!(state.last_seen_window_start_ts_i64, 1_700_000_060);
    assert_eq!(state.last_seen_window_end_ts_i64, 1_700_000_120);
    assert_eq!(state.last_observed_lines_u64, 10);
    assert_eq!(state.last_observed_bytes_u64, 1_000);
}

#[test]
fn expected_source_update_does_not_regress_last_seen_on_older_replay() {
    let first = sample_expected_update(
        SILENCE_SUBJECT_KIND_TENANT_V1,
        1_700_000_060,
        1_700_000_120,
        20,
    );
    let state = update_expected_source_state_from_window_v1(None, &first).unwrap();
    let older = sample_expected_update(
        SILENCE_SUBJECT_KIND_TENANT_V1,
        1_700_000_000,
        1_700_000_060,
        30,
    );
    let state = update_expected_source_state_from_window_v1(Some(&state), &older).unwrap();

    assert_eq!(state.observed_windows_total_u64, 2);
    assert_eq!(state.mature_windows_total_u64, 2);
    assert_eq!(state.last_seen_window_start_ts_i64, 1_700_000_060);
    assert_eq!(state.last_seen_window_end_ts_i64, 1_700_000_120);
    assert_eq!(state.last_observed_lines_u64, 20);
    assert_eq!(state.last_update_ts_i64, 1_700_000_120);
}

#[test]
fn expected_source_update_rejects_invalid_window_inputs() {
    let mut update = sample_expected_update(SILENCE_SUBJECT_KIND_DEVICE_V1, 10, 70, 1);
    update.window_size_s_u32 = 0;
    assert_eq!(
        update_expected_source_state_from_window_v1(None, &update).unwrap_err(),
        SilenceStateErrorV1::InvalidWindowSize { value: 0 }
    );

    let update = sample_expected_update(SILENCE_SUBJECT_KIND_DEVICE_V1, 70, 70, 1);
    assert_eq!(
        update_expected_source_state_from_window_v1(None, &update).unwrap_err(),
        SilenceStateErrorV1::InvalidWindowBounds {
            window_start_ts: 70,
            window_end_ts: 70,
        }
    );
}

fn mature_expected_state(subject_kind: u8) -> ExpectedSourceStateV1 {
    let mut state = sample_expected_state(subject_kind);
    state.observed_windows_total_u64 = 20;
    state.mature_windows_total_u64 = 12;
    state.last_seen_window_start_ts_i64 = 1_700_000_000;
    state.last_seen_window_end_ts_i64 = 1_700_000_060;
    state.last_observed_lines_u64 = 30;
    state.last_observed_bytes_u64 = 3_000;
    state.last_bucket_u8 = 8;
    state
}

fn sample_vdrop_config(eval_ts_i64: i64) -> VDropEvaluationConfigV1 {
    VDropEvaluationConfigV1 {
        eval_ts_i64,
        min_mature_windows_u64: 3,
        min_expected_windows_missed_u64: 2,
        min_expected_lines_u64: 10,
    }
}

#[test]
fn vdrop_candidate_evaluator_emits_device_candidate_after_missed_windows() {
    let state = mature_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    let cfg = sample_vdrop_config(1_700_000_240);
    let eval = evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), None, &cfg);

    let candidate = match eval {
        VDropEvaluationV1::Candidate(candidate) => candidate,
        other => panic!("expected candidate, got {:?}", other),
    };

    assert_eq!(candidate.subject_kind_u8, SILENCE_SUBJECT_KIND_DEVICE_V1);
    assert_eq!(candidate.subject_key, "device-a");
    assert_eq!(candidate.tenant_id, "tenant-a");
    assert_eq!(candidate.window_start_ts_i64, 1_700_000_060);
    assert_eq!(candidate.window_end_ts_i64, 1_700_000_240);
    assert_eq!(candidate.last_seen_ts_i64, 1_700_000_060);
    assert_eq!(candidate.expected_windows_missed_u64, 3);
    assert_eq!(candidate.expected_lines_u64, 90);
    assert_eq!(candidate.observed_lines_u64, 0);
    assert_eq!(candidate.drop_ratio_f32, 1.0);
    assert_eq!(candidate.bucket_u8, 8);
    assert_eq!(
        candidate.reason_details,
        vec![
            ("subject_kind".to_string(), "device".to_string()),
            ("tenant_id".to_string(), "tenant-a".to_string()),
            ("device_key".to_string(), "device-a".to_string()),
            ("window_start_ts".to_string(), "1700000060".to_string()),
            ("window_end_ts".to_string(), "1700000240".to_string()),
            ("last_seen_ts".to_string(), "1700000060".to_string()),
            ("expected_windows_missed".to_string(), "3".to_string()),
            ("expected_lines".to_string(), "90".to_string()),
            ("observed_lines".to_string(), "0".to_string()),
            ("drop_ratio".to_string(), "1.000000".to_string()),
            ("bucket".to_string(), "8".to_string()),
        ]
    );
}

#[test]
fn vdrop_candidate_evaluator_emits_tenant_candidate_without_device_detail() {
    let state = mature_expected_state(SILENCE_SUBJECT_KIND_TENANT_V1);
    let cfg = sample_vdrop_config(1_700_000_180);
    let eval = evaluate_vdrop_candidate_v1("tenant-a", "tenant-a", Some(&state), None, &cfg);

    let candidate = match eval {
        VDropEvaluationV1::Candidate(candidate) => candidate,
        other => panic!("expected candidate, got {:?}", other),
    };

    assert_eq!(candidate.subject_kind_u8, SILENCE_SUBJECT_KIND_TENANT_V1);
    assert_eq!(candidate.subject_key, "tenant-a");
    assert_eq!(candidate.expected_windows_missed_u64, 2);
    assert!(!candidate
        .reason_details
        .iter()
        .any(|(key, _)| key == "device_key"));
    assert_eq!(
        candidate.reason_details[0],
        ("subject_kind".to_string(), "tenant".to_string())
    );
}

#[test]
fn vdrop_candidate_evaluator_suppresses_cold_or_not_silent_subjects() {
    let mut state = mature_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    state.mature_windows_total_u64 = 2;
    let cfg = sample_vdrop_config(1_700_000_240);
    assert_eq!(
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), None, &cfg),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::NotMature {
            mature_windows_total: 2,
            min_mature_windows: 3,
        })
    );

    let state = mature_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    let cfg = sample_vdrop_config(1_700_000_060);
    assert_eq!(
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), None, &cfg),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::NotSilent {
            eval_ts: 1_700_000_060,
            last_seen_ts: 1_700_000_060,
        })
    );

    let cfg = sample_vdrop_config(1_700_000_119);
    assert_eq!(
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), None, &cfg),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::NotEnoughMissedWindows {
            expected_windows_missed: 0,
            min_expected_windows_missed: 2,
        })
    );
}

#[test]
fn vdrop_candidate_evaluator_fails_closed_on_invalid_state_or_open_silence() {
    let cfg = sample_vdrop_config(1_700_000_240);
    assert_eq!(
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", None, None, &cfg),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::MissingExpectedSourceState)
    );

    let mut state = mature_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    state.last_bucket_u8 = 99;
    assert_eq!(
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), None, &cfg),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::InvalidBucket { value: 99 })
    );

    let mut state = mature_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    state.mature_windows_total_u64 = 21;
    assert_eq!(
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), None, &cfg),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::CounterInversion {
            observed_windows_total: 20,
            mature_windows_total: 21,
        })
    );

    let state = mature_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    let open = OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_DEVICE_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_OPEN_V1,
        silence_start_ts_i64: 1_700_000_060,
        last_alert_window_start_ts_i64: 1_700_000_120,
        last_alert_window_end_ts_i64: 1_700_000_180,
        last_alert_id: "0123456789abcdef".to_string(),
    };
    assert_eq!(
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), Some(&open), &cfg),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::OpenSilenceAlreadyExists)
    );
}

#[test]
fn vdrop_candidate_evaluator_requires_expected_activity_floor() {
    let mut state = mature_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    state.last_observed_lines_u64 = 3;
    let cfg = sample_vdrop_config(1_700_000_240);
    assert_eq!(
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), None, &cfg),
        VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::BelowExpectedLineFloor {
            expected_lines: 9,
            min_expected_lines: 10,
        })
    );
}

#[test]
fn vdrop_candidate_evaluator_does_not_suppress_closed_open_silence_state() {
    let state = mature_expected_state(SILENCE_SUBJECT_KIND_DEVICE_V1);
    let closed = OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_DEVICE_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_CLOSED_V1,
        silence_start_ts_i64: 1_700_000_060,
        last_alert_window_start_ts_i64: 1_700_000_120,
        last_alert_window_end_ts_i64: 1_700_000_180,
        last_alert_id: "0123456789abcdef".to_string(),
    };
    let cfg = sample_vdrop_config(1_700_000_240);

    let eval =
        evaluate_vdrop_candidate_v1("tenant-a", "device-a", Some(&state), Some(&closed), &cfg);
    let candidate = match eval {
        VDropEvaluationV1::Candidate(candidate) => candidate,
        other => panic!(
            "expected closed silence state to allow new candidate, got {:?}",
            other
        ),
    };

    assert_eq!(candidate.subject_kind_u8, SILENCE_SUBJECT_KIND_DEVICE_V1);
    assert_eq!(candidate.subject_key, "device-a");
    assert_eq!(candidate.expected_windows_missed_u64, 3);
}

fn sample_device_stats_for_sharp_drop(
    mean_lines: f64,
    line_stddev: f64,
    n: u32,
) -> sparx::db::baseline_sketch::DeviceStatsV1 {
    sparx::db::baseline_sketch::DeviceStatsV1 {
        line_count: sparx::db::baseline_sketch::WelfordF64V1 {
            n,
            mean: mean_lines,
            m2: line_stddev * line_stddev * f64::from(n.saturating_sub(1)),
        },
        byte_count: sparx::db::baseline_sketch::WelfordF64V1 {
            n,
            mean: mean_lines * 100.0,
            m2: line_stddev * line_stddev * 10_000.0 * f64::from(n.saturating_sub(1)),
        },
        score_total: sparx::db::baseline_sketch::WelfordF64V1 {
            n: 0,
            mean: 0.0,
            m2: 0.0,
        },
        last_update_ts: 1_700_000_000,
    }
}

fn sample_sharp_drop_current_window(
    subject_kind: u8,
    observed_lines: u64,
) -> SharpDropCurrentWindowV1 {
    SharpDropCurrentWindowV1 {
        subject_kind_u8: subject_kind,
        subject_key: if subject_kind == SILENCE_SUBJECT_KIND_DEVICE_V1 {
            "device-a".to_string()
        } else {
            "tenant-a".to_string()
        },
        tenant_id: "tenant-a".to_string(),
        window_start_ts_i64: 1_700_000_060,
        window_end_ts_i64: 1_700_000_120,
        observed_lines_u64: observed_lines,
        observed_bytes_u64: observed_lines * 100,
        bucket_u8: 8,
    }
}

fn sample_sharp_drop_config() -> SharpDropEvaluationConfigV1 {
    SharpDropEvaluationConfigV1 {
        min_maturity_count_u64: 3,
        min_expected_lines_f64: 25.0,
        min_absolute_drop_lines_f64: 25.0,
        max_observed_expected_ratio_f32: SHARP_DROP_DEFAULT_MAX_OBSERVED_EXPECTED_RATIO_V1,
        min_drop_ratio_f32: SHARP_DROP_DEFAULT_MIN_DROP_RATIO_V1,
        variance_gate_stddevs_f32: SHARP_DROP_DEFAULT_VARIANCE_GATE_STDDEVS_V1,
    }
}

fn assert_close_f32(actual: f32, expected: f32) {
    assert!(
        (actual - expected).abs() < 0.000_001,
        "actual={actual} expected={expected}"
    );
}

fn assert_close_f64(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 0.000_001,
        "actual={actual} expected={expected}"
    );
}

#[test]
fn sharp_drop_expected_volume_uses_device_stats_line_baseline() {
    let stats = sample_device_stats_for_sharp_drop(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);

    assert_eq!(expected.maturity_count_u32, 12);
    assert_close_f64(expected.expected_lines_f64, 100.0);
    assert_close_f64(expected.expected_bytes_f64, 10_000.0);
    assert_close_f64(expected.line_stddev_f64, 10.0);
}

#[test]
fn sharp_drop_evaluator_emits_device_candidate_for_reduced_nonzero_activity() {
    let stats = sample_device_stats_for_sharp_drop(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);
    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_DEVICE_V1, 20);
    let cfg = sample_sharp_drop_config();
    let eval = evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg);

    let candidate = match eval {
        SharpDropEvaluationV1::Candidate(candidate) => candidate,
        other => panic!("expected sharp-drop candidate, got {:?}", other),
    };

    assert_eq!(candidate.subject_kind_u8, SILENCE_SUBJECT_KIND_DEVICE_V1);
    assert_eq!(candidate.subject_key, "device-a");
    assert_eq!(candidate.tenant_id, "tenant-a");
    assert_eq!(candidate.window_start_ts_i64, 1_700_000_060);
    assert_eq!(candidate.window_end_ts_i64, 1_700_000_120);
    assert_close_f64(candidate.expected_lines_f64, 100.0);
    assert_eq!(candidate.observed_lines_u64, 20);
    assert_close_f32(candidate.observed_expected_ratio_f32, 0.2);
    assert_close_f32(candidate.drop_ratio_f32, 0.8);
    assert_close_f64(candidate.absolute_drop_lines_f64, 80.0);
    assert_close_f32(candidate.line_stddevs_below_mean_f32.unwrap(), 8.0);
    assert_eq!(candidate.maturity_count_u32, 12);
    assert_eq!(candidate.bucket_u8, 8);
    assert_eq!(
        candidate.reason_details,
        vec![
            ("drop_kind".to_string(), "sharp_drop".to_string()),
            ("subject_kind".to_string(), "device".to_string()),
            ("tenant_id".to_string(), "tenant-a".to_string()),
            ("device_key".to_string(), "device-a".to_string()),
            ("window_start_ts".to_string(), "1700000060".to_string()),
            ("window_end_ts".to_string(), "1700000120".to_string()),
            ("bucket".to_string(), "8".to_string()),
            ("expected_lines".to_string(), "100.000000".to_string()),
            ("observed_lines".to_string(), "20".to_string()),
            (
                "observed_expected_ratio".to_string(),
                "0.200000".to_string()
            ),
            ("drop_ratio".to_string(), "0.800000".to_string()),
            ("baseline_n".to_string(), "12".to_string()),
            ("baseline_mean_lines".to_string(), "100.000000".to_string()),
            ("baseline_stddev_lines".to_string(), "10.000000".to_string()),
            ("z_drop".to_string(), "8.000000".to_string()),
            (
                "max_observed_expected_ratio".to_string(),
                "0.250000".to_string()
            ),
            ("min_drop_ratio".to_string(), "0.750000".to_string()),
            (
                "min_absolute_drop_lines".to_string(),
                "25.000000".to_string()
            ),
            ("expected_bytes".to_string(), "10000.000000".to_string()),
            ("observed_bytes".to_string(), "2000".to_string()),
            ("absolute_drop_lines".to_string(), "80.000000".to_string()),
        ]
    );
}

#[test]
fn sharp_drop_evaluator_suppresses_zero_observed_for_hard_silence_priority() {
    let stats = sample_device_stats_for_sharp_drop(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);
    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_DEVICE_V1, 0);
    let cfg = sample_sharp_drop_config();

    assert_eq!(
        evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg),
        SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::HardSilencePriority)
    );
}

#[test]
fn sharp_drop_evaluator_suppresses_immature_or_low_expected_baseline() {
    let stats = sample_device_stats_for_sharp_drop(100.0, 10.0, 2);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);
    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_DEVICE_V1, 20);
    let cfg = sample_sharp_drop_config();
    assert_eq!(
        evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg),
        SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::NotMature {
            maturity_count: 2,
            min_maturity_count: 3,
        })
    );

    let stats = sample_device_stats_for_sharp_drop(20.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);
    assert_eq!(
        evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg),
        SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::BelowExpectedLineFloor {
            expected_lines: 20.0,
            min_expected_lines: 25.0,
        })
    );
}

#[test]
fn sharp_drop_evaluator_suppresses_non_severe_ratio_or_variance_distance() {
    let stats = sample_device_stats_for_sharp_drop(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);
    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_DEVICE_V1, 30);
    let cfg = sample_sharp_drop_config();
    let eval = evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg);
    match eval {
        SharpDropEvaluationV1::Suppressed(
            SharpDropSuppressionReasonV1::ObservedRatioAboveThreshold {
                observed_expected_ratio,
                max_observed_expected_ratio,
            },
        ) => {
            assert_close_f32(observed_expected_ratio, 0.3);
            assert_close_f32(max_observed_expected_ratio, 0.25);
        }
        other => panic!("expected observed ratio suppression, got {:?}", other),
    }

    let stats = sample_device_stats_for_sharp_drop(100.0, 50.0, 12);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);
    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_DEVICE_V1, 20);
    let eval = evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg);
    match eval {
        SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::VarianceGateNotMet {
            line_stddevs_below_mean,
            min_line_stddevs_below_mean,
        }) => {
            assert_close_f32(line_stddevs_below_mean, 1.6);
            assert_close_f32(min_line_stddevs_below_mean, 3.0);
        }
        other => panic!("expected variance gate suppression, got {:?}", other),
    }
}

#[test]
fn sharp_drop_evaluator_suppresses_small_absolute_drop_before_ratio_gate() {
    let stats = sample_device_stats_for_sharp_drop(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);
    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_DEVICE_V1, 80);
    let mut cfg = sample_sharp_drop_config();
    cfg.max_observed_expected_ratio_f32 = 1.0;
    cfg.min_drop_ratio_f32 = 0.0;

    let eval = evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg);
    match eval {
        SharpDropEvaluationV1::Suppressed(
            SharpDropSuppressionReasonV1::AbsoluteDropBelowFloor {
                absolute_drop_lines,
                min_absolute_drop_lines,
            },
        ) => {
            assert_close_f64(absolute_drop_lines, 20.0);
            assert_close_f64(min_absolute_drop_lines, 25.0);
        }
        other => panic!("expected absolute drop floor suppression, got {:?}", other),
    }
}

#[test]
fn sharp_drop_evaluator_rejects_invalid_expected_volume() {
    let expected = SharpDropExpectedVolumeV1 {
        maturity_count_u32: 12,
        expected_lines_f64: f64::NAN,
        expected_bytes_f64: 10_000.0,
        line_stddev_f64: 10.0,
    };
    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_DEVICE_V1, 20);
    let cfg = sample_sharp_drop_config();

    assert_eq!(
        evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg),
        SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidExpectedVolume {
            field: "expected_lines_f64",
        })
    );
}

#[test]
fn sharp_drop_tenant_expected_volume_sums_mature_device_baselines() {
    let device_a = sharp_drop_expected_volume_from_device_stats_v1(
        &sample_device_stats_for_sharp_drop(100.0, 10.0, 12),
    );
    let device_b = sharp_drop_expected_volume_from_device_stats_v1(
        &sample_device_stats_for_sharp_drop(80.0, 6.0, 8),
    );
    let expected = sum_sharp_drop_expected_volumes_v1(&[device_a, device_b]);

    assert_eq!(expected.maturity_count_u32, 2);
    assert_close_f64(expected.expected_lines_f64, 180.0);
    assert_close_f64(expected.expected_bytes_f64, 18_000.0);
    assert_close_f64(expected.line_stddev_f64, (136.0f64).sqrt());

    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_TENANT_V1, 30);
    let mut cfg = sample_sharp_drop_config();
    cfg.min_maturity_count_u64 = SHARP_DROP_DEFAULT_TENANT_MATURE_DEVICE_FLOOR_V1;
    let eval = evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg);
    let candidate = match eval {
        SharpDropEvaluationV1::Candidate(candidate) => candidate,
        other => panic!("expected tenant sharp-drop candidate, got {:?}", other),
    };

    assert_eq!(candidate.subject_kind_u8, SILENCE_SUBJECT_KIND_TENANT_V1);
    assert_eq!(candidate.subject_key, "tenant-a");
    assert_eq!(candidate.maturity_count_u32, 2);
    assert!(!candidate
        .reason_details
        .iter()
        .any(|(key, _)| key == "device_key"));
}

fn sample_sharp_drop_candidate_for_state() -> SharpDropCandidateV1 {
    let stats = sample_device_stats_for_sharp_drop(100.0, 10.0, 12);
    let expected = sharp_drop_expected_volume_from_device_stats_v1(&stats);
    let current = sample_sharp_drop_current_window(SILENCE_SUBJECT_KIND_DEVICE_V1, 20);
    let cfg = sample_sharp_drop_config();
    match evaluate_sharp_drop_candidate_v1(&current, &expected, &cfg) {
        SharpDropEvaluationV1::Candidate(candidate) => candidate,
        other => panic!("expected sharp-drop candidate, got {:?}", other),
    }
}

#[test]
fn open_drop_state_roundtrips_fixed_header_and_alert_id() {
    let candidate = sample_sharp_drop_candidate_for_state();
    let state = open_drop_state_from_candidate_v1(&candidate, "0123456789abcdef0123456789abcdef");
    let encoded = encode_open_drop_state_v1(&state);

    assert_eq!(encoded.len(), OPEN_DROP_STATE_V1_FIXED_LEN + 32);
    assert_eq!(&encoded[0..2], &1u16.to_le_bytes());
    assert_eq!(encoded[2], SILENCE_SUBJECT_KIND_DEVICE_V1);
    assert_eq!(encoded[3], OPEN_DROP_FLAG_OPEN_V1);
    assert_eq!(&encoded[4..12], &1_700_000_060i64.to_le_bytes());
    assert_eq!(&encoded[12..20], &1_700_000_060i64.to_le_bytes());
    assert_eq!(&encoded[20..28], &1_700_000_120i64.to_le_bytes());
    assert_eq!(&encoded[28..30], &32u16.to_le_bytes());
    assert_eq!(decode_open_drop_state_v1(&encoded).unwrap(), state);
}

#[test]
fn open_drop_state_rejects_malformed_payloads() {
    let candidate = sample_sharp_drop_candidate_for_state();
    let state = open_drop_state_from_candidate_v1(&candidate, "0123456789abcdef0123456789abcdef");
    let mut encoded = encode_open_drop_state_v1(&state);

    assert_eq!(
        decode_open_drop_state_v1(&encoded[..OPEN_DROP_STATE_V1_FIXED_LEN - 1]),
        Err(SilenceStateErrorV1::MinimumLength {
            minimum: OPEN_DROP_STATE_V1_FIXED_LEN,
            actual: OPEN_DROP_STATE_V1_FIXED_LEN - 1,
        })
    );

    encoded[28..30].copy_from_slice(&31u16.to_le_bytes());
    assert_eq!(
        decode_open_drop_state_v1(&encoded),
        Err(SilenceStateErrorV1::TrailingBytes { remaining: 1 })
    );

    let mut encoded = encode_open_drop_state_v1(&state);
    encoded[OPEN_DROP_STATE_V1_FIXED_LEN] = b'G';
    assert_eq!(
        decode_open_drop_state_v1(&encoded),
        Err(SilenceStateErrorV1::InvalidAlertIdByte(b'G'))
    );

    let mut encoded = encode_open_drop_state_v1(&state);
    encoded[2] = 99;
    assert_eq!(
        decode_open_drop_state_v1(&encoded),
        Err(SilenceStateErrorV1::InvalidSubjectKind(99))
    );
}

#[test]
fn open_drop_state_suppresses_only_matching_open_state() {
    let candidate = sample_sharp_drop_candidate_for_state();
    let state = open_drop_state_from_candidate_v1(&candidate, "0123456789abcdef0123456789abcdef");

    assert!(open_drop_state_suppresses_candidate_v1(
        &candidate,
        Some(&state)
    ));
    assert!(!open_drop_state_suppresses_candidate_v1(&candidate, None));

    let recovered = close_open_drop_state_by_recovery_v1(&state);
    assert_eq!(
        recovered.state_flags_u8,
        OPEN_DROP_FLAG_CLOSED_BY_RECOVERY_V1
    );
    assert!(!open_drop_state_suppresses_candidate_v1(
        &candidate,
        Some(&recovered)
    ));

    let superseded = close_open_drop_state_by_hard_silence_v1(&state);
    assert_eq!(
        superseded.state_flags_u8,
        OPEN_DROP_FLAG_CLOSED_BY_HARD_SILENCE_V1
    );
    assert!(!open_drop_state_suppresses_candidate_v1(
        &candidate,
        Some(&superseded)
    ));

    let mut tenant_state = state.clone();
    tenant_state.subject_kind_u8 = SILENCE_SUBJECT_KIND_TENANT_V1;
    assert!(!open_drop_state_suppresses_candidate_v1(
        &candidate,
        Some(&tenant_state)
    ));
}
