// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Expected-source silence state encodings.
// See: contracts/35_expected_source_state_vdrop_plan_v0_1.md
// Covers expected-source state, V_DROP evaluation, alert construction, runtime policy, and bounded diagnostics.
// Includes sharp-drop evaluator primitives.
// Includes sharp-drop open-state and dedup primitives.

use super::baseline_sketch::{DeviceStatsV1, WelfordF64V1};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SilenceStateErrorV1 {
    InvalidLength { expected: usize, actual: usize },
    MinimumLength { minimum: usize, actual: usize },
    UnknownSchemaVersion(u16),
    InvalidSubjectKind(u8),
    InvalidReservedField { field: &'static str, value: u64 },
    TrailingBytes { remaining: usize },
    InvalidAlertIdLength { declared: usize, remaining: usize },
    InvalidAlertIdByte(u8),
    InvalidWindowSize { value: u32 },
    InvalidWindowBounds { window_start_ts: i64, window_end_ts: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedSourceStateUpdateV1 {
    pub subject_kind_u8: u8,
    pub window_size_s_u32: u32,
    pub window_start_ts_i64: i64,
    pub window_end_ts_i64: i64,
    pub observed_lines_u64: u64,
    pub observed_bytes_u64: u64,
    pub bucket_u8: u8,
    pub update_ts_i64: i64,
    pub min_lines_per_window_u32: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedSourceStateV1 {
    pub schema_version_u16: u16,
    pub subject_kind_u8: u8,
    pub state_flags_u8: u8,
    pub window_size_s_u32: u32,
    pub observed_windows_total_u64: u64,
    pub mature_windows_total_u64: u64,
    pub last_seen_window_start_ts_i64: i64,
    pub last_seen_window_end_ts_i64: i64,
    pub last_observed_lines_u64: u64,
    pub last_observed_bytes_u64: u64,
    pub last_bucket_u8: u8,
    pub reserved_u8_0: u8,
    pub reserved_u16_0: u16,
    pub last_update_ts_i64: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenSilenceStateV1 {
    pub schema_version_u16: u16,
    pub subject_kind_u8: u8,
    pub state_flags_u8: u8,
    pub silence_start_ts_i64: i64,
    pub last_alert_window_start_ts_i64: i64,
    pub last_alert_window_end_ts_i64: i64,
    pub last_alert_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenDropStateV1 {
    pub schema_version_u16: u16,
    pub subject_kind_u8: u8,
    pub state_flags_u8: u8,
    pub drop_start_ts_i64: i64,
    pub last_alert_window_start_ts_i64: i64,
    pub last_alert_window_end_ts_i64: i64,
    pub last_alert_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VDropEvaluationConfigV1 {
    pub eval_ts_i64: i64,
    pub min_mature_windows_u64: u64,
    pub min_expected_windows_missed_u64: u64,
    pub min_expected_lines_u64: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VDropCandidateV1 {
    pub subject_kind_u8: u8,
    pub subject_key: String,
    pub tenant_id: String,
    pub window_start_ts_i64: i64,
    pub window_end_ts_i64: i64,
    pub last_seen_ts_i64: i64,
    pub expected_windows_missed_u64: u64,
    pub expected_lines_u64: u64,
    pub observed_lines_u64: u64,
    pub drop_ratio_f32: f32,
    pub bucket_u8: u8,
    pub reason_details: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VDropSuppressionReasonV1 {
    MissingExpectedSourceState,
    InvalidSubjectKind(u8),
    InvalidEvaluationConfig { field: &'static str, value: u64 },
    InvalidWindowSize { value: u32 },
    InvalidWindowBounds { window_start_ts: i64, window_end_ts: i64 },
    InvalidBucket { value: u8 },
    CounterInversion { observed_windows_total: u64, mature_windows_total: u64 },
    NotMature { mature_windows_total: u64, min_mature_windows: u64 },
    NotSilent { eval_ts: i64, last_seen_ts: i64 },
    NotEnoughMissedWindows { expected_windows_missed: u64, min_expected_windows_missed: u64 },
    BelowExpectedLineFloor { expected_lines: u64, min_expected_lines: u64 },
    OpenSilenceAlreadyExists,
}

#[derive(Clone, Debug, PartialEq)]
pub enum VDropEvaluationV1 {
    Candidate(VDropCandidateV1),
    Suppressed(VDropSuppressionReasonV1),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SharpDropExpectedVolumeV1 {
    pub maturity_count_u32: u32,
    pub expected_lines_f64: f64,
    pub expected_bytes_f64: f64,
    pub line_stddev_f64: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharpDropCurrentWindowV1 {
    pub subject_kind_u8: u8,
    pub subject_key: String,
    pub tenant_id: String,
    pub window_start_ts_i64: i64,
    pub window_end_ts_i64: i64,
    pub observed_lines_u64: u64,
    pub observed_bytes_u64: u64,
    pub bucket_u8: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SharpDropEvaluationConfigV1 {
    pub min_maturity_count_u64: u64,
    pub min_expected_lines_f64: f64,
    pub min_absolute_drop_lines_f64: f64,
    pub max_observed_expected_ratio_f32: f32,
    pub min_drop_ratio_f32: f32,
    pub variance_gate_stddevs_f32: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SharpDropCandidateV1 {
    pub subject_kind_u8: u8,
    pub subject_key: String,
    pub tenant_id: String,
    pub window_start_ts_i64: i64,
    pub window_end_ts_i64: i64,
    pub expected_lines_f64: f64,
    pub observed_lines_u64: u64,
    pub expected_bytes_f64: f64,
    pub observed_bytes_u64: u64,
    pub observed_expected_ratio_f32: f32,
    pub drop_ratio_f32: f32,
    pub absolute_drop_lines_f64: f64,
    pub line_stddevs_below_mean_f32: Option<f32>,
    pub maturity_count_u32: u32,
    pub bucket_u8: u8,
    pub reason_details: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SharpDropSuppressionReasonV1 {
    InvalidSubjectKind(u8),
    InvalidWindowBounds { window_start_ts: i64, window_end_ts: i64 },
    InvalidBucket { value: u8 },
    InvalidEvaluationConfig { field: &'static str },
    InvalidExpectedVolume { field: &'static str },
    NotMature { maturity_count: u32, min_maturity_count: u64 },
    BelowExpectedLineFloor { expected_lines: f64, min_expected_lines: f64 },
    HardSilencePriority,
    AbsoluteDropBelowFloor { absolute_drop_lines: f64, min_absolute_drop_lines: f64 },
    ObservedRatioAboveThreshold { observed_expected_ratio: f32, max_observed_expected_ratio: f32 },
    DropRatioBelowThreshold { drop_ratio: f32, min_drop_ratio: f32 },
    VarianceGateNotMet { line_stddevs_below_mean: f32, min_line_stddevs_below_mean: f32 },
}

#[derive(Clone, Debug, PartialEq)]
pub enum SharpDropEvaluationV1 {
    Candidate(SharpDropCandidateV1),
    Suppressed(SharpDropSuppressionReasonV1),
}

pub const SILENCE_SCHEMA_VERSION_V1: u16 = 1;
pub const SILENCE_SUBJECT_KIND_DEVICE_V1: u8 = 1;
pub const SILENCE_SUBJECT_KIND_TENANT_V1: u8 = 2;
pub const SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1: u8 = 3;
pub const OPEN_SILENCE_FLAG_OPEN_V1: u8 = 1;
pub const OPEN_SILENCE_FLAG_CLOSED_V1: u8 = 2;
pub const OPEN_DROP_FLAG_OPEN_V1: u8 = 1;
pub const OPEN_DROP_FLAG_CLOSED_BY_RECOVERY_V1: u8 = 2;
pub const OPEN_DROP_FLAG_CLOSED_BY_HARD_SILENCE_V1: u8 = 4;
pub const EXPECTED_SOURCE_STATE_V1_LEN: usize = 68;
pub const OPEN_SILENCE_STATE_V1_FIXED_LEN: usize = 30;
pub const OPEN_DROP_STATE_V1_FIXED_LEN: usize = 30;
pub const SHARP_DROP_DEFAULT_MAX_OBSERVED_EXPECTED_RATIO_V1: f32 = 0.25;
pub const SHARP_DROP_DEFAULT_MIN_DROP_RATIO_V1: f32 = 0.75;
pub const SHARP_DROP_DEFAULT_VARIANCE_GATE_STDDEVS_V1: f32 = 3.0;
pub const SHARP_DROP_DEFAULT_TENANT_MATURE_DEVICE_FLOOR_V1: u64 = 2;

const SHARP_DROP_EPSILON_F64_V1: f64 = 1.0e-12;

fn require_exact_len(bytes: &[u8], expected: usize) -> Result<(), SilenceStateErrorV1> {
    if bytes.len() != expected {
        return Err(SilenceStateErrorV1::InvalidLength {
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn require_min_len(bytes: &[u8], minimum: usize) -> Result<(), SilenceStateErrorV1> {
    if bytes.len() < minimum {
        return Err(SilenceStateErrorV1::MinimumLength {
            minimum,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn validate_schema_version(version: u16) -> Result<(), SilenceStateErrorV1> {
    if version != SILENCE_SCHEMA_VERSION_V1 {
        return Err(SilenceStateErrorV1::UnknownSchemaVersion(version));
    }
    Ok(())
}

fn validate_subject_kind(kind: u8) -> Result<(), SilenceStateErrorV1> {
    match kind {
        SILENCE_SUBJECT_KIND_DEVICE_V1 | SILENCE_SUBJECT_KIND_TENANT_V1 | SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 => Ok(()),
        other => Err(SilenceStateErrorV1::InvalidSubjectKind(other)),
    }
}

fn validate_lower_hex_ascii(value: &[u8]) -> Result<(), SilenceStateErrorV1> {
    for b in value {
        if !matches!(*b, b'0'..=b'9' | b'a'..=b'f') {
            return Err(SilenceStateErrorV1::InvalidAlertIdByte(*b));
        }
    }
    Ok(())
}

fn encode_u16_le(value: u16, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_u16_le(bytes: &[u8]) -> u16 {
    let mut raw = [0u8; 2];
    raw.copy_from_slice(bytes);
    u16::from_le_bytes(raw)
}

fn encode_u32_le(value: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_u32_le(bytes: &[u8]) -> u32 {
    let mut raw = [0u8; 4];
    raw.copy_from_slice(bytes);
    u32::from_le_bytes(raw)
}

fn encode_u64_le(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_u64_le(bytes: &[u8]) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    u64::from_le_bytes(raw)
}

fn encode_i64_le(value: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_i64_le(bytes: &[u8]) -> i64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    i64::from_le_bytes(raw)
}

fn validate_expected_source_update_v1(update: &ExpectedSourceStateUpdateV1) -> Result<(), SilenceStateErrorV1> {
    validate_subject_kind(update.subject_kind_u8)?;
    if update.window_size_s_u32 == 0 {
        return Err(SilenceStateErrorV1::InvalidWindowSize {
            value: update.window_size_s_u32,
        });
    }
    if update.window_end_ts_i64 <= update.window_start_ts_i64 {
        return Err(SilenceStateErrorV1::InvalidWindowBounds {
            window_start_ts: update.window_start_ts_i64,
            window_end_ts: update.window_end_ts_i64,
        });
    }
    Ok(())
}

pub fn update_expected_source_state_from_window_v1(
    previous: Option<&ExpectedSourceStateV1>,
    update: &ExpectedSourceStateUpdateV1,
) -> Result<ExpectedSourceStateV1, SilenceStateErrorV1> {
    validate_expected_source_update_v1(update)?;

    let window_is_mature = update.min_lines_per_window_u32 == 0
        || update.observed_lines_u64 >= u64::from(update.min_lines_per_window_u32);

    let observed_windows_total_u64 = previous
        .map(|state| state.observed_windows_total_u64.saturating_add(1))
        .unwrap_or(1);
    let mature_windows_total_u64 = previous
        .map(|state| {
            if window_is_mature {
                state.mature_windows_total_u64.saturating_add(1)
            } else {
                state.mature_windows_total_u64
            }
        })
        .unwrap_or(if window_is_mature { 1 } else { 0 });

    let mut out = previous.cloned().unwrap_or(ExpectedSourceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: update.subject_kind_u8,
        state_flags_u8: 0,
        window_size_s_u32: update.window_size_s_u32,
        observed_windows_total_u64: 0,
        mature_windows_total_u64: 0,
        last_seen_window_start_ts_i64: update.window_start_ts_i64,
        last_seen_window_end_ts_i64: update.window_end_ts_i64,
        last_observed_lines_u64: update.observed_lines_u64,
        last_observed_bytes_u64: update.observed_bytes_u64,
        last_bucket_u8: update.bucket_u8,
        reserved_u8_0: 0,
        reserved_u16_0: 0,
        last_update_ts_i64: update.update_ts_i64,
    });

    validate_schema_version(out.schema_version_u16)?;
    if out.subject_kind_u8 != update.subject_kind_u8 {
        return Err(SilenceStateErrorV1::InvalidSubjectKind(out.subject_kind_u8));
    }
    if out.reserved_u8_0 != 0 {
        return Err(SilenceStateErrorV1::InvalidReservedField {
            field: "reserved_u8_0",
            value: u64::from(out.reserved_u8_0),
        });
    }
    if out.reserved_u16_0 != 0 {
        return Err(SilenceStateErrorV1::InvalidReservedField {
            field: "reserved_u16_0",
            value: u64::from(out.reserved_u16_0),
        });
    }

    out.window_size_s_u32 = update.window_size_s_u32;
    out.observed_windows_total_u64 = observed_windows_total_u64;
    out.mature_windows_total_u64 = mature_windows_total_u64;
    if previous
        .map(|state| update.window_end_ts_i64 >= state.last_seen_window_end_ts_i64)
        .unwrap_or(true)
    {
        out.last_seen_window_start_ts_i64 = update.window_start_ts_i64;
        out.last_seen_window_end_ts_i64 = update.window_end_ts_i64;
        out.last_observed_lines_u64 = update.observed_lines_u64;
        out.last_observed_bytes_u64 = update.observed_bytes_u64;
        out.last_bucket_u8 = update.bucket_u8;
    }
    out.last_update_ts_i64 = previous
        .map(|state| state.last_update_ts_i64.max(update.update_ts_i64))
        .unwrap_or(update.update_ts_i64);

    Ok(out)
}

pub fn encode_expected_source_state_v1(value: &ExpectedSourceStateV1) -> Vec<u8> {
    let mut out = Vec::with_capacity(EXPECTED_SOURCE_STATE_V1_LEN);
    encode_u16_le(value.schema_version_u16, &mut out);
    out.push(value.subject_kind_u8);
    out.push(value.state_flags_u8);
    encode_u32_le(value.window_size_s_u32, &mut out);
    encode_u64_le(value.observed_windows_total_u64, &mut out);
    encode_u64_le(value.mature_windows_total_u64, &mut out);
    encode_i64_le(value.last_seen_window_start_ts_i64, &mut out);
    encode_i64_le(value.last_seen_window_end_ts_i64, &mut out);
    encode_u64_le(value.last_observed_lines_u64, &mut out);
    encode_u64_le(value.last_observed_bytes_u64, &mut out);
    out.push(value.last_bucket_u8);
    out.push(value.reserved_u8_0);
    encode_u16_le(value.reserved_u16_0, &mut out);
    encode_i64_le(value.last_update_ts_i64, &mut out);
    out
}

pub fn decode_expected_source_state_v1(
    bytes: &[u8],
) -> Result<ExpectedSourceStateV1, SilenceStateErrorV1> {
    require_exact_len(bytes, EXPECTED_SOURCE_STATE_V1_LEN)?;

    let value = ExpectedSourceStateV1 {
        schema_version_u16: decode_u16_le(&bytes[0..2]),
        subject_kind_u8: bytes[2],
        state_flags_u8: bytes[3],
        window_size_s_u32: decode_u32_le(&bytes[4..8]),
        observed_windows_total_u64: decode_u64_le(&bytes[8..16]),
        mature_windows_total_u64: decode_u64_le(&bytes[16..24]),
        last_seen_window_start_ts_i64: decode_i64_le(&bytes[24..32]),
        last_seen_window_end_ts_i64: decode_i64_le(&bytes[32..40]),
        last_observed_lines_u64: decode_u64_le(&bytes[40..48]),
        last_observed_bytes_u64: decode_u64_le(&bytes[48..56]),
        last_bucket_u8: bytes[56],
        reserved_u8_0: bytes[57],
        reserved_u16_0: decode_u16_le(&bytes[58..60]),
        last_update_ts_i64: decode_i64_le(&bytes[60..68]),
    };

    validate_schema_version(value.schema_version_u16)?;
    validate_subject_kind(value.subject_kind_u8)?;
    if value.reserved_u8_0 != 0 {
        return Err(SilenceStateErrorV1::InvalidReservedField {
            field: "reserved_u8_0",
            value: u64::from(value.reserved_u8_0),
        });
    }
    if value.reserved_u16_0 != 0 {
        return Err(SilenceStateErrorV1::InvalidReservedField {
            field: "reserved_u16_0",
            value: u64::from(value.reserved_u16_0),
        });
    }

    Ok(value)
}

pub fn encode_open_silence_state_v1(value: &OpenSilenceStateV1) -> Vec<u8> {
    let alert_id = value.last_alert_id.as_bytes();
    let alert_id_len = u16::try_from(alert_id.len()).unwrap();

    let mut out = Vec::with_capacity(OPEN_SILENCE_STATE_V1_FIXED_LEN + alert_id.len());
    encode_u16_le(value.schema_version_u16, &mut out);
    out.push(value.subject_kind_u8);
    out.push(value.state_flags_u8);
    encode_i64_le(value.silence_start_ts_i64, &mut out);
    encode_i64_le(value.last_alert_window_start_ts_i64, &mut out);
    encode_i64_le(value.last_alert_window_end_ts_i64, &mut out);
    encode_u16_le(alert_id_len, &mut out);
    out.extend_from_slice(alert_id);
    out
}

pub fn decode_open_silence_state_v1(
    bytes: &[u8],
) -> Result<OpenSilenceStateV1, SilenceStateErrorV1> {
    require_min_len(bytes, OPEN_SILENCE_STATE_V1_FIXED_LEN)?;

    let schema_version_u16 = decode_u16_le(&bytes[0..2]);
    let subject_kind_u8 = bytes[2];
    let state_flags_u8 = bytes[3];
    let silence_start_ts_i64 = decode_i64_le(&bytes[4..12]);
    let last_alert_window_start_ts_i64 = decode_i64_le(&bytes[12..20]);
    let last_alert_window_end_ts_i64 = decode_i64_le(&bytes[20..28]);
    let alert_id_len = usize::from(decode_u16_le(&bytes[28..30]));
    let remaining = bytes.len() - OPEN_SILENCE_STATE_V1_FIXED_LEN;

    if alert_id_len > remaining {
        return Err(SilenceStateErrorV1::InvalidAlertIdLength {
            declared: alert_id_len,
            remaining,
        });
    }
    if alert_id_len < remaining {
        return Err(SilenceStateErrorV1::TrailingBytes {
            remaining: remaining - alert_id_len,
        });
    }

    let alert_id_bytes = &bytes[OPEN_SILENCE_STATE_V1_FIXED_LEN..];
    validate_lower_hex_ascii(alert_id_bytes)?;
    let last_alert_id = std::str::from_utf8(alert_id_bytes)
        .map_err(|_| SilenceStateErrorV1::InvalidAlertIdByte(0xff))?
        .to_string();

    validate_schema_version(schema_version_u16)?;
    validate_subject_kind(subject_kind_u8)?;

    Ok(OpenSilenceStateV1 {
        schema_version_u16,
        subject_kind_u8,
        state_flags_u8,
        silence_start_ts_i64,
        last_alert_window_start_ts_i64,
        last_alert_window_end_ts_i64,
        last_alert_id,
    })
}

pub fn encode_open_drop_state_v1(value: &OpenDropStateV1) -> Vec<u8> {
    let alert_id = value.last_alert_id.as_bytes();
    let alert_id_len = u16::try_from(alert_id.len()).unwrap();

    let mut out = Vec::with_capacity(OPEN_DROP_STATE_V1_FIXED_LEN + alert_id.len());
    encode_u16_le(value.schema_version_u16, &mut out);
    out.push(value.subject_kind_u8);
    out.push(value.state_flags_u8);
    encode_i64_le(value.drop_start_ts_i64, &mut out);
    encode_i64_le(value.last_alert_window_start_ts_i64, &mut out);
    encode_i64_le(value.last_alert_window_end_ts_i64, &mut out);
    encode_u16_le(alert_id_len, &mut out);
    out.extend_from_slice(alert_id);
    out
}

pub fn decode_open_drop_state_v1(
    bytes: &[u8],
) -> Result<OpenDropStateV1, SilenceStateErrorV1> {
    require_min_len(bytes, OPEN_DROP_STATE_V1_FIXED_LEN)?;

    let schema_version_u16 = decode_u16_le(&bytes[0..2]);
    let subject_kind_u8 = bytes[2];
    let state_flags_u8 = bytes[3];
    let drop_start_ts_i64 = decode_i64_le(&bytes[4..12]);
    let last_alert_window_start_ts_i64 = decode_i64_le(&bytes[12..20]);
    let last_alert_window_end_ts_i64 = decode_i64_le(&bytes[20..28]);
    let alert_id_len = usize::from(decode_u16_le(&bytes[28..30]));
    let remaining = bytes.len() - OPEN_DROP_STATE_V1_FIXED_LEN;

    if alert_id_len > remaining {
        return Err(SilenceStateErrorV1::InvalidAlertIdLength {
            declared: alert_id_len,
            remaining,
        });
    }
    if alert_id_len < remaining {
        return Err(SilenceStateErrorV1::TrailingBytes {
            remaining: remaining - alert_id_len,
        });
    }

    let alert_id_bytes = &bytes[OPEN_DROP_STATE_V1_FIXED_LEN..];
    validate_lower_hex_ascii(alert_id_bytes)?;
    let last_alert_id = std::str::from_utf8(alert_id_bytes)
        .map_err(|_| SilenceStateErrorV1::InvalidAlertIdByte(0xff))?
        .to_string();

    validate_schema_version(schema_version_u16)?;
    validate_subject_kind(subject_kind_u8)?;

    Ok(OpenDropStateV1 {
        schema_version_u16,
        subject_kind_u8,
        state_flags_u8,
        drop_start_ts_i64,
        last_alert_window_start_ts_i64,
        last_alert_window_end_ts_i64,
        last_alert_id,
    })
}

pub fn open_drop_state_from_candidate_v1(candidate: &SharpDropCandidateV1, alert_id: &str) -> OpenDropStateV1 {
    OpenDropStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: candidate.subject_kind_u8,
        state_flags_u8: OPEN_DROP_FLAG_OPEN_V1,
        drop_start_ts_i64: candidate.window_start_ts_i64,
        last_alert_window_start_ts_i64: candidate.window_start_ts_i64,
        last_alert_window_end_ts_i64: candidate.window_end_ts_i64,
        last_alert_id: alert_id.to_string(),
    }
}

pub fn open_drop_state_suppresses_candidate_v1(
    candidate: &SharpDropCandidateV1,
    open_drop: Option<&OpenDropStateV1>,
) -> bool {
    let Some(open_drop) = open_drop else {
        return false;
    };
    open_drop.subject_kind_u8 == candidate.subject_kind_u8
        && (open_drop.state_flags_u8 & OPEN_DROP_FLAG_OPEN_V1) != 0
}

pub fn close_open_drop_state_by_recovery_v1(value: &OpenDropStateV1) -> OpenDropStateV1 {
    let mut out = value.clone();
    out.state_flags_u8 = (out.state_flags_u8 & !OPEN_DROP_FLAG_OPEN_V1) | OPEN_DROP_FLAG_CLOSED_BY_RECOVERY_V1;
    out
}

pub fn close_open_drop_state_by_hard_silence_v1(value: &OpenDropStateV1) -> OpenDropStateV1 {
    let mut out = value.clone();
    out.state_flags_u8 = (out.state_flags_u8 & !OPEN_DROP_FLAG_OPEN_V1) | OPEN_DROP_FLAG_CLOSED_BY_HARD_SILENCE_V1;
    out
}

pub fn evaluate_vdrop_candidate_v1(
    tenant_id: &str,
    subject_key: &str,
    state: Option<&ExpectedSourceStateV1>,
    open_silence: Option<&OpenSilenceStateV1>,
    cfg: &VDropEvaluationConfigV1,
) -> VDropEvaluationV1 {
    let state = match state {
        Some(value) => value,
        None => return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::MissingExpectedSourceState),
    };

    if let Err(SilenceStateErrorV1::InvalidSubjectKind(kind)) = validate_subject_kind(state.subject_kind_u8) {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::InvalidSubjectKind(kind));
    }
    if cfg.min_expected_windows_missed_u64 == 0 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::InvalidEvaluationConfig {
            field: "min_expected_windows_missed_u64",
            value: cfg.min_expected_windows_missed_u64,
        });
    }
    if state.window_size_s_u32 == 0 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::InvalidWindowSize {
            value: state.window_size_s_u32,
        });
    }
    if state.last_seen_window_end_ts_i64 <= state.last_seen_window_start_ts_i64 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::InvalidWindowBounds {
            window_start_ts: state.last_seen_window_start_ts_i64,
            window_end_ts: state.last_seen_window_end_ts_i64,
        });
    }
    if state.last_bucket_u8 > 47 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::InvalidBucket {
            value: state.last_bucket_u8,
        });
    }
    if state.mature_windows_total_u64 > state.observed_windows_total_u64 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::CounterInversion {
            observed_windows_total: state.observed_windows_total_u64,
            mature_windows_total: state.mature_windows_total_u64,
        });
    }
    if state.mature_windows_total_u64 < cfg.min_mature_windows_u64 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::NotMature {
            mature_windows_total: state.mature_windows_total_u64,
            min_mature_windows: cfg.min_mature_windows_u64,
        });
    }
    if let Some(open) = open_silence {
        if open.subject_kind_u8 == state.subject_kind_u8 && (open.state_flags_u8 & OPEN_SILENCE_FLAG_OPEN_V1) != 0 {
            return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::OpenSilenceAlreadyExists);
        }
    }
    if cfg.eval_ts_i64 <= state.last_seen_window_end_ts_i64 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::NotSilent {
            eval_ts: cfg.eval_ts_i64,
            last_seen_ts: state.last_seen_window_end_ts_i64,
        });
    }

    let elapsed_s_i64 = cfg.eval_ts_i64 - state.last_seen_window_end_ts_i64;
    let expected_windows_missed_u64 = u64::try_from(elapsed_s_i64 / i64::from(state.window_size_s_u32)).unwrap_or(0);
    if expected_windows_missed_u64 < cfg.min_expected_windows_missed_u64 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::NotEnoughMissedWindows {
            expected_windows_missed: expected_windows_missed_u64,
            min_expected_windows_missed: cfg.min_expected_windows_missed_u64,
        });
    }

    let expected_lines_u64 = state
        .last_observed_lines_u64
        .saturating_mul(expected_windows_missed_u64);
    if expected_lines_u64 < cfg.min_expected_lines_u64 {
        return VDropEvaluationV1::Suppressed(VDropSuppressionReasonV1::BelowExpectedLineFloor {
            expected_lines: expected_lines_u64,
            min_expected_lines: cfg.min_expected_lines_u64,
        });
    }

    let missed_seconds = i64::try_from(expected_windows_missed_u64)
        .unwrap_or(i64::MAX)
        .saturating_mul(i64::from(state.window_size_s_u32));
    let window_start_ts_i64 = state.last_seen_window_end_ts_i64;
    let window_end_ts_i64 = state.last_seen_window_end_ts_i64.saturating_add(missed_seconds);
    let observed_lines_u64 = 0;
    let drop_ratio_f32 = 1.0;

    let mut reason_details = vec![
        ("subject_kind".to_string(), subject_kind_label_v1(state.subject_kind_u8).to_string()),
        ("tenant_id".to_string(), tenant_id.to_string()),
    ];
    if state.subject_kind_u8 == SILENCE_SUBJECT_KIND_DEVICE_V1 {
        reason_details.push(("device_key".to_string(), subject_key.to_string()));
    }
    reason_details.push(("window_start_ts".to_string(), window_start_ts_i64.to_string()));
    reason_details.push(("window_end_ts".to_string(), window_end_ts_i64.to_string()));
    reason_details.push(("last_seen_ts".to_string(), state.last_seen_window_end_ts_i64.to_string()));
    reason_details.push(("expected_windows_missed".to_string(), expected_windows_missed_u64.to_string()));
    reason_details.push(("expected_lines".to_string(), expected_lines_u64.to_string()));
    reason_details.push(("observed_lines".to_string(), observed_lines_u64.to_string()));
    reason_details.push(("drop_ratio".to_string(), format!("{:.6}", drop_ratio_f32)));
    reason_details.push(("bucket".to_string(), state.last_bucket_u8.to_string()));

    VDropEvaluationV1::Candidate(VDropCandidateV1 {
        subject_kind_u8: state.subject_kind_u8,
        subject_key: subject_key.to_string(),
        tenant_id: tenant_id.to_string(),
        window_start_ts_i64,
        window_end_ts_i64,
        last_seen_ts_i64: state.last_seen_window_end_ts_i64,
        expected_windows_missed_u64,
        expected_lines_u64,
        observed_lines_u64,
        drop_ratio_f32,
        bucket_u8: state.last_bucket_u8,
        reason_details,
    })
}


pub fn sharp_drop_expected_volume_from_device_stats_v1(stats: &DeviceStatsV1) -> SharpDropExpectedVolumeV1 {
    SharpDropExpectedVolumeV1 {
        maturity_count_u32: stats.line_count.n,
        expected_lines_f64: stats.line_count.mean,
        expected_bytes_f64: stats.byte_count.mean,
        line_stddev_f64: sample_stddev_welford_v1(&stats.line_count).unwrap_or(0.0),
    }
}

pub fn sum_sharp_drop_expected_volumes_v1(parts: &[SharpDropExpectedVolumeV1]) -> SharpDropExpectedVolumeV1 {
    let mut expected_lines_f64 = 0.0;
    let mut expected_bytes_f64 = 0.0;
    let mut variance_sum_f64 = 0.0;

    for part in parts {
        expected_lines_f64 += part.expected_lines_f64;
        expected_bytes_f64 += part.expected_bytes_f64;
        if part.line_stddev_f64.is_finite() && part.line_stddev_f64 > SHARP_DROP_EPSILON_F64_V1 {
            variance_sum_f64 += part.line_stddev_f64 * part.line_stddev_f64;
        }
    }

    SharpDropExpectedVolumeV1 {
        maturity_count_u32: u32::try_from(parts.len()).unwrap_or(u32::MAX),
        expected_lines_f64,
        expected_bytes_f64,
        line_stddev_f64: variance_sum_f64.sqrt(),
    }
}

pub fn evaluate_sharp_drop_candidate_v1(
    current: &SharpDropCurrentWindowV1,
    expected: &SharpDropExpectedVolumeV1,
    cfg: &SharpDropEvaluationConfigV1,
) -> SharpDropEvaluationV1 {
    if let Err(SilenceStateErrorV1::InvalidSubjectKind(kind)) = validate_subject_kind(current.subject_kind_u8) {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidSubjectKind(kind));
    }
    if current.window_end_ts_i64 <= current.window_start_ts_i64 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidWindowBounds {
            window_start_ts: current.window_start_ts_i64,
            window_end_ts: current.window_end_ts_i64,
        });
    }
    if current.bucket_u8 > 47 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidBucket {
            value: current.bucket_u8,
        });
    }
    if cfg.min_maturity_count_u64 == 0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidEvaluationConfig {
            field: "min_maturity_count_u64",
        });
    }
    if !cfg.min_expected_lines_f64.is_finite() || cfg.min_expected_lines_f64 <= 0.0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidEvaluationConfig {
            field: "min_expected_lines_f64",
        });
    }
    if !cfg.min_absolute_drop_lines_f64.is_finite() || cfg.min_absolute_drop_lines_f64 < 0.0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidEvaluationConfig {
            field: "min_absolute_drop_lines_f64",
        });
    }
    if !cfg.max_observed_expected_ratio_f32.is_finite()
        || cfg.max_observed_expected_ratio_f32 < 0.0
        || cfg.max_observed_expected_ratio_f32 > 1.0
    {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidEvaluationConfig {
            field: "max_observed_expected_ratio_f32",
        });
    }
    if !cfg.min_drop_ratio_f32.is_finite() || cfg.min_drop_ratio_f32 < 0.0 || cfg.min_drop_ratio_f32 > 1.0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidEvaluationConfig {
            field: "min_drop_ratio_f32",
        });
    }
    if !cfg.variance_gate_stddevs_f32.is_finite() || cfg.variance_gate_stddevs_f32 < 0.0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidEvaluationConfig {
            field: "variance_gate_stddevs_f32",
        });
    }
    if !expected.expected_lines_f64.is_finite() || expected.expected_lines_f64 <= 0.0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidExpectedVolume {
            field: "expected_lines_f64",
        });
    }
    if !expected.expected_bytes_f64.is_finite() || expected.expected_bytes_f64 < 0.0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidExpectedVolume {
            field: "expected_bytes_f64",
        });
    }
    if !expected.line_stddev_f64.is_finite() || expected.line_stddev_f64 < 0.0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::InvalidExpectedVolume {
            field: "line_stddev_f64",
        });
    }
    if u64::from(expected.maturity_count_u32) < cfg.min_maturity_count_u64 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::NotMature {
            maturity_count: expected.maturity_count_u32,
            min_maturity_count: cfg.min_maturity_count_u64,
        });
    }
    if expected.expected_lines_f64 < cfg.min_expected_lines_f64 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::BelowExpectedLineFloor {
            expected_lines: expected.expected_lines_f64,
            min_expected_lines: cfg.min_expected_lines_f64,
        });
    }
    if current.observed_lines_u64 == 0 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::HardSilencePriority);
    }

    let absolute_drop_lines_f64 = expected.expected_lines_f64 - current.observed_lines_u64 as f64;
    if absolute_drop_lines_f64 < cfg.min_absolute_drop_lines_f64 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::AbsoluteDropBelowFloor {
            absolute_drop_lines: absolute_drop_lines_f64,
            min_absolute_drop_lines: cfg.min_absolute_drop_lines_f64,
        });
    }

    let observed_expected_ratio_f64 = (current.observed_lines_u64 as f64) / expected.expected_lines_f64;
    let observed_expected_ratio_f32 = observed_expected_ratio_f64 as f32;
    if observed_expected_ratio_f32 > cfg.max_observed_expected_ratio_f32 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::ObservedRatioAboveThreshold {
            observed_expected_ratio: observed_expected_ratio_f32,
            max_observed_expected_ratio: cfg.max_observed_expected_ratio_f32,
        });
    }

    let drop_ratio_f32 = (1.0 - observed_expected_ratio_f64).clamp(0.0, 1.0) as f32;
    if drop_ratio_f32 < cfg.min_drop_ratio_f32 {
        return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::DropRatioBelowThreshold {
            drop_ratio: drop_ratio_f32,
            min_drop_ratio: cfg.min_drop_ratio_f32,
        });
    }

    let line_stddevs_below_mean_f32 = if expected.line_stddev_f64 > SHARP_DROP_EPSILON_F64_V1 {
        let value = ((expected.expected_lines_f64 - current.observed_lines_u64 as f64) / expected.line_stddev_f64).max(0.0) as f32;
        if value < cfg.variance_gate_stddevs_f32 {
            return SharpDropEvaluationV1::Suppressed(SharpDropSuppressionReasonV1::VarianceGateNotMet {
                line_stddevs_below_mean: value,
                min_line_stddevs_below_mean: cfg.variance_gate_stddevs_f32,
            });
        }
        Some(value)
    } else {
        None
    };

    let mut reason_details = vec![
        ("drop_kind".to_string(), "sharp_drop".to_string()),
        ("subject_kind".to_string(), subject_kind_label_v1(current.subject_kind_u8).to_string()),
        ("tenant_id".to_string(), current.tenant_id.clone()),
    ];
    if current.subject_kind_u8 == SILENCE_SUBJECT_KIND_DEVICE_V1 {
        reason_details.push(("device_key".to_string(), current.subject_key.clone()));
    }
    reason_details.push(("window_start_ts".to_string(), current.window_start_ts_i64.to_string()));
    reason_details.push(("window_end_ts".to_string(), current.window_end_ts_i64.to_string()));
    reason_details.push(("bucket".to_string(), current.bucket_u8.to_string()));
    reason_details.push(("expected_lines".to_string(), format_f64_six_v1(expected.expected_lines_f64)));
    reason_details.push(("observed_lines".to_string(), current.observed_lines_u64.to_string()));
    reason_details.push((
        "observed_expected_ratio".to_string(),
        format_f64_six_v1(f64::from(observed_expected_ratio_f32)),
    ));
    reason_details.push(("drop_ratio".to_string(), format_f64_six_v1(f64::from(drop_ratio_f32))));
    reason_details.push(("baseline_n".to_string(), expected.maturity_count_u32.to_string()));
    reason_details.push(("baseline_mean_lines".to_string(), format_f64_six_v1(expected.expected_lines_f64)));
    reason_details.push(("baseline_stddev_lines".to_string(), format_f64_six_v1(expected.line_stddev_f64)));
    reason_details.push((
        "z_drop".to_string(),
        match line_stddevs_below_mean_f32 {
            Some(value) => format_f64_six_v1(f64::from(value)),
            None => "none".to_string(),
        },
    ));
    reason_details.push((
        "max_observed_expected_ratio".to_string(),
        format_f64_six_v1(f64::from(cfg.max_observed_expected_ratio_f32)),
    ));
    reason_details.push(("min_drop_ratio".to_string(), format_f64_six_v1(f64::from(cfg.min_drop_ratio_f32))));
    reason_details.push(("min_absolute_drop_lines".to_string(), format_f64_six_v1(cfg.min_absolute_drop_lines_f64)));
    reason_details.push(("expected_bytes".to_string(), format_f64_six_v1(expected.expected_bytes_f64)));
    reason_details.push(("observed_bytes".to_string(), current.observed_bytes_u64.to_string()));
    reason_details.push(("absolute_drop_lines".to_string(), format_f64_six_v1(absolute_drop_lines_f64)));

    SharpDropEvaluationV1::Candidate(SharpDropCandidateV1 {
        subject_kind_u8: current.subject_kind_u8,
        subject_key: current.subject_key.clone(),
        tenant_id: current.tenant_id.clone(),
        window_start_ts_i64: current.window_start_ts_i64,
        window_end_ts_i64: current.window_end_ts_i64,
        expected_lines_f64: expected.expected_lines_f64,
        observed_lines_u64: current.observed_lines_u64,
        expected_bytes_f64: expected.expected_bytes_f64,
        observed_bytes_u64: current.observed_bytes_u64,
        observed_expected_ratio_f32,
        drop_ratio_f32,
        absolute_drop_lines_f64,
        line_stddevs_below_mean_f32,
        maturity_count_u32: expected.maturity_count_u32,
        bucket_u8: current.bucket_u8,
        reason_details,
    })
}

fn sample_stddev_welford_v1(state: &WelfordF64V1) -> Option<f64> {
    if state.n < 2 {
        return None;
    }
    let variance = state.m2 / f64::from(state.n.saturating_sub(1));
    if !variance.is_finite() || variance <= SHARP_DROP_EPSILON_F64_V1 {
        return None;
    }
    Some(variance.sqrt())
}

fn format_f64_six_v1(value: f64) -> String {
    format!("{:.6}", value)
}

fn subject_kind_label_v1(subject_kind_u8: u8) -> &'static str {
    match subject_kind_u8 {
        SILENCE_SUBJECT_KIND_DEVICE_V1 => "device",
        SILENCE_SUBJECT_KIND_TENANT_V1 => "tenant",
        SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 => "source_stream",
        _ => "unknown",
    }
}
