// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Source-stream V_DROP identity, catalog, and stats-state primitives.
// See: contracts/39_source_stream_vdrop_implementation_plan_v0_1.md
// Includes storage-agnostic identity, catalog, and stats primitives.
// Includes storage-agnostic evaluator helpers.
// Includes alert and open-state construction primitives.
// Runtime source-stream V_DROP is wired behind the default-off gate.

use std::error::Error;
use std::fmt;

use crate::db::baseline_sketch::WelfordF64V1;
use crate::db::silence::{
    evaluate_sharp_drop_candidate_v1, evaluate_vdrop_candidate_v1, ExpectedSourceStateV1,
    OpenDropStateV1, OpenSilenceStateV1, SharpDropCandidateV1, SharpDropCurrentWindowV1,
    SharpDropEvaluationConfigV1, SharpDropEvaluationV1, SharpDropExpectedVolumeV1,
    VDropCandidateV1, VDropEvaluationConfigV1, VDropEvaluationV1, OPEN_DROP_FLAG_OPEN_V1,
    OPEN_SILENCE_FLAG_OPEN_V1, SILENCE_SCHEMA_VERSION_V1, SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
};
use crate::stable_hash::{stable_hash_hex128_v1, STABLE_HASH_HEX128_LEN_V1};

#[derive(Clone, Debug, PartialEq)]
pub enum SourceStreamErrorV1 {
    EmptyPath,
    AbsolutePath,
    InvalidPathComponent {
        component: String,
    },
    InvalidPathByte(u8),
    InvalidStoragePart {
        field: &'static str,
    },
    InvalidLength {
        expected: usize,
        actual: usize,
    },
    MinimumLength {
        minimum: usize,
        actual: usize,
    },
    UnknownSchemaVersion(u16),
    InvalidReservedField {
        field: &'static str,
        value: u64,
    },
    InvalidSourceStreamId,
    InvalidTimestampBounds {
        first_seen_ts: i64,
        last_seen_ts: i64,
    },
    InvalidBucket {
        value: u8,
    },
    InvalidStringLength {
        field: &'static str,
        declared: usize,
        remaining: usize,
    },
    InvalidStringByte {
        field: &'static str,
        byte: u8,
    },
    TrailingBytes {
        remaining: usize,
    },
    InvalidStatsField {
        field: &'static str,
    },
    StatsCounterOverflow {
        field: &'static str,
    },
}

impl fmt::Display for SourceStreamErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for SourceStreamErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStreamIdentityV1 {
    pub tenant_id: String,
    pub device_key: String,
    pub canonical_source_path: String,
    pub source_stream_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStreamSubjectV1 {
    pub tenant_id: String,
    pub device_key: String,
    pub source_stream_id: String,
    pub canonical_source_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStreamCurrentWindowV1 {
    pub subject: SourceStreamSubjectV1,
    pub window_start_ts_i64: i64,
    pub window_end_ts_i64: i64,
    pub observed_lines_u64: u64,
    pub observed_bytes_u64: u64,
    pub bucket_u8: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceStreamCatalogV1 {
    pub schema_version_u16: u16,
    pub source_stream_id: String,
    pub device_key: String,
    pub canonical_source_path: String,
    pub first_seen_ts_i64: i64,
    pub last_seen_ts_i64: i64,
    pub state_flags_u8: u8,
    pub reserved_u8_0: u8,
    pub reserved_u16_0: u16,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SourceStreamStatsV1 {
    pub line_count: WelfordF64V1,
    pub byte_count: WelfordF64V1,
    pub score_total: WelfordF64V1,
    pub last_update_ts: i64,
}

pub const SOURCE_STREAM_SCHEMA_VERSION_V1: u16 = 1;
pub const SOURCE_STREAM_ID_HEX_LEN_V1: usize = STABLE_HASH_HEX128_LEN_V1;
pub const SOURCE_STREAM_CATALOG_V1_FIXED_LEN: usize = 28;
pub const SOURCE_STREAM_STATS_V1_LEN: usize = 68;
pub const SOURCE_STREAM_FLAG_ACTIVE_V1: u8 = 1;
pub const SOURCE_STREAM_FLAG_RETIRED_V1: u8 = 2;
pub const SOURCE_STREAM_FLAG_ROTATION_SUPPRESSED_V1: u8 = 4;

fn require_exact_len(bytes: &[u8], expected: usize) -> Result<(), SourceStreamErrorV1> {
    if bytes.len() != expected {
        return Err(SourceStreamErrorV1::InvalidLength {
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn require_min_len(bytes: &[u8], minimum: usize) -> Result<(), SourceStreamErrorV1> {
    if bytes.len() < minimum {
        return Err(SourceStreamErrorV1::MinimumLength {
            minimum,
            actual: bytes.len(),
        });
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

fn encode_i64_le(value: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_i64_le(bytes: &[u8]) -> i64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    i64::from_le_bytes(raw)
}

fn encode_f64_le(value: f64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_f64_le(bytes: &[u8]) -> f64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    f64::from_le_bytes(raw)
}

fn encode_welford_f64_v1(value: &WelfordF64V1, out: &mut Vec<u8>) {
    encode_u32_le(value.n, out);
    encode_f64_le(value.mean, out);
    encode_f64_le(value.m2, out);
}

fn decode_welford_f64_v1(bytes: &[u8]) -> WelfordF64V1 {
    WelfordF64V1 {
        n: decode_u32_le(&bytes[0..4]),
        mean: decode_f64_le(&bytes[4..12]),
        m2: decode_f64_le(&bytes[12..20]),
    }
}

fn validate_schema_version_v1(version: u16) -> Result<(), SourceStreamErrorV1> {
    if version != SOURCE_STREAM_SCHEMA_VERSION_V1 {
        return Err(SourceStreamErrorV1::UnknownSchemaVersion(version));
    }
    Ok(())
}

fn validate_ascii_storage_part_v1(
    field: &'static str,
    value: &str,
) -> Result<(), SourceStreamErrorV1> {
    if value.is_empty()
        || value
            .as_bytes()
            .iter()
            .any(|b| *b == b'/' || *b < 0x20 || *b == 0x7f)
    {
        return Err(SourceStreamErrorV1::InvalidStoragePart { field });
    }
    Ok(())
}

fn validate_source_stream_id_v1(value: &str) -> Result<(), SourceStreamErrorV1> {
    if value.len() != SOURCE_STREAM_ID_HEX_LEN_V1 {
        return Err(SourceStreamErrorV1::InvalidSourceStreamId);
    }
    if !value
        .bytes()
        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(SourceStreamErrorV1::InvalidSourceStreamId);
    }
    Ok(())
}

fn validate_catalog_v1(value: &SourceStreamCatalogV1) -> Result<(), SourceStreamErrorV1> {
    validate_schema_version_v1(value.schema_version_u16)?;
    validate_source_stream_id_v1(&value.source_stream_id)?;
    validate_ascii_storage_part_v1("device_key", &value.device_key)?;
    let canonical = canonicalize_source_stream_path_v1(&value.canonical_source_path)?;
    if canonical != value.canonical_source_path {
        return Err(SourceStreamErrorV1::InvalidPathComponent {
            component: value.canonical_source_path.clone(),
        });
    }
    if value.reserved_u8_0 != 0 {
        return Err(SourceStreamErrorV1::InvalidReservedField {
            field: "reserved_u8_0",
            value: u64::from(value.reserved_u8_0),
        });
    }
    if value.reserved_u16_0 != 0 {
        return Err(SourceStreamErrorV1::InvalidReservedField {
            field: "reserved_u16_0",
            value: u64::from(value.reserved_u16_0),
        });
    }
    if value.last_seen_ts_i64 < value.first_seen_ts_i64 {
        return Err(SourceStreamErrorV1::InvalidTimestampBounds {
            first_seen_ts: value.first_seen_ts_i64,
            last_seen_ts: value.last_seen_ts_i64,
        });
    }
    Ok(())
}

fn validate_welford_v1(
    field: &'static str,
    value: &WelfordF64V1,
) -> Result<(), SourceStreamErrorV1> {
    if !value.mean.is_finite() || !value.m2.is_finite() || value.m2 < 0.0 {
        return Err(SourceStreamErrorV1::InvalidStatsField { field });
    }
    if value.n == 0 && (value.mean != 0.0 || value.m2 != 0.0) {
        return Err(SourceStreamErrorV1::InvalidStatsField { field });
    }
    Ok(())
}

fn validate_source_stream_stats_v1(value: &SourceStreamStatsV1) -> Result<(), SourceStreamErrorV1> {
    validate_welford_v1("line_count", &value.line_count)?;
    validate_welford_v1("byte_count", &value.byte_count)?;
    validate_welford_v1("score_total", &value.score_total)?;
    if value.score_total.n != 0 || value.score_total.mean != 0.0 || value.score_total.m2 != 0.0 {
        return Err(SourceStreamErrorV1::InvalidReservedField {
            field: "score_total",
            value: u64::from(value.score_total.n),
        });
    }
    Ok(())
}

fn read_variable_string_v1(
    field: &'static str,
    bytes: &[u8],
    offset: &mut usize,
    len: usize,
) -> Result<String, SourceStreamErrorV1> {
    let remaining = bytes.len().saturating_sub(*offset);
    if len > remaining {
        return Err(SourceStreamErrorV1::InvalidStringLength {
            field,
            declared: len,
            remaining,
        });
    }
    let raw = &bytes[*offset..*offset + len];
    for b in raw {
        if *b < 0x20 || *b == 0x7f {
            return Err(SourceStreamErrorV1::InvalidStringByte { field, byte: *b });
        }
    }
    *offset += len;
    String::from_utf8(raw.to_vec())
        .map_err(|_| SourceStreamErrorV1::InvalidStringByte { field, byte: 0xff })
}

pub fn canonicalize_source_stream_path_v1(path: &str) -> Result<String, SourceStreamErrorV1> {
    if path.is_empty() {
        return Err(SourceStreamErrorV1::EmptyPath);
    }
    let normalized = path.replace('\\', "/");
    if normalized.starts_with('/') {
        return Err(SourceStreamErrorV1::AbsolutePath);
    }
    for b in normalized.as_bytes() {
        if *b < 0x20 || *b == 0x7f {
            return Err(SourceStreamErrorV1::InvalidPathByte(*b));
        }
    }

    let mut parts = Vec::new();
    for part in normalized.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(SourceStreamErrorV1::InvalidPathComponent {
                component: part.to_string(),
            });
        }
        parts.push(part);
    }
    Ok(parts.join("/"))
}

pub fn source_stream_contract_input_v1(
    tenant_id: &str,
    device_key: &str,
    canonical_source_path: &str,
) -> String {
    let mut out = String::new();
    out.push_str("source_stream/v1\n");
    out.push_str("tenant_id=");
    out.push_str(tenant_id);
    out.push('\n');
    out.push_str("device_key=");
    out.push_str(device_key);
    out.push('\n');
    out.push_str("source_path=");
    out.push_str(canonical_source_path);
    out.push('\n');
    out
}

pub fn source_stream_id_v1(
    tenant_id: &str,
    device_key: &str,
    canonical_source_path: &str,
) -> Result<String, SourceStreamErrorV1> {
    let canonical = canonicalize_source_stream_path_v1(canonical_source_path)?;
    Ok(stable_hash_hex128_v1(&source_stream_contract_input_v1(
        tenant_id, device_key, &canonical,
    )))
}

pub fn source_stream_identity_from_path_v1(
    tenant_id: &str,
    device_key: &str,
    source_path: &str,
) -> Result<SourceStreamIdentityV1, SourceStreamErrorV1> {
    validate_ascii_storage_part_v1("device_key", device_key)?;
    let canonical_source_path = canonicalize_source_stream_path_v1(source_path)?;
    let source_stream_id = source_stream_id_v1(tenant_id, device_key, &canonical_source_path)?;
    Ok(SourceStreamIdentityV1 {
        tenant_id: tenant_id.to_string(),
        device_key: device_key.to_string(),
        canonical_source_path,
        source_stream_id,
    })
}

pub fn source_stream_catalog_from_identity_v1(
    identity: &SourceStreamIdentityV1,
    first_seen_ts_i64: i64,
    last_seen_ts_i64: i64,
) -> Result<SourceStreamCatalogV1, SourceStreamErrorV1> {
    let catalog = SourceStreamCatalogV1 {
        schema_version_u16: SOURCE_STREAM_SCHEMA_VERSION_V1,
        source_stream_id: identity.source_stream_id.clone(),
        device_key: identity.device_key.clone(),
        canonical_source_path: identity.canonical_source_path.clone(),
        first_seen_ts_i64,
        last_seen_ts_i64,
        state_flags_u8: SOURCE_STREAM_FLAG_ACTIVE_V1,
        reserved_u8_0: 0,
        reserved_u16_0: 0,
    };
    validate_catalog_v1(&catalog)?;
    Ok(catalog)
}

pub fn update_source_stream_catalog_observed_v1(
    previous: Option<&SourceStreamCatalogV1>,
    identity: &SourceStreamIdentityV1,
    observed_ts_i64: i64,
) -> Result<SourceStreamCatalogV1, SourceStreamErrorV1> {
    let mut next = match previous {
        Some(value) => value.clone(),
        None => source_stream_catalog_from_identity_v1(identity, observed_ts_i64, observed_ts_i64)?,
    };
    validate_catalog_v1(&next)?;
    if next.source_stream_id != identity.source_stream_id || next.device_key != identity.device_key
    {
        return Err(SourceStreamErrorV1::InvalidSourceStreamId);
    }
    if next.canonical_source_path != identity.canonical_source_path {
        return Err(SourceStreamErrorV1::InvalidPathComponent {
            component: next.canonical_source_path.clone(),
        });
    }
    next.first_seen_ts_i64 = next.first_seen_ts_i64.min(observed_ts_i64);
    next.last_seen_ts_i64 = next.last_seen_ts_i64.max(observed_ts_i64);
    validate_catalog_v1(&next)?;
    Ok(next)
}

pub fn encode_source_stream_catalog_v1(
    value: &SourceStreamCatalogV1,
) -> Result<Vec<u8>, SourceStreamErrorV1> {
    validate_catalog_v1(value)?;
    let id = value.source_stream_id.as_bytes();
    let device = value.device_key.as_bytes();
    let path = value.canonical_source_path.as_bytes();
    let id_len = u16::try_from(id.len()).map_err(|_| SourceStreamErrorV1::InvalidStringLength {
        field: "source_stream_id",
        declared: id.len(),
        remaining: usize::from(u16::MAX),
    })?;
    let device_len =
        u16::try_from(device.len()).map_err(|_| SourceStreamErrorV1::InvalidStringLength {
            field: "device_key",
            declared: device.len(),
            remaining: usize::from(u16::MAX),
        })?;
    let path_len =
        u16::try_from(path.len()).map_err(|_| SourceStreamErrorV1::InvalidStringLength {
            field: "canonical_source_path",
            declared: path.len(),
            remaining: usize::from(u16::MAX),
        })?;

    let mut out = Vec::with_capacity(
        SOURCE_STREAM_CATALOG_V1_FIXED_LEN + id.len() + device.len() + path.len(),
    );
    encode_u16_le(value.schema_version_u16, &mut out);
    out.push(value.state_flags_u8);
    out.push(value.reserved_u8_0);
    encode_u16_le(value.reserved_u16_0, &mut out);
    encode_i64_le(value.first_seen_ts_i64, &mut out);
    encode_i64_le(value.last_seen_ts_i64, &mut out);
    encode_u16_le(id_len, &mut out);
    encode_u16_le(device_len, &mut out);
    encode_u16_le(path_len, &mut out);
    out.extend_from_slice(id);
    out.extend_from_slice(device);
    out.extend_from_slice(path);
    Ok(out)
}

pub fn decode_source_stream_catalog_v1(
    bytes: &[u8],
) -> Result<SourceStreamCatalogV1, SourceStreamErrorV1> {
    require_min_len(bytes, SOURCE_STREAM_CATALOG_V1_FIXED_LEN)?;
    let schema_version_u16 = decode_u16_le(&bytes[0..2]);
    let state_flags_u8 = bytes[2];
    let reserved_u8_0 = bytes[3];
    let reserved_u16_0 = decode_u16_le(&bytes[4..6]);
    let first_seen_ts_i64 = decode_i64_le(&bytes[6..14]);
    let last_seen_ts_i64 = decode_i64_le(&bytes[14..22]);
    let id_len = usize::from(decode_u16_le(&bytes[22..24]));
    let device_len = usize::from(decode_u16_le(&bytes[24..26]));
    let path_len = usize::from(decode_u16_le(&bytes[26..28]));
    let mut offset = SOURCE_STREAM_CATALOG_V1_FIXED_LEN;
    let source_stream_id = read_variable_string_v1("source_stream_id", bytes, &mut offset, id_len)?;
    let device_key = read_variable_string_v1("device_key", bytes, &mut offset, device_len)?;
    let canonical_source_path =
        read_variable_string_v1("canonical_source_path", bytes, &mut offset, path_len)?;
    if offset != bytes.len() {
        return Err(SourceStreamErrorV1::TrailingBytes {
            remaining: bytes.len() - offset,
        });
    }
    let value = SourceStreamCatalogV1 {
        schema_version_u16,
        source_stream_id,
        device_key,
        canonical_source_path,
        first_seen_ts_i64,
        last_seen_ts_i64,
        state_flags_u8,
        reserved_u8_0,
        reserved_u16_0,
    };
    validate_catalog_v1(&value)?;
    Ok(value)
}

pub fn encode_source_stream_stats_v1(
    value: &SourceStreamStatsV1,
) -> Result<Vec<u8>, SourceStreamErrorV1> {
    validate_source_stream_stats_v1(value)?;
    let mut out = Vec::with_capacity(SOURCE_STREAM_STATS_V1_LEN);
    encode_welford_f64_v1(&value.line_count, &mut out);
    encode_welford_f64_v1(&value.byte_count, &mut out);
    encode_welford_f64_v1(&value.score_total, &mut out);
    encode_i64_le(value.last_update_ts, &mut out);
    Ok(out)
}

pub fn decode_source_stream_stats_v1(
    bytes: &[u8],
) -> Result<SourceStreamStatsV1, SourceStreamErrorV1> {
    require_exact_len(bytes, SOURCE_STREAM_STATS_V1_LEN)?;
    let value = SourceStreamStatsV1 {
        line_count: decode_welford_f64_v1(&bytes[0..20]),
        byte_count: decode_welford_f64_v1(&bytes[20..40]),
        score_total: decode_welford_f64_v1(&bytes[40..60]),
        last_update_ts: decode_i64_le(&bytes[60..68]),
    };
    validate_source_stream_stats_v1(&value)?;
    Ok(value)
}

pub fn source_stream_subject_from_identity_v1(
    identity: &SourceStreamIdentityV1,
) -> SourceStreamSubjectV1 {
    SourceStreamSubjectV1 {
        tenant_id: identity.tenant_id.clone(),
        device_key: identity.device_key.clone(),
        source_stream_id: identity.source_stream_id.clone(),
        canonical_source_path: identity.canonical_source_path.clone(),
    }
}

pub fn validate_source_stream_subject_v1(
    subject: &SourceStreamSubjectV1,
) -> Result<(), SourceStreamErrorV1> {
    validate_ascii_storage_part_v1("tenant_id", &subject.tenant_id)?;
    validate_ascii_storage_part_v1("device_key", &subject.device_key)?;
    validate_source_stream_id_v1(&subject.source_stream_id)?;
    let canonical = canonicalize_source_stream_path_v1(&subject.canonical_source_path)?;
    if canonical != subject.canonical_source_path {
        return Err(SourceStreamErrorV1::InvalidPathComponent {
            component: subject.canonical_source_path.clone(),
        });
    }
    Ok(())
}

pub fn sharp_drop_expected_volume_from_source_stream_stats_v1(
    stats: &SourceStreamStatsV1,
) -> Result<SharpDropExpectedVolumeV1, SourceStreamErrorV1> {
    validate_source_stream_stats_v1(stats)?;
    Ok(SharpDropExpectedVolumeV1 {
        maturity_count_u32: stats.line_count.n,
        expected_lines_f64: stats.line_count.mean,
        expected_bytes_f64: stats.byte_count.mean,
        line_stddev_f64: sample_stddev_source_stream_welford_v1(&stats.line_count).unwrap_or(0.0),
    })
}

pub fn evaluate_source_stream_hard_silence_candidate_v1(
    subject: &SourceStreamSubjectV1,
    state: Option<&ExpectedSourceStateV1>,
    open_silence: Option<&OpenSilenceStateV1>,
    cfg: &VDropEvaluationConfigV1,
) -> Result<VDropEvaluationV1, SourceStreamErrorV1> {
    validate_source_stream_subject_v1(subject)?;
    if let Some(state) = state {
        if state.subject_kind_u8 != SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 {
            return Ok(VDropEvaluationV1::Suppressed(
                crate::db::silence::VDropSuppressionReasonV1::InvalidSubjectKind(
                    state.subject_kind_u8,
                ),
            ));
        }
    }

    let mut evaluation = evaluate_vdrop_candidate_v1(
        &subject.tenant_id,
        &subject.source_stream_id,
        state,
        open_silence,
        cfg,
    );
    if let VDropEvaluationV1::Candidate(candidate) = &mut evaluation {
        decorate_source_stream_vdrop_details_v1(candidate, subject);
    }
    Ok(evaluation)
}

pub fn evaluate_source_stream_sharp_drop_candidate_v1(
    current: &SourceStreamCurrentWindowV1,
    expected: &SharpDropExpectedVolumeV1,
    cfg: &SharpDropEvaluationConfigV1,
) -> Result<SharpDropEvaluationV1, SourceStreamErrorV1> {
    validate_source_stream_subject_v1(&current.subject)?;
    let generic_current = SharpDropCurrentWindowV1 {
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        subject_key: current.subject.source_stream_id.clone(),
        tenant_id: current.subject.tenant_id.clone(),
        window_start_ts_i64: current.window_start_ts_i64,
        window_end_ts_i64: current.window_end_ts_i64,
        observed_lines_u64: current.observed_lines_u64,
        observed_bytes_u64: current.observed_bytes_u64,
        bucket_u8: current.bucket_u8,
    };
    let mut evaluation = evaluate_sharp_drop_candidate_v1(&generic_current, expected, cfg);
    if let SharpDropEvaluationV1::Candidate(candidate) = &mut evaluation {
        decorate_source_stream_sharp_drop_details_v1(candidate, &current.subject);
    }
    Ok(evaluation)
}

fn decorate_source_stream_vdrop_details_v1(
    candidate: &mut crate::db::silence::VDropCandidateV1,
    subject: &SourceStreamSubjectV1,
) {
    let insert_at = candidate
        .reason_details
        .iter()
        .position(|(key, _)| key == "window_start_ts")
        .unwrap_or(candidate.reason_details.len());
    let details = vec![
        ("device_key".to_string(), subject.device_key.clone()),
        (
            "source_stream_id".to_string(),
            subject.source_stream_id.clone(),
        ),
        (
            "source_path".to_string(),
            subject.canonical_source_path.clone(),
        ),
    ];
    candidate
        .reason_details
        .splice(insert_at..insert_at, details);
}

fn decorate_source_stream_sharp_drop_details_v1(
    candidate: &mut crate::db::silence::SharpDropCandidateV1,
    subject: &SourceStreamSubjectV1,
) {
    let insert_at = candidate
        .reason_details
        .iter()
        .position(|(key, _)| key == "window_start_ts")
        .unwrap_or(candidate.reason_details.len());
    let details = vec![
        ("device_key".to_string(), subject.device_key.clone()),
        (
            "source_stream_id".to_string(),
            subject.source_stream_id.clone(),
        ),
        (
            "source_path".to_string(),
            subject.canonical_source_path.clone(),
        ),
    ];
    candidate
        .reason_details
        .splice(insert_at..insert_at, details);
}

pub fn source_stream_open_silence_state_from_candidate_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &VDropCandidateV1,
    alert_id: &str,
) -> Result<OpenSilenceStateV1, SourceStreamErrorV1> {
    validate_source_stream_candidate_match_v1(
        subject,
        candidate.subject_kind_u8,
        &candidate.tenant_id,
        &candidate.subject_key,
    )?;
    Ok(OpenSilenceStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        state_flags_u8: OPEN_SILENCE_FLAG_OPEN_V1,
        silence_start_ts_i64: candidate.window_start_ts_i64,
        last_alert_window_start_ts_i64: candidate.window_start_ts_i64,
        last_alert_window_end_ts_i64: candidate.window_end_ts_i64,
        last_alert_id: alert_id.to_string(),
    })
}

pub fn source_stream_open_drop_state_from_candidate_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &SharpDropCandidateV1,
    alert_id: &str,
) -> Result<OpenDropStateV1, SourceStreamErrorV1> {
    validate_source_stream_candidate_match_v1(
        subject,
        candidate.subject_kind_u8,
        &candidate.tenant_id,
        &candidate.subject_key,
    )?;
    Ok(OpenDropStateV1 {
        schema_version_u16: SILENCE_SCHEMA_VERSION_V1,
        subject_kind_u8: SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1,
        state_flags_u8: OPEN_DROP_FLAG_OPEN_V1,
        drop_start_ts_i64: candidate.window_start_ts_i64,
        last_alert_window_start_ts_i64: candidate.window_start_ts_i64,
        last_alert_window_end_ts_i64: candidate.window_end_ts_i64,
        last_alert_id: alert_id.to_string(),
    })
}

pub fn source_stream_open_silence_state_suppresses_candidate_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &VDropCandidateV1,
    open_silence: Option<&OpenSilenceStateV1>,
) -> Result<bool, SourceStreamErrorV1> {
    validate_source_stream_candidate_match_v1(
        subject,
        candidate.subject_kind_u8,
        &candidate.tenant_id,
        &candidate.subject_key,
    )?;
    Ok(open_silence
        .map(|state| {
            state.subject_kind_u8 == SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
                && (state.state_flags_u8 & OPEN_SILENCE_FLAG_OPEN_V1) != 0
        })
        .unwrap_or(false))
}

pub fn source_stream_open_drop_state_suppresses_candidate_v1(
    subject: &SourceStreamSubjectV1,
    candidate: &SharpDropCandidateV1,
    open_drop: Option<&OpenDropStateV1>,
) -> Result<bool, SourceStreamErrorV1> {
    validate_source_stream_candidate_match_v1(
        subject,
        candidate.subject_kind_u8,
        &candidate.tenant_id,
        &candidate.subject_key,
    )?;
    Ok(open_drop
        .map(|state| {
            state.subject_kind_u8 == SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1
                && (state.state_flags_u8 & OPEN_DROP_FLAG_OPEN_V1) != 0
        })
        .unwrap_or(false))
}

fn validate_source_stream_candidate_match_v1(
    subject: &SourceStreamSubjectV1,
    subject_kind_u8: u8,
    tenant_id: &str,
    subject_key: &str,
) -> Result<(), SourceStreamErrorV1> {
    validate_source_stream_subject_v1(subject)?;
    if subject_kind_u8 != SILENCE_SUBJECT_KIND_SOURCE_STREAM_V1 {
        return Err(SourceStreamErrorV1::InvalidStatsField {
            field: "subject_kind_u8",
        });
    }
    if tenant_id != subject.tenant_id.as_str() {
        return Err(SourceStreamErrorV1::InvalidStoragePart { field: "tenant_id" });
    }
    if subject_key != subject.source_stream_id.as_str() {
        return Err(SourceStreamErrorV1::InvalidSourceStreamId);
    }
    Ok(())
}

fn sample_stddev_source_stream_welford_v1(state: &WelfordF64V1) -> Option<f64> {
    if state.n < 2 {
        return None;
    }
    let variance = state.m2 / f64::from(state.n.saturating_sub(1));
    if !variance.is_finite() || variance <= 1.0e-12 {
        return None;
    }
    Some(variance.sqrt())
}

pub fn empty_source_stream_stats_v1(last_update_ts: i64) -> SourceStreamStatsV1 {
    SourceStreamStatsV1 {
        line_count: empty_welford_v1(),
        byte_count: empty_welford_v1(),
        score_total: empty_welford_v1(),
        last_update_ts,
    }
}

pub fn update_source_stream_stats_from_observation_v1(
    previous: Option<&SourceStreamStatsV1>,
    observed_lines_u64: u64,
    observed_bytes_u64: u64,
    update_ts_i64: i64,
) -> Result<SourceStreamStatsV1, SourceStreamErrorV1> {
    let mut next = previous
        .cloned()
        .unwrap_or_else(|| empty_source_stream_stats_v1(update_ts_i64));
    validate_source_stream_stats_v1(&next)?;
    next.line_count = update_welford_v1("line_count", &next.line_count, observed_lines_u64 as f64)?;
    next.byte_count = update_welford_v1("byte_count", &next.byte_count, observed_bytes_u64 as f64)?;
    next.score_total = empty_welford_v1();
    next.last_update_ts = next.last_update_ts.max(update_ts_i64);
    validate_source_stream_stats_v1(&next)?;
    Ok(next)
}

fn empty_welford_v1() -> WelfordF64V1 {
    WelfordF64V1 {
        n: 0,
        mean: 0.0,
        m2: 0.0,
    }
}

fn update_welford_v1(
    field: &'static str,
    previous: &WelfordF64V1,
    value: f64,
) -> Result<WelfordF64V1, SourceStreamErrorV1> {
    validate_welford_v1(field, previous)?;
    if previous.n == u32::MAX {
        return Err(SourceStreamErrorV1::StatsCounterOverflow { field });
    }
    if !value.is_finite() || value < 0.0 {
        return Err(SourceStreamErrorV1::InvalidStatsField { field });
    }
    let n = previous.n + 1;
    let n_f64 = f64::from(n);
    let delta = value - previous.mean;
    let mean = previous.mean + delta / n_f64;
    let delta2 = value - mean;
    let m2 = previous.m2 + delta * delta2;
    Ok(WelfordF64V1 { n, mean, m2 })
}
