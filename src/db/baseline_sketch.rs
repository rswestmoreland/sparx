// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Baseline sketch encodings.
// See: contracts/22_baseline_sketch_encoding_v0_1.md
// Covers DF maps, centroid maps, and device stats encodings.

#[derive(Clone, Debug, PartialEq)]
pub enum BaselineSketchErrorV1 {
    InvalidLength { expected: usize, actual: usize },
    VarintOverflow,
    UnexpectedEof,
    TrailingBytes { remaining: usize },
    ZeroCount,
    FeatureIdsNotStrictlyIncreasing { prev: u32, next: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DfCountPairV1 {
    pub feature_id: u32,
    pub df_count: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CentroidValuePairV1 {
    pub feature_id: u32,
    pub value: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WelfordF64V1 {
    pub n: u32,
    pub mean: f64,
    pub m2: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeviceStatsV1 {
    pub line_count: WelfordF64V1,
    pub byte_count: WelfordF64V1,
    pub score_total: WelfordF64V1,
    pub last_update_ts: i64,
}

pub const DEVICE_STATS_V1_LEN: usize = 68;

fn require_exact_len(bytes: &[u8], expected: usize) -> Result<(), BaselineSketchErrorV1> {
    if bytes.len() != expected {
        return Err(BaselineSketchErrorV1::InvalidLength {
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn consume_varint_u64(bytes: &[u8]) -> Result<(u64, usize), BaselineSketchErrorV1> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;

    for (idx, b) in bytes.iter().enumerate() {
        if shift >= 64 {
            return Err(BaselineSketchErrorV1::VarintOverflow);
        }

        let part = u64::from(*b & 0x7f);
        let max_part = u64::MAX >> shift;
        if part > max_part {
            return Err(BaselineSketchErrorV1::VarintOverflow);
        }

        value |= part << shift;

        if (*b & 0x80) == 0 {
            return Ok((value, idx + 1));
        }

        shift += 7;
    }

    Err(BaselineSketchErrorV1::UnexpectedEof)
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

fn encode_f32_le(value: f32, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_f32_le(bytes: &[u8]) -> f32 {
    let mut raw = [0u8; 4];
    raw.copy_from_slice(bytes);
    f32::from_le_bytes(raw)
}

fn encode_varint_u32(value: u32, out: &mut Vec<u8>) {
    let mut value = value;
    loop {
        let low = value & 0x7f;
        let mut byte = u8::try_from(low).unwrap();
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn decode_varint_u32_prefix(bytes: &[u8]) -> Result<(u32, usize), BaselineSketchErrorV1> {
    let (value, used) = consume_varint_u64(bytes)?;
    let value = u32::try_from(value).map_err(|_| BaselineSketchErrorV1::VarintOverflow)?;
    Ok((value, used))
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

pub fn encode_dfn_v1(value: u32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_dfn_v1(bytes: &[u8]) -> Result<u32, BaselineSketchErrorV1> {
    require_exact_len(bytes, 4)?;
    Ok(decode_u32_le(bytes))
}

pub fn encode_dfm_v1(pairs: &[DfCountPairV1]) -> Result<Vec<u8>, BaselineSketchErrorV1> {
    let mut sorted = pairs.to_vec();
    sorted.sort_by_key(|pair| pair.feature_id);

    let mut prev_feature_id: Option<u32> = None;
    for pair in &sorted {
        if pair.df_count == 0 {
            return Err(BaselineSketchErrorV1::ZeroCount);
        }
        if let Some(prev) = prev_feature_id {
            if pair.feature_id <= prev {
                return Err(BaselineSketchErrorV1::FeatureIdsNotStrictlyIncreasing {
                    prev,
                    next: pair.feature_id,
                });
            }
        }
        prev_feature_id = Some(pair.feature_id);
    }

    let mut out = Vec::new();
    let pair_count = u32::try_from(sorted.len()).unwrap();
    encode_varint_u32(pair_count, &mut out);
    for pair in &sorted {
        encode_varint_u32(pair.feature_id, &mut out);
        encode_varint_u32(pair.df_count, &mut out);
    }
    Ok(out)
}

pub fn decode_dfm_v1(bytes: &[u8]) -> Result<Vec<DfCountPairV1>, BaselineSketchErrorV1> {
    let (pair_count_u32, mut offset) = decode_varint_u32_prefix(bytes)?;
    let pair_count =
        usize::try_from(pair_count_u32).map_err(|_| BaselineSketchErrorV1::VarintOverflow)?;
    let mut out = Vec::with_capacity(pair_count);
    let mut prev_feature_id: Option<u32> = None;

    for _ in 0..pair_count {
        let (feature_id, used_feature_id) = decode_varint_u32_prefix(&bytes[offset..])?;
        offset += used_feature_id;

        let (df_count, used_df_count) = decode_varint_u32_prefix(&bytes[offset..])?;
        offset += used_df_count;

        if df_count == 0 {
            return Err(BaselineSketchErrorV1::ZeroCount);
        }
        if let Some(prev) = prev_feature_id {
            if feature_id <= prev {
                return Err(BaselineSketchErrorV1::FeatureIdsNotStrictlyIncreasing {
                    prev,
                    next: feature_id,
                });
            }
        }
        prev_feature_id = Some(feature_id);

        out.push(DfCountPairV1 {
            feature_id,
            df_count,
        });
    }

    if offset != bytes.len() {
        return Err(BaselineSketchErrorV1::TrailingBytes {
            remaining: bytes.len() - offset,
        });
    }

    Ok(out)
}

pub fn encode_centroid_v1(pairs: &[CentroidValuePairV1]) -> Result<Vec<u8>, BaselineSketchErrorV1> {
    let mut sorted = pairs.to_vec();
    sorted.sort_by_key(|pair| pair.feature_id);

    let mut prev_feature_id: Option<u32> = None;
    for pair in &sorted {
        if let Some(prev) = prev_feature_id {
            if pair.feature_id <= prev {
                return Err(BaselineSketchErrorV1::FeatureIdsNotStrictlyIncreasing {
                    prev,
                    next: pair.feature_id,
                });
            }
        }
        prev_feature_id = Some(pair.feature_id);
    }

    let mut out = Vec::new();
    let pair_count = u32::try_from(sorted.len()).unwrap();
    encode_varint_u32(pair_count, &mut out);
    for pair in &sorted {
        encode_varint_u32(pair.feature_id, &mut out);
        encode_f32_le(pair.value, &mut out);
    }
    Ok(out)
}

pub fn decode_centroid_v1(bytes: &[u8]) -> Result<Vec<CentroidValuePairV1>, BaselineSketchErrorV1> {
    let (pair_count_u32, mut offset) = decode_varint_u32_prefix(bytes)?;
    let pair_count =
        usize::try_from(pair_count_u32).map_err(|_| BaselineSketchErrorV1::VarintOverflow)?;
    let mut out = Vec::with_capacity(pair_count);
    let mut prev_feature_id: Option<u32> = None;

    for _ in 0..pair_count {
        let (feature_id, used_feature_id) = decode_varint_u32_prefix(&bytes[offset..])?;
        offset += used_feature_id;

        let end = offset
            .checked_add(4)
            .ok_or(BaselineSketchErrorV1::VarintOverflow)?;
        if end > bytes.len() {
            return Err(BaselineSketchErrorV1::UnexpectedEof);
        }
        let value = decode_f32_le(&bytes[offset..end]);
        offset = end;

        if let Some(prev) = prev_feature_id {
            if feature_id <= prev {
                return Err(BaselineSketchErrorV1::FeatureIdsNotStrictlyIncreasing {
                    prev,
                    next: feature_id,
                });
            }
        }
        prev_feature_id = Some(feature_id);

        out.push(CentroidValuePairV1 { feature_id, value });
    }

    if offset != bytes.len() {
        return Err(BaselineSketchErrorV1::TrailingBytes {
            remaining: bytes.len() - offset,
        });
    }

    Ok(out)
}

pub fn encode_stats_v1(value: &DeviceStatsV1) -> Vec<u8> {
    let mut out = Vec::with_capacity(DEVICE_STATS_V1_LEN);
    encode_welford_f64_v1(&value.line_count, &mut out);
    encode_welford_f64_v1(&value.byte_count, &mut out);
    encode_welford_f64_v1(&value.score_total, &mut out);
    encode_i64_le(value.last_update_ts, &mut out);
    out
}

pub fn decode_stats_v1(bytes: &[u8]) -> Result<DeviceStatsV1, BaselineSketchErrorV1> {
    require_exact_len(bytes, DEVICE_STATS_V1_LEN)?;
    Ok(DeviceStatsV1 {
        line_count: decode_welford_f64_v1(&bytes[0..20]),
        byte_count: decode_welford_f64_v1(&bytes[20..40]),
        score_total: decode_welford_f64_v1(&bytes[40..60]),
        last_update_ts: decode_i64_le(&bytes[60..68]),
    })
}
