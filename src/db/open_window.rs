// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Open-window checkpoint encodings.
// See: contracts/26_open_window_checkpoint_encoding_v0_1.md
// Covers win_active and win_row value encodings.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenWindowErrorV1 {
    InvalidLength { expected: usize, actual: usize },
    VarintOverflow,
    UnexpectedEof,
    TrailingBytes { remaining: usize },
    InvalidUtf8,
    ZeroCount,
    FeatureIdsNotStrictlyIncreasing { prev: u32, next: u32 },
    DuplicateTopKValue,
    TopKOrderViolation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WinActiveV1 {
    pub active_window_start_ts: i64,
    pub active_window_id: u64,
    pub last_update_ts: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SparseCountPairV1 {
    pub feature_id: u32,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WinMetaV1 {
    pub window_start_ts: i64,
    pub window_end_ts: i64,
    pub lines: u32,
    pub bytes: u64,
    pub dropped_features: u32,
    pub dropped_words: u32,
    pub dropped_shapes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopKStringEntryV1 {
    pub value: String,
    pub count: u32,
}

pub const WIN_ACTIVE_V1_LEN: usize = 24;
pub const WIN_META_V1_LEN: usize = 40;

fn require_exact_len(bytes: &[u8], expected: usize) -> Result<(), OpenWindowErrorV1> {
    if bytes.len() != expected {
        return Err(OpenWindowErrorV1::InvalidLength {
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn consume_varint_u64(bytes: &[u8]) -> Result<(u64, usize), OpenWindowErrorV1> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;

    for (idx, b) in bytes.iter().enumerate() {
        if shift >= 64 {
            return Err(OpenWindowErrorV1::VarintOverflow);
        }

        let part = u64::from(*b & 0x7f);
        let max_part = u64::MAX >> shift;
        if part > max_part {
            return Err(OpenWindowErrorV1::VarintOverflow);
        }

        value |= part << shift;

        if (*b & 0x80) == 0 {
            return Ok((value, idx + 1));
        }

        shift += 7;
    }

    Err(OpenWindowErrorV1::UnexpectedEof)
}

fn encode_i64_le(value: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_i64_le(bytes: &[u8]) -> i64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    i64::from_le_bytes(raw)
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

fn decode_varint_u32_prefix(bytes: &[u8]) -> Result<(u32, usize), OpenWindowErrorV1> {
    let (value, used) = consume_varint_u64(bytes)?;
    let value = u32::try_from(value).map_err(|_| OpenWindowErrorV1::VarintOverflow)?;
    Ok((value, used))
}

fn encode_string_v1(value: &str, out: &mut Vec<u8>) {
    let raw = value.as_bytes();
    let len = u32::try_from(raw.len()).unwrap();
    encode_varint_u32(len, out);
    out.extend_from_slice(raw);
}

fn decode_string_v1_prefix(bytes: &[u8]) -> Result<(String, usize), OpenWindowErrorV1> {
    let (len_u32, used) = decode_varint_u32_prefix(bytes)?;
    let len = usize::try_from(len_u32).map_err(|_| OpenWindowErrorV1::VarintOverflow)?;
    let end = used.checked_add(len).ok_or(OpenWindowErrorV1::VarintOverflow)?;
    if end > bytes.len() {
        return Err(OpenWindowErrorV1::UnexpectedEof);
    }

    let raw = &bytes[used..end];
    let s = std::str::from_utf8(raw).map_err(|_| OpenWindowErrorV1::InvalidUtf8)?;
    Ok((s.to_string(), end))
}

fn topk_entry_cmp(a: &TopKStringEntryV1, b: &TopKStringEntryV1) -> std::cmp::Ordering {
    b.count
        .cmp(&a.count)
        .then_with(|| a.value.as_bytes().cmp(b.value.as_bytes()))
}

pub fn encode_win_active_v1(value: &WinActiveV1) -> Vec<u8> {
    let mut out = Vec::with_capacity(WIN_ACTIVE_V1_LEN);
    encode_i64_le(value.active_window_start_ts, &mut out);
    encode_u64_le(value.active_window_id, &mut out);
    encode_i64_le(value.last_update_ts, &mut out);
    out
}

pub fn decode_win_active_v1(bytes: &[u8]) -> Result<WinActiveV1, OpenWindowErrorV1> {
    require_exact_len(bytes, WIN_ACTIVE_V1_LEN)?;
    Ok(WinActiveV1 {
        active_window_start_ts: decode_i64_le(&bytes[0..8]),
        active_window_id: decode_u64_le(&bytes[8..16]),
        last_update_ts: decode_i64_le(&bytes[16..24]),
    })
}

pub fn encode_sparse_counts_v1(pairs: &[SparseCountPairV1]) -> Result<Vec<u8>, OpenWindowErrorV1> {
    let mut sorted = pairs.to_vec();
    sorted.sort_by_key(|pair| pair.feature_id);

    let mut prev_feature_id: Option<u32> = None;
    for pair in &sorted {
        if pair.count == 0 {
            return Err(OpenWindowErrorV1::ZeroCount);
        }
        if let Some(prev) = prev_feature_id {
            if pair.feature_id <= prev {
                return Err(OpenWindowErrorV1::FeatureIdsNotStrictlyIncreasing {
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
        encode_varint_u32(pair.count, &mut out);
    }
    Ok(out)
}

pub fn decode_sparse_counts_v1(bytes: &[u8]) -> Result<Vec<SparseCountPairV1>, OpenWindowErrorV1> {
    let (pair_count_u32, mut offset) = decode_varint_u32_prefix(bytes)?;
    let pair_count = usize::try_from(pair_count_u32).map_err(|_| OpenWindowErrorV1::VarintOverflow)?;
    let mut out = Vec::with_capacity(pair_count);
    let mut prev_feature_id: Option<u32> = None;

    for _ in 0..pair_count {
        let (feature_id, used_feature_id) = decode_varint_u32_prefix(&bytes[offset..])?;
        offset += used_feature_id;

        let (count, used_count) = decode_varint_u32_prefix(&bytes[offset..])?;
        offset += used_count;

        if count == 0 {
            return Err(OpenWindowErrorV1::ZeroCount);
        }
        if let Some(prev) = prev_feature_id {
            if feature_id <= prev {
                return Err(OpenWindowErrorV1::FeatureIdsNotStrictlyIncreasing {
                    prev,
                    next: feature_id,
                });
            }
        }
        prev_feature_id = Some(feature_id);

        out.push(SparseCountPairV1 { feature_id, count });
    }

    if offset != bytes.len() {
        return Err(OpenWindowErrorV1::TrailingBytes {
            remaining: bytes.len() - offset,
        });
    }

    Ok(out)
}

pub fn encode_win_meta_v1(value: &WinMetaV1) -> Vec<u8> {
    let mut out = Vec::with_capacity(WIN_META_V1_LEN);
    encode_i64_le(value.window_start_ts, &mut out);
    encode_i64_le(value.window_end_ts, &mut out);
    encode_u32_le(value.lines, &mut out);
    encode_u64_le(value.bytes, &mut out);
    encode_u32_le(value.dropped_features, &mut out);
    encode_u32_le(value.dropped_words, &mut out);
    encode_u32_le(value.dropped_shapes, &mut out);
    out
}

pub fn decode_win_meta_v1(bytes: &[u8]) -> Result<WinMetaV1, OpenWindowErrorV1> {
    require_exact_len(bytes, WIN_META_V1_LEN)?;
    Ok(WinMetaV1 {
        window_start_ts: decode_i64_le(&bytes[0..8]),
        window_end_ts: decode_i64_le(&bytes[8..16]),
        lines: decode_u32_le(&bytes[16..20]),
        bytes: decode_u64_le(&bytes[20..28]),
        dropped_features: decode_u32_le(&bytes[28..32]),
        dropped_words: decode_u32_le(&bytes[32..36]),
        dropped_shapes: decode_u32_le(&bytes[36..40]),
    })
}

pub fn encode_topk_strings_v1(entries: &[TopKStringEntryV1]) -> Result<Vec<u8>, OpenWindowErrorV1> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(topk_entry_cmp);

    let mut prev_value: Option<&str> = None;
    let mut prev_entry: Option<&TopKStringEntryV1> = None;
    for entry in &sorted {
        if entry.count == 0 {
            return Err(OpenWindowErrorV1::ZeroCount);
        }
        if let Some(prev) = prev_value {
            if entry.value == prev {
                return Err(OpenWindowErrorV1::DuplicateTopKValue);
            }
        }
        if let Some(prev) = prev_entry {
            if topk_entry_cmp(prev, entry) == std::cmp::Ordering::Greater {
                return Err(OpenWindowErrorV1::TopKOrderViolation);
            }
        }
        prev_value = Some(entry.value.as_str());
        prev_entry = Some(entry);
    }

    let mut out = Vec::new();
    let entry_count = u32::try_from(sorted.len()).unwrap();
    encode_varint_u32(entry_count, &mut out);
    for entry in &sorted {
        encode_varint_u32(entry.count, &mut out);
        encode_string_v1(&entry.value, &mut out);
    }
    Ok(out)
}

pub fn decode_topk_strings_v1(bytes: &[u8]) -> Result<Vec<TopKStringEntryV1>, OpenWindowErrorV1> {
    let (entry_count_u32, mut offset) = decode_varint_u32_prefix(bytes)?;
    let entry_count = usize::try_from(entry_count_u32).map_err(|_| OpenWindowErrorV1::VarintOverflow)?;
    let mut out = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        let (count, used_count) = decode_varint_u32_prefix(&bytes[offset..])?;
        offset += used_count;

        if count == 0 {
            return Err(OpenWindowErrorV1::ZeroCount);
        }

        let (value, used_value) = decode_string_v1_prefix(&bytes[offset..])?;
        offset += used_value;

        out.push(TopKStringEntryV1 { value, count });
    }

    if offset != bytes.len() {
        return Err(OpenWindowErrorV1::TrailingBytes {
            remaining: bytes.len() - offset,
        });
    }

    let mut prev: Option<&TopKStringEntryV1> = None;
    for entry in &out {
        if let Some(prev_entry) = prev {
            if prev_entry.value == entry.value {
                return Err(OpenWindowErrorV1::DuplicateTopKValue);
            }
            if topk_entry_cmp(prev_entry, entry) == std::cmp::Ordering::Greater {
                return Err(OpenWindowErrorV1::TopKOrderViolation);
            }
        }
        prev = Some(entry);
    }

    Ok(out)
}

pub fn encode_win_row_feat_v1(pairs: &[SparseCountPairV1]) -> Result<Vec<u8>, OpenWindowErrorV1> {
    encode_sparse_counts_v1(pairs)
}

pub fn decode_win_row_feat_v1(bytes: &[u8]) -> Result<Vec<SparseCountPairV1>, OpenWindowErrorV1> {
    decode_sparse_counts_v1(bytes)
}

pub fn encode_win_row_meta_v1(value: &WinMetaV1) -> Vec<u8> {
    encode_win_meta_v1(value)
}

pub fn decode_win_row_meta_v1(bytes: &[u8]) -> Result<WinMetaV1, OpenWindowErrorV1> {
    decode_win_meta_v1(bytes)
}

pub fn encode_win_row_ent_srcip_v1(entries: &[TopKStringEntryV1]) -> Result<Vec<u8>, OpenWindowErrorV1> {
    encode_topk_strings_v1(entries)
}

pub fn decode_win_row_ent_srcip_v1(bytes: &[u8]) -> Result<Vec<TopKStringEntryV1>, OpenWindowErrorV1> {
    decode_topk_strings_v1(bytes)
}

pub fn encode_win_row_ent_dstip_v1(entries: &[TopKStringEntryV1]) -> Result<Vec<u8>, OpenWindowErrorV1> {
    encode_topk_strings_v1(entries)
}

pub fn decode_win_row_ent_dstip_v1(bytes: &[u8]) -> Result<Vec<TopKStringEntryV1>, OpenWindowErrorV1> {
    decode_topk_strings_v1(bytes)
}

pub fn encode_win_row_ent_userid_v1(entries: &[TopKStringEntryV1]) -> Result<Vec<u8>, OpenWindowErrorV1> {
    encode_topk_strings_v1(entries)
}

pub fn decode_win_row_ent_userid_v1(bytes: &[u8]) -> Result<Vec<TopKStringEntryV1>, OpenWindowErrorV1> {
    decode_topk_strings_v1(bytes)
}

pub fn encode_win_row_ent_domain_v1(entries: &[TopKStringEntryV1]) -> Result<Vec<u8>, OpenWindowErrorV1> {
    encode_topk_strings_v1(entries)
}

pub fn decode_win_row_ent_domain_v1(bytes: &[u8]) -> Result<Vec<TopKStringEntryV1>, OpenWindowErrorV1> {
    decode_topk_strings_v1(bytes)
}

pub fn encode_win_row_ent_host_v1(entries: &[TopKStringEntryV1]) -> Result<Vec<u8>, OpenWindowErrorV1> {
    encode_topk_strings_v1(entries)
}

pub fn decode_win_row_ent_host_v1(bytes: &[u8]) -> Result<Vec<TopKStringEntryV1>, OpenWindowErrorV1> {
    decode_topk_strings_v1(bytes)
}
