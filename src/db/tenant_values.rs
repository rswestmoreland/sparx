// Tenant DB simple value encodings.
// See: contracts/31_tenant_db_simple_value_encodings_v0_1.md
// Phase 2b covers tenant DB values that are not open-window or baseline sketches.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantValueErrorV1 {
    InvalidLength { expected: usize, actual: usize },
    VarintOverflow,
    UnexpectedEof,
    TrailingBytes { remaining: usize },
    InvalidUtf8,
    InvalidBoolByte(u8),
    UnknownSchemaVersion(u32),
}

fn require_exact_len(bytes: &[u8], expected: usize) -> Result<(), TenantValueErrorV1> {
    if bytes.len() != expected {
        return Err(TenantValueErrorV1::InvalidLength {
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn consume_varint_u64(bytes: &[u8]) -> Result<(u64, usize), TenantValueErrorV1> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;

    for (idx, b) in bytes.iter().enumerate() {
        if shift >= 64 {
            return Err(TenantValueErrorV1::VarintOverflow);
        }

        let part = u64::from(*b & 0x7f);
        let max_part = u64::MAX >> shift;
        if part > max_part {
            return Err(TenantValueErrorV1::VarintOverflow);
        }

        value |= part << shift;

        if (*b & 0x80) == 0 {
            return Ok((value, idx + 1));
        }

        shift += 7;
    }

    Err(TenantValueErrorV1::UnexpectedEof)
}

pub fn encode_u32_le(value: u32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_u32_le(bytes: &[u8]) -> Result<u32, TenantValueErrorV1> {
    require_exact_len(bytes, 4)?;
    let mut raw = [0u8; 4];
    raw.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(raw))
}

pub fn encode_u64_le(value: u64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_u64_le(bytes: &[u8]) -> Result<u64, TenantValueErrorV1> {
    require_exact_len(bytes, 8)?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(raw))
}

pub fn encode_i64_le(value: i64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_i64_le(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    require_exact_len(bytes, 8)?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    Ok(i64::from_le_bytes(raw))
}

pub fn encode_f64_le(value: f64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub fn decode_f64_le(bytes: &[u8]) -> Result<f64, TenantValueErrorV1> {
    require_exact_len(bytes, 8)?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(bytes);
    Ok(f64::from_le_bytes(raw))
}

pub fn encode_u8_bool(value: bool) -> Vec<u8> {
    vec![if value { 1 } else { 0 }]
}

pub fn decode_u8_bool(bytes: &[u8]) -> Result<bool, TenantValueErrorV1> {
    require_exact_len(bytes, 1)?;
    match bytes[0] {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(TenantValueErrorV1::InvalidBoolByte(other)),
    }
}

pub fn encode_varint_u32(value: u32) -> Vec<u8> {
    let mut value = value;
    let mut out = Vec::new();

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

    out
}

pub fn decode_varint_u32_prefix(bytes: &[u8]) -> Result<(u32, usize), TenantValueErrorV1> {
    let (value, used) = consume_varint_u64(bytes)?;
    let value = u32::try_from(value).map_err(|_| TenantValueErrorV1::VarintOverflow)?;
    Ok((value, used))
}

pub fn decode_varint_u32(bytes: &[u8]) -> Result<u32, TenantValueErrorV1> {
    let (value, used) = decode_varint_u32_prefix(bytes)?;
    if used != bytes.len() {
        return Err(TenantValueErrorV1::TrailingBytes {
            remaining: bytes.len() - used,
        });
    }
    Ok(value)
}

pub fn encode_string_v1(value: &str) -> Vec<u8> {
    let raw = value.as_bytes();
    let len = u32::try_from(raw.len()).unwrap();
    let mut out = encode_varint_u32(len);
    out.extend_from_slice(raw);
    out
}

pub fn decode_string_v1(bytes: &[u8]) -> Result<String, TenantValueErrorV1> {
    let (len_u32, used) = decode_varint_u32_prefix(bytes)?;
    let len = usize::try_from(len_u32).map_err(|_| TenantValueErrorV1::VarintOverflow)?;
    let end = used.checked_add(len).ok_or(TenantValueErrorV1::VarintOverflow)?;

    if end > bytes.len() {
        return Err(TenantValueErrorV1::UnexpectedEof);
    }
    if end != bytes.len() {
        return Err(TenantValueErrorV1::TrailingBytes {
            remaining: bytes.len() - end,
        });
    }

    let raw = &bytes[used..end];
    let s = std::str::from_utf8(raw).map_err(|_| TenantValueErrorV1::InvalidUtf8)?;
    Ok(s.to_string())
}

pub fn encode_meta_schema_version_v1(version: u32) -> Vec<u8> {
    encode_u32_le(version)
}

pub fn decode_meta_schema_version_v1(bytes: &[u8]) -> Result<u32, TenantValueErrorV1> {
    decode_u32_le(bytes)
}

pub fn decode_meta_schema_version_exact_v1(
    bytes: &[u8],
    expected_version: u32,
) -> Result<u32, TenantValueErrorV1> {
    let version = decode_meta_schema_version_v1(bytes)?;
    if version != expected_version {
        return Err(TenantValueErrorV1::UnknownSchemaVersion(version));
    }
    Ok(version)
}

pub fn encode_meta_schema_created_ts_v1(ts: i64) -> Vec<u8> {
    encode_i64_le(ts)
}

pub fn decode_meta_schema_created_ts_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_meta_schema_last_migrate_ts_v1(ts: i64) -> Vec<u8> {
    encode_i64_le(ts)
}

pub fn decode_meta_schema_last_migrate_ts_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_meta_ingest_last_flush_ts_v1(ts: i64) -> Vec<u8> {
    encode_i64_le(ts)
}

pub fn decode_meta_ingest_last_flush_ts_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_meta_ingest_worker_epoch_v1(epoch: u64) -> Vec<u8> {
    encode_u64_le(epoch)
}

pub fn decode_meta_ingest_worker_epoch_v1(bytes: &[u8]) -> Result<u64, TenantValueErrorV1> {
    decode_u64_le(bytes)
}

pub fn encode_meta_df_ring_current_day_epoch_v1(epoch: i64) -> Vec<u8> {
    encode_i64_le(epoch)
}

pub fn decode_meta_df_ring_current_day_epoch_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_meta_df_ring_day_slot_epoch_v1(epoch: i64) -> Vec<u8> {
    encode_i64_le(epoch)
}

pub fn decode_meta_df_ring_day_slot_epoch_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_meta_df_ring_last_roll_epoch_v1(epoch: i64) -> Vec<u8> {
    encode_i64_le(epoch)
}

pub fn decode_meta_df_ring_last_roll_epoch_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_feat_dict_meta_next_id_v1(next_id: u32) -> Vec<u8> {
    encode_u32_le(next_id)
}

pub fn decode_feat_dict_meta_next_id_v1(bytes: &[u8]) -> Result<u32, TenantValueErrorV1> {
    decode_u32_le(bytes)
}

pub fn encode_feat_dict_meta_entries_v1(entries: u32) -> Vec<u8> {
    encode_u32_le(entries)
}

pub fn decode_feat_dict_meta_entries_v1(bytes: &[u8]) -> Result<u32, TenantValueErrorV1> {
    decode_u32_le(bytes)
}

pub fn encode_feat_dict_meta_last_gc_ts_v1(ts: i64) -> Vec<u8> {
    encode_i64_le(ts)
}

pub fn decode_feat_dict_meta_last_gc_ts_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_device_path_v1(path: &str) -> Vec<u8> {
    encode_string_v1(path)
}

pub fn decode_device_path_v1(bytes: &[u8]) -> Result<String, TenantValueErrorV1> {
    decode_string_v1(bytes)
}

pub fn encode_device_created_ts_v1(ts: i64) -> Vec<u8> {
    encode_i64_le(ts)
}

pub fn decode_device_created_ts_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_device_last_seen_ts_v1(ts: i64) -> Vec<u8> {
    encode_i64_le(ts)
}

pub fn decode_device_last_seen_ts_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_cursor_inode_v1(inode: u64) -> Vec<u8> {
    encode_u64_le(inode)
}

pub fn decode_cursor_inode_v1(bytes: &[u8]) -> Result<u64, TenantValueErrorV1> {
    decode_u64_le(bytes)
}

pub fn encode_cursor_mtime_v1(mtime: i64) -> Vec<u8> {
    encode_i64_le(mtime)
}

pub fn decode_cursor_mtime_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_cursor_size_v1(size: u64) -> Vec<u8> {
    encode_u64_le(size)
}

pub fn decode_cursor_size_v1(bytes: &[u8]) -> Result<u64, TenantValueErrorV1> {
    decode_u64_le(bytes)
}

pub fn encode_cursor_offset_v1(offset: u64) -> Vec<u8> {
    encode_u64_le(offset)
}

pub fn decode_cursor_offset_v1(bytes: &[u8]) -> Result<u64, TenantValueErrorV1> {
    decode_u64_le(bytes)
}

pub fn encode_cursor_is_gzip_v1(is_gzip: bool) -> Vec<u8> {
    encode_u8_bool(is_gzip)
}

pub fn decode_cursor_is_gzip_v1(bytes: &[u8]) -> Result<bool, TenantValueErrorV1> {
    decode_u8_bool(bytes)
}

pub fn encode_cursor_last_read_ts_v1(ts: i64) -> Vec<u8> {
    encode_i64_le(ts)
}

pub fn decode_cursor_last_read_ts_v1(bytes: &[u8]) -> Result<i64, TenantValueErrorV1> {
    decode_i64_le(bytes)
}

pub fn encode_feat_dict_str_to_id_v1(feature_id: u32) -> Vec<u8> {
    encode_u32_le(feature_id)
}

pub fn decode_feat_dict_str_to_id_v1(bytes: &[u8]) -> Result<u32, TenantValueErrorV1> {
    decode_u32_le(bytes)
}

pub fn encode_feat_dict_id_to_str_v1(feature_string: &str) -> Vec<u8> {
    encode_string_v1(feature_string)
}

pub fn decode_feat_dict_id_to_str_v1(bytes: &[u8]) -> Result<String, TenantValueErrorV1> {
    decode_string_v1(bytes)
}

pub fn encode_metrics_counter_v1(value: u64) -> Vec<u8> {
    encode_u64_le(value)
}

pub fn decode_metrics_counter_v1(bytes: &[u8]) -> Result<u64, TenantValueErrorV1> {
    decode_u64_le(bytes)
}

pub fn encode_metrics_gauge_v1(value: f64) -> Vec<u8> {
    encode_f64_le(value)
}

pub fn decode_metrics_gauge_v1(bytes: &[u8]) -> Result<f64, TenantValueErrorV1> {
    decode_f64_le(bytes)
}

pub fn encode_migrate_journal_v1(status_line: &str) -> Vec<u8> {
    encode_string_v1(status_line)
}

pub fn decode_migrate_journal_v1(bytes: &[u8]) -> Result<String, TenantValueErrorV1> {
    decode_string_v1(bytes)
}
