use sparx::db::tenant_values::*;

#[test]
fn primitive_fixed_width_roundtrips_are_little_endian() {
    let u32_value = 0x78563412u32;
    let u32_bytes = encode_u32_le(u32_value);
    assert_eq!(u32_bytes, vec![0x12, 0x34, 0x56, 0x78]);
    assert_eq!(decode_u32_le(&u32_bytes).unwrap(), u32_value);

    let u64_value = 0x0807060504030201u64;
    let u64_bytes = encode_u64_le(u64_value);
    assert_eq!(u64_bytes, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(decode_u64_le(&u64_bytes).unwrap(), u64_value);

    let i64_value = -0x0102030405060708i64;
    let i64_bytes = encode_i64_le(i64_value);
    assert_eq!(decode_i64_le(&i64_bytes).unwrap(), i64_value);

    let f64_value = 12345.25f64;
    let f64_bytes = encode_f64_le(f64_value);
    assert_eq!(decode_f64_le(&f64_bytes).unwrap(), f64_value);
}

#[test]
fn primitive_varint_roundtrips_cover_single_and_multi_byte_values() {
    let cases = [0u32, 1u32, 127u32, 128u32, 255u32, 16384u32, u32::MAX];

    for value in cases {
        let encoded = encode_varint_u32(value);
        assert_eq!(decode_varint_u32(&encoded).unwrap(), value);
    }

    assert_eq!(encode_varint_u32(0), vec![0]);
    assert_eq!(encode_varint_u32(127), vec![127]);
    assert_eq!(encode_varint_u32(128), vec![128, 1]);
}

#[test]
fn primitive_bool_roundtrip_and_validation_match_contract() {
    assert_eq!(encode_u8_bool(false), vec![0]);
    assert_eq!(encode_u8_bool(true), vec![1]);
    assert_eq!(decode_u8_bool(&[0]).unwrap(), false);
    assert_eq!(decode_u8_bool(&[1]).unwrap(), true);
    assert_eq!(
        decode_u8_bool(&[2]).unwrap_err(),
        TenantValueErrorV1::InvalidBoolByte(2)
    );
}


#[test]
fn cursor_is_gzip_rejects_values_other_than_zero_or_one() {
    assert_eq!(
        decode_cursor_is_gzip_v1(&[9]).unwrap_err(),
        TenantValueErrorV1::InvalidBoolByte(9)
    );
}

#[test]
fn simple_value_helpers_roundtrip_by_contract_type() {
    assert_eq!(
        decode_meta_schema_version_v1(&encode_meta_schema_version_v1(1)).unwrap(),
        1
    );
    assert_eq!(
        decode_meta_schema_created_ts_v1(&encode_meta_schema_created_ts_v1(1700000000)).unwrap(),
        1700000000
    );
    assert_eq!(
        decode_meta_schema_last_migrate_ts_v1(&encode_meta_schema_last_migrate_ts_v1(1700000100)).unwrap(),
        1700000100
    );
    assert_eq!(
        decode_meta_ingest_last_flush_ts_v1(&encode_meta_ingest_last_flush_ts_v1(1700000200)).unwrap(),
        1700000200
    );
    assert_eq!(
        decode_meta_ingest_worker_epoch_v1(&encode_meta_ingest_worker_epoch_v1(77)).unwrap(),
        77
    );
    assert_eq!(
        decode_meta_df_ring_current_day_epoch_v1(&encode_meta_df_ring_current_day_epoch_v1(1700000300)).unwrap(),
        1700000300
    );
    assert_eq!(
        decode_meta_df_ring_day_slot_epoch_v1(&encode_meta_df_ring_day_slot_epoch_v1(1700000400)).unwrap(),
        1700000400
    );
    assert_eq!(
        decode_meta_df_ring_last_roll_epoch_v1(&encode_meta_df_ring_last_roll_epoch_v1(1700000500)).unwrap(),
        1700000500
    );
    assert_eq!(
        decode_feat_dict_meta_next_id_v1(&encode_feat_dict_meta_next_id_v1(99)).unwrap(),
        99
    );
    assert_eq!(
        decode_feat_dict_meta_entries_v1(&encode_feat_dict_meta_entries_v1(100)).unwrap(),
        100
    );
    assert_eq!(
        decode_feat_dict_meta_last_gc_ts_v1(&encode_feat_dict_meta_last_gc_ts_v1(1700000600)).unwrap(),
        1700000600
    );
    assert_eq!(
        decode_device_path_v1(&encode_device_path_v1("tenant_a/device_01")).unwrap(),
        "tenant_a/device_01"
    );
    assert_eq!(
        decode_device_created_ts_v1(&encode_device_created_ts_v1(1700000700)).unwrap(),
        1700000700
    );
    assert_eq!(
        decode_device_last_seen_ts_v1(&encode_device_last_seen_ts_v1(1700000800)).unwrap(),
        1700000800
    );
    assert_eq!(
        decode_cursor_inode_v1(&encode_cursor_inode_v1(123456789)).unwrap(),
        123456789
    );
    assert_eq!(
        decode_cursor_mtime_v1(&encode_cursor_mtime_v1(1700000900)).unwrap(),
        1700000900
    );
    assert_eq!(
        decode_cursor_size_v1(&encode_cursor_size_v1(8192)).unwrap(),
        8192
    );
    assert_eq!(
        decode_cursor_offset_v1(&encode_cursor_offset_v1(4096)).unwrap(),
        4096
    );
    assert_eq!(
        decode_cursor_is_gzip_v1(&encode_cursor_is_gzip_v1(true)).unwrap(),
        true
    );
    assert_eq!(
        decode_cursor_last_read_ts_v1(&encode_cursor_last_read_ts_v1(1700001000)).unwrap(),
        1700001000
    );
    assert_eq!(
        decode_feat_dict_str_to_id_v1(&encode_feat_dict_str_to_id_v1(41)).unwrap(),
        41
    );
    assert_eq!(
        decode_feat_dict_id_to_str_v1(&encode_feat_dict_id_to_str_v1("canon=login_fail")).unwrap(),
        "canon=login_fail"
    );
    assert_eq!(
        decode_metrics_counter_v1(&encode_metrics_counter_v1(55)).unwrap(),
        55
    );
    assert_eq!(
        decode_metrics_gauge_v1(&encode_metrics_gauge_v1(3.5)).unwrap(),
        3.5
    );
    assert_eq!(
        decode_migrate_journal_v1(&encode_migrate_journal_v1("ok phase2b")).unwrap(),
        "ok phase2b"
    );
}

#[test]
fn string_length_varint_supports_long_feature_strings() {
    let feature = format!("feature:{}", "x".repeat(300));
    let encoded = encode_feat_dict_id_to_str_v1(&feature);
    let expected_prefix = encode_varint_u32(feature.len() as u32);
    assert_eq!(&encoded[..expected_prefix.len()], expected_prefix.as_slice());
    assert_eq!(decode_feat_dict_id_to_str_v1(&encoded).unwrap(), feature);
}

#[test]
fn string_decode_rejects_trailing_bytes() {
    let mut encoded = encode_string_v1("abc");
    encoded.push(0);
    assert_eq!(
        decode_string_v1(&encoded).unwrap_err(),
        TenantValueErrorV1::TrailingBytes { remaining: 1 }
    );
}

#[test]
fn schema_version_unknown_returns_error_scaffold() {
    let encoded = encode_meta_schema_version_v1(2);
    assert_eq!(
        decode_meta_schema_version_exact_v1(&encoded, 1).unwrap_err(),
        TenantValueErrorV1::UnknownSchemaVersion(2)
    );
}

#[test]
fn fixed_width_decoders_reject_invalid_lengths() {
    assert_eq!(
        decode_u32_le(&[1, 2, 3]).unwrap_err(),
        TenantValueErrorV1::InvalidLength {
            expected: 4,
            actual: 3,
        }
    );
    assert_eq!(
        decode_u64_le(&[1, 2, 3]).unwrap_err(),
        TenantValueErrorV1::InvalidLength {
            expected: 8,
            actual: 3,
        }
    );
}
