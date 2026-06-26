// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;

use sparx::ingest::{
    open_file_reader_v1, FileReaderV1, GzipFileReaderV1, PlainFileReaderV1, ZlgFileReaderV1,
};

fn temp_case_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!("sparx_{}_{}_{}", name, std::process::id(), nanos));
    p
}

fn write_plain(path: &Path, body: &str) {
    fs::write(path, body.as_bytes()).unwrap();
}

fn write_gzip(path: &Path, body: &str) {
    let file = fs::File::create(path).unwrap();
    let mut enc = GzEncoder::new(file, Compression::default());
    enc.write_all(body.as_bytes()).unwrap();
    enc.finish().unwrap();
}

fn read_all_chunks_text(reader: &mut FileReaderV1) -> (String, Vec<(u64, u64)>) {
    let mut out = String::new();
    let mut spans = Vec::new();
    loop {
        match reader.read_chunk_v1().unwrap() {
            None => return (out, spans),
            Some(chunk) => {
                out.push_str(&String::from_utf8_lossy(&chunk.data));
                spans.push((chunk.offset_start, chunk.offset_end));
            }
        }
    }
}

#[test]
fn plain_reader_reads_from_requested_offset_in_chunks() {
    let root = temp_case_dir("reader_plain");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("events.log");
    write_plain(&path, "abcdefghij");

    let mut reader = PlainFileReaderV1::open_v1(&path, 3, 4).unwrap();
    let c1 = reader.read_chunk_v1().unwrap().unwrap();
    let c2 = reader.read_chunk_v1().unwrap().unwrap();
    let c3 = reader.read_chunk_v1().unwrap();

    assert_eq!(String::from_utf8_lossy(&c1.data), "defg");
    assert_eq!((c1.offset_start, c1.offset_end, c1.is_gzip), (3, 7, false));
    assert_eq!(String::from_utf8_lossy(&c2.data), "hij");
    assert_eq!((c2.offset_start, c2.offset_end, c2.is_gzip), (7, 10, false));
    assert!(c3.is_none());
    assert_eq!(reader.current_source_offset_v1(), 10);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn open_file_reader_dispatches_plain_variant() {
    let root = temp_case_dir("reader_plain_dispatch");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("events.log");
    write_plain(&path, "abc");

    let mut reader = open_file_reader_v1(&path, false, 1, 8).unwrap();
    let (body, spans) = read_all_chunks_text(&mut reader);
    assert_eq!(body, "bc");
    assert_eq!(spans, vec![(1, 3)]);
    assert_eq!(reader.current_source_offset_v1(), 3);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn gzip_reader_streams_decompressed_bytes_with_monotonic_compressed_offsets() {
    let root = temp_case_dir("reader_gzip");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("events.gz");
    let body = "alpha\nbeta\ngamma\ndelta\n";
    write_gzip(&path, body);
    let source_len = fs::metadata(&path).unwrap().len();

    let mut reader = GzipFileReaderV1::open_v1(&path, 0, 5).unwrap();
    let mut text = String::new();
    let mut spans = Vec::new();

    while let Some(chunk) = reader.read_chunk_v1().unwrap() {
        text.push_str(&String::from_utf8_lossy(&chunk.data));
        assert!(chunk.offset_end >= chunk.offset_start);
        assert!(chunk.offset_end <= source_len);
        if let Some((_, prev_end)) = spans.last().copied() {
            assert_eq!(chunk.offset_start, prev_end);
        }
        spans.push((chunk.offset_start, chunk.offset_end));
    }

    assert_eq!(text, body);
    assert!(!spans.is_empty());
    assert_eq!(spans[0].0, 0);
    assert_eq!(spans.last().unwrap().1, source_len);
    assert_eq!(reader.current_source_offset_v1(), source_len);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn gzip_reader_resume_from_saved_compressed_offset_returns_remaining_text() {
    let root = temp_case_dir("reader_gzip_resume");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("events.gz");
    let body = "one\ntwo\nthree\nfour\nfive\n";
    write_gzip(&path, body);

    let mut initial = GzipFileReaderV1::open_v1(&path, 0, 6).unwrap();
    let c1 = initial.read_chunk_v1().unwrap().unwrap();
    let c2 = initial.read_chunk_v1().unwrap().unwrap();
    let saved_offset = c2.offset_end;
    let prefix = format!(
        "{}{}",
        String::from_utf8_lossy(&c1.data),
        String::from_utf8_lossy(&c2.data)
    );

    let resumed = GzipFileReaderV1::open_v1(&path, saved_offset, 6).unwrap();
    let mut resumed_reader = FileReaderV1::Gzip(Box::new(resumed));
    let (suffix, spans) = read_all_chunks_text(&mut resumed_reader);

    assert_eq!(format!("{}{}", prefix, suffix), body);
    assert!(!spans.is_empty());
    assert_eq!(spans[0].0, saved_offset);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn gzip_reader_starting_at_full_compressed_length_is_caught_up() {
    let root = temp_case_dir("reader_gzip_caught_up");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("events.gz");
    write_gzip(&path, "hello\nworld\n");
    let source_len = fs::metadata(&path).unwrap().len();

    let mut reader = GzipFileReaderV1::open_v1(&path, source_len, 8).unwrap();
    assert!(reader.read_chunk_v1().unwrap().is_none());
    assert_eq!(reader.current_source_offset_v1(), source_len);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_gzip_surfaces_read_error() {
    let root = temp_case_dir("reader_gzip_invalid");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("broken.gz");
    write_plain(&path, "not-a-gzip-stream");

    let mut reader = GzipFileReaderV1::open_v1(&path, 0, 8).unwrap();
    let err = reader.read_chunk_v1().unwrap_err();
    assert!(
        err.kind() == std::io::ErrorKind::InvalidData
            || err.kind() == std::io::ErrorKind::UnexpectedEof
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn zlg_reader_streams_stored_chunks_and_dispatches_by_suffix() {
    let root = temp_case_dir("reader_zlg_stored");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("events.zlg");
    write_zlg_archive(&path, &[(b"alpha\n", true), (b"beta\ngamma\n", true)]);
    let source_len = fs::metadata(&path).unwrap().len();

    let mut reader = open_file_reader_v1(&path, false, 0, 8).unwrap();
    let mut text = String::new();
    let mut spans = Vec::new();
    while let Some(chunk) = reader.read_chunk_v1().unwrap() {
        assert!(!chunk.is_gzip);
        text.push_str(&String::from_utf8_lossy(&chunk.data));
        spans.push((chunk.offset_start, chunk.offset_end));
    }

    assert_eq!(text, "alpha\nbeta\ngamma\n");
    assert_eq!(spans.len(), 2);
    assert_eq!(spans[0].0, 32);
    assert_eq!(reader.current_source_offset_v1(), source_len);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn zlg_reader_decodes_zstd_chunks_and_resumes_from_archive_offset() {
    let root = temp_case_dir("reader_zlg_zstd_resume");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("events.zlg");
    write_zlg_archive(&path, &[(b"one\ntwo\n", false), (b"three\nfour\n", false)]);

    let mut first = ZlgFileReaderV1::open_v1(&path, 0, 8).unwrap();
    let c1 = first.read_chunk_v1().unwrap().unwrap();
    let saved_offset = c1.offset_end;
    assert_eq!(String::from_utf8_lossy(&c1.data), "one\ntwo\n");

    let resumed = ZlgFileReaderV1::open_v1(&path, saved_offset, 8).unwrap();
    let mut resumed_reader = FileReaderV1::Zlg(Box::new(resumed));
    let (suffix, spans) = read_all_chunks_text(&mut resumed_reader);

    assert_eq!(suffix, "three\nfour\n");
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].0, saved_offset);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_zlg_surfaces_read_error() {
    let root = temp_case_dir("reader_zlg_invalid");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("broken.zlg");
    write_plain(&path, "not-a-zlg-archive");

    let err = ZlgFileReaderV1::open_v1(&path, 0, 8).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn zero_chunk_bytes_are_rejected() {
    let root = temp_case_dir("reader_zero_chunk");
    fs::create_dir_all(&root).unwrap();
    let plain = root.join("events.log");
    let gzip = root.join("events.gz");
    let zlg = root.join("events.zlg");
    write_plain(&plain, "abc");
    write_gzip(&gzip, "abc");
    write_zlg_archive(&zlg, &[(b"abc", true)]);

    assert_eq!(
        PlainFileReaderV1::open_v1(&plain, 0, 0).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        GzipFileReaderV1::open_v1(&gzip, 0, 0).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        ZlgFileReaderV1::open_v1(&zlg, 0, 0).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );

    fs::remove_dir_all(root).unwrap();
}

const TEST_ZLG_GLOBAL_MAGIC: &[u8; 8] = b"ZLG1P0\0\0";
const TEST_ZLG_CHUNK_MAGIC: &[u8; 4] = b"ZCH1";
const TEST_ZLG_DIR_MAGIC: &[u8; 4] = b"ZDR1";
const TEST_ZLG_FOOTER_MAGIC: &[u8; 4] = b"ZFT1";
const TEST_ZLG_CHUNK_FLAG_STORED: u16 = 0x8000;

struct TestZlgEntry {
    chunk_offset: u64,
    summary_offset: u64,
    summary_len: u32,
    flags: u32,
    compressed_offset: u64,
    compressed_len: u64,
    uncompressed_len: u64,
    first_line_number: u64,
    line_count: u64,
}

fn write_zlg_archive(path: &Path, chunks: &[(&[u8], bool)]) {
    fs::write(path, build_zlg_archive_bytes(chunks)).unwrap();
}

fn build_zlg_archive_bytes(chunks: &[(&[u8], bool)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(TEST_ZLG_GLOBAL_MAGIC);
    push_u16(&mut out, 1);
    push_u16(&mut out, 32);
    push_u32(&mut out, 0);
    push_u32(&mut out, 20);
    push_u32(&mut out, 6);
    out.extend_from_slice(&[0_u8; 8]);

    let mut entries = Vec::new();
    let mut first_line = 1_u64;
    let mut total_lines = 0_u64;
    let mut total_uncompressed = 0_u64;

    for (idx, (data, stored)) in chunks.iter().enumerate() {
        let chunk_offset = out.len() as u64;
        let payload = if *stored {
            data.to_vec()
        } else {
            zstd::stream::encode_all(*data, 3).unwrap()
        };
        let flags = if *stored { TEST_ZLG_CHUNK_FLAG_STORED } else { 0 };
        let line_count = data.iter().filter(|b| **b == b'\n').count() as u64;
        let summary_len = 0_u32;
        let crc = crc32fast::hash(*data);

        out.extend_from_slice(TEST_ZLG_CHUNK_MAGIC);
        push_u16(&mut out, 64);
        push_u16(&mut out, flags);
        push_u64(&mut out, idx as u64);
        push_u64(&mut out, first_line);
        push_u64(&mut out, line_count);
        push_u64(&mut out, data.len() as u64);
        push_u64(&mut out, payload.len() as u64);
        push_u32(&mut out, summary_len);
        push_u32(&mut out, crc);
        push_u64(&mut out, 0);
        let summary_offset = out.len() as u64;
        let compressed_offset = summary_offset + summary_len as u64;
        out.extend_from_slice(&payload);

        entries.push(TestZlgEntry {
            chunk_offset,
            summary_offset,
            summary_len,
            flags: flags as u32,
            compressed_offset,
            compressed_len: payload.len() as u64,
            uncompressed_len: data.len() as u64,
            first_line_number: first_line,
            line_count,
        });
        first_line += line_count;
        total_lines += line_count;
        total_uncompressed += data.len() as u64;
    }

    let directory_offset = out.len() as u64;
    out.extend_from_slice(TEST_ZLG_DIR_MAGIC);
    push_u32(&mut out, 64);
    push_u64(&mut out, entries.len() as u64);
    for entry in &entries {
        push_u64(&mut out, entry.chunk_offset);
        push_u64(&mut out, entry.summary_offset);
        push_u32(&mut out, entry.summary_len);
        push_u32(&mut out, entry.flags);
        push_u64(&mut out, entry.compressed_offset);
        push_u64(&mut out, entry.compressed_len);
        push_u64(&mut out, entry.uncompressed_len);
        push_u64(&mut out, entry.first_line_number);
        push_u64(&mut out, entry.line_count);
    }
    let directory_len = out.len() as u64 - directory_offset;
    out.extend_from_slice(TEST_ZLG_FOOTER_MAGIC);
    push_u32(&mut out, 48);
    push_u64(&mut out, entries.len() as u64);
    push_u64(&mut out, total_lines);
    push_u64(&mut out, total_uncompressed);
    push_u64(&mut out, directory_offset);
    push_u64(&mut out, directory_len);
    out
}

fn push_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
