use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;

use sparx::ingest::{open_file_reader_v1, FileReaderV1, GzipFileReaderV1, PlainFileReaderV1};

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
    let mut resumed_reader = FileReaderV1::Gzip(resumed);
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
        err.kind() == std::io::ErrorKind::InvalidData || err.kind() == std::io::ErrorKind::UnexpectedEof
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn zero_chunk_bytes_are_rejected() {
    let root = temp_case_dir("reader_zero_chunk");
    fs::create_dir_all(&root).unwrap();
    let plain = root.join("events.log");
    let gzip = root.join("events.gz");
    write_plain(&plain, "abc");
    write_gzip(&gzip, "abc");

    assert_eq!(
        PlainFileReaderV1::open_v1(&plain, 0, 0).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        GzipFileReaderV1::open_v1(&gzip, 0, 0).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );

    fs::remove_dir_all(root).unwrap();
}
