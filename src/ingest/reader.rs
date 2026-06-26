// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Reader abstraction for streamed plain-text, gzip, and zlg files.
// See: contracts/17_format_handling_v0_1.md
//   and contracts/31_tenant_db_simple_value_encodings_v0_1.md

use std::fs::{self, File};
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::Path;

use crc32fast::hash as crc32;
use flate2::read::MultiGzDecoder;

const ZLG_GLOBAL_MAGIC_V1: &[u8; 8] = b"ZLG1P0\0\0";
const ZLG_CHUNK_MAGIC_V1: &[u8; 4] = b"ZCH1";
const ZLG_DIR_MAGIC_V1: &[u8; 4] = b"ZDR1";
const ZLG_FOOTER_MAGIC_V1: &[u8; 4] = b"ZFT1";
const ZLG_GLOBAL_HEADER_LEN_V1: u16 = 32;
const ZLG_CHUNK_HEADER_LEN_V1: u16 = 64;
const ZLG_DIRECTORY_ENTRY_LEN_V1: u32 = 64;
const ZLG_FORMAT_VERSION_V1: u16 = 1;
const ZLG_CHUNK_FLAG_STORED_V1: u16 = 0x8000;
const ZLG_FOOTER_LEN_V1: u64 = 48;
const ZLG_MAX_SUMMARY_LEN_V1: u64 = 64 * 1024 * 1024;
const ZLG_MAX_COMPRESSED_CHUNK_LEN_V1: u64 = 1024 * 1024 * 1024;
const ZLG_MAX_UNCOMPRESSED_CHUNK_LEN_V1: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadChunkV1 {
    pub data: Vec<u8>,
    pub offset_start: u64,
    pub offset_end: u64,
    pub is_gzip: bool,
}

#[derive(Debug)]
pub enum FileReaderV1 {
    Plain(PlainFileReaderV1),
    Gzip(Box<GzipFileReaderV1>),
    Zlg(Box<ZlgFileReaderV1>),
}

impl FileReaderV1 {
    pub fn read_chunk_v1(&mut self) -> io::Result<Option<ReadChunkV1>> {
        match self {
            FileReaderV1::Plain(r) => r.read_chunk_v1(),
            FileReaderV1::Gzip(r) => r.read_chunk_v1(),
            FileReaderV1::Zlg(r) => r.read_chunk_v1(),
        }
    }

    pub fn current_source_offset_v1(&self) -> u64 {
        match self {
            FileReaderV1::Plain(r) => r.current_source_offset_v1(),
            FileReaderV1::Gzip(r) => r.current_source_offset_v1(),
            FileReaderV1::Zlg(r) => r.current_source_offset_v1(),
        }
    }
}

#[derive(Debug)]
pub struct PlainFileReaderV1 {
    file: File,
    next_offset: u64,
    chunk_bytes: usize,
}

impl PlainFileReaderV1 {
    pub fn open_v1(path: &Path, start_offset: u64, chunk_bytes: usize) -> io::Result<Self> {
        let chunk_bytes = validate_chunk_bytes_v1(chunk_bytes)?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(start_offset))?;
        Ok(Self {
            file,
            next_offset: start_offset,
            chunk_bytes,
        })
    }

    pub fn current_source_offset_v1(&self) -> u64 {
        self.next_offset
    }

    pub fn read_chunk_v1(&mut self) -> io::Result<Option<ReadChunkV1>> {
        let mut buf = vec![0_u8; self.chunk_bytes];
        let n = self.file.read(&mut buf)?;
        if n == 0 {
            return Ok(None);
        }
        buf.truncate(n);
        let start = self.next_offset;
        let end = start + (n as u64);
        self.next_offset = end;
        Ok(Some(ReadChunkV1 {
            data: buf,
            offset_start: start,
            offset_end: end,
            is_gzip: false,
        }))
    }
}

#[derive(Debug)]
pub struct GzipFileReaderV1 {
    decoder: MultiGzDecoder<CountingReaderV1<File>>,
    source_len: u64,
    chunk_bytes: usize,
    next_offset: u64,
    scratch: Vec<u8>,
}

impl GzipFileReaderV1 {
    pub fn open_v1(path: &Path, start_offset: u64, chunk_bytes: usize) -> io::Result<Self> {
        let chunk_bytes = validate_chunk_bytes_v1(chunk_bytes)?;
        let source_len = fs::metadata(path)?.len();
        let file = File::open(path)?;
        let counting = CountingReaderV1::new(file, 1);
        let decoder = MultiGzDecoder::new(counting);
        let mut out = Self {
            decoder,
            source_len,
            chunk_bytes,
            next_offset: start_offset.min(source_len),
            scratch: vec![0_u8; chunk_bytes],
        };
        out.skip_to_compressed_offset_v1(start_offset)?;
        Ok(out)
    }

    pub fn current_source_offset_v1(&self) -> u64 {
        self.decoder.get_ref().bytes_read_v1()
    }

    pub fn read_chunk_v1(&mut self) -> io::Result<Option<ReadChunkV1>> {
        let start = self.next_offset;
        let mut out = Vec::with_capacity(self.chunk_bytes);

        loop {
            let n = self
                .decoder
                .read(&mut self.scratch)
                .map_err(normalize_gzip_error_v1)?;
            if n == 0 {
                if out.is_empty() {
                    return Ok(None);
                }
                break;
            }
            out.extend_from_slice(&self.scratch[..n]);
            let now = self.current_source_offset_v1();
            if now > start {
                if now >= self.source_len {
                    self.drain_to_eof_v1(&mut out)?;
                    break;
                }
                if out.len() >= self.chunk_bytes {
                    break;
                }
            }
        }

        let end = self.current_source_offset_v1();
        self.next_offset = end;
        Ok(Some(ReadChunkV1 {
            data: out,
            offset_start: start,
            offset_end: end,
            is_gzip: true,
        }))
    }

    fn skip_to_compressed_offset_v1(&mut self, start_offset: u64) -> io::Result<()> {
        if start_offset == 0 {
            return Ok(());
        }

        while self.current_source_offset_v1() < start_offset {
            let n = self
                .decoder
                .read(&mut self.scratch)
                .map_err(normalize_gzip_error_v1)?;
            if n == 0 {
                return Ok(());
            }
        }

        if start_offset >= self.source_len {
            self.drain_to_eof_v1(&mut Vec::new())?;
        }

        Ok(())
    }

    fn drain_to_eof_v1(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        loop {
            let n = self
                .decoder
                .read(&mut self.scratch)
                .map_err(normalize_gzip_error_v1)?;
            if n == 0 {
                return Ok(());
            }
            out.extend_from_slice(&self.scratch[..n]);
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ZlgChunkHeaderV1 {
    flags: u16,
    uncompressed_len: u64,
    compressed_len: u64,
    summary_len: u32,
    crc32: u32,
}

#[derive(Debug)]
pub struct ZlgFileReaderV1 {
    file: File,
    source_len: u64,
    next_offset: u64,
    finished: bool,
}

impl ZlgFileReaderV1 {
    pub fn open_v1(path: &Path, start_offset: u64, chunk_bytes: usize) -> io::Result<Self> {
        validate_chunk_bytes_v1(chunk_bytes)?;
        let source_len = fs::metadata(path)?.len();
        let mut file = File::open(path)?;
        read_zlg_global_header_v1(&mut file)?;
        let mut out = Self {
            file,
            source_len,
            next_offset: ZLG_GLOBAL_HEADER_LEN_V1 as u64,
            finished: false,
        };
        out.skip_to_source_offset_v1(start_offset.min(source_len))?;
        Ok(out)
    }

    pub fn current_source_offset_v1(&self) -> u64 {
        self.next_offset
    }

    pub fn read_chunk_v1(&mut self) -> io::Result<Option<ReadChunkV1>> {
        if self.finished || self.next_offset >= self.source_len {
            self.finished = true;
            self.next_offset = self.source_len;
            return Ok(None);
        }

        let record_start = self.next_offset;
        let Some(magic) = read_zlg_record_magic_v1(&mut self.file)? else {
            self.finished = true;
            self.next_offset = self.source_len;
            return Ok(None);
        };
        self.next_offset = self.next_offset.saturating_add(4);

        if &magic == ZLG_DIR_MAGIC_V1 {
            self.skip_zlg_directory_and_footer_v1()?;
            self.finished = true;
            self.next_offset = self.source_len;
            return Ok(None);
        }
        if &magic != ZLG_CHUNK_MAGIC_V1 {
            return Err(invalid_data_v1("unexpected zlg record magic"));
        }

        let header = read_zlg_chunk_header_after_magic_v1(&mut self.file)?;
        self.next_offset = self
            .next_offset
            .checked_add((ZLG_CHUNK_HEADER_LEN_V1 as u64).saturating_sub(4))
            .ok_or_else(|| invalid_data_v1("zlg chunk offset overflow"))?;
        let summary_len = checked_zlg_alloc_len_v1(
            header.summary_len as u64,
            ZLG_MAX_SUMMARY_LEN_V1,
            "zlg chunk search summary",
        )?;
        copy_zlg_n_to_sink_v1(&mut self.file, summary_len as u64)?;
        self.next_offset = self
            .next_offset
            .checked_add(summary_len as u64)
            .ok_or_else(|| invalid_data_v1("zlg summary offset overflow"))?;

        let compressed_len = checked_zlg_alloc_len_v1(
            header.compressed_len,
            ZLG_MAX_COMPRESSED_CHUNK_LEN_V1,
            "zlg compressed chunk payload",
        )?;
        let mut compressed = vec![0_u8; compressed_len];
        self.file.read_exact(&mut compressed)?;
        self.next_offset = self
            .next_offset
            .checked_add(compressed_len as u64)
            .ok_or_else(|| invalid_data_v1("zlg payload offset overflow"))?;

        let data = decode_zlg_chunk_payload_v1(header, compressed)?;
        Ok(Some(ReadChunkV1 {
            data,
            offset_start: record_start,
            offset_end: self.next_offset,
            is_gzip: false,
        }))
    }

    fn skip_to_source_offset_v1(&mut self, start_offset: u64) -> io::Result<()> {
        if start_offset <= self.next_offset {
            return Ok(());
        }
        if start_offset >= self.source_len {
            self.file.seek(SeekFrom::Start(self.source_len))?;
            self.next_offset = self.source_len;
            self.finished = true;
            return Ok(());
        }

        loop {
            let record_start = self.next_offset;
            let Some(magic) = read_zlg_record_magic_v1(&mut self.file)? else {
                self.finished = true;
                self.next_offset = self.source_len;
                return Ok(());
            };
            self.next_offset = self.next_offset.saturating_add(4);

            if &magic == ZLG_DIR_MAGIC_V1 {
                self.skip_zlg_directory_and_footer_v1()?;
                self.finished = true;
                self.next_offset = self.source_len;
                return Ok(());
            }
            if &magic != ZLG_CHUNK_MAGIC_V1 {
                return Err(invalid_data_v1("unexpected zlg record magic"));
            }

            let header = read_zlg_chunk_header_after_magic_v1(&mut self.file)?;
            self.next_offset = self
                .next_offset
                .checked_add((ZLG_CHUNK_HEADER_LEN_V1 as u64).saturating_sub(4))
                .ok_or_else(|| invalid_data_v1("zlg chunk offset overflow"))?;
            let skip_len = (header.summary_len as u64)
                .checked_add(header.compressed_len)
                .ok_or_else(|| invalid_data_v1("zlg chunk skip length overflow"))?;
            let record_end = self
                .next_offset
                .checked_add(skip_len)
                .ok_or_else(|| invalid_data_v1("zlg record end overflow"))?;
            if record_end <= start_offset {
                checked_zlg_alloc_len_v1(
                    header.summary_len as u64,
                    ZLG_MAX_SUMMARY_LEN_V1,
                    "zlg chunk search summary",
                )?;
                checked_zlg_alloc_len_v1(
                    header.compressed_len,
                    ZLG_MAX_COMPRESSED_CHUNK_LEN_V1,
                    "zlg compressed chunk payload",
                )?;
                copy_zlg_n_to_sink_v1(&mut self.file, skip_len)?;
                self.next_offset = record_end;
                continue;
            }

            self.file.seek(SeekFrom::Start(record_start))?;
            self.next_offset = record_start;
            return Ok(());
        }
    }

    fn skip_zlg_directory_and_footer_v1(&mut self) -> io::Result<()> {
        let entry_len = read_zlg_u32_v1(&mut self.file)?;
        if entry_len != ZLG_DIRECTORY_ENTRY_LEN_V1 {
            return Err(invalid_data_v1("unsupported zlg directory entry length"));
        }
        let entry_count = read_zlg_u64_v1(&mut self.file)?;
        let entries_len = (entry_len as u64)
            .checked_mul(entry_count)
            .ok_or_else(|| invalid_data_v1("zlg directory length overflow"))?;
        copy_zlg_n_to_sink_v1(&mut self.file, entries_len)?;

        let mut footer_magic = [0_u8; 4];
        match self.file.read_exact(&mut footer_magic) {
            Ok(()) => {
                if &footer_magic != ZLG_FOOTER_MAGIC_V1 {
                    return Err(invalid_data_v1("expected zlg footer magic after directory"));
                }
                let footer_len = read_zlg_u32_v1(&mut self.file)?;
                if footer_len as u64 != ZLG_FOOTER_LEN_V1 {
                    return Err(invalid_data_v1("unsupported zlg footer length"));
                }
                copy_zlg_n_to_sink_v1(&mut self.file, ZLG_FOOTER_LEN_V1 - 8)?;
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {}
            Err(err) => return Err(err),
        }
        Ok(())
    }
}

pub fn open_file_reader_v1(
    path: &Path,
    is_gzip: bool,
    start_offset: u64,
    chunk_bytes: usize,
) -> io::Result<FileReaderV1> {
    if is_zlg_path_v1(path) {
        Ok(FileReaderV1::Zlg(Box::new(ZlgFileReaderV1::open_v1(
            path,
            start_offset,
            chunk_bytes,
        )?)))
    } else if is_gzip {
        Ok(FileReaderV1::Gzip(Box::new(GzipFileReaderV1::open_v1(
            path,
            start_offset,
            chunk_bytes,
        )?)))
    } else {
        Ok(FileReaderV1::Plain(PlainFileReaderV1::open_v1(
            path,
            start_offset,
            chunk_bytes,
        )?))
    }
}

#[derive(Debug)]
struct CountingReaderV1<R> {
    inner: R,
    bytes_read: u64,
    max_read_bytes_per_call: usize,
}

impl<R> CountingReaderV1<R> {
    fn new(inner: R, max_read_bytes_per_call: usize) -> Self {
        Self {
            inner,
            bytes_read: 0,
            max_read_bytes_per_call,
        }
    }

    fn bytes_read_v1(&self) -> u64 {
        self.bytes_read
    }
}

impl<R: Read> Read for CountingReaderV1<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let limit = buf.len().min(self.max_read_bytes_per_call);
        let n = self.inner.read(&mut buf[..limit])?;
        self.bytes_read += n as u64;
        Ok(n)
    }
}

fn normalize_gzip_error_v1(err: io::Error) -> io::Error {
    match err.kind() {
        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => err,
        _ => io::Error::new(io::ErrorKind::InvalidData, err),
    }
}

fn validate_chunk_bytes_v1(chunk_bytes: usize) -> io::Result<usize> {
    if chunk_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "chunk_bytes must be greater than zero",
        ));
    }
    Ok(chunk_bytes)
}

fn is_zlg_path_v1(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(crate::ingest::is_zlg_name_v1)
        .unwrap_or(false)
}

fn read_zlg_global_header_v1<R: Read>(reader: &mut R) -> io::Result<()> {
    let mut magic = [0_u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != ZLG_GLOBAL_MAGIC_V1 {
        return Err(invalid_data_v1("unsupported or invalid zlg magic"));
    }
    let version = read_zlg_u16_v1(reader)?;
    if version != ZLG_FORMAT_VERSION_V1 {
        return Err(invalid_data_v1("unsupported zlg format version"));
    }
    let header_len = read_zlg_u16_v1(reader)?;
    if header_len != ZLG_GLOBAL_HEADER_LEN_V1 {
        return Err(invalid_data_v1("unsupported zlg global header length"));
    }
    let mut rest = [0_u8; 20];
    reader.read_exact(&mut rest)?;
    Ok(())
}

fn read_zlg_record_magic_v1<R: Read>(reader: &mut R) -> io::Result<Option<[u8; 4]>> {
    let mut magic = [0_u8; 4];
    match reader.read(&mut magic[..1])? {
        0 => Ok(None),
        1 => {
            reader.read_exact(&mut magic[1..])?;
            Ok(Some(magic))
        }
        _ => unreachable!("single-byte read returned more than one byte"),
    }
}

fn read_zlg_chunk_header_after_magic_v1<R: Read>(reader: &mut R) -> io::Result<ZlgChunkHeaderV1> {
    let header_len = read_zlg_u16_v1(reader)?;
    if header_len != ZLG_CHUNK_HEADER_LEN_V1 {
        return Err(invalid_data_v1("unsupported zlg chunk header length"));
    }
    let flags = read_zlg_u16_v1(reader)?;
    let _chunk_index = read_zlg_u64_v1(reader)?;
    let _first_line_number = read_zlg_u64_v1(reader)?;
    let _line_count = read_zlg_u64_v1(reader)?;
    let uncompressed_len = read_zlg_u64_v1(reader)?;
    let compressed_len = read_zlg_u64_v1(reader)?;
    let summary_len = read_zlg_u32_v1(reader)?;
    let crc32 = read_zlg_u32_v1(reader)?;
    let _reserved = read_zlg_u64_v1(reader)?;
    Ok(ZlgChunkHeaderV1 {
        flags,
        uncompressed_len,
        compressed_len,
        summary_len,
        crc32,
    })
}

fn decode_zlg_chunk_payload_v1(
    header: ZlgChunkHeaderV1,
    compressed: Vec<u8>,
) -> io::Result<Vec<u8>> {
    checked_zlg_alloc_len_v1(
        header.uncompressed_len,
        ZLG_MAX_UNCOMPRESSED_CHUNK_LEN_V1,
        "zlg uncompressed chunk payload",
    )?;
    let data = if header.flags & ZLG_CHUNK_FLAG_STORED_V1 != 0 {
        compressed
    } else {
        zstd::stream::decode_all(Cursor::new(compressed))
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
    };
    if data.len() as u64 != header.uncompressed_len {
        return Err(invalid_data_v1("zlg chunk decoded length mismatch"));
    }
    let crc = crc32(&data);
    if crc != header.crc32 {
        return Err(invalid_data_v1("zlg chunk crc mismatch"));
    }
    Ok(data)
}

fn checked_zlg_alloc_len_v1(len: u64, max_len: u64, label: &str) -> io::Result<usize> {
    if len > max_len {
        return Err(invalid_data_v1(&format!(
            "{label} length exceeds safety limit"
        )));
    }
    usize::try_from(len).map_err(|_| invalid_data_v1(&format!("{label} length overflows usize")))
}

fn copy_zlg_n_to_sink_v1<R: Read>(reader: &mut R, mut len: u64) -> io::Result<()> {
    let mut buffer = [0_u8; 8192];
    while len > 0 {
        let want = buffer.len().min(len as usize);
        reader.read_exact(&mut buffer[..want])?;
        len -= want as u64;
    }
    Ok(())
}

fn read_zlg_u16_v1<R: Read>(reader: &mut R) -> io::Result<u16> {
    let mut bytes = [0_u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_zlg_u32_v1<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_zlg_u64_v1<R: Read>(reader: &mut R) -> io::Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn invalid_data_v1(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}
