// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Reader abstraction for streamed plain-text and gzip files.
// See: contracts/17_format_handling_v0_1.md
//   and contracts/31_tenant_db_simple_value_encodings_v0_1.md

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use flate2::read::MultiGzDecoder;

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
    Gzip(GzipFileReaderV1),
}

impl FileReaderV1 {
    pub fn read_chunk_v1(&mut self) -> io::Result<Option<ReadChunkV1>> {
        match self {
            FileReaderV1::Plain(r) => r.read_chunk_v1(),
            FileReaderV1::Gzip(r) => r.read_chunk_v1(),
        }
    }

    pub fn current_source_offset_v1(&self) -> u64 {
        match self {
            FileReaderV1::Plain(r) => r.current_source_offset_v1(),
            FileReaderV1::Gzip(r) => r.current_source_offset_v1(),
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
        let mut scratch = vec![0_u8; self.chunk_bytes];

        loop {
            let n = self.read_decoder_v1(&mut scratch)?;
            if n == 0 {
                if out.is_empty() {
                    return Ok(None);
                }
                break;
            }
            out.extend_from_slice(&scratch[..n]);
            let now = self.current_source_offset_v1();
            if now > start {
                if now >= self.source_len {
                    self.drain_to_eof_v1(&mut out, &mut scratch)?;
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

        let mut discard = vec![0_u8; self.chunk_bytes];
        while self.current_source_offset_v1() < start_offset {
            let n = self.read_decoder_v1(&mut discard)?;
            if n == 0 {
                return Ok(());
            }
        }

        if start_offset >= self.source_len {
            self.drain_to_eof_v1(&mut Vec::new(), &mut discard)?;
        }

        Ok(())
    }

    fn drain_to_eof_v1(&mut self, out: &mut Vec<u8>, scratch: &mut [u8]) -> io::Result<()> {
        loop {
            let n = self.read_decoder_v1(scratch)?;
            if n == 0 {
                return Ok(());
            }
            out.extend_from_slice(&scratch[..n]);
        }
    }
}

pub fn open_file_reader_v1(path: &Path, is_gzip: bool, start_offset: u64, chunk_bytes: usize) -> io::Result<FileReaderV1> {
    if is_gzip {
        Ok(FileReaderV1::Gzip(GzipFileReaderV1::open_v1(
            path,
            start_offset,
            chunk_bytes,
        )?))
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


impl GzipFileReaderV1 {
    fn read_decoder_v1(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.decoder.read(buf).map_err(normalize_gzip_error_v1)
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
