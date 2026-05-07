// Raw alert drilldown and extract helpers.
//
// Phase 11e implements alert drill/extract using AlertV1.provenance only.
// Paths resolve relative to <watch-root>/<tenant_id>/<device_path>/<file_rel>.

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::alert::{AlertV1, FileSpanV1};
use crate::config::ConfigV1;
use crate::ingest::reader::open_file_reader_v1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrillSpanResultV1 {
    pub span_index: usize,
    pub path: String,
    pub offset_start: u64,
    pub offset_end: u64,
    pub gzip_skipped: bool,
    pub lines: Vec<String>,
    pub bytes_emitted: u64,
    pub lines_emitted: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrillAlertResultV1 {
    pub spans: Vec<DrillSpanResultV1>,
    pub spans_emitted: u64,
    pub gzip_spans_skipped: u64,
    pub bytes_emitted: u64,
    pub lines_emitted: u64,
    pub max_bytes: Option<u64>,
    pub max_lines: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtractAlertResultV1 {
    pub out_path: String,
    pub spans_written: u64,
    pub bytes_written: u64,
    pub lines_written: u64,
    pub max_bytes: Option<u64>,
    pub max_lines: Option<u64>,
}

pub fn resolve_provenance_path_v1(cfg: &ConfigV1, alert: &AlertV1, span: &FileSpanV1) -> PathBuf {
    Path::new(&cfg.sparx.tenant_root)
        .join(&alert.tenant_id)
        .join(&alert.device_path)
        .join(&span.file_rel)
}

pub fn drill_alert_v1(
    cfg: &ConfigV1,
    alert: &AlertV1,
    max_bytes: Option<u64>,
    max_lines: Option<u64>,
) -> io::Result<DrillAlertResultV1> {
    let mut bytes_remaining = max_bytes.unwrap_or(u64::MAX);
    let mut lines_remaining = max_lines.unwrap_or(u64::MAX);
    let mut spans = Vec::new();
    let mut spans_emitted = 0_u64;
    let mut gzip_spans_skipped = 0_u64;
    let mut bytes_emitted = 0_u64;
    let mut lines_emitted = 0_u64;

    for (span_index, span) in alert.provenance.iter().enumerate() {
        if bytes_remaining == 0 || lines_remaining == 0 {
            break;
        }

        let path = resolve_provenance_path_v1(cfg, alert, span);
        if span.is_gzip {
            gzip_spans_skipped += 1;
            spans.push(DrillSpanResultV1 {
                span_index,
                path: path.display().to_string(),
                offset_start: span.offset_start,
                offset_end: span.offset_end,
                gzip_skipped: true,
                lines: Vec::new(),
                bytes_emitted: 0,
                lines_emitted: 0,
            });
            continue;
        }

        let (lines, span_bytes, span_lines) = read_plain_span_lines_v1(
            &path,
            span.offset_start,
            span.offset_end,
            &mut bytes_remaining,
            &mut lines_remaining,
        )?;
        if span_bytes > 0 || span_lines > 0 {
            spans_emitted += 1;
            bytes_emitted += span_bytes;
            lines_emitted += span_lines;
        }
        spans.push(DrillSpanResultV1 {
            span_index,
            path: path.display().to_string(),
            offset_start: span.offset_start,
            offset_end: span.offset_end,
            gzip_skipped: false,
            lines,
            bytes_emitted: span_bytes,
            lines_emitted: span_lines,
        });
    }

    Ok(DrillAlertResultV1 {
        spans,
        spans_emitted,
        gzip_spans_skipped,
        bytes_emitted,
        lines_emitted,
        max_bytes,
        max_lines,
    })
}

pub fn extract_alert_v1(
    cfg: &ConfigV1,
    alert: &AlertV1,
    out_path: &Path,
    max_bytes: Option<u64>,
    max_lines: Option<u64>,
) -> io::Result<ExtractAlertResultV1> {
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(out_path)?;
    let mut writer = BufWriter::new(file);

    let mut bytes_remaining = max_bytes.unwrap_or(u64::MAX);
    let mut lines_remaining = max_lines.unwrap_or(u64::MAX);
    let mut spans_written = 0_u64;
    let mut bytes_written = 0_u64;
    let mut lines_written = 0_u64;
    let chunk_bytes = (cfg.ingest.read_chunk_bytes as usize).max(1);

    for span in &alert.provenance {
        if bytes_remaining == 0 || lines_remaining == 0 {
            break;
        }

        let path = resolve_provenance_path_v1(cfg, alert, span);
        let mut reader = open_file_reader_v1(&path, span.is_gzip, span.offset_start, chunk_bytes)?;
        let mut wrote_this_span = false;

        while bytes_remaining > 0 && lines_remaining > 0 {
            let chunk_opt = reader.read_chunk_v1()?;
            let chunk = match chunk_opt {
                Some(chunk) => chunk,
                None => break,
            };

            if chunk.offset_start >= span.offset_end {
                break;
            }

            let mut data = chunk.data;
            if !span.is_gzip && chunk.offset_end > span.offset_end {
                let keep_len = (span.offset_end.saturating_sub(chunk.offset_start)) as usize;
                data.truncate(keep_len.min(data.len()));
            }

            if data.is_empty() {
                break;
            }

            let write_len = capped_len_v1(&data, bytes_remaining, lines_remaining);
            if write_len == 0 {
                break;
            }

            writer.write_all(&data[..write_len])?;
            wrote_this_span = true;
            bytes_remaining -= write_len as u64;
            bytes_written += write_len as u64;
            let chunk_lines = count_lines_in_bytes_v1(&data[..write_len]);
            lines_remaining = lines_remaining.saturating_sub(chunk_lines);
            lines_written += chunk_lines;

            if write_len < data.len() {
                break;
            }
            if chunk.offset_end >= span.offset_end {
                break;
            }
        }

        if wrote_this_span {
            spans_written += 1;
        }
    }

    writer.flush()?;
    Ok(ExtractAlertResultV1 {
        out_path: out_path.display().to_string(),
        spans_written,
        bytes_written,
        lines_written,
        max_bytes,
        max_lines,
    })
}

fn read_plain_span_lines_v1(
    path: &Path,
    offset_start: u64,
    offset_end: u64,
    bytes_remaining: &mut u64,
    lines_remaining: &mut u64,
) -> io::Result<(Vec<String>, u64, u64)> {
    if *bytes_remaining == 0 || *lines_remaining == 0 {
        return Ok((Vec::new(), 0, 0));
    }

    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let start = offset_start.min(file_len);
    let end = offset_end.min(file_len);
    if end <= start {
        return Ok((Vec::new(), 0, 0));
    }

    file.seek(SeekFrom::Start(start))?;
    let max_read = (end - start).min(*bytes_remaining) as usize;
    let mut buf = vec![0_u8; max_read];
    let mut read_total = 0_usize;
    while read_total < buf.len() {
        let n = file.read(&mut buf[read_total..])?;
        if n == 0 {
            break;
        }
        read_total += n;
    }
    buf.truncate(read_total);
    let keep_len = capped_len_v1(&buf, *bytes_remaining, *lines_remaining);
    let text = String::from_utf8_lossy(&buf[..keep_len]).to_string();
    let lines: Vec<String> = text.lines().map(|line| line.to_string()).collect();
    let line_count = lines.len() as u64;
    *bytes_remaining -= keep_len as u64;
    *lines_remaining = lines_remaining.saturating_sub(line_count);
    Ok((lines, keep_len as u64, line_count))
}

fn capped_len_v1(data: &[u8], bytes_remaining: u64, lines_remaining: u64) -> usize {
    if bytes_remaining == 0 || lines_remaining == 0 {
        return 0;
    }

    let max_len = data.len().min(bytes_remaining as usize);
    if lines_remaining == u64::MAX {
        return max_len;
    }

    let mut line_count = 0_u64;
    let mut line_started = false;
    for (idx, b) in data.iter().take(max_len).enumerate() {
        if !line_started {
            line_started = true;
            line_count += 1;
            if line_count > lines_remaining {
                return idx;
            }
        }
        if *b == b'\n' {
            line_started = false;
        }
    }
    max_len
}

fn count_lines_in_bytes_v1(data: &[u8]) -> u64 {
    if data.is_empty() {
        return 0;
    }
    let mut count = 0_u64;
    let mut line_started = false;
    for b in data {
        if !line_started {
            line_started = true;
            count += 1;
        }
        if *b == b'\n' {
            line_started = false;
        }
    }
    count
}
