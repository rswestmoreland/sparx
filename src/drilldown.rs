// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Raw alert drilldown and extract helpers.
//
// Implements alert drill/extract using AlertV1.provenance only.
// Paths resolve through validated tenant/device provenance and stay under the tenant root.

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use crate::alert::{AlertV1, FileSpanV1};
use crate::config::ConfigV1;
use crate::ingest::{discover_tenant_devices_v1, is_zlg_name_v1};
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

pub fn resolve_provenance_path_v1(
    cfg: &ConfigV1,
    alert: &AlertV1,
    span: &FileSpanV1,
) -> io::Result<PathBuf> {
    validate_path_component_v1("tenant_id", &alert.tenant_id)?;
    validate_relative_path_v1("file_rel", &span.file_rel)?;

    let tenant_root = Path::new(&cfg.sparx.tenant_root);
    let base = resolve_alert_device_base_v1(tenant_root, alert)?;
    let path = base.join(&span.file_rel);
    ensure_path_under_root_v1(tenant_root, &path)
}

fn resolve_alert_device_base_v1(tenant_root: &Path, alert: &AlertV1) -> io::Result<PathBuf> {
    if alert.device_path.starts_with("source_stream:") {
        return resolve_source_stream_device_base_v1(tenant_root, alert);
    }

    validate_relative_path_v1("device_path", &alert.device_path)?;
    let tenant_prefix = format!("{}/", alert.tenant_id);
    if alert.device_path == alert.tenant_id || alert.device_path.starts_with(&tenant_prefix) {
        return Ok(tenant_root.join(&alert.device_path));
    }

    Ok(tenant_root.join(&alert.tenant_id).join(&alert.device_path))
}

fn resolve_source_stream_device_base_v1(
    tenant_root: &Path,
    alert: &AlertV1,
) -> io::Result<PathBuf> {
    validate_path_component_v1("device_key", &alert.device_key)?;
    let devices = discover_tenant_devices_v1(tenant_root, false)?;
    for device in devices {
        if device.tenant_id == alert.tenant_id && device.device_key == alert.device_key {
            return Ok(tenant_root
                .join(device.tenant_id)
                .join(device.device_dir_rel));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "could not resolve source-stream device for tenant {} and device_key {}",
            alert.tenant_id, alert.device_key
        ),
    ))
}

fn ensure_path_under_root_v1(root: &Path, path: &Path) -> io::Result<PathBuf> {
    let root_canon = root.canonicalize()?;
    let path_canon = path.canonicalize()?;
    if !path_canon.starts_with(&root_canon) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "resolved provenance path escapes tenant root: {}",
                path.display()
            ),
        ));
    }
    Ok(path_canon)
}

fn validate_path_component_v1(field: &str, value: &str) -> io::Result<()> {
    if value.is_empty() || value == "." || value == ".." {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {} path component", field),
        ));
    }
    if value
        .bytes()
        .any(|b| b == b'/' || b == b'\\' || b < 0x20 || b == 0x7f)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {} path component", field),
        ));
    }
    Ok(())
}

fn validate_relative_path_v1(field: &str, value: &str) -> io::Result<()> {
    if value.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {} relative path", field),
        ));
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {} absolute path", field),
        ));
    }
    if value.bytes().any(|b| b == b'\\' || b < 0x20 || b == 0x7f) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {} relative path", field),
        ));
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid {} relative path", field),
                ));
            }
        }
    }
    Ok(())
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

        let path = resolve_provenance_path_v1(cfg, alert, span)?;
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

        let (lines, span_bytes, span_lines) = if is_zlg_path_v1(&path) {
            read_reader_span_lines_v1(
                &path,
                span.offset_start,
                span.offset_end,
                &mut bytes_remaining,
                &mut lines_remaining,
                (cfg.ingest.read_chunk_bytes as usize).max(1),
            )?
        } else {
            read_plain_span_lines_v1(
                &path,
                span.offset_start,
                span.offset_end,
                &mut bytes_remaining,
                &mut lines_remaining,
            )?
        };
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

        let path = resolve_provenance_path_v1(cfg, alert, span)?;
        let path_is_zlg = is_zlg_path_v1(&path);
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
            if !span.is_gzip && !path_is_zlg && chunk.offset_end > span.offset_end {
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

fn read_reader_span_lines_v1(
    path: &Path,
    offset_start: u64,
    offset_end: u64,
    bytes_remaining: &mut u64,
    lines_remaining: &mut u64,
    chunk_bytes: usize,
) -> io::Result<(Vec<String>, u64, u64)> {
    let mut reader = open_file_reader_v1(path, false, offset_start, chunk_bytes)?;
    let mut lines = Vec::new();
    let mut bytes_emitted = 0_u64;
    let mut lines_emitted = 0_u64;

    while *bytes_remaining > 0 && *lines_remaining > 0 {
        let Some(chunk) = reader.read_chunk_v1()? else {
            break;
        };
        if chunk.offset_start >= offset_end {
            break;
        }

        let write_len = capped_len_v1(&chunk.data, *bytes_remaining, *lines_remaining);
        if write_len == 0 {
            break;
        }
        let data = &chunk.data[..write_len];
        let text = String::from_utf8_lossy(data);
        for line in text.split_inclusive('\n') {
            if *lines_remaining == 0 {
                break;
            }
            lines.push(line.trim_end_matches('\n').trim_end_matches('\r').to_string());
            *lines_remaining = (*lines_remaining).saturating_sub(1);
            lines_emitted += 1;
        }
        *bytes_remaining = (*bytes_remaining).saturating_sub(write_len as u64);
        bytes_emitted += write_len as u64;

        if write_len < chunk.data.len() || chunk.offset_end >= offset_end {
            break;
        }
    }

    Ok((lines, bytes_emitted, lines_emitted))
}

fn is_zlg_path_v1(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(is_zlg_name_v1)
        .unwrap_or(false)
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
    *lines_remaining = (*lines_remaining).saturating_sub(line_count);
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
