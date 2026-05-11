// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Output sink interfaces and implementations.
// See: contracts/29_output_sink_contract_v0_1.md
//
// The active runtime/config surface uses JsonlAlertSinkV1 or StdoutAlertSinkV1.
// For `output.sink=jsonl`, the runtime now uses SpoolingJsonlAlertSinkV1 for
// automatic write-failure fallback and bounded deterministic replay. The spool
// helpers also remain directly covered by focused sink tests.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{Datelike, TimeZone, Utc};

use crate::alert::AlertV1;

pub const JSONL_ROTATE_MB_DEFAULT_V1: u32 = 256;
pub const JSONL_FLUSH_INTERVAL_S_DEFAULT_V1: u32 = 5;
pub const SPOOL_MAX_MB_DEFAULT_V1: u32 = 2048;
pub const AUTOMATED_SPOOL_REPLAY_MAX_FILES_DEFAULT_V1: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkErrorV1 {
    pub msg: String,
}

pub trait AlertSinkV1 {
    fn emit(&mut self, alert: &AlertV1) -> Result<(), SinkErrorV1>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonlSinkConfigV1 {
    pub alert_out_root: String,
    pub jsonl_rotate_mb: u32,
    pub jsonl_flush_interval_s: u32,
}

impl Default for JsonlSinkConfigV1 {
    fn default() -> Self {
        Self {
            alert_out_root: "/var/lib/sparx/alerts".to_string(),
            jsonl_rotate_mb: JSONL_ROTATE_MB_DEFAULT_V1,
            jsonl_flush_interval_s: JSONL_FLUSH_INTERVAL_S_DEFAULT_V1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpoolConfigV1 {
    pub data_root: String,
    pub spool_max_mb: u32,
}

impl Default for SpoolConfigV1 {
    fn default() -> Self {
        Self {
            data_root: "/var/lib/sparx".to_string(),
            spool_max_mb: SPOOL_MAX_MB_DEFAULT_V1,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpoolCountersV1 {
    pub sink_spool_total: u64,
    pub sink_spool_replayed_total: u64,
    pub sink_spool_replay_fail_total: u64,
    pub sink_spool_drop_total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpoolEmitOutcomeV1 {
    Delivered { path: PathBuf },
    Spooled { path: PathBuf },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpoolReplayReportV1 {
    pub replayed_paths: Vec<PathBuf>,
    pub failed_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpoolCapReportV1 {
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub dropped_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpoolBacklogSummaryV1 {
    pub files: u64,
    pub bytes: u64,
    pub oldest_file_ts: Option<u64>,
    pub oldest_age_s: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpoolBacklogTenantSummaryV1 {
    pub tenant_id: String,
    pub files: u64,
    pub bytes: u64,
    pub oldest_file_ts: Option<u64>,
    pub oldest_age_s: Option<u64>,
}

#[derive(Debug)]
pub struct JsonlAlertSinkV1 {
    cfg: JsonlSinkConfigV1,
    current: Option<OpenJsonlFileV1>,
}

#[derive(Debug)]
pub struct SpoolingJsonlAlertSinkV1 {
    jsonl: JsonlAlertSinkV1,
    spool_cfg: SpoolConfigV1,
    counters: SpoolCountersV1,
}

#[derive(Debug)]
struct OpenJsonlFileV1 {
    tenant_id: String,
    device_key: String,
    ymd: YmdV1,
    seq: u32,
    path: PathBuf,
    writer: BufWriter<File>,
    bytes_written: u64,
    last_flush_ts: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct YmdV1 {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

impl JsonlAlertSinkV1 {
    pub fn new(cfg: JsonlSinkConfigV1) -> Self {
        Self { cfg, current: None }
    }

    pub fn emit_at_v1(&mut self, alert: &AlertV1, now_ts: i64) -> Result<PathBuf, SinkErrorV1> {
        self.ensure_open_v1(alert)?;
        if self.should_rotate_before_write_v1(alert) {
            self.rotate_for_alert_v1(alert)?;
        }

        let line = serde_json::to_vec(alert).map_err(|e| SinkErrorV1 {
            msg: format!("serialize alert json failed: {}", e),
        })?;
        let mut path = PathBuf::new();
        if let Some(current) = self.current.as_mut() {
            current.writer.write_all(&line).map_err(io_err_v1)?;
            current.writer.write_all(b"\n").map_err(io_err_v1)?;
            current.bytes_written = current.bytes_written.saturating_add((line.len() + 1) as u64);
            path = current.path.clone();
            if flush_due_v1(current.last_flush_ts, now_ts, self.cfg.jsonl_flush_interval_s) {
                current.writer.flush().map_err(io_err_v1)?;
                current.last_flush_ts = Some(now_ts);
            }
        }
        Ok(path)
    }

    pub fn flush_v1(&mut self) -> Result<(), SinkErrorV1> {
        if let Some(current) = self.current.as_mut() {
            current.writer.flush().map_err(io_err_v1)?;
        }
        Ok(())
    }

    pub fn shutdown_v1(&mut self) -> Result<(), SinkErrorV1> {
        self.flush_v1()?;
        self.current = None;
        Ok(())
    }

    fn ensure_open_v1(&mut self, alert: &AlertV1) -> Result<(), SinkErrorV1> {
        let alert_ymd = ymd_for_ts_v1(alert.window_start_ts)?;
        match self.current.as_ref() {
            Some(current)
                if current.tenant_id == alert.tenant_id
                    && current.device_key == alert.device_key
                    && current.ymd == alert_ymd =>
            {
                Ok(())
            }
            Some(_) => self.rotate_for_alert_v1(alert),
            None => self.open_for_alert_v1(alert, 0),
        }
    }

    fn should_rotate_before_write_v1(&self, alert: &AlertV1) -> bool {
        let Some(current) = self.current.as_ref() else {
            return false;
        };
        match ymd_for_ts_v1(alert.window_start_ts) {
            Ok(alert_ymd) => {
                if current.tenant_id != alert.tenant_id || current.device_key != alert.device_key || current.ymd != alert_ymd {
                    return true;
                }
            }
            Err(_) => return false,
        }
        current.bytes_written > jsonl_rotate_bytes_v1(self.cfg.jsonl_rotate_mb)
    }

    fn rotate_for_alert_v1(&mut self, alert: &AlertV1) -> Result<(), SinkErrorV1> {
        if let Some(current) = self.current.as_mut() {
            current.writer.flush().map_err(io_err_v1)?;
        }
        self.current = None;
        self.open_for_alert_v1(alert, 0)
    }

    fn open_for_alert_v1(&mut self, alert: &AlertV1, start_seq: u32) -> Result<(), SinkErrorV1> {
        let ymd = ymd_for_ts_v1(alert.window_start_ts)?;
        let dir = jsonl_day_dir_v1(&self.cfg.alert_out_root, &alert.tenant_id, &alert.device_key, alert.window_start_ts)?;
        ensure_dir_with_mode_v1(&dir, 0o750)?;

        let mut seq = start_seq;
        loop {
            let path = jsonl_alert_path_v1(&self.cfg.alert_out_root, &alert.tenant_id, &alert.device_key, alert.window_start_ts, seq)?;
            let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    seq = seq.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(io_err_v1(e)),
            };
            set_file_mode_v1(&path, 0o640)?;
            self.current = Some(OpenJsonlFileV1 {
                tenant_id: alert.tenant_id.clone(),
                device_key: alert.device_key.clone(),
                ymd,
                seq,
                path,
                writer: BufWriter::new(file),
                bytes_written: 0,
                last_flush_ts: None,
            });
            break;
        }
        Ok(())
    }

    pub fn current_path_v1(&self) -> Option<&Path> {
        self.current.as_ref().map(|current| current.path.as_path())
    }

    pub fn current_seq_v1(&self) -> Option<u32> {
        self.current.as_ref().map(|current| current.seq)
    }
}

impl AlertSinkV1 for JsonlAlertSinkV1 {
    fn emit(&mut self, alert: &AlertV1) -> Result<(), SinkErrorV1> {
        self.emit_at_v1(alert, alert.window_end_ts).map(|_| ())
    }
}

impl SpoolingJsonlAlertSinkV1 {
    pub fn new(jsonl_cfg: JsonlSinkConfigV1, spool_cfg: SpoolConfigV1) -> Self {
        Self {
            jsonl: JsonlAlertSinkV1::new(jsonl_cfg),
            spool_cfg,
            counters: SpoolCountersV1::default(),
        }
    }

    pub fn emit_at_v1(&mut self, alert: &AlertV1, now_ts: i64) -> Result<SpoolEmitOutcomeV1, SinkErrorV1> {
        match self.jsonl.emit_at_v1(alert, now_ts) {
            Ok(path) => Ok(SpoolEmitOutcomeV1::Delivered { path }),
            Err(_) => {
                let path = write_spool_alert_v1(&self.spool_cfg.data_root, alert)?;
                self.counters.sink_spool_total = self.counters.sink_spool_total.saturating_add(1);
                let cap_report = enforce_spool_cap_v1(&self.spool_cfg.data_root, self.spool_cfg.spool_max_mb)?;
                self.counters.sink_spool_drop_total = self
                    .counters
                    .sink_spool_drop_total
                    .saturating_add(cap_report.dropped_paths.len() as u64);
                Ok(SpoolEmitOutcomeV1::Spooled { path })
            }
        }
    }

    pub fn replay_spooled_alerts_v1(&mut self, now_ts: i64) -> Result<SpoolReplayReportV1, SinkErrorV1> {
        self.replay_spooled_alerts_limited_v1(now_ts, usize::MAX)
    }

    pub fn replay_spooled_alerts_limited_v1(&mut self, now_ts: i64, max_files: usize) -> Result<SpoolReplayReportV1, SinkErrorV1> {
        let mut spool_files = sorted_spool_files_for_replay_v1(&self.spool_cfg.data_root)?;
        if max_files != usize::MAX && spool_files.len() > max_files {
            spool_files.truncate(max_files);
        }
        let mut replayed_paths = Vec::new();
        let mut failed_paths = Vec::new();

        for path in spool_files {
            let alert = match read_spooled_alert_v1(&path) {
                Ok(alert) => alert,
                Err(_) => {
                    self.counters.sink_spool_replay_fail_total = self.counters.sink_spool_replay_fail_total.saturating_add(1);
                    failed_paths.push(path);
                    continue;
                }
            };

            match self.jsonl.emit_at_v1(&alert, now_ts) {
                Ok(_) => {
                    if self.jsonl.flush_v1().is_ok() && fs::remove_file(&path).is_ok() {
                        self.counters.sink_spool_replayed_total = self.counters.sink_spool_replayed_total.saturating_add(1);
                        replayed_paths.push(path);
                    } else {
                        self.counters.sink_spool_replay_fail_total = self.counters.sink_spool_replay_fail_total.saturating_add(1);
                        failed_paths.push(path);
                    }
                }
                Err(_) => {
                    self.counters.sink_spool_replay_fail_total = self.counters.sink_spool_replay_fail_total.saturating_add(1);
                    failed_paths.push(path);
                }
            }
        }

        Ok(SpoolReplayReportV1 {
            replayed_paths,
            failed_paths,
        })
    }

    pub fn enforce_spool_cap_now_v1(&mut self) -> Result<SpoolCapReportV1, SinkErrorV1> {
        let report = enforce_spool_cap_v1(&self.spool_cfg.data_root, self.spool_cfg.spool_max_mb)?;
        self.counters.sink_spool_drop_total = self
            .counters
            .sink_spool_drop_total
            .saturating_add(report.dropped_paths.len() as u64);
        Ok(report)
    }

    pub fn flush_v1(&mut self) -> Result<(), SinkErrorV1> {
        self.jsonl.flush_v1()
    }

    pub fn shutdown_v1(&mut self) -> Result<(), SinkErrorV1> {
        self.jsonl.shutdown_v1()
    }

    pub fn counters_v1(&self) -> &SpoolCountersV1 {
        &self.counters
    }
}

impl AlertSinkV1 for SpoolingJsonlAlertSinkV1 {
    fn emit(&mut self, alert: &AlertV1) -> Result<(), SinkErrorV1> {
        self.emit_at_v1(alert, alert.window_end_ts).map(|_| ())
    }
}

#[derive(Debug)]
pub struct StdoutAlertSinkV1<W: Write> {
    writer: W,
}

impl<W: Write> StdoutAlertSinkV1<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn emit_line_v1(&mut self, alert: &AlertV1) -> Result<(), SinkErrorV1> {
        let line = serde_json::to_vec(alert).map_err(|e| SinkErrorV1 {
            msg: format!("serialize alert json failed: {}", e),
        })?;
        self.writer.write_all(&line).map_err(io_err_v1)?;
        self.writer.write_all(b"\n").map_err(io_err_v1)?;
        self.writer.flush().map_err(io_err_v1)?;
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> AlertSinkV1 for StdoutAlertSinkV1<W> {
    fn emit(&mut self, alert: &AlertV1) -> Result<(), SinkErrorV1> {
        self.emit_line_v1(alert)
    }
}

pub fn jsonl_rotate_bytes_v1(rotate_mb: u32) -> u64 {
    u64::from(rotate_mb) * 1024 * 1024
}

pub fn spool_max_bytes_v1(spool_max_mb: u32) -> u64 {
    u64::from(spool_max_mb) * 1024 * 1024
}

pub fn jsonl_day_dir_v1(
    alert_out_root: &str,
    tenant_id: &str,
    device_key: &str,
    window_start_ts: i64,
) -> Result<PathBuf, SinkErrorV1> {
    validate_fs_component_v1("tenant_id", tenant_id)?;
    validate_fs_component_v1("device_key", device_key)?;
    let ymd = ymd_for_ts_v1(window_start_ts)?;
    let mut out = PathBuf::from(alert_out_root);
    out.push(format!("tenant={}", tenant_id));
    out.push(format!("device={}", device_key));
    out.push(format!("{:04}", ymd.year));
    out.push(format!("{:02}", ymd.month));
    out.push(format!("{:02}", ymd.day));
    Ok(out)
}

pub fn jsonl_file_name_v1(device_key: &str, window_start_ts: i64, seq: u32) -> Result<String, SinkErrorV1> {
    validate_fs_component_v1("device_key", device_key)?;
    let ymd = ymd_for_ts_v1(window_start_ts)?;
    Ok(format!(
        "alerts_{}_{:04}{:02}{:02}_{:04}.jsonl",
        device_key, ymd.year, ymd.month, ymd.day, seq
    ))
}

pub fn jsonl_alert_path_v1(
    alert_out_root: &str,
    tenant_id: &str,
    device_key: &str,
    window_start_ts: i64,
    seq: u32,
) -> Result<PathBuf, SinkErrorV1> {
    let mut out = jsonl_day_dir_v1(alert_out_root, tenant_id, device_key, window_start_ts)?;
    out.push(jsonl_file_name_v1(device_key, window_start_ts, seq)?);
    Ok(out)
}

pub fn spool_alert_dir_v1(data_root: &str, tenant_id: &str) -> Result<PathBuf, SinkErrorV1> {
    validate_fs_component_v1("tenant_id", tenant_id)?;
    let mut out = PathBuf::from(data_root);
    out.push("spool");
    out.push("alerts");
    out.push(format!("tenant={}", tenant_id));
    Ok(out)
}

pub fn spool_alert_file_name_v1(alert_id: &str) -> Result<String, SinkErrorV1> {
    validate_fs_component_v1("alert_id", alert_id)?;
    Ok(format!("spool_{}.json", alert_id))
}

pub fn spool_alert_path_v1(data_root: &str, tenant_id: &str, alert_id: &str) -> Result<PathBuf, SinkErrorV1> {
    let mut out = spool_alert_dir_v1(data_root, tenant_id)?;
    out.push(spool_alert_file_name_v1(alert_id)?);
    Ok(out)
}

pub fn write_spool_alert_v1(data_root: &str, alert: &AlertV1) -> Result<PathBuf, SinkErrorV1> {
    let dir = spool_alert_dir_v1(data_root, &alert.tenant_id)?;
    ensure_dir_with_mode_v1(&dir, 0o750)?;
    let path = spool_alert_path_v1(data_root, &alert.tenant_id, &alert.alert_id)?;
    let bytes = serde_json::to_vec(alert).map_err(|e| SinkErrorV1 {
        msg: format!("serialize alert json failed: {}", e),
    })?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(io_err_v1)?;
    file.write_all(&bytes).map_err(io_err_v1)?;
    set_file_mode_v1(&path, 0o640)?;
    Ok(path)
}

pub fn read_spooled_alert_v1(path: &Path) -> Result<AlertV1, SinkErrorV1> {
    let bytes = fs::read(path).map_err(io_err_v1)?;
    serde_json::from_slice(&bytes).map_err(|e| SinkErrorV1 {
        msg: format!("parse spooled alert json failed: {}", e),
    })
}

pub fn sorted_spool_files_for_replay_v1(data_root: &str) -> Result<Vec<PathBuf>, SinkErrorV1> {
    let mut files = collect_spool_files_v1(data_root)?;
    files.sort_by(|a, b| {
        let a_name = a.file_name().and_then(|v| v.to_str()).unwrap_or("");
        let b_name = b.file_name().and_then(|v| v.to_str()).unwrap_or("");
        a_name.cmp(b_name).then_with(|| a.cmp(b))
    });
    Ok(files)
}

pub fn spool_backlog_summary_v1(data_root: &str) -> Result<SpoolBacklogSummaryV1, SinkErrorV1> {
    let tenants = spool_backlog_per_tenant_v1(data_root)?;
    let mut total_files = 0u64;
    let mut total_bytes = 0u64;
    let mut oldest_file_ts = None;
    let mut oldest_age_s = None;
    for tenant in tenants {
        total_files = total_files.saturating_add(tenant.files);
        total_bytes = total_bytes.saturating_add(tenant.bytes);
        oldest_file_ts = min_option_u64_v1(oldest_file_ts, tenant.oldest_file_ts);
        oldest_age_s = max_option_u64_v1(oldest_age_s, tenant.oldest_age_s);
    }
    Ok(SpoolBacklogSummaryV1 {
        files: total_files,
        bytes: total_bytes,
        oldest_file_ts,
        oldest_age_s,
    })
}

pub fn spool_backlog_per_tenant_v1(data_root: &str) -> Result<Vec<SpoolBacklogTenantSummaryV1>, SinkErrorV1> {
    let root = PathBuf::from(data_root).join("spool").join("alerts");
    if !root.exists() {
        return Ok(Vec::new());
    }

    let now_ts = unix_now_ts_v1()?;
    let mut tenants = Vec::new();
    for tenant_entry in fs::read_dir(&root).map_err(io_err_v1)? {
        let tenant_entry = tenant_entry.map_err(io_err_v1)?;
        let tenant_type = tenant_entry.file_type().map_err(io_err_v1)?;
        if !tenant_type.is_dir() || tenant_type.is_symlink() {
            continue;
        }
        let tenant_path = tenant_entry.path();
        let Some(dir_name) = tenant_path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        let Some(tenant_id) = dir_name.strip_prefix("tenant=") else {
            continue;
        };
        let mut files = 0u64;
        let mut bytes = 0u64;
        let mut oldest_file_ts = None;
        for file_entry in fs::read_dir(&tenant_path).map_err(io_err_v1)? {
            let file_entry = file_entry.map_err(io_err_v1)?;
            let file_type = file_entry.file_type().map_err(io_err_v1)?;
            if !file_type.is_file() || file_type.is_symlink() {
                continue;
            }
            let file_path = file_entry.path();
            let Some(name) = file_path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            if !name.starts_with("spool_") || !name.ends_with(".json") {
                continue;
            }
            files = files.saturating_add(1);
            bytes = bytes.saturating_add(file_len_v1(&file_path)?);
            oldest_file_ts = min_option_u64_v1(oldest_file_ts, Some(file_mtime_ts_v1(&file_path)?));
        }
        if files == 0 {
            continue;
        }
        let oldest_age_s = oldest_file_ts.map(|ts| now_ts.saturating_sub(ts));
        tenants.push(SpoolBacklogTenantSummaryV1 {
            tenant_id: tenant_id.to_string(),
            files,
            bytes,
            oldest_file_ts,
            oldest_age_s,
        });
    }
    tenants.sort_by(|a, b| a.tenant_id.cmp(&b.tenant_id));
    Ok(tenants)
}

pub fn enforce_spool_cap_v1(data_root: &str, spool_max_mb: u32) -> Result<SpoolCapReportV1, SinkErrorV1> {
    let mut files = collect_spool_files_v1(data_root)?;
    files.sort();

    let mut bytes_before = 0u64;
    for path in &files {
        bytes_before = bytes_before.saturating_add(file_len_v1(path)?);
    }

    let max_bytes = spool_max_bytes_v1(spool_max_mb);
    let mut bytes_after = bytes_before;
    let mut dropped_paths = Vec::new();

    for path in files {
        if bytes_after <= max_bytes {
            break;
        }
        let len = file_len_v1(&path)?;
        fs::remove_file(&path).map_err(io_err_v1)?;
        bytes_after = bytes_after.saturating_sub(len);
        dropped_paths.push(path);
    }

    Ok(SpoolCapReportV1 {
        bytes_before,
        bytes_after,
        dropped_paths,
    })
}

pub fn ymd_for_ts_v1(ts: i64) -> Result<YmdV1, SinkErrorV1> {
    let dt = Utc.timestamp_opt(ts, 0).single().ok_or(SinkErrorV1 {
        msg: format!("invalid utc timestamp: {}", ts),
    })?;
    Ok(YmdV1 {
        year: dt.year(),
        month: dt.month(),
        day: dt.day(),
    })
}

fn flush_due_v1(last_flush_ts: Option<i64>, now_ts: i64, interval_s: u32) -> bool {
    match last_flush_ts {
        None => true,
        Some(last) => now_ts.saturating_sub(last) >= i64::from(interval_s),
    }
}

fn min_option_u64_v1(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn max_option_u64_v1(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn unix_now_ts_v1() -> Result<u64, SinkErrorV1> {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).map_err(|e| SinkErrorV1 {
        msg: format!("current time before unix epoch: {}", e),
    })?;
    Ok(duration.as_secs())
}

fn file_mtime_ts_v1(path: &Path) -> Result<u64, SinkErrorV1> {
    let modified = fs::metadata(path).map_err(io_err_v1)?.modified().map_err(io_err_v1)?;
    let duration = modified.duration_since(UNIX_EPOCH).map_err(|e| SinkErrorV1 {
        msg: format!("file modified time before unix epoch for {}: {}", path.display(), e),
    })?;
    Ok(duration.as_secs())
}

fn collect_spool_files_v1(data_root: &str) -> Result<Vec<PathBuf>, SinkErrorV1> {
    let root = PathBuf::from(data_root).join("spool").join("alerts");
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for tenant_entry in fs::read_dir(&root).map_err(io_err_v1)? {
        let tenant_entry = tenant_entry.map_err(io_err_v1)?;
        let tenant_type = tenant_entry.file_type().map_err(io_err_v1)?;
        if !tenant_type.is_dir() || tenant_type.is_symlink() {
            continue;
        }
        let tenant_path = tenant_entry.path();
        for file_entry in fs::read_dir(&tenant_path).map_err(io_err_v1)? {
            let file_entry = file_entry.map_err(io_err_v1)?;
            let file_type = file_entry.file_type().map_err(io_err_v1)?;
            if !file_type.is_file() || file_type.is_symlink() {
                continue;
            }
            let file_path = file_entry.path();
            let Some(name) = file_path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            if name.starts_with("spool_") && name.ends_with(".json") {
                files.push(file_path);
            }
        }
    }
    Ok(files)
}

fn file_len_v1(path: &Path) -> Result<u64, SinkErrorV1> {
    Ok(fs::metadata(path).map_err(io_err_v1)?.len())
}

fn ensure_dir_with_mode_v1(path: &Path, mode: u32) -> Result<(), SinkErrorV1> {
    fs::create_dir_all(path).map_err(io_err_v1)?;
    set_dir_mode_v1(path, mode)?;
    Ok(())
}

#[cfg(unix)]
fn set_dir_mode_v1(path: &Path, mode: u32) -> Result<(), SinkErrorV1> {
    use std::os::unix::fs::PermissionsExt;

    let perms = fs::Permissions::from_mode(mode);
    fs::set_permissions(path, perms).map_err(io_err_v1)
}

#[cfg(not(unix))]
fn set_dir_mode_v1(_path: &Path, _mode: u32) -> Result<(), SinkErrorV1> {
    Ok(())
}

#[cfg(unix)]
fn set_file_mode_v1(path: &Path, mode: u32) -> Result<(), SinkErrorV1> {
    use std::os::unix::fs::PermissionsExt;

    let perms = fs::Permissions::from_mode(mode);
    fs::set_permissions(path, perms).map_err(io_err_v1)
}

#[cfg(not(unix))]
fn set_file_mode_v1(_path: &Path, _mode: u32) -> Result<(), SinkErrorV1> {
    Ok(())
}

fn validate_fs_component_v1(field: &str, value: &str) -> Result<(), SinkErrorV1> {
    if value.is_empty() || value == "." || value == ".." {
        return Err(SinkErrorV1 {
            msg: format!("invalid {} filesystem component", field),
        });
    }
    if value.bytes().any(|b| b == b'/' || b == b'\\' || b < 0x20 || b == 0x7f) {
        return Err(SinkErrorV1 {
            msg: format!("invalid {} filesystem component", field),
        });
    }
    Ok(())
}

fn io_err_v1(err: std::io::Error) -> SinkErrorV1 {
    SinkErrorV1 { msg: err.to_string() }
}
