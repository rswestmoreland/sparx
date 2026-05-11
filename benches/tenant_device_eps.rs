// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Tenant/device EPS benchmark.
//
// This custom bench target intentionally avoids external benchmark crates. It
// creates a deterministic multi-tenant, multi-device corpus, then reports two
// separate throughput metrics:
//
// - ingestion EPS: file scan, line read, syslog parse, tokenization, feature
//   emission, dictionary resolution, and sparse row/window population
// - detection EPS: alert scoring/build/encoding over the finalized sparse rows
//
// Optional durable oneshot timing can be enabled for storage-inclusive checks,
// but it is not part of the default benchmark because it measures a different
// end-to-end runtime cost profile.

use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use sparx::alert::{build_alert_v1, AlertScoringConfigV1};
use sparx::baseline::{BucketBaselineV1, CentroidPairV1, DfPairV1};
use sparx::cli::route::route_command_v1;
use sparx::cli::{CommandV1, MigrateModeV1};
use sparx::config::load::default_config_v1;
use sparx::config::validate::validate_config_v1;
use sparx::features::{emit_line_features_v1, FeatureDictionaryConfigV1, FeatureDictionaryV1};
use sparx::ingest::device_key_v1;
use sparx::tokenize::{parse_syslog_envelope_v1, tokenize_message_v1};
use sparx::window::{
    align_window_start_ts_v1, FinalizedWindowRowV1, WindowAccumulatorV1, WindowApplyLineResultV1,
    WindowCapsV1,
};

const DEFAULT_TENANTS: usize = 2;
const DEFAULT_DEVICES_PER_TENANT: usize = 5;
const DEFAULT_FILES_PER_DEVICE: usize = 2;
const DEFAULT_EVENTS_PER_FILE: usize = 500;
const DEFAULT_READ_CHUNK_BYTES: u32 = 262_144;
const DEFAULT_EVENTS_PER_TIMESTAMP: usize = 100;
const MAX_TOTAL_EVENTS: usize = 5_000_000;

#[derive(Clone, Debug)]
struct TenantDeviceEpsBenchConfigV1 {
    tenants: usize,
    devices_per_tenant: usize,
    files_per_device: usize,
    events_per_file: usize,
    read_chunk_bytes: u32,
    events_per_timestamp: usize,
    source_stream_enabled: bool,
    durable_oneshot_enabled: bool,
    keep_root: bool,
}

impl TenantDeviceEpsBenchConfigV1 {
    fn from_env_v1() -> Result<Self, String> {
        let cfg = Self {
            tenants: env_usize_v1("SPARX_BENCH_TENANTS", DEFAULT_TENANTS)?,
            devices_per_tenant: env_usize_v1(
                "SPARX_BENCH_DEVICES_PER_TENANT",
                DEFAULT_DEVICES_PER_TENANT,
            )?,
            files_per_device: env_usize_v1(
                "SPARX_BENCH_FILES_PER_DEVICE",
                DEFAULT_FILES_PER_DEVICE,
            )?,
            events_per_file: env_usize_v1("SPARX_BENCH_EVENTS_PER_FILE", DEFAULT_EVENTS_PER_FILE)?,
            read_chunk_bytes: env_u32_v1("SPARX_BENCH_READ_CHUNK_BYTES", DEFAULT_READ_CHUNK_BYTES)?,
            events_per_timestamp: env_usize_v1(
                "SPARX_BENCH_EVENTS_PER_TIMESTAMP",
                DEFAULT_EVENTS_PER_TIMESTAMP,
            )?,
            source_stream_enabled: env_bool_v1("SPARX_BENCH_SOURCE_STREAM", false)?,
            durable_oneshot_enabled: env_bool_v1("SPARX_BENCH_DURABLE_ONESHOT", false)?,
            keep_root: env_bool_v1("SPARX_BENCH_KEEP_ROOT", false)?,
        };
        cfg.validate_v1()?;
        Ok(cfg)
    }

    fn validate_v1(&self) -> Result<(), String> {
        let total_events = self.total_events_v1()?;
        if total_events > MAX_TOTAL_EVENTS {
            return Err(format!(
                "benchmark corpus too large: total_events={} max_total_events={}",
                total_events, MAX_TOTAL_EVENTS
            ));
        }
        if self.read_chunk_bytes == 0 {
            return Err("SPARX_BENCH_READ_CHUNK_BYTES must be greater than zero".to_string());
        }
        Ok(())
    }

    fn total_events_v1(&self) -> Result<usize, String> {
        self.tenants
            .checked_mul(self.devices_per_tenant)
            .and_then(|v| v.checked_mul(self.files_per_device))
            .and_then(|v| v.checked_mul(self.events_per_file))
            .ok_or_else(|| "benchmark corpus size overflow".to_string())
    }
}

#[derive(Clone, Debug)]
struct BenchSparseRowV1 {
    tenant_id: String,
    device_path: String,
    row: FinalizedWindowRowV1,
}

#[derive(Clone, Debug)]
struct IngestionProbeResultV1 {
    events: usize,
    bytes: u64,
    sparse_rows: Vec<BenchSparseRowV1>,
    dictionary: FeatureDictionaryV1,
}

#[derive(Clone, Copy, Debug)]
struct TimingV1 {
    elapsed_s: f64,
    eps: f64,
}

#[derive(Clone, Debug)]
struct DetectionProbeResultV1 {
    rows_evaluated: usize,
    events_represented: usize,
    alerts_emitted: usize,
    encoded_alert_bytes: usize,
}

fn main() {
    if let Err(e) = run_tenant_device_eps_bench_v1() {
        eprintln!("sparx EPS benchmark failed: {}", e);
        std::process::exit(1);
    }
}

fn run_tenant_device_eps_bench_v1() -> Result<(), String> {
    let bench_cfg = TenantDeviceEpsBenchConfigV1::from_env_v1()?;
    let total_events = bench_cfg.total_events_v1()?;
    let root = unique_temp_root_v1()?;
    fs::create_dir_all(&root).map_err(|e| format!("create bench root failed: {}", e))?;

    let mut cfg = default_config_v1();
    cfg.sparx.data_root = root.join("state").display().to_string();
    cfg.sparx.tenant_root = root.join("watch").display().to_string();
    cfg.sparx.global_db_path = root.join("state/global.db").display().to_string();
    cfg.sparx.tenant_db_root = root.join("state/tenants").display().to_string();
    cfg.sparx.alert_out_root = root.join("state/alerts").display().to_string();
    cfg.sparx.pid_file = None;
    cfg.sparx.mode = "oneshot".to_string();
    cfg.output.sink = "jsonl".to_string();
    cfg.output.jsonl_flush_interval_s = 0;
    cfg.ingest.read_chunk_bytes = bench_cfg.read_chunk_bytes;
    cfg.metrics.prometheus_enabled = false;
    cfg.metrics.health_enabled = false;
    cfg.vdrop.source_stream_enabled = bench_cfg.source_stream_enabled;

    validate_config_v1(&cfg).map_err(|e| format!("bench config invalid: {}", e.msg))?;
    write_bench_corpus_v1(&cfg.sparx.tenant_root, &bench_cfg)?;

    let ingest_start = Instant::now();
    let ingest_result = run_ingestion_probe_v1(&cfg, &bench_cfg)?;
    let ingest_timing = timing_v1(ingest_result.events, ingest_start.elapsed().as_secs_f64());

    let detect_start = Instant::now();
    let detect_result = run_detection_probe_v1(&cfg, &ingest_result)?;
    let detect_elapsed_s = detect_start.elapsed().as_secs_f64();
    let detection_event_eps = eps_v1(detect_result.events_represented, detect_elapsed_s);
    let detection_row_eps = eps_v1(detect_result.rows_evaluated, detect_elapsed_s);
    let detection_alert_eps = eps_v1(detect_result.alerts_emitted, detect_elapsed_s);

    let durable_timing = if bench_cfg.durable_oneshot_enabled {
        let durable_start = Instant::now();
        run_durable_oneshot_probe_v1(&cfg, &bench_cfg)?;
        Some(timing_v1(total_events, durable_start.elapsed().as_secs_f64()))
    } else {
        None
    };

    println!("sparx tenant/device EPS benchmark");
    println!("tenants={}", bench_cfg.tenants);
    println!("devices_per_tenant={}", bench_cfg.devices_per_tenant);
    println!("files_per_device={}", bench_cfg.files_per_device);
    println!("events_per_file={}", bench_cfg.events_per_file);
    println!("total_events={}", total_events);
    println!("events_per_timestamp={}", bench_cfg.events_per_timestamp);
    println!(
        "approx_event_time_span_s_per_file={}",
        bench_cfg
            .events_per_file
            .div_ceil(bench_cfg.events_per_timestamp)
    );
    println!("read_chunk_bytes={}", bench_cfg.read_chunk_bytes);
    println!("source_stream_enabled={}", bench_cfg.source_stream_enabled);
    println!(
        "durable_oneshot_enabled={}",
        bench_cfg.durable_oneshot_enabled
    );
    println!("ingest_events={}", ingest_result.events);
    println!("ingest_bytes={}", ingest_result.bytes);
    println!("ingest_sparse_rows={}", ingest_result.sparse_rows.len());
    println!("ingest_elapsed_s={:.6}", ingest_timing.elapsed_s);
    println!("ingest_eps={:.2}", ingest_timing.eps);
    println!("detection_events={}", detect_result.events_represented);
    println!("detection_sparse_rows={}", detect_result.rows_evaluated);
    println!("detection_alerts_emitted={}", detect_result.alerts_emitted);
    println!("detection_encoded_alert_bytes={}", detect_result.encoded_alert_bytes);
    println!("detection_elapsed_s={:.6}", detect_elapsed_s);
    println!("detection_event_eps={:.2}", detection_event_eps);
    println!("detection_row_eps={:.2}", detection_row_eps);
    println!("detection_alert_eps={:.2}", detection_alert_eps);
    if let Some(timing) = durable_timing {
        println!("durable_oneshot_elapsed_s={:.6}", timing.elapsed_s);
        println!("durable_oneshot_total_eps={:.2}", timing.eps);
    }

    if bench_cfg.keep_root {
        println!("bench_root={}", root.display());
    } else {
        fs::remove_dir_all(&root).map_err(|e| format!("remove bench root failed: {}", e))?;
    }
    Ok(())
}

fn run_ingestion_probe_v1(
    cfg: &sparx::config::ConfigV1,
    bench_cfg: &TenantDeviceEpsBenchConfigV1,
) -> Result<IngestionProbeResultV1, String> {
    let mut dict = FeatureDictionaryV1::new_empty_v1(
        FeatureDictionaryConfigV1::from(&cfg.features),
        1,
        0,
    );
    let caps = WindowCapsV1::from(&cfg.caps);
    let mut sparse_rows = Vec::new();
    let mut events = 0usize;
    let mut bytes = 0u64;

    for tenant_idx in 0..bench_cfg.tenants {
        let tenant_id = tenant_id_v1(tenant_idx);
        for device_idx in 0..bench_cfg.devices_per_tenant {
            let device_path = device_id_v1(device_idx);
            let device_key = device_key_v1(&tenant_id, &device_path);
            let mut acc: Option<WindowAccumulatorV1> = None;
            for file_idx in 0..bench_cfg.files_per_device {
                let file_path = Path::new(&cfg.sparx.tenant_root)
                    .join(&tenant_id)
                    .join(&device_path)
                    .join(format!("app{:02}.log", file_idx));
                read_ingest_probe_file_v1(
                    &file_path,
                    &tenant_id,
                    &device_path,
                    &device_key,
                    cfg.ingest.window_size_s,
                    caps.clone(),
                    &mut dict,
                    &mut acc,
                    &mut sparse_rows,
                    &mut events,
                    &mut bytes,
                )?;
            }
            if let Some(acc) = acc.take() {
                sparse_rows.push(BenchSparseRowV1 {
                    tenant_id: tenant_id.clone(),
                    device_path: device_path.clone(),
                    row: acc.finalize_idle_v1().finalized_row,
                });
            }
        }
    }

    Ok(IngestionProbeResultV1 {
        events,
        bytes,
        sparse_rows,
        dictionary: dict,
    })
}

#[allow(clippy::too_many_arguments)]
fn read_ingest_probe_file_v1(
    file_path: &Path,
    tenant_id: &str,
    device_path: &str,
    device_key: &str,
    window_size_s: u32,
    caps: WindowCapsV1,
    dict: &mut FeatureDictionaryV1,
    acc: &mut Option<WindowAccumulatorV1>,
    sparse_rows: &mut Vec<BenchSparseRowV1>,
    events: &mut usize,
    bytes: &mut u64,
) -> Result<(), String> {
    let file = File::open(file_path).map_err(|e| format!("open log file failed: {}", e))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .map_err(|e| format!("read log line failed: {}", e))?;
        if read == 0 {
            break;
        }
        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
        *bytes = bytes.saturating_add(read as u64);
        *events = events.saturating_add(1);
        apply_ingest_probe_line_v1(
            tenant_id,
            device_path,
            device_key,
            window_size_s,
            caps.clone(),
            dict,
            acc,
            sparse_rows,
            trimmed,
            read,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_ingest_probe_line_v1(
    tenant_id: &str,
    device_path: &str,
    device_key: &str,
    window_size_s: u32,
    caps: WindowCapsV1,
    dict: &mut FeatureDictionaryV1,
    acc: &mut Option<WindowAccumulatorV1>,
    sparse_rows: &mut Vec<BenchSparseRowV1>,
    line: &str,
    line_bytes: usize,
) -> Result<(), String> {
    let parsed = parse_syslog_envelope_v1(line, 0);
    let line_ts = parsed.envelope.ts_guess.unwrap_or(0);
    let tokenized = tokenize_message_v1(&parsed.msg, None);
    let emitted = emit_line_features_v1(&parsed.envelope, &tokenized.events);
    let window_start_ts = align_window_start_ts_v1(line_ts, window_size_s)
        .map_err(|e| format!("align window failed: {:?}", e))?;

    if acc.is_none() {
        *acc = Some(
            WindowAccumulatorV1::new_v1(device_key, window_start_ts, 1, window_size_s, line_ts, caps)
                .map_err(|e| format!("create window accumulator failed: {:?}", e))?,
        );
    }

    loop {
        let active = acc
            .as_mut()
            .ok_or_else(|| "window accumulator missing after initialization".to_string())?;
        match active
            .apply_line_v1(line_ts, line_ts, line_bytes, &emitted, dict)
            .map_err(|e| format!("apply line failed: {:?}", e))?
        {
            WindowApplyLineResultV1::Applied(_) => return Ok(()),
            WindowApplyLineResultV1::DifferentWindow {
                line_window_start_ts,
            } => {
                let active = acc
                    .take()
                    .ok_or_else(|| "window accumulator missing before finalize".to_string())?;
                let (plan, next) = active
                    .finalize_and_advance_v1(line_window_start_ts, line_ts)
                    .map_err(|e| format!("finalize and advance failed: {:?}", e))?;
                sparse_rows.push(BenchSparseRowV1 {
                    tenant_id: tenant_id.to_string(),
                    device_path: device_path.to_string(),
                    row: plan.finalized_row,
                });
                *acc = Some(next);
            }
        }
    }
}

fn run_detection_probe_v1(
    cfg: &sparx::config::ConfigV1,
    ingest: &IngestionProbeResultV1,
) -> Result<DetectionProbeResultV1, String> {
    let mut alert_cfg = AlertScoringConfigV1::from_sections_v1(&cfg.scoring, cfg.ingest.window_size_s);
    alert_cfg.cold_start_days = 0;
    alert_cfg.cold_start_min_windows = 0;
    alert_cfg.min_lines_per_window = 1;
    alert_cfg.outlier_threshold = 0.0;
    alert_cfg.noise_threshold = 0.0;
    alert_cfg.info_threshold = 0.0;

    let mut result = DetectionProbeResultV1 {
        rows_evaluated: 0,
        events_represented: 0,
        alerts_emitted: 0,
        encoded_alert_bytes: 0,
    };

    for sparse_row in &ingest.sparse_rows {
        let baseline = baseline_from_row_v1(&sparse_row.row);
        let alert_result = build_alert_v1(
            &sparse_row.tenant_id,
            &format!("{}/{}", sparse_row.tenant_id, sparse_row.device_path),
            &sparse_row.row,
            &ingest.dictionary,
            &baseline,
            None,
            &alert_cfg,
            &[],
        )
        .map_err(|e| format!("alert build failed: {:?}", e))?;

        result.rows_evaluated = result.rows_evaluated.saturating_add(1);
        result.events_represented = result
            .events_represented
            .saturating_add(sparse_row.row.meta.lines as usize);
        if alert_result.alert.is_some() {
            result.alerts_emitted = result.alerts_emitted.saturating_add(1);
        }
        if let Some(kv) = alert_result.primary_put {
            result.encoded_alert_bytes = result.encoded_alert_bytes.saturating_add(kv.value.len());
        }
    }

    Ok(result)
}

fn baseline_from_row_v1(row: &FinalizedWindowRowV1) -> BucketBaselineV1 {
    let df = row
        .sparse_counts
        .iter()
        .map(|pair| DfPairV1 {
            feature_id: pair.feature_id,
            df_count: 1,
        })
        .collect();
    let centroid = row
        .sparse_counts
        .iter()
        .map(|pair| CentroidPairV1 {
            feature_id: pair.feature_id,
            value: 0.1,
        })
        .collect();
    BucketBaselineV1 {
        bucket: row.key.bucket,
        n_bucket: 100,
        df,
        centroid,
    }
}

fn run_durable_oneshot_probe_v1(
    cfg: &sparx::config::ConfigV1,
    bench_cfg: &TenantDeviceEpsBenchConfigV1,
) -> Result<(), String> {
    for tenant_idx in 0..bench_cfg.tenants {
        let tenant_id = tenant_id_v1(tenant_idx);
        let result = route_command_v1(
            &CommandV1::OneShot {
                tenant_id: tenant_id.clone(),
                since: None,
                until: None,
                device_path: None,
                migrate: MigrateModeV1::Auto,
            },
            cfg,
        );
        if result.exit_code != 0 {
            return Err(format!(
                "oneshot failed for tenant={} exit_code={} stderr={}",
                tenant_id,
                result.exit_code,
                result.msg_stderr.unwrap_or_else(|| "<none>".to_string())
            ));
        }
    }
    Ok(())
}

fn timing_v1(events: usize, elapsed_s: f64) -> TimingV1 {
    TimingV1 {
        elapsed_s,
        eps: eps_v1(events, elapsed_s),
    }
}

fn eps_v1(events: usize, elapsed_s: f64) -> f64 {
    if elapsed_s > 0.0 {
        events as f64 / elapsed_s
    } else {
        0.0
    }
}

fn write_bench_corpus_v1(
    tenant_root: &str,
    bench_cfg: &TenantDeviceEpsBenchConfigV1,
) -> Result<(), String> {
    for tenant_idx in 0..bench_cfg.tenants {
        let tenant_id = tenant_id_v1(tenant_idx);
        for device_idx in 0..bench_cfg.devices_per_tenant {
            let device_id = device_id_v1(device_idx);
            let device_dir = Path::new(tenant_root).join(&tenant_id).join(&device_id);
            fs::create_dir_all(&device_dir)
                .map_err(|e| format!("create device dir failed: {}", e))?;
            for file_idx in 0..bench_cfg.files_per_device {
                let file_path = device_dir.join(format!("app{:02}.log", file_idx));
                write_device_file_v1(
                    &file_path,
                    tenant_idx,
                    device_idx,
                    file_idx,
                    bench_cfg.events_per_file,
                    bench_cfg.events_per_timestamp,
                )?;
            }
        }
    }
    Ok(())
}

fn write_device_file_v1(
    file_path: &Path,
    tenant_idx: usize,
    device_idx: usize,
    file_idx: usize,
    events_per_file: usize,
    events_per_timestamp: usize,
) -> Result<(), String> {
    let file = File::create(file_path).map_err(|e| format!("create log file failed: {}", e))?;
    let mut writer = BufWriter::new(file);
    for event_idx in 0..events_per_file {
        let event_second = file_idx
            .saturating_mul(events_per_file)
            .saturating_add(event_idx)
            / events_per_timestamp;
        let ts = event_timestamp_v1(event_second);
        let action = match event_idx % 5 {
            0 => "login",
            1 => "query",
            2 => "update",
            3 => "download",
            _ => "logout",
        };
        let result = if event_idx % 97 == 0 {
            "failure"
        } else {
            "success"
        };
        let src_a = 10 + (tenant_idx % 100);
        let src_b = device_idx % 250;
        let src_c = event_idx % 250;
        let dst_a = 172;
        let dst_b = 16 + (file_idx % 16);
        let dst_c = event_idx % 250;
        writeln!(
            writer,
            "<34>1 {} edge{:03} app{:02} {} ID{:04} - src_ip={}.{}.{}.{} dst_ip={}.{}.{}.{} user=user{:04} action={} result={} bytes={} path=/api/v{}/resource/{} status={}",
            ts,
            device_idx,
            file_idx,
            1000 + file_idx,
            event_idx % 10_000,
            src_a,
            src_b,
            src_c,
            10 + (event_idx % 200),
            dst_a,
            dst_b,
            dst_c,
            20 + (device_idx % 200),
            event_idx % 1000,
            action,
            result,
            200 + (event_idx % 4000),
            1 + (event_idx % 3),
            event_idx % 100,
            if result == "success" { 200 } else { 403 }
        )
        .map_err(|e| format!("write log line failed: {}", e))?;
    }
    writer
        .flush()
        .map_err(|e| format!("flush log file failed: {}", e))
}

fn event_timestamp_v1(event_idx: usize) -> String {
    let total_seconds = event_idx % (28 * 24 * 60 * 60);
    let day = 1 + total_seconds / 86_400;
    let rem_day = total_seconds % 86_400;
    let hour = rem_day / 3_600;
    let rem_hour = rem_day % 3_600;
    let minute = rem_hour / 60;
    let second = rem_hour % 60;
    format!(
        "2099-01-{:02}T{:02}:{:02}:{:02}Z",
        day, hour, minute, second
    )
}

fn tenant_id_v1(idx: usize) -> String {
    format!("tenant{:03}", idx)
}

fn device_id_v1(idx: usize) -> String {
    format!("edge{:03}", idx)
}

fn unique_temp_root_v1() -> Result<PathBuf, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system time error: {}", e))?;
    Ok(env::temp_dir().join(format!(
        "sparx-tenant-device-eps-{}-{}",
        std::process::id(),
        now.as_nanos()
    )))
}

fn env_usize_v1(name: &str, default_value: usize) -> Result<usize, String> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value
                .trim()
                .parse::<usize>()
                .map_err(|_| format!("{} must be a positive integer", name))?;
            if parsed == 0 {
                return Err(format!("{} must be greater than zero", name));
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(e) => Err(format!("{} read failed: {}", name, e)),
    }
}

fn env_u32_v1(name: &str, default_value: u32) -> Result<u32, String> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value
                .trim()
                .parse::<u32>()
                .map_err(|_| format!("{} must be a positive integer", name))?;
            if parsed == 0 {
                return Err(format!("{} must be greater than zero", name));
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(e) => Err(format!("{} read failed: {}", name, e)),
    }
}

fn env_bool_v1(name: &str, default_value: bool) -> Result<bool, String> {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" | "on" => Ok(true),
            "0" | "false" | "no" | "n" | "off" => Ok(false),
            _ => Err(format!("{} must be a boolean", name)),
        },
        Err(env::VarError::NotPresent) => Ok(default_value),
        Err(e) => Err(format!("{} read failed: {}", name, e)),
    }
}
