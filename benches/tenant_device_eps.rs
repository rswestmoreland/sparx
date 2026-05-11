// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// End-to-end tenant/device EPS benchmark.
//
// This custom bench target intentionally avoids external benchmark crates. It
// creates a deterministic multi-tenant, multi-device corpus, runs the existing
// oneshot runtime path, and reports total processed events per second.

use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use sparx::cli::route::route_command_v1;
use sparx::cli::{CommandV1, MigrateModeV1};
use sparx::config::load::default_config_v1;
use sparx::config::validate::validate_config_v1;

const DEFAULT_TENANTS: usize = 2;
const DEFAULT_DEVICES_PER_TENANT: usize = 8;
const DEFAULT_FILES_PER_DEVICE: usize = 2;
const DEFAULT_EVENTS_PER_FILE: usize = 2_000;
const DEFAULT_READ_CHUNK_BYTES: u32 = 262_144;
const MAX_TOTAL_EVENTS: usize = 5_000_000;

#[derive(Clone, Debug)]
struct TenantDeviceEpsBenchConfigV1 {
    tenants: usize,
    devices_per_tenant: usize,
    files_per_device: usize,
    events_per_file: usize,
    read_chunk_bytes: u32,
    source_stream_enabled: bool,
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
            source_stream_enabled: env_bool_v1("SPARX_BENCH_SOURCE_STREAM", false)?,
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

    let start = Instant::now();
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
            &cfg,
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
    let elapsed = start.elapsed();
    let elapsed_s = elapsed.as_secs_f64();
    let total_eps = if elapsed_s > 0.0 {
        total_events as f64 / elapsed_s
    } else {
        0.0
    };

    println!("sparx tenant/device EPS benchmark");
    println!("tenants={}", bench_cfg.tenants);
    println!("devices_per_tenant={}", bench_cfg.devices_per_tenant);
    println!("files_per_device={}", bench_cfg.files_per_device);
    println!("events_per_file={}", bench_cfg.events_per_file);
    println!("total_events={}", total_events);
    println!("elapsed_s={:.6}", elapsed_s);
    println!("total_eps={:.2}", total_eps);
    println!("read_chunk_bytes={}", bench_cfg.read_chunk_bytes);
    println!("source_stream_enabled={}", bench_cfg.source_stream_enabled);

    if bench_cfg.keep_root {
        println!("bench_root={}", root.display());
    } else {
        fs::remove_dir_all(&root).map_err(|e| format!("remove bench root failed: {}", e))?;
    }
    Ok(())
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
) -> Result<(), String> {
    let file = File::create(file_path).map_err(|e| format!("create log file failed: {}", e))?;
    let mut writer = BufWriter::new(file);
    for event_idx in 0..events_per_file {
        let ts = event_timestamp_v1(
            file_idx
                .saturating_mul(events_per_file)
                .saturating_add(event_idx),
        );
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
