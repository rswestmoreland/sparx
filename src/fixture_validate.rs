// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Fixture corpus validation helpers.
// See: contracts/07_cli_contract_v0_1.md and contracts/12_fixture_corpus_v0_1.md
// Validates fixture layout and sample files deterministically.
// ASCII-only.

use std::fs;
use std::io::Read;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixtureValidationReportV1 {
    pub root: String,
    pub tenant_count: usize,
    pub device_file_count: usize,
    pub golden_file_count: usize,
    pub gen_file_count: usize,
    pub errors: Vec<String>,
}

impl FixtureValidationReportV1 {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

pub fn validate_fixture_root_v1(root: &Path) -> Result<FixtureValidationReportV1, String> {
    let md = fs::metadata(root).map_err(|e| format!("fixture_root metadata failed: {}", e))?;
    if !md.is_dir() {
        return Err("fixture_root is not a directory".to_string());
    }

    let mut report = FixtureValidationReportV1 {
        root: root.to_string_lossy().to_string(),
        tenant_count: 0,
        device_file_count: 0,
        golden_file_count: 0,
        gen_file_count: 0,
        errors: Vec::new(),
    };

    let tenants_dir = root.join("tenants");
    let golden_dir = root.join("golden");
    let gen_dir = root.join("gen");

    validate_required_dir_v1(&tenants_dir, "tenants", &mut report.errors)?;
    validate_required_dir_v1(&golden_dir, "golden", &mut report.errors)?;
    validate_required_dir_v1(&gen_dir, "gen", &mut report.errors)?;

    if report.errors.is_empty() {
        validate_tenants_dir_v1(&tenants_dir, &mut report)?;
        report.golden_file_count = validate_support_tree_v1(&golden_dir, "golden", &mut report.errors)?;
        report.gen_file_count = validate_support_tree_v1(&gen_dir, "gen", &mut report.errors)?;
    }

    Ok(report)
}

fn validate_required_dir_v1(path: &Path, label: &str, errors: &mut Vec<String>) -> Result<(), String> {
    match fs::metadata(path) {
        Ok(md) => {
            if !md.is_dir() {
                errors.push(format!("{} path is not a directory: {}", label, path.display()));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            errors.push(format!("missing required directory: {}", path.display()));
            Ok(())
        }
        Err(e) => Err(format!("{} metadata failed: {}", path.display(), e)),
    }
}

fn validate_tenants_dir_v1(tenants_dir: &Path, report: &mut FixtureValidationReportV1) -> Result<(), String> {
    let tenant_entries = sorted_dir_entries_v1(tenants_dir)?;
    if tenant_entries.is_empty() {
        report.errors.push(format!("no tenant fixture directories under {}", tenants_dir.display()));
        return Ok(());
    }

    for entry in tenant_entries {
        let tenant_path = entry.path();
        let file_type = entry.file_type().map_err(|e| format!("{} file_type failed: {}", tenant_path.display(), e))?;
        if !file_type.is_dir() {
            report.errors.push(format!("non-directory entry under tenants: {}", tenant_path.display()));
            continue;
        }
        report.tenant_count += 1;

        let devices_dir = tenant_path.join("devices");
        match fs::metadata(&devices_dir) {
            Ok(md) => {
                if !md.is_dir() {
                    report.errors.push(format!("devices path is not a directory: {}", devices_dir.display()));
                    continue;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                report.errors.push(format!("missing devices directory: {}", devices_dir.display()));
                continue;
            }
            Err(e) => return Err(format!("{} metadata failed: {}", devices_dir.display(), e)),
        }

        let device_entries = sorted_dir_entries_v1(&devices_dir)?;
        if device_entries.is_empty() {
            report.errors.push(format!("no fixture files under {}", devices_dir.display()));
            continue;
        }

        for device_entry in device_entries {
            let path = device_entry.path();
            let file_type = device_entry.file_type().map_err(|e| format!("{} file_type failed: {}", path.display(), e))?;
            if !file_type.is_file() {
                report.errors.push(format!("non-file entry under devices: {}", path.display()));
                continue;
            }

            let ext = match path.extension().and_then(|s| s.to_str()) {
                Some(v) => v,
                None => {
                    report.errors.push(format!("fixture file missing supported extension: {}", path.display()));
                    continue;
                }
            };
            if !matches!(ext, "log" | "gz" | "jsonl" | "csv" | "cef") {
                report.errors.push(format!("unsupported fixture extension .{}: {}", ext, path.display()));
                continue;
            }
            if path.file_stem().and_then(|s| s.to_str()).unwrap_or("").is_empty() {
                report.errors.push(format!("fixture file has empty device name: {}", path.display()));
                continue;
            }

            validate_fixture_file_v1(&path, ext, &mut report.errors)?;
            report.device_file_count += 1;
        }
    }

    Ok(())
}

fn validate_fixture_file_v1(path: &Path, ext: &str, errors: &mut Vec<String>) -> Result<(), String> {
    let md = fs::metadata(path).map_err(|e| format!("{} metadata failed: {}", path.display(), e))?;
    if md.len() == 0 {
        errors.push(format!("empty fixture file: {}", path.display()));
        return Ok(());
    }

    match ext {
        "gz" => {
            let file = fs::File::open(path).map_err(|e| format!("{} open failed: {}", path.display(), e))?;
            let mut decoder = flate2::read::GzDecoder::new(file);
            let mut buf = [0u8; 1];
            match decoder.read(&mut buf) {
                Ok(0) => errors.push(format!("gzip fixture has empty payload: {}", path.display())),
                Ok(_) => {}
                Err(e) => errors.push(format!("invalid gzip fixture: {}: {}", path.display(), e)),
            }
        }
        "jsonl" => validate_jsonl_file_v1(path, errors)?,
        "csv" => validate_csv_file_v1(path, errors)?,
        "cef" => validate_cef_file_v1(path, errors)?,
        "log" => {}
        _ => {}
    }

    Ok(())
}

fn validate_jsonl_file_v1(path: &Path, errors: &mut Vec<String>) -> Result<(), String> {
    let s = fs::read_to_string(path).map_err(|e| format!("{} read failed: {}", path.display(), e))?;
    let mut non_empty = 0usize;
    for (idx, line) in s.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        non_empty += 1;
        if let Err(e) = serde_json::from_str::<serde_json::Value>(trimmed) {
            errors.push(format!("invalid jsonl at {} line {}: {}", path.display(), idx + 1, e));
        }
    }
    if non_empty == 0 {
        errors.push(format!("jsonl fixture has no JSON records: {}", path.display()));
    }
    Ok(())
}

fn validate_csv_file_v1(path: &Path, errors: &mut Vec<String>) -> Result<(), String> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| format!("{} csv open failed: {}", path.display(), e))?;

    let header_len = rdr
        .headers()
        .map_err(|e| format!("{} csv headers failed: {}", path.display(), e))?
        .len();
    if header_len == 0 {
        errors.push(format!("csv fixture missing header columns: {}", path.display()));
        return Ok(());
    }

    for rec in rdr.records() {
        if let Err(e) = rec {
            errors.push(format!("invalid csv record in {}: {}", path.display(), e));
            break;
        }
    }

    Ok(())
}

fn validate_cef_file_v1(path: &Path, errors: &mut Vec<String>) -> Result<(), String> {
    let s = fs::read_to_string(path).map_err(|e| format!("{} read failed: {}", path.display(), e))?;
    let first = s.lines().find(|line| !line.trim().is_empty());
    match first {
        Some(line) if line.contains("CEF:") => Ok(()),
        Some(_) => {
            errors.push(format!("cef fixture missing CEF: marker: {}", path.display()));
            Ok(())
        }
        None => {
            errors.push(format!("cef fixture has no non-empty lines: {}", path.display()));
            Ok(())
        }
    }
}

fn validate_support_tree_v1(root: &Path, label: &str, errors: &mut Vec<String>) -> Result<usize, String> {
    let mut count = 0usize;
    validate_support_tree_inner_v1(root, label, errors, &mut count)?;
    Ok(count)
}

fn validate_support_tree_inner_v1(
    dir: &Path,
    label: &str,
    errors: &mut Vec<String>,
    count: &mut usize,
) -> Result<(), String> {
    for entry in sorted_dir_entries_v1(dir)? {
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| format!("{} file_type failed: {}", path.display(), e))?;
        if file_type.is_dir() {
            validate_support_tree_inner_v1(&path, label, errors, count)?;
            continue;
        }
        if !file_type.is_file() {
            errors.push(format!("non-file entry under {} tree: {}", label, path.display()));
            continue;
        }
        let md = fs::metadata(&path).map_err(|e| format!("{} metadata failed: {}", path.display(), e))?;
        if md.len() == 0 {
            errors.push(format!("empty {} file: {}", label, path.display()));
            continue;
        }
        validate_support_file_v1(&path, label, errors)?;
        *count += 1;
    }
    Ok(())
}

fn validate_support_file_v1(path: &Path, label: &str, errors: &mut Vec<String>) -> Result<(), String> {
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
    match ext {
        "json" => {
            let s = fs::read_to_string(path).map_err(|e| format!("{} read failed: {}", path.display(), e))?;
            if let Err(e) = serde_json::from_str::<serde_json::Value>(&s) {
                errors.push(format!("invalid {} json file {}: {}", label, path.display(), e));
            }
        }
        "jsonl" => validate_jsonl_file_v1(path, errors)?,
        "toml" => {
            let s = fs::read_to_string(path).map_err(|e| format!("{} read failed: {}", path.display(), e))?;
            if let Err(e) = toml::from_str::<toml::Value>(&s) {
                errors.push(format!("invalid {} toml file {}: {}", label, path.display(), e));
            }
        }
        _ => {
            let _ = fs::read(path).map_err(|e| format!("{} read failed: {}", path.display(), e))?;
        }
    }
    Ok(())
}

fn sorted_dir_entries_v1(path: &Path) -> Result<Vec<fs::DirEntry>, String> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)
        .map_err(|e| format!("{} read_dir failed: {}", path.display(), e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{} read_dir entry failed: {}", path.display(), e))?;
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    Ok(entries)
}
