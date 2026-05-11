// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Tenant policy loading and validation.
//
// Parses tenant policy TOML from the contract-defined watch-root path, validates
// policy version and override shapes, and keeps output ordering deterministic.

use std::collections::BTreeMap;
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantPolicyV1 {
    pub policy_version: u32,
    pub key_overrides: BTreeMap<String, String>,
    pub ip_bucket: Option<String>,
    pub min_identity_confidence: u8,
    pub vdrop_enabled: Option<bool>,
    pub vdrop_device_enabled: Option<bool>,
    pub vdrop_tenant_enabled: Option<bool>,
    pub vdrop_source_stream_enabled: Option<bool>,
    pub vdrop_min_expected_windows_missed: Option<u32>,
    pub vdrop_min_mature_windows: Option<u64>,
    pub vdrop_min_expected_lines: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TenantPolicyLoadErrorKindV1 {
    MissingTenant,
    MissingPolicy,
    Io,
    Parse,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantPolicyLoadErrorV1 {
    pub kind: TenantPolicyLoadErrorKindV1,
    pub details: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TenantPolicyTomlV1 {
    policy_version: Option<u32>,
    #[serde(default)]
    key_overrides: BTreeMap<String, String>,
    ip_bucket: Option<String>,
    min_identity_confidence: Option<u8>,
    vdrop_enabled: Option<bool>,
    vdrop_device_enabled: Option<bool>,
    vdrop_tenant_enabled: Option<bool>,
    vdrop_source_stream_enabled: Option<bool>,
    vdrop_min_expected_windows_missed: Option<u32>,
    vdrop_min_mature_windows: Option<u64>,
    vdrop_min_expected_lines: Option<u64>,
}

pub fn load_tenant_policy_v1(
    tenant_dir: &Path,
    policy_path: &Path,
) -> Result<TenantPolicyV1, TenantPolicyLoadErrorV1> {
    let tenant_md = fs::metadata(tenant_dir).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            TenantPolicyLoadErrorV1 {
                kind: TenantPolicyLoadErrorKindV1::MissingTenant,
                details: vec![format!("tenant directory not found: {}", tenant_dir.display())],
            }
        } else {
            TenantPolicyLoadErrorV1 {
                kind: TenantPolicyLoadErrorKindV1::Io,
                details: vec![format!(
                    "failed to stat tenant directory {}: {}",
                    tenant_dir.display(),
                    e
                )],
            }
        }
    })?;
    if !tenant_md.is_dir() {
        return Err(TenantPolicyLoadErrorV1 {
            kind: TenantPolicyLoadErrorKindV1::MissingTenant,
            details: vec![format!("tenant directory is not a directory: {}", tenant_dir.display())],
        });
    }

    let text = fs::read_to_string(policy_path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            TenantPolicyLoadErrorV1 {
                kind: TenantPolicyLoadErrorKindV1::MissingPolicy,
                details: vec![format!("tenant policy not found: {}", policy_path.display())],
            }
        } else {
            TenantPolicyLoadErrorV1 {
                kind: TenantPolicyLoadErrorKindV1::Io,
                details: vec![format!(
                    "failed to read tenant policy {}: {}",
                    policy_path.display(),
                    e
                )],
            }
        }
    })?;

    let raw: TenantPolicyTomlV1 = toml::from_str(&text).map_err(|e| TenantPolicyLoadErrorV1 {
        kind: TenantPolicyLoadErrorKindV1::Parse,
        details: vec![format!("failed to parse tenant policy TOML: {}", e)],
    })?;

    let mut details: Vec<String> = Vec::new();

    match raw.policy_version {
        Some(1) => {}
        Some(v) => details.push(format!("invalid policy_version: {} (expected 1)", v)),
        None => details.push("missing policy_version".to_string()),
    }

    for (norm_key, category) in &raw.key_overrides {
        if norm_key.trim().is_empty() {
            details.push("key_overrides contains empty normalized key".to_string());
        }
        if !is_valid_category_v1(category) {
            details.push(format!(
                "invalid category for key_overrides.{}: {}",
                norm_key, category
            ));
        }
    }

    if let Some(ip_bucket) = &raw.ip_bucket {
        if let Err(e) = validate_cidr_v1(ip_bucket) {
            details.push(format!("invalid ip_bucket: {} ({})", ip_bucket, e));
        }
    }

    if let Some(value) = raw.vdrop_min_expected_windows_missed {
        if value == 0 {
            details.push("invalid vdrop_min_expected_windows_missed: 0".to_string());
        }
    }

    if details.is_empty() {
        Ok(TenantPolicyV1 {
            policy_version: 1,
            key_overrides: raw.key_overrides,
            ip_bucket: raw.ip_bucket,
            min_identity_confidence: raw.min_identity_confidence.unwrap_or(2),
            vdrop_enabled: raw.vdrop_enabled,
            vdrop_device_enabled: raw.vdrop_device_enabled,
            vdrop_tenant_enabled: raw.vdrop_tenant_enabled,
            vdrop_source_stream_enabled: raw.vdrop_source_stream_enabled,
            vdrop_min_expected_windows_missed: raw.vdrop_min_expected_windows_missed,
            vdrop_min_mature_windows: raw.vdrop_min_mature_windows,
            vdrop_min_expected_lines: raw.vdrop_min_expected_lines,
        })
    } else {
        Err(TenantPolicyLoadErrorV1 {
            kind: TenantPolicyLoadErrorKindV1::Invalid,
            details,
        })
    }
}

fn is_valid_category_v1(s: &str) -> bool {
    matches!(
        s,
        "SourceIp"
            | "DestIp"
            | "SourcePort"
            | "DestPort"
            | "SourceHost"
            | "DestHost"
            | "User"
            | "Process"
            | "Command"
            | "Path"
            | "Url"
            | "Domain"
            | "FileHash"
            | "Timestamp"
    )
}

fn validate_cidr_v1(s: &str) -> Result<(), String> {
    let (addr_s, prefix_s) = s
        .split_once('/')
        .ok_or_else(|| "missing '/' prefix separator".to_string())?;
    let addr: IpAddr = addr_s
        .parse()
        .map_err(|_| "invalid IP address".to_string())?;
    let prefix: u8 = prefix_s
        .parse()
        .map_err(|_| "invalid prefix length".to_string())?;
    let max_bits = match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if prefix > max_bits {
        return Err(format!("prefix length {} exceeds {}", prefix, max_bits));
    }
    Ok(())
}

pub fn tenant_policy_path_parts_v1(tenant_root: &Path, tenant_id: &str) -> (PathBuf, PathBuf) {
    let tenant_dir = tenant_root.join(tenant_id);
    let policy_path = tenant_dir.join(".sparx").join("policy.toml");
    (tenant_dir, policy_path)
}


pub fn resolve_vdrop_source_stream_enabled_v1(
    config_vdrop_enabled: bool,
    config_source_stream_enabled: bool,
    tenant_policy: Option<&TenantPolicyV1>,
) -> bool {
    let vdrop_enabled = tenant_policy
        .and_then(|policy| policy.vdrop_enabled)
        .unwrap_or(config_vdrop_enabled);
    let source_stream_enabled = tenant_policy
        .and_then(|policy| policy.vdrop_source_stream_enabled)
        .unwrap_or(config_source_stream_enabled);
    vdrop_enabled && source_stream_enabled
}
