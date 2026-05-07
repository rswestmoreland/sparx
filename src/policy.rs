// Tenant policy loading and validation.
//
// Phase 11c:
// - parse tenant policy TOML from the contract-defined watch-root path
// - validate policy_version, key_overrides categories, optional ip_bucket CIDR,
//   and default min_identity_confidence
// - keep output-facing ordering deterministic via BTreeMap

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

    if details.is_empty() {
        Ok(TenantPolicyV1 {
            policy_version: 1,
            key_overrides: raw.key_overrides,
            ip_bucket: raw.ip_bucket,
            min_identity_confidence: raw.min_identity_confidence.unwrap_or(2),
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
