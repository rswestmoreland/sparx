use std::collections::BTreeMap;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use crate::features::{EmittedFeatureV1, FeatureStringV1};
use crate::tokenize::{SyslogEnvelopeV1, TokenEventV1};
use crate::types::FeatureFamilyV1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SemanticCategoryV1 {
    SourceIp,
    DestIp,
    SourcePort,
    DestPort,
    SourceHost,
    DestHost,
    User,
    Process,
    Command,
    Path,
    Url,
    Domain,
    FileHash,
    Timestamp,
}

impl SemanticCategoryV1 {
    pub fn as_feature_str_v1(&self) -> &'static str {
        match self {
            SemanticCategoryV1::SourceIp => "SourceIp",
            SemanticCategoryV1::DestIp => "DestIp",
            SemanticCategoryV1::SourcePort => "SourcePort",
            SemanticCategoryV1::DestPort => "DestPort",
            SemanticCategoryV1::SourceHost => "SourceHost",
            SemanticCategoryV1::DestHost => "DestHost",
            SemanticCategoryV1::User => "User",
            SemanticCategoryV1::Process => "Process",
            SemanticCategoryV1::Command => "Command",
            SemanticCategoryV1::Path => "Path",
            SemanticCategoryV1::Url => "Url",
            SemanticCategoryV1::Domain => "Domain",
            SemanticCategoryV1::FileHash => "FileHash",
            SemanticCategoryV1::Timestamp => "Timestamp",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SemanticMatchV1 {
    pub category: SemanticCategoryV1,
    pub confidence: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UserKindV1 {
    Bare,
    Upn,
    Win,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserIdentityV1 {
    pub raw: String,
    pub principal: String,
    pub domain: Option<String>,
    pub kind: UserKindV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetadataIdentityKindV1 {
    SourceIp,
    DestIp,
    UserRaw,
    UserId,
    Domain,
    Host,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataIdentityV1 {
    pub kind: MetadataIdentityKindV1,
    pub value: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeatureEmissionLineV1 {
    pub features: Vec<EmittedFeatureV1>,
    pub metadata: Vec<MetadataIdentityV1>,
    pub structured_pairs_found: bool,
}

pub fn emit_line_features_v1(
    envelope: &SyslogEnvelopeV1,
    events: &[TokenEventV1],
) -> FeatureEmissionLineV1 {
    let structured_pairs_found = events.iter().any(is_structured_pair_v1);
    let mut features = FeatureAccumulatorV1::default();
    let mut metadata = Vec::new();

    emit_syslog_features_v1(envelope, &mut features);

    for event in events {
        match event {
            TokenEventV1::Kv { key_norm, value_raw }
            | TokenEventV1::JsonKv {
                key_path_norm: key_norm,
                value_raw,
            }
            | TokenEventV1::CsvKv { key_norm, value_raw } => {
                emit_structured_pair_v1(key_norm, value_raw, &mut features, &mut metadata);
            }
            TokenEventV1::Word { token_raw } => {
                emit_word_v1(token_raw, structured_pairs_found, &mut features);
            }
            TokenEventV1::ResidualText { text_raw } => {
                emit_residual_text_v1(text_raw, structured_pairs_found, &mut features);
            }
            TokenEventV1::CefHeader { field, value } => {
                let mut text = String::new();
                if !field.is_empty() {
                    text.push_str(field);
                    text.push(' ');
                }
                text.push_str(value);
                emit_residual_text_v1(&text, structured_pairs_found, &mut features);
            }
        }
    }

    FeatureEmissionLineV1 {
        features: features.into_sorted_vec_v1(),
        metadata,
        structured_pairs_found,
    }
}

pub fn normalize_key_v1(raw_key: &str) -> String {
    let trimmed = raw_key.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let mut with_boundaries = String::with_capacity(trimmed.len() + 8);
    let chars: Vec<char> = trimmed.chars().collect();
    for (idx, ch) in chars.iter().enumerate() {
        if idx > 0 && is_camel_boundary_v1(&chars, idx) {
            with_boundaries.push('_');
        }
        with_boundaries.push(*ch);
    }

    let mut out = String::with_capacity(with_boundaries.len());
    let mut last_was_us = false;
    for ch in with_boundaries.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };
        if mapped == '_' {
            if !last_was_us {
                out.push('_');
                last_was_us = true;
            }
        } else {
            out.push(mapped);
            last_was_us = false;
        }
    }
    out.trim_matches('_').to_string()
}

pub fn classify_key_v1(raw_key: &str) -> Option<SemanticMatchV1> {
    let norm_key = normalize_key_v1(raw_key);
    if norm_key.is_empty() {
        return None;
    }

    match norm_key.as_str() {
        "sport" => {
            return Some(SemanticMatchV1 {
                category: SemanticCategoryV1::SourcePort,
                confidence: 3,
            })
        }
        "dport" => {
            return Some(SemanticMatchV1 {
                category: SemanticCategoryV1::DestPort,
                confidence: 3,
            })
        }
        "srcip" | "src_ip" | "source_ip" | "source_ip_address" | "sourceip" => {
            return Some(SemanticMatchV1 {
                category: SemanticCategoryV1::SourceIp,
                confidence: 3,
            })
        }
        "dstip" | "dst_ip" | "destination_ip" | "dest_ip" | "destinationip" => {
            return Some(SemanticMatchV1 {
                category: SemanticCategoryV1::DestIp,
                confidence: 3,
            })
        }
        _ => {}
    }

    let parts: Vec<&str> = norm_key.split('_').filter(|s| !s.is_empty()).collect();
    let has_src_dir = parts.iter().any(|p| is_src_dir_token_v1(p));
    let has_dst_dir = parts.iter().any(|p| is_dst_dir_token_v1(p));
    if has_src_dir && has_dst_dir {
        return None;
    }

    let has_ip = parts.iter().any(|p| matches!(*p, "ip" | "ipaddr"));
    let has_addr = parts.iter().any(|p| matches!(*p, "addr" | "address"));
    let has_port = parts.iter().any(|p| is_port_token_v1(p));
    let has_host = parts.iter().any(|p| is_host_token_v1(p));
    let has_user = parts.iter().any(|p| is_user_token_v1(p));
    let has_proc = parts.iter().any(|p| is_proc_token_v1(p));
    let has_cmd = parts.iter().any(|p| is_cmd_token_v1(p));
    let has_path = parts.iter().any(|p| is_path_token_v1(p));
    let has_url = parts.iter().any(|p| is_url_token_v1(p));
    let has_domain = parts.iter().any(|p| is_domain_token_v1(p));
    let has_hash = parts.iter().any(|p| is_hash_token_v1(p));
    let has_timestamp = parts.iter().any(|p| matches!(*p, "timestamp" | "time" | "ts"));

    if has_src_dir && has_port {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::SourcePort,
            confidence: 3,
        });
    }
    if has_dst_dir && has_port {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::DestPort,
            confidence: 3,
        });
    }
    if has_src_dir && (has_ip || has_addr) {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::SourceIp,
            confidence: if has_ip { 3 } else { 2 },
        });
    }
    if has_dst_dir && (has_ip || has_addr) {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::DestIp,
            confidence: if has_ip { 3 } else { 2 },
        });
    }
    if has_src_dir && has_host {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::SourceHost,
            confidence: 2,
        });
    }
    if has_dst_dir && has_host {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::DestHost,
            confidence: 2,
        });
    }
    if has_user {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::User,
            confidence: 2,
        });
    }
    if has_proc {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::Process,
            confidence: 2,
        });
    }
    if has_cmd {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::Command,
            confidence: 2,
        });
    }
    if has_path {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::Path,
            confidence: 2,
        });
    }
    if has_url {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::Url,
            confidence: 2,
        });
    }
    if has_domain {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::Domain,
            confidence: 2,
        });
    }
    if has_hash {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::FileHash,
            confidence: 2,
        });
    }
    if has_timestamp {
        return Some(SemanticMatchV1 {
            category: SemanticCategoryV1::Timestamp,
            confidence: 1,
        });
    }

    None
}

pub fn normalize_word_feature_v1(raw: &str) -> Option<String> {
    let trimmed = raw.trim_matches(|c: char| {
        c.is_ascii_whitespace() || matches!(c, ',' | ';' | ')' | '(' | '[' | ']' | '{' | '}' | '"' | '\'')
    });
    if trimmed.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if ch.is_ascii_alphanumeric()
            || matches!(ch, '_' | '.' | '-' | '/' | ':' | '@' | '\\')
        {
            out.push(ch.to_ascii_lowercase());
        }
    }
    if out.len() < 2 {
        return None;
    }
    if out.len() > 64 {
        out.truncate(64);
    }
    if out.chars().all(|c| !c.is_ascii_alphanumeric()) {
        return None;
    }
    Some(out)
}

pub fn normalize_user_identity_v1(raw: &str) -> Option<UserIdentityV1> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let (principal_raw, domain, kind) = if let Some((left, right)) = trimmed.split_once('\\') {
        if left.is_empty() || right.is_empty() {
            return None;
        }
        (
            right.trim().to_string(),
            Some(left.trim().to_string()),
            UserKindV1::Win,
        )
    } else if let Some((left, right)) = trimmed.rsplit_once('@') {
        if left.is_empty() || right.is_empty() || !right.contains('.') && !right.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return None;
        }
        (
            left.trim().to_string(),
            Some(right.trim().to_string()),
            UserKindV1::Upn,
        )
    } else {
        (trimmed.to_string(), None, UserKindV1::Bare)
    };

    let principal = sanitize_principal_v1(&principal_raw);
    if principal.is_empty() {
        return None;
    }

    let domain = domain.and_then(|s| {
        let d = s.trim().to_ascii_lowercase();
        if d.is_empty() { None } else { Some(d) }
    });

    Some(UserIdentityV1 {
        raw: trimmed.to_string(),
        principal,
        domain,
        kind,
    })
}

fn sanitize_principal_v1(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.trim().chars() {
        let lc = ch.to_ascii_lowercase();
        if lc.is_ascii_lowercase() || lc.is_ascii_digit() || matches!(lc, '.' | '_' | '-') {
            out.push(lc);
        } else {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

fn emit_syslog_features_v1(envelope: &SyslogEnvelopeV1, features: &mut FeatureAccumulatorV1) {
    if let Some(pri) = envelope.pri {
        features.push_v1(FeatureFamilyV1::Syslog, format!("syslog_pri={}", pri));
    }
    if let Some(app) = envelope.app.as_deref().and_then(normalize_word_feature_v1) {
        features.push_v1(FeatureFamilyV1::Syslog, format!("syslog_app={}", app));
    }
    if let Some(version) = envelope.version {
        features.push_v1(FeatureFamilyV1::Syslog, format!("syslog_ver={}", version));
    }
}

fn emit_structured_pair_v1(
    raw_key: &str,
    value_raw: &str,
    features: &mut FeatureAccumulatorV1,
    metadata: &mut Vec<MetadataIdentityV1>,
) {
    let key_norm = normalize_key_v1(raw_key);
    if key_norm.is_empty() {
        return;
    }
    features.push_v1(FeatureFamilyV1::KeyPres, format!("k={}", key_norm));

    let semantic = classify_key_v1(&key_norm);
    let Some(semantic) = semantic else {
        return;
    };

    let category = semantic.category;
    features.push_v1(
        FeatureFamilyV1::Canon,
        format!("canon={}", category.as_feature_str_v1()),
    );

    match category {
        SemanticCategoryV1::SourceIp | SemanticCategoryV1::DestIp => {
            if let Some(ipv4) = parse_ipv4_v1(value_raw) {
                features.push_v1(
                    FeatureFamilyV1::Shape,
                    format!("{}=<IPV4>", category.as_feature_str_v1()),
                );
                features.push_v1(
                    FeatureFamilyV1::Bucket,
                    format!(
                        "{}_net@{}/24",
                        category.as_feature_str_v1(),
                        ipv4_bucket_24_v1(ipv4)
                    ),
                );
                metadata.push(MetadataIdentityV1 {
                    kind: if category == SemanticCategoryV1::SourceIp {
                        MetadataIdentityKindV1::SourceIp
                    } else {
                        MetadataIdentityKindV1::DestIp
                    },
                    value: ipv4.to_string(),
                });
            } else if parse_ipv6_v1(value_raw).is_some() {
                features.push_v1(
                    FeatureFamilyV1::Shape,
                    format!("{}=<IPV6>", category.as_feature_str_v1()),
                );
                metadata.push(MetadataIdentityV1 {
                    kind: if category == SemanticCategoryV1::SourceIp {
                        MetadataIdentityKindV1::SourceIp
                    } else {
                        MetadataIdentityKindV1::DestIp
                    },
                    value: value_raw.trim().to_ascii_lowercase(),
                });
            }
        }
        SemanticCategoryV1::User => {
            if let Some(user) = normalize_user_identity_v1(value_raw) {
                features.push_v1(
                    FeatureFamilyV1::Shape,
                    format!("User={}", user.principal),
                );
                metadata.push(MetadataIdentityV1 {
                    kind: MetadataIdentityKindV1::UserRaw,
                    value: user.raw,
                });
                metadata.push(MetadataIdentityV1 {
                    kind: MetadataIdentityKindV1::UserId,
                    value: user.principal.clone(),
                });
                if let Some(domain) = user.domain {
                    metadata.push(MetadataIdentityV1 {
                        kind: MetadataIdentityKindV1::Domain,
                        value: domain,
                    });
                }
            }
        }
        SemanticCategoryV1::Path => {
            if let Some(shape) = detect_path_shape_v1(value_raw) {
                features.push_v1(
                    FeatureFamilyV1::Shape,
                    format!("Path={}", shape),
                );
            }
        }
        SemanticCategoryV1::Url => {
            if is_url_v1(value_raw) {
                features.push_v1(FeatureFamilyV1::Shape, "Url=<URL>".to_string());
            }
        }
        SemanticCategoryV1::Domain => {
            if let Some(hostname) = normalize_hostname_v1(value_raw) {
                features.push_v1(FeatureFamilyV1::Shape, "Domain=<HOSTNAME>".to_string());
                metadata.push(MetadataIdentityV1 {
                    kind: MetadataIdentityKindV1::Domain,
                    value: hostname,
                });
            }
        }
        SemanticCategoryV1::SourceHost | SemanticCategoryV1::DestHost => {
            if let Some(hostname) = normalize_hostname_v1(value_raw) {
                features.push_v1(
                    FeatureFamilyV1::Shape,
                    format!("{}=<HOSTNAME>", category.as_feature_str_v1()),
                );
                metadata.push(MetadataIdentityV1 {
                    kind: MetadataIdentityKindV1::Host,
                    value: hostname,
                });
            }
        }
        SemanticCategoryV1::SourcePort | SemanticCategoryV1::DestPort => {
            if is_port_v1(value_raw) {
                features.push_v1(
                    FeatureFamilyV1::Shape,
                    format!("{}=<PORT>", category.as_feature_str_v1()),
                );
            }
        }
        SemanticCategoryV1::Timestamp => {
            if looks_like_rfc3339_v1(value_raw) {
                features.push_v1(
                    FeatureFamilyV1::Shape,
                    format!("{}=<RFC3339_TS>", category.as_feature_str_v1()),
                );
            }
        }
        _ => {}
    }
}

fn emit_word_v1(raw_word: &str, structured_pairs_found: bool, features: &mut FeatureAccumulatorV1) {
    if let Some(word) = normalize_word_feature_v1(raw_word) {
        features.push_v1(FeatureFamilyV1::Word, format!("w={}", word));
    }
    if !structured_pairs_found {
        if parse_ipv4_v1(raw_word).is_some() {
            features.push_v1(FeatureFamilyV1::Shape, "shape=<IPV4>".to_string());
        } else if parse_ipv6_v1(raw_word).is_some() {
            features.push_v1(FeatureFamilyV1::Shape, "shape=<IPV6>".to_string());
        }
    }
}

fn emit_residual_text_v1(text_raw: &str, structured_pairs_found: bool, features: &mut FeatureAccumulatorV1) {
    for part in text_raw.split_ascii_whitespace() {
        emit_word_v1(part, structured_pairs_found, features);
    }
}

fn detect_path_shape_v1(raw: &str) -> Option<&'static str> {
    let trimmed = raw.trim();
    if trimmed.starts_with('/') {
        Some("<UNIX_PATH>")
    } else if looks_like_windows_path_v1(trimmed) {
        Some("<WIN_PATH>")
    } else {
        None
    }
}

fn normalize_hostname_v1(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if trimmed.is_empty() || trimmed.len() > 255 {
        return None;
    }
    if parse_ipv4_v1(&trimmed).is_some() || parse_ipv6_v1(&trimmed).is_some() {
        return None;
    }
    if trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
        && trimmed.chars().any(|c| c.is_ascii_alphabetic())
    {
        Some(trimmed)
    } else {
        None
    }
}

fn is_url_v1(raw: &str) -> bool {
    let lowered = raw.trim().to_ascii_lowercase();
    lowered.starts_with("http://") || lowered.starts_with("https://")
}

fn is_port_v1(raw: &str) -> bool {
    match raw.trim().parse::<u16>() {
        Ok(v) => v > 0,
        Err(_) => false,
    }
}

fn looks_like_rfc3339_v1(raw: &str) -> bool {
    let trimmed = raw.trim();
    trimmed.len() >= 20
        && trimmed.as_bytes().get(4) == Some(&b'-')
        && trimmed.as_bytes().get(7) == Some(&b'-')
        && trimmed.contains('T')
}

fn parse_ipv4_v1(raw: &str) -> Option<Ipv4Addr> {
    Ipv4Addr::from_str(raw.trim()).ok()
}

fn parse_ipv6_v1(raw: &str) -> Option<Ipv6Addr> {
    Ipv6Addr::from_str(raw.trim()).ok()
}

fn ipv4_bucket_24_v1(ip: Ipv4Addr) -> String {
    let octets = ip.octets();
    format!("{}.{}.{}.0", octets[0], octets[1], octets[2])
}

fn looks_like_windows_path_v1(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    if bytes.len() >= 3 && bytes[1] == b':' && (bytes[2] == b'\\' || bytes[2] == b'/') {
        return bytes[0].is_ascii_alphabetic();
    }
    raw.starts_with("\\\\")
}

fn is_structured_pair_v1(event: &TokenEventV1) -> bool {
    matches!(
        event,
        TokenEventV1::Kv { .. } | TokenEventV1::JsonKv { .. } | TokenEventV1::CsvKv { .. }
    )
}

#[derive(Default)]
struct FeatureAccumulatorV1 {
    counts: BTreeMap<(u8, String), u32>,
}

impl FeatureAccumulatorV1 {
    fn push_v1(&mut self, family: FeatureFamilyV1, feature: String) {
        let order = family_order_v1(family);
        *self.counts.entry((order, feature)).or_insert(0) += 1;
    }

    fn into_sorted_vec_v1(self) -> Vec<EmittedFeatureV1> {
        let mut out = Vec::with_capacity(self.counts.len());
        for ((_, feature), count) in self.counts {
            let family = family_from_feature_v1(&feature);
            out.push(EmittedFeatureV1 {
                feature: FeatureStringV1 { s: feature },
                family,
                count,
            });
        }
        out
    }
}

fn family_order_v1(family: FeatureFamilyV1) -> u8 {
    match family {
        FeatureFamilyV1::Shape => 0,
        FeatureFamilyV1::Bucket => 1,
        FeatureFamilyV1::KeyPres => 2,
        FeatureFamilyV1::Canon => 3,
        FeatureFamilyV1::Word => 4,
        FeatureFamilyV1::Syslog => 5,
    }
}

fn family_from_feature_v1(feature: &str) -> FeatureFamilyV1 {
    if feature.starts_with("k=") {
        FeatureFamilyV1::KeyPres
    } else if feature.starts_with("canon=") {
        FeatureFamilyV1::Canon
    } else if feature.starts_with("syslog_") {
        FeatureFamilyV1::Syslog
    } else if feature.starts_with("w=") {
        FeatureFamilyV1::Word
    } else if feature.contains("_net@") {
        FeatureFamilyV1::Bucket
    } else {
        FeatureFamilyV1::Shape
    }
}

fn is_camel_boundary_v1(chars: &[char], idx: usize) -> bool {
    let prev = chars[idx - 1];
    let cur = chars[idx];
    if prev.is_ascii_lowercase() && cur.is_ascii_uppercase() {
        return true;
    }
    if prev.is_ascii_alphabetic() && cur.is_ascii_digit() {
        return true;
    }
    if prev.is_ascii_digit() && cur.is_ascii_alphabetic() {
        return true;
    }
    if idx + 1 < chars.len() && prev.is_ascii_uppercase() && cur.is_ascii_uppercase() && chars[idx + 1].is_ascii_lowercase() {
        return true;
    }
    false
}

fn is_src_dir_token_v1(part: &str) -> bool {
    matches!(
        part,
        "src" | "source" | "client" | "remote" | "raddr" | "peer" | "origin" | "from" | "caller"
    )
}

fn is_dst_dir_token_v1(part: &str) -> bool {
    matches!(
        part,
        "dst" | "dest" | "destination" | "server" | "local" | "laddr" | "target" | "to" | "callee"
    )
}

fn is_port_token_v1(part: &str) -> bool {
    matches!(part, "port" | "sport" | "dport")
}

fn is_host_token_v1(part: &str) -> bool {
    matches!(part, "host" | "hostname" | "computer" | "device" | "node" | "machine")
}

fn is_user_token_v1(part: &str) -> bool {
    matches!(
        part,
        "user" | "username" | "account" | "acct" | "principal" | "subject" | "actor" | "login" | "logon" | "uid"
    )
}

fn is_proc_token_v1(part: &str) -> bool {
    matches!(part, "process" | "proc" | "image" | "exe" | "program" | "binary" | "app")
}

fn is_cmd_token_v1(part: &str) -> bool {
    matches!(part, "cmd" | "command" | "commandline" | "argv" | "args")
}

fn is_path_token_v1(part: &str) -> bool {
    matches!(part, "path" | "filepath" | "file" | "filename" | "directory" | "dir")
}

fn is_url_token_v1(part: &str) -> bool {
    matches!(part, "url" | "uri" | "request" | "resource")
}

fn is_domain_token_v1(part: &str) -> bool {
    matches!(part, "domain" | "fqdn" | "hostdomain" | "dns" | "servername" | "sni")
}

fn is_hash_token_v1(part: &str) -> bool {
    matches!(part, "hash" | "sha256" | "sha1" | "md5" | "checksum" | "fingerprint")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_key_examples_match_contract() {
        assert_eq!(normalize_key_v1("SourceIpAddress"), "source_ip_address");
        assert_eq!(normalize_key_v1("src-address"), "src_address");
        assert_eq!(normalize_key_v1("source.address"), "source_address");
    }

    #[test]
    fn classify_key_examples_match_contract() {
        let src = classify_key_v1("srcIp").unwrap();
        assert_eq!(src.category, SemanticCategoryV1::SourceIp);
        assert_eq!(src.confidence, 3);
        let user = classify_key_v1("username").unwrap();
        assert_eq!(user.category, SemanticCategoryV1::User);
        assert_eq!(user.confidence, 2);
    }

    #[test]
    fn principal_normalization_handles_upn_and_windows_forms() {
        let upn = normalize_user_identity_v1("Alice@Contoso.com").unwrap();
        assert_eq!(upn.principal, "alice");
        assert_eq!(upn.domain.as_deref(), Some("contoso.com"));
        assert_eq!(upn.kind, UserKindV1::Upn);

        let win = normalize_user_identity_v1("CONTOSO\\Alice").unwrap();
        assert_eq!(win.principal, "alice");
        assert_eq!(win.domain.as_deref(), Some("contoso"));
        assert_eq!(win.kind, UserKindV1::Win);
    }
}
