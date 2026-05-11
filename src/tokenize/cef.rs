// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// CEF parsing helpers, including reverse key/value extension parsing.
// See: contracts/18_syslog_envelope_and_cef_reverse_kv_v0_1.md

use crate::tokenize::TokenEventV1;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CefParseResultV1 {
    pub kv_events: Vec<TokenEventV1>,
    pub header_events: Vec<TokenEventV1>,
    pub residual_text: Option<String>,
}

pub fn parse_cef_message_v1(msg: &str) -> Result<Option<CefParseResultV1>, ()> {
    let trimmed = msg.trim_start();
    if !trimmed.starts_with("CEF:") {
        return Ok(None);
    }

    let (fields, extension) = parse_cef_header_v1(trimmed).ok_or(())?;

    let mut result = CefParseResultV1::default();
    result.header_events = build_cef_header_events_v1(&fields);

    let ext_trimmed = extension.trim();
    if ext_trimmed.is_empty() {
        return Ok(Some(result));
    }

    let (pairs, residual_text) = parse_cef_extension_reverse_v1(ext_trimmed);
    if pairs.is_empty() {
        return Err(());
    }

    result.kv_events = pairs
        .into_iter()
        .map(|(key_norm, value_raw)| TokenEventV1::Kv { key_norm, value_raw })
        .collect();
    result.residual_text = residual_text;
    Ok(Some(result))
}

fn parse_cef_header_v1(msg: &str) -> Option<(Vec<String>, String)> {
    let mut fields: Vec<String> = Vec::with_capacity(7);
    let mut current = String::new();
    let mut escaped = false;
    let mut chars = msg.chars();

    if chars.next() != Some('C') || chars.next() != Some('E') || chars.next() != Some('F') || chars.next() != Some(':') {
        return None;
    }

    let mut rest = String::new();
    for ch in chars {
        if fields.len() == 7 {
            rest.push(ch);
            continue;
        }
        if escaped {
            current.push(match ch {
                'n' => '\n',
                '\\' => '\\',
                '|' => '|',
                '=' => '=',
                other => other,
            });
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '|' {
            fields.push(std::mem::take(&mut current));
            continue;
        }
        current.push(ch);
    }

    if fields.len() < 7 {
        fields.push(current);
    } else {
        rest.insert_str(0, &current);
    }

    if fields.len() != 7 {
        return None;
    }

    Some((fields, rest))
}

fn build_cef_header_events_v1(fields: &[String]) -> Vec<TokenEventV1> {
    let names = [
        "version",
        "device_vendor",
        "device_product",
        "device_version",
        "signature_id",
        "name",
        "severity",
    ];

    let mut out = Vec::new();
    for (idx, field_name) in names.iter().enumerate() {
        if let Some(value) = fields.get(idx) {
            if !value.is_empty() {
                out.push(TokenEventV1::CefHeader {
                    field: (*field_name).to_string(),
                    value: value.clone(),
                });
            }
        }
    }
    out
}

fn parse_cef_extension_reverse_v1(extension: &str) -> (Vec<(String, String)>, Option<String>) {
    let bytes = extension.as_bytes();
    let mut end = bytes.len();
    let mut pairs_rev: Vec<(String, String)> = Vec::new();

    while end > 0 {
        let eq_idx = match find_prev_unescaped_eq_v1(extension, end) {
            Some(v) => v,
            None => break,
        };
        let (key_start, key_end) = match find_key_bounds_v1(extension, eq_idx) {
            Some(v) => v,
            None => {
                end = eq_idx;
                continue;
            }
        };

        let value_slice = extension[eq_idx + 1..end].trim();
        let key_raw = &extension[key_start..key_end];
        let key_norm = normalize_cef_key_v1(key_raw);
        if key_norm.is_empty() {
            end = key_start;
            continue;
        }

        let value_raw = strip_quotes_once_v1(value_slice);
        let value_raw = unescape_cef_text_v1(value_raw);
        pairs_rev.push((key_norm, value_raw));
        end = key_start;
    }

    pairs_rev.reverse();

    let residual_text = extension[..end].trim();
    let residual_text = if residual_text.is_empty() {
        None
    } else {
        Some(unescape_cef_text_v1(residual_text))
    };

    (pairs_rev, residual_text)
}

fn find_prev_unescaped_eq_v1(s: &str, end: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if end == 0 {
        return None;
    }
    let mut idx = end;
    while idx > 0 {
        idx -= 1;
        if bytes[idx] != b'=' {
            continue;
        }
        let mut backslashes = 0usize;
        let mut scan = idx;
        while scan > 0 && bytes[scan - 1] == b'\\' {
            backslashes += 1;
            scan -= 1;
        }
        if backslashes % 2 == 0 {
            return Some(idx);
        }
    }
    None
}

fn find_key_bounds_v1(s: &str, eq_idx: usize) -> Option<(usize, usize)> {
    if eq_idx == 0 {
        return None;
    }
    let bytes = s.as_bytes();
    let mut start = eq_idx;
    while start > 0 && is_cef_key_byte_v1(bytes[start - 1]) {
        start -= 1;
    }
    let key_len = eq_idx.saturating_sub(start);
    if key_len == 0 || key_len > 32 {
        return None;
    }
    if start > 0 && !bytes[start - 1].is_ascii_whitespace() {
        return None;
    }
    Some((start, eq_idx))
}

fn is_cef_key_byte_v1(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-')
}

fn normalize_cef_key_v1(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    let mut last_us = false;
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_us = false;
        } else if matches!(ch, '_' | '.' | '-') {
            if !last_us && !out.is_empty() {
                out.push('_');
            }
            last_us = true;
        } else {
            return String::new();
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}

fn strip_quotes_once_v1(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'"' && bytes[value.len() - 1] == b'"') || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'') {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn unescape_cef_text_v1(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\\') => out.push('\\'),
            Some('|') => out.push('|'),
            Some('=') => out.push('='),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}
