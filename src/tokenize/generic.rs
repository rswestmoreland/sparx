// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Generic tokenizer for JSON, CSV, key/value, CEF, and plaintext fallback.
// See: contracts/17_format_handling_v0_1.md and contracts/23_tokenizer_details_v0_1.md

use crate::tokenize::cef::parse_cef_message_v1;
use crate::tokenize::TokenEventV1;

const MAX_LINE_LEN_V1: usize = 16_384;
const MAX_TOKENS_PER_LINE_V1: usize = 256;
const MAX_KV_PER_LINE_V1: usize = 64;
const MAX_WORDS_FROM_QUOTED_VALUE_V1: usize = 32;
const MAX_WORD_LEN_V1: usize = 64;
const MIN_WORD_LEN_V1: usize = 2;
const JSON_MAX_DEPTH_V1: usize = 8;
const JSON_MAX_KVS_V1: usize = 256;
const JSON_ARRAY_SCALAR_CAP_V1: usize = 16;
const CSV_MAX_COLS_V1: usize = 256;
const CSV_MAX_KVS_V1: usize = 256;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CsvHeaderModeV1 {
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenizeStatsV1 {
    pub utf8_decode_fallback_total_delta: u32,
    pub lines_too_long_total_delta: u32,
    pub token_cap_hits_total_delta: u32,
    pub kv_cap_hits_total_delta: u32,
    pub cef_parse_errors_total_delta: u32,
    pub json_parse_errors_total_delta: u32,
    pub json_kv_cap_hits_total_delta: u32,
    pub csv_parse_errors_total_delta: u32,
    pub word_cap_hits_total_delta: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenizeResultV1 {
    pub msg: String,
    pub events: Vec<TokenEventV1>,
    pub stats: TokenizeStatsV1,
}

pub fn tokenize_message_bytes_v1(msg_bytes: &[u8], csv_header_mode: Option<&CsvHeaderModeV1>) -> TokenizeResultV1 {
    let mut stats = TokenizeStatsV1::default();
    let bytes = if msg_bytes.len() > MAX_LINE_LEN_V1 {
        stats.lines_too_long_total_delta = 1;
        &msg_bytes[..MAX_LINE_LEN_V1]
    } else {
        msg_bytes
    };

    let mut msg = match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => {
            stats.utf8_decode_fallback_total_delta = 1;
            String::from_utf8_lossy(bytes).into_owned()
        }
    };
    truncate_string_bytes_v1(&mut msg, MAX_LINE_LEN_V1);

    let mut result = tokenize_message_v1(&msg, csv_header_mode);
    merge_stats_v1(&mut result.stats, &stats);
    result.msg = msg;
    result
}

pub fn tokenize_message_v1(msg: &str, csv_header_mode: Option<&CsvHeaderModeV1>) -> TokenizeResultV1 {
    let mut builder = EventBuilderV1::default();
    let trimmed = msg.trim();

    match try_tokenize_cef_v1(trimmed, &mut builder) {
        Ok(true) => {
            return TokenizeResultV1 { msg: msg.to_string(), events: builder.events, stats: builder.stats };
        }
        Ok(false) => {}
        Err(()) => {
            builder.stats.cef_parse_errors_total_delta = 1;
            emit_words_v1(msg, &mut builder, false);
            return TokenizeResultV1 { msg: msg.to_string(), events: builder.events, stats: builder.stats };
        }
    }

    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        match try_tokenize_json_object_v1(trimmed, &mut builder) {
            Ok(true) => {
                return TokenizeResultV1 { msg: msg.to_string(), events: builder.events, stats: builder.stats };
            }
            Ok(false) => {}
            Err(()) => {
                builder.stats.json_parse_errors_total_delta = 1;
            }
        }
    }

    if let Some(csv_header_mode) = csv_header_mode {
        if try_tokenize_csv_row_v1(msg, csv_header_mode, &mut builder) {
            return TokenizeResultV1 { msg: msg.to_string(), events: builder.events, stats: builder.stats };
        }
    }

    let kv_found = tokenize_generic_kv_v1(msg, &mut builder);
    if !kv_found {
        emit_words_v1(msg, &mut builder, false);
    }

    TokenizeResultV1 {
        msg: msg.to_string(),
        events: builder.events,
        stats: builder.stats,
    }
}

#[derive(Default)]
struct EventBuilderV1 {
    events: Vec<TokenEventV1>,
    stats: TokenizeStatsV1,
    kv_count: usize,
    quoted_word_count: usize,
}

impl EventBuilderV1 {
    fn can_emit_token_v1(&self) -> bool {
        self.events.len() < MAX_TOKENS_PER_LINE_V1
    }

    fn mark_token_cap_hit_v1(&mut self) {
        if self.stats.token_cap_hits_total_delta == 0 {
            self.stats.token_cap_hits_total_delta = 1;
        }
    }

    fn mark_kv_cap_hit_v1(&mut self) {
        if self.stats.kv_cap_hits_total_delta == 0 {
            self.stats.kv_cap_hits_total_delta = 1;
        }
    }

    fn mark_json_kv_cap_hit_v1(&mut self) {
        if self.stats.json_kv_cap_hits_total_delta == 0 {
            self.stats.json_kv_cap_hits_total_delta = 1;
        }
    }

    fn mark_word_cap_hit_v1(&mut self) {
        if self.stats.word_cap_hits_total_delta == 0 {
            self.stats.word_cap_hits_total_delta = 1;
        }
    }

    fn push_kv_v1(&mut self, event: TokenEventV1, is_json: bool) -> bool {
        if self.kv_count >= MAX_KV_PER_LINE_V1 {
            self.mark_kv_cap_hit_v1();
            if is_json {
                self.mark_json_kv_cap_hit_v1();
            }
            return false;
        }
        if !self.can_emit_token_v1() {
            self.mark_token_cap_hit_v1();
            if is_json {
                self.mark_json_kv_cap_hit_v1();
            } else {
                self.mark_kv_cap_hit_v1();
            }
            return false;
        }
        self.kv_count += 1;
        self.events.push(event);
        true
    }

    fn push_word_v1(&mut self, token_raw: String, from_quoted_value: bool) -> bool {
        if from_quoted_value && self.quoted_word_count >= MAX_WORDS_FROM_QUOTED_VALUE_V1 {
            self.mark_word_cap_hit_v1();
            return false;
        }
        if !self.can_emit_token_v1() {
            self.mark_token_cap_hit_v1();
            self.mark_word_cap_hit_v1();
            return false;
        }
        if from_quoted_value {
            self.quoted_word_count += 1;
        }
        self.events.push(TokenEventV1::Word { token_raw });
        true
    }

    fn push_cef_header_v1(&mut self, field: String, value: String) -> bool {
        if !self.can_emit_token_v1() {
            self.mark_token_cap_hit_v1();
            return false;
        }
        self.events.push(TokenEventV1::CefHeader { field, value });
        true
    }
}

fn truncate_string_bytes_v1(s: &mut String, max_bytes: usize) {
    if s.len() <= max_bytes {
        return;
    }
    let mut cut = max_bytes;
    while cut > 0 && !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
}

fn try_tokenize_cef_v1(msg: &str, builder: &mut EventBuilderV1) -> Result<bool, ()> {
    let parsed = match parse_cef_message_v1(msg)? {
        Some(v) => v,
        None => return Ok(false),
    };

    for event in parsed.kv_events {
        if !builder.push_kv_v1(event, false) {
            break;
        }
    }
    if let Some(residual_text) = parsed.residual_text {
        emit_words_v1(&residual_text, builder, false);
    }
    for event in parsed.header_events {
        if let TokenEventV1::CefHeader { field, value } = event {
            if !builder.push_cef_header_v1(field, value) {
                break;
            }
        }
    }
    Ok(true)
}

fn try_tokenize_json_object_v1(msg: &str, builder: &mut EventBuilderV1) -> Result<bool, ()> {
    let value: serde_json::Value = serde_json::from_str(msg).map_err(|_| ())?;
    let obj = match value {
        serde_json::Value::Object(map) => map,
        _ => return Ok(false),
    };

    let mut json_kv_count = 0usize;
    let mut path: Vec<String> = Vec::new();
    let mut pairs: Vec<(String, String)> = Vec::new();
    flatten_json_object_v1(&obj, &mut path, 1, &mut json_kv_count, &mut pairs);

    for (key_path_norm, value_raw) in pairs {
        if !builder.push_kv_v1(TokenEventV1::JsonKv { key_path_norm, value_raw }, true) {
            break;
        }
    }
    Ok(true)
}

fn flatten_json_object_v1(
    obj: &serde_json::Map<String, serde_json::Value>,
    path: &mut Vec<String>,
    depth: usize,
    kv_count: &mut usize,
    out: &mut Vec<(String, String)>,
) {
    if depth > JSON_MAX_DEPTH_V1 || *kv_count >= JSON_MAX_KVS_V1 {
        return;
    }

    let mut entries: Vec<(&String, &serde_json::Value)> = obj.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    for (key, value) in entries {
        if *kv_count >= JSON_MAX_KVS_V1 {
            break;
        }
        path.push(key.clone());
        flatten_json_value_v1(value, path, depth, kv_count, out);
        path.pop();
    }
}

fn flatten_json_value_v1(
    value: &serde_json::Value,
    path: &mut Vec<String>,
    depth: usize,
    kv_count: &mut usize,
    out: &mut Vec<(String, String)>,
) {
    if depth > JSON_MAX_DEPTH_V1 || *kv_count >= JSON_MAX_KVS_V1 {
        return;
    }
    match value {
        serde_json::Value::Object(map) => {
            flatten_json_object_v1(map, path, depth + 1, kv_count, out);
        }
        serde_json::Value::Array(items) => {
            let mut scalar_count = 0usize;
            for item in items {
                if *kv_count >= JSON_MAX_KVS_V1 || scalar_count >= JSON_ARRAY_SCALAR_CAP_V1 {
                    break;
                }
                if is_json_scalar_v1(item) {
                    let key_path_norm = normalize_json_path_v1(path);
                    out.push((key_path_norm, json_scalar_to_string_v1(item)));
                    *kv_count += 1;
                    scalar_count += 1;
                }
            }
        }
        _ => {
            let key_path_norm = normalize_json_path_v1(path);
            out.push((key_path_norm, json_scalar_to_string_v1(value)));
            *kv_count += 1;
        }
    }
}

fn is_json_scalar_v1(value: &serde_json::Value) -> bool {
    matches!(
        value,
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) | serde_json::Value::String(_)
    )
}

fn json_scalar_to_string_v1(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(v) => {
            if *v { "true".to_string() } else { "false".to_string() }
        }
        serde_json::Value::Number(v) => v.to_string(),
        serde_json::Value::String(v) => v.clone(),
        _ => String::new(),
    }
}

fn try_tokenize_csv_row_v1(msg: &str, csv_header_mode: &CsvHeaderModeV1, builder: &mut EventBuilderV1) -> bool {
    if csv_header_mode.columns.is_empty() {
        return false;
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(msg.as_bytes());
    let mut records = reader.records();
    let record = match records.next() {
        Some(Ok(record)) => record,
        Some(Err(_)) => {
            builder.stats.csv_parse_errors_total_delta = 1;
            return false;
        }
        None => return false,
    };

    let max_cols = csv_header_mode.columns.len().min(CSV_MAX_COLS_V1).min(CSV_MAX_KVS_V1);
    let row_len = record.len();
    if row_len != csv_header_mode.columns.len() {
        builder.stats.csv_parse_errors_total_delta = 1;
    }

    for idx in 0..max_cols.min(row_len) {
        let key_norm = normalize_key_like_v1(&csv_header_mode.columns[idx]);
        if key_norm.is_empty() {
            continue;
        }
        let value_raw = record.get(idx).unwrap_or("").to_string();
        if !builder.push_kv_v1(TokenEventV1::CsvKv { key_norm, value_raw }, false) {
            break;
        }
    }
    true
}

fn tokenize_generic_kv_v1(msg: &str, builder: &mut EventBuilderV1) -> bool {
    let bytes = msg.as_bytes();
    let len = bytes.len();
    let mut i = 0usize;
    let mut found_any = false;
    let mut residual_start = 0usize;
    let mut residuals: Vec<String> = Vec::new();
    let mut quoted_texts: Vec<String> = Vec::new();

    while i < len {
        let key_start = match find_key_start_v1(msg, i) {
            Some(v) => v,
            None => break,
        };
        let key_end = scan_key_end_v1(msg, key_start);
        if key_end <= key_start {
            i = key_start + 1;
            continue;
        }
        let key_raw = &msg[key_start..key_end];
        if key_raw.len() > 64 {
            i = key_start + 1;
            continue;
        }

        let sep = bytes.get(key_end).copied();
        let mut value_start = key_end + 1;
        let separator = match sep {
            Some(b'=') => '=',
            Some(b':') => {
                if !colon_is_kv_separator_v1(msg, key_start, key_end) {
                    i = key_end + 1;
                    continue;
                }
                ':'
            }
            _ => {
                i = key_end + 1;
                continue;
            }
        };
        if separator == ':' && value_start < len && bytes[value_start] == b':' {
            i = key_end + 1;
            continue;
        }
        if value_start < len && bytes[value_start] == b' ' {
            value_start += 1;
        }
        if value_start >= len {
            break;
        }

        let parsed_value = parse_value_v1(msg, value_start);
        let key_norm = normalize_key_like_v1(key_raw);
        if key_norm.is_empty() {
            i = parsed_value.next_index.max(key_end + 1);
            continue;
        }

        if residual_start < key_start {
            residuals.push(msg[residual_start..key_start].to_string());
        }

        if builder.push_kv_v1(TokenEventV1::Kv { key_norm, value_raw: parsed_value.value_raw.clone() }, false) {
            found_any = true;
        }
        if parsed_value.was_quoted && parsed_value.value_raw.contains(|ch: char| ch.is_ascii_whitespace()) {
            quoted_texts.push(parsed_value.value_raw.clone());
        }

        residual_start = parsed_value.next_index;
        i = skip_pair_separators_v1(msg, parsed_value.next_index);
    }

    if !found_any {
        return false;
    }

    if residual_start < len {
        residuals.push(msg[residual_start..].to_string());
    }

    for residual in residuals {
        emit_words_v1(&residual, builder, false);
    }
    for quoted in quoted_texts {
        emit_words_v1(&quoted, builder, true);
    }

    true
}

struct ParsedValueV1 {
    value_raw: String,
    next_index: usize,
    was_quoted: bool,
}

fn parse_value_v1(msg: &str, value_start: usize) -> ParsedValueV1 {
    let bytes = msg.as_bytes();
    let len = bytes.len();
    let first = bytes[value_start];

    if first == b'"' || first == b'\'' {
        let (value_raw, end_idx) = parse_quoted_value_v1(msg, value_start, first);
        return ParsedValueV1 { value_raw, next_index: end_idx, was_quoted: true };
    }

    if matches!(first, b'[' | b'{' | b'(') {
        if let Some((value_raw, end_idx)) = parse_bracketed_value_v1(msg, value_start) {
            return ParsedValueV1 { value_raw, next_index: end_idx, was_quoted: false };
        }
    }

    let mut end = value_start;
    while end < len {
        let b = bytes[end];
        if b.is_ascii_whitespace() || b == b',' || b == b';' {
            break;
        }
        end += 1;
    }
    let mut value_raw = msg[value_start..end].to_string();
    if let Some(last) = value_raw.chars().last() {
        if matches!(last, ',' | ';' | ')' | ']' | '}') {
            value_raw.pop();
        }
    }
    ParsedValueV1 { value_raw, next_index: end, was_quoted: false }
}

fn parse_quoted_value_v1(msg: &str, value_start: usize, quote: u8) -> (String, usize) {
    let bytes = msg.as_bytes();
    let mut out = String::new();
    let mut idx = value_start + 1;
    let mut escaped = false;
    while idx < bytes.len() {
        let b = bytes[idx];
        if escaped {
            out.push(match b {
                b'n' => '\n',
                b'\\' => '\\',
                b'"' => '"',
                b'\'' => '\'',
                _ => b as char,
            });
            escaped = false;
            idx += 1;
            continue;
        }
        if b == b'\\' {
            escaped = true;
            idx += 1;
            continue;
        }
        if b == quote {
            idx += 1;
            return (out, idx);
        }
        out.push(b as char);
        idx += 1;
    }
    (out, idx)
}

fn parse_bracketed_value_v1(msg: &str, value_start: usize) -> Option<(String, usize)> {
    let bytes = msg.as_bytes();
    let opener = bytes.get(value_start).copied()?;
    let closer = match opener {
        b'[' => b']',
        b'{' => b'}',
        b'(' => b')',
        _ => return None,
    };

    let mut depth = 0usize;
    let mut idx = value_start;
    let mut in_quote: Option<u8> = None;
    let mut escaped = false;
    while idx < bytes.len() {
        let b = bytes[idx];
        if let Some(q) = in_quote {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == q {
                in_quote = None;
            }
            idx += 1;
            continue;
        }
        if b == b'"' || b == b'\'' {
            in_quote = Some(b);
            idx += 1;
            continue;
        }
        if b == opener {
            depth += 1;
            if depth > 2 {
                return None;
            }
        } else if b == closer {
            if depth == 0 {
                return None;
            }
            depth -= 1;
            if depth == 0 {
                let end = idx + 1;
                return Some((msg[value_start..end].to_string(), end));
            }
        }
        idx += 1;
    }
    None
}

fn emit_words_v1(text: &str, builder: &mut EventBuilderV1, from_quoted_value: bool) {
    let bytes = text.as_bytes();
    let mut start: Option<usize> = None;
    let mut idx = 0usize;
    while idx < bytes.len() {
        let b = bytes[idx];
        if is_word_byte_v1(b) {
            if start.is_none() {
                start = Some(idx);
            }
        } else if let Some(s) = start.take() {
            push_word_slice_v1(&text[s..idx], builder, from_quoted_value);
        }
        idx += 1;
    }
    if let Some(s) = start {
        push_word_slice_v1(&text[s..], builder, from_quoted_value);
    }
}

fn push_word_slice_v1(raw: &str, builder: &mut EventBuilderV1, from_quoted_value: bool) {
    let trimmed = raw.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.' && ch != '-' && ch != '/' && ch != ':' && ch != '@' && ch != '\\');
    if trimmed.len() < MIN_WORD_LEN_V1 {
        return;
    }
    let token_raw = if trimmed.len() > MAX_WORD_LEN_V1 {
        trimmed[..MAX_WORD_LEN_V1].to_string()
    } else {
        trimmed.to_string()
    };
    let _ = builder.push_word_v1(token_raw, from_quoted_value);
}

fn is_word_byte_v1(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'/' | b':' | b'@' | b'\\')
}

fn find_key_start_v1(msg: &str, from: usize) -> Option<usize> {
    let bytes = msg.as_bytes();
    let len = bytes.len();
    let mut idx = from;
    while idx < len {
        if is_key_char_v1(bytes[idx]) && is_key_boundary_prev_v1(bytes, idx) {
            return Some(idx);
        }
        idx += 1;
    }
    None
}

fn scan_key_end_v1(msg: &str, key_start: usize) -> usize {
    let bytes = msg.as_bytes();
    let mut idx = key_start;
    while idx < bytes.len() && is_key_char_v1(bytes[idx]) {
        idx += 1;
    }
    idx
}

fn is_key_boundary_prev_v1(bytes: &[u8], idx: usize) -> bool {
    if idx == 0 {
        return true;
    }
    matches!(bytes[idx - 1], b' ' | b'\t' | b'\n' | b'\r' | b';' | b',' | b'(' | b'[' | b'{' )
}

fn is_key_char_v1(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-')
}

fn colon_is_kv_separator_v1(msg: &str, key_start: usize, key_end: usize) -> bool {
    let bytes = msg.as_bytes();
    if key_end >= bytes.len() || bytes[key_end] != b':' {
        return false;
    }
    let key = &msg[key_start..key_end];
    if key.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if key.eq_ignore_ascii_case("http") || key.eq_ignore_ascii_case("https") {
        return false;
    }
    let next = key_end + 1;
    if next < bytes.len() && bytes[next] == b':' {
        return false;
    }
    if next + 1 < bytes.len() && bytes[next] == b'/' && bytes[next + 1] == b'/' {
        return false;
    }
    if key.len() == 1 && key.as_bytes()[0].is_ascii_alphabetic() && next < bytes.len() && matches!(bytes[next], b'\\' | b'/') {
        return false;
    }
    true
}

fn skip_pair_separators_v1(msg: &str, from: usize) -> usize {
    let bytes = msg.as_bytes();
    let mut idx = from;
    while idx < bytes.len() {
        match bytes[idx] {
            b' ' | b'\t' | b'\n' | b'\r' | b',' | b';' => idx += 1,
            _ => break,
        }
    }
    idx
}

fn normalize_json_path_v1(path: &[String]) -> String {
    let joined = path.join("_");
    normalize_key_like_v1(&joined)
}

fn normalize_key_like_v1(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_us = false;
    for ch in s.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            Some(ch.to_ascii_lowercase())
        } else {
            Some('_')
        };
        if let Some(c) = mapped {
            if c == '_' {
                if !last_us && !out.is_empty() {
                    out.push('_');
                }
                last_us = true;
            } else {
                out.push(c);
                last_us = false;
            }
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    while out.starts_with('_') {
        out.remove(0);
    }
    out
}

fn merge_stats_v1(dst: &mut TokenizeStatsV1, src: &TokenizeStatsV1) {
    dst.utf8_decode_fallback_total_delta += src.utf8_decode_fallback_total_delta;
    dst.lines_too_long_total_delta += src.lines_too_long_total_delta;
    dst.token_cap_hits_total_delta += src.token_cap_hits_total_delta;
    dst.kv_cap_hits_total_delta += src.kv_cap_hits_total_delta;
    dst.cef_parse_errors_total_delta += src.cef_parse_errors_total_delta;
    dst.json_parse_errors_total_delta += src.json_parse_errors_total_delta;
    dst.json_kv_cap_hits_total_delta += src.json_kv_cap_hits_total_delta;
    dst.csv_parse_errors_total_delta += src.csv_parse_errors_total_delta;
    dst.word_cap_hits_total_delta += src.word_cap_hits_total_delta;
}
