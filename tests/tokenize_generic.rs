// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use sparx::tokenize::{tokenize_message_bytes_v1, tokenize_message_v1, CsvHeaderModeV1, TokenEventV1};

fn kv_pairs(events: &[TokenEventV1]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for event in events {
        if let TokenEventV1::Kv { key_norm, value_raw } = event {
            out.push((key_norm.clone(), value_raw.clone()));
        }
    }
    out
}

fn json_pairs(events: &[TokenEventV1]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for event in events {
        if let TokenEventV1::JsonKv { key_path_norm, value_raw } = event {
            out.push((key_path_norm.clone(), value_raw.clone()));
        }
    }
    out
}

fn csv_pairs(events: &[TokenEventV1]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for event in events {
        if let TokenEventV1::CsvKv { key_norm, value_raw } = event {
            out.push((key_norm.clone(), value_raw.clone()));
        }
    }
    out
}

fn words(events: &[TokenEventV1]) -> Vec<String> {
    let mut out = Vec::new();
    for event in events {
        if let TokenEventV1::Word { token_raw } = event {
            out.push(token_raw.clone());
        }
    }
    out
}

#[test]
fn kv_parses_quoted_values_with_spaces() {
    let result = tokenize_message_v1("user=alice msg=\"hello world\" action='sign in' status=ok", None);
    let kvs = kv_pairs(&result.events);
    assert_eq!(
        kvs,
        vec![
            ("user".to_string(), "alice".to_string()),
            ("msg".to_string(), "hello world".to_string()),
            ("action".to_string(), "sign in".to_string()),
            ("status".to_string(), "ok".to_string()),
        ]
    );
    assert_eq!(
        words(&result.events),
        vec!["hello".to_string(), "world".to_string(), "sign".to_string(), "in".to_string()]
    );
}

#[test]
fn kv_does_not_split_http_urls_on_colon() {
    let result = tokenize_message_v1("http://example.com/path user=alice", None);
    assert_eq!(
        kv_pairs(&result.events),
        vec![("user".to_string(), "alice".to_string())]
    );
    assert!(words(&result.events).contains(&"http://example.com/path".to_string()));
}

#[test]
fn word_tokenizer_preserves_paths_and_domain_user() {
    let result = tokenize_message_v1(
        "DOMAIN\\user opened C:\\Windows\\Temp\\evil.exe from 10.0.0.1",
        None,
    );
    assert_eq!(
        words(&result.events),
        vec![
            "DOMAIN\\user".to_string(),
            "opened".to_string(),
            "C:\\Windows\\Temp\\evil.exe".to_string(),
            "from".to_string(),
            "10.0.0.1".to_string(),
        ]
    );
}

#[test]
fn json_flatten_depth_and_caps() {
    let msg = concat!(
        "{",
        "\"top\":{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{\"f\":{\"g\":{\"h\":1}}}}}}}},",
        "\"arr\":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]",
        "}"
    );
    let result = tokenize_message_v1(msg, None);
    let pairs = json_pairs(&result.events);
    let arr_values: Vec<String> = pairs
        .iter()
        .filter(|(k, _)| k == "arr")
        .map(|(_, v)| v.clone())
        .collect();
    assert_eq!(arr_values.len(), 16);
    assert_eq!(arr_values.first().cloned(), Some("0".to_string()));
    assert_eq!(arr_values.last().cloned(), Some("15".to_string()));
    assert!(!pairs.iter().any(|(k, _)| k == "top_a_b_c_d_e_f_g_h"));
}

#[test]
fn csv_header_mode_emits_csv_kvs() {
    let header = CsvHeaderModeV1 {
        columns: vec!["src_ip".to_string(), "dst_ip".to_string(), "message".to_string()],
    };
    let result = tokenize_message_v1("10.0.0.1,10.0.0.2,\"hello world\"", Some(&header));
    assert_eq!(
        csv_pairs(&result.events),
        vec![
            ("src_ip".to_string(), "10.0.0.1".to_string()),
            ("dst_ip".to_string(), "10.0.0.2".to_string()),
            ("message".to_string(), "hello world".to_string()),
        ]
    );
}

#[test]
fn cap_drop_order_is_deterministic() {
    let mut msg = String::new();
    for idx in 0..64 {
        if idx > 0 {
            msg.push(' ');
        }
        msg.push_str(&format!("k{}=v{}", idx, idx));
    }
    msg.push(' ');
    for idx in 0..300 {
        if idx > 0 {
            msg.push(' ');
        }
        msg.push_str(&format!("word{}", idx));
    }

    let result = tokenize_message_v1(&msg, None);
    assert_eq!(result.events.len(), 256);
    let kv_count = result
        .events
        .iter()
        .filter(|event| matches!(event, TokenEventV1::Kv { .. }))
        .count();
    let word_count = result
        .events
        .iter()
        .filter(|event| matches!(event, TokenEventV1::Word { .. }))
        .count();
    assert_eq!(kv_count, 64);
    assert_eq!(word_count, 192);
    assert_eq!(result.stats.token_cap_hits_total_delta, 1);
    assert_eq!(result.stats.word_cap_hits_total_delta, 1);
    assert_eq!(result.stats.kv_cap_hits_total_delta, 0);
}

#[test]
fn fallback_to_plaintext_on_parse_error() {
    let result = tokenize_message_v1("{\"broken\": [1, }", None);
    assert_eq!(result.stats.json_parse_errors_total_delta, 1);
    assert!(words(&result.events).contains(&"broken".to_string()));
}

#[test]
fn bytes_entrypoint_applies_lossy_decode_and_length_cap() {
    let mut bytes = vec![b'a'; 16_385];
    bytes[10] = 0xff;
    let result = tokenize_message_bytes_v1(&bytes, None);
    assert_eq!(result.msg.len(), 16_384);
    assert_eq!(result.stats.lines_too_long_total_delta, 1);
    assert_eq!(result.stats.utf8_decode_fallback_total_delta, 1);
}
