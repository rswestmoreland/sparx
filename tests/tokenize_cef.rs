// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use sparx::tokenize::{tokenize_message_v1, TokenEventV1};

fn kv_pairs(events: &[TokenEventV1]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for event in events {
        if let TokenEventV1::Kv {
            key_norm,
            value_raw,
        } = event
        {
            out.push((key_norm.clone(), value_raw.clone()));
        }
    }
    out
}

fn cef_headers(events: &[TokenEventV1]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for event in events {
        if let TokenEventV1::CefHeader { field, value } = event {
            out.push((field.clone(), value.clone()));
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
fn cef_reverse_kv_handles_spaces_in_values() {
    let msg = concat!(
        "CEF:0|Acme|Sensor|1.2|42|Suspicious Login|8|",
        "src=10.0.0.1 dst=10.0.0.2 suser=DOMAIN\\alice ",
        "msg=login failed for user filePath=C:\\Program Files\\Acme\\agent.exe"
    );
    let result = tokenize_message_v1(msg, None);
    assert_eq!(
        kv_pairs(&result.events),
        vec![
            ("src".to_string(), "10.0.0.1".to_string()),
            ("dst".to_string(), "10.0.0.2".to_string()),
            ("suser".to_string(), "DOMAIN\\alice".to_string()),
            ("msg".to_string(), "login failed for user".to_string()),
            (
                "filepath".to_string(),
                "C:\\Program Files\\Acme\\agent.exe".to_string(),
            ),
        ]
    );

    let headers = cef_headers(&result.events);
    assert!(headers.contains(&("device_vendor".to_string(), "Acme".to_string())));
    assert!(headers.contains(&("name".to_string(), "Suspicious Login".to_string())));
}

#[test]
fn cef_reverse_kv_unescapes_quotes_pipes_and_equals() {
    let msg = concat!(
        "CEF:0|Acme|Sensor|1.2|7|Pipe\\|Name|5|",
        "cs1Label=Rule cs1=bad\\=thing ",
        "request=GET\\|POST ",
        "msg=\"hello world\""
    );
    let result = tokenize_message_v1(msg, None);
    assert_eq!(
        kv_pairs(&result.events),
        vec![
            ("cs1label".to_string(), "Rule".to_string()),
            ("cs1".to_string(), "bad=thing".to_string()),
            ("request".to_string(), "GET|POST".to_string()),
            ("msg".to_string(), "hello world".to_string()),
        ]
    );
    assert!(cef_headers(&result.events).contains(&("name".to_string(), "Pipe|Name".to_string())));
}

#[test]
fn cef_parse_failure_falls_back_to_plaintext_and_increments_counter() {
    let result = tokenize_message_v1("CEF:0|Acme|Broken header only", None);
    assert_eq!(result.stats.cef_parse_errors_total_delta, 1);
    assert!(words(&result.events).contains(&"CEF:0".to_string()));
    assert!(words(&result.events).contains(&"Acme".to_string()));
}

#[test]
fn cef_residual_text_is_tokenized_as_words() {
    let msg = "CEF:0|Acme|Sensor|1.0|9|Notice|4|leading free text src=10.1.2.3 msg=done";
    let result = tokenize_message_v1(msg, None);
    assert_eq!(
        kv_pairs(&result.events),
        vec![
            ("src".to_string(), "10.1.2.3".to_string()),
            ("msg".to_string(), "done".to_string()),
        ]
    );
    assert_eq!(
        words(&result.events),
        vec![
            "leading".to_string(),
            "free".to_string(),
            "text".to_string()
        ]
    );
}
