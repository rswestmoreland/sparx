// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use sparx::features::{emit_line_features_v1, MetadataIdentityKindV1};
use sparx::tokenize::{SyslogEnvelopeV1, TokenEventV1};
use sparx::types::FeatureFamilyV1;

fn feature_strings(result: &sparx::features::FeatureEmissionLineV1) -> Vec<String> {
    result
        .features
        .iter()
        .map(|f| f.feature.s.clone())
        .collect()
}

fn metadata_values(
    result: &sparx::features::FeatureEmissionLineV1,
    kind: MetadataIdentityKindV1,
) -> Vec<String> {
    result
        .metadata
        .iter()
        .filter(|m| m.kind == kind)
        .map(|m| m.value.clone())
        .collect()
}

#[test]
fn emits_k_for_all_structured_keys_and_syslog_features() {
    let envelope = SyslogEnvelopeV1 {
        pri: Some(134),
        version: Some(1),
        app: Some("sshd".to_string()),
        ..Default::default()
    };
    let events = vec![
        TokenEventV1::Kv {
            key_norm: "srcIp".to_string(),
            value_raw: "10.2.3.4".to_string(),
        },
        TokenEventV1::JsonKv {
            key_path_norm: "userName".to_string(),
            value_raw: "alice@example.com".to_string(),
        },
        TokenEventV1::CsvKv {
            key_norm: "filePath".to_string(),
            value_raw: "/tmp/a.sh".to_string(),
        },
    ];

    let result = emit_line_features_v1(&envelope, &events);
    let features = feature_strings(&result);

    assert!(features.contains(&"k=src_ip".to_string()));
    assert!(features.contains(&"k=user_name".to_string()));
    assert!(features.contains(&"k=file_path".to_string()));
    assert!(features.contains(&"syslog_pri=134".to_string()));
    assert!(features.contains(&"syslog_ver=1".to_string()));
    assert!(features.contains(&"syslog_app=sshd".to_string()));
}

#[test]
fn emits_categorized_shape_features_for_ip_user_and_path() {
    let events = vec![
        TokenEventV1::Kv {
            key_norm: "src_ip".to_string(),
            value_raw: "10.2.3.4".to_string(),
        },
        TokenEventV1::Kv {
            key_norm: "user".to_string(),
            value_raw: "CONTOSO\\Alice".to_string(),
        },
        TokenEventV1::Kv {
            key_norm: "path".to_string(),
            value_raw: "C:\\Windows\\Temp\\evil.exe".to_string(),
        },
    ];

    let result = emit_line_features_v1(&SyslogEnvelopeV1::default(), &events);
    let features = feature_strings(&result);

    assert!(features.contains(&"canon=SourceIp".to_string()));
    assert!(features.contains(&"SourceIp=<IPV4>".to_string()));
    assert!(features.contains(&"SourceIp_net@10.2.3.0/24".to_string()));
    assert!(features.contains(&"canon=User".to_string()));
    assert!(features.contains(&"User=alice".to_string()));
    assert!(features.contains(&"canon=Path".to_string()));
    assert!(features.contains(&"Path=<WIN_PATH>".to_string()));
}

#[test]
fn exact_identities_are_metadata_only_not_sparse_features() {
    let events = vec![
        TokenEventV1::Kv {
            key_norm: "src_ip".to_string(),
            value_raw: "10.2.3.4".to_string(),
        },
        TokenEventV1::Kv {
            key_norm: "user".to_string(),
            value_raw: "alice@example.com".to_string(),
        },
    ];

    let result = emit_line_features_v1(&SyslogEnvelopeV1::default(), &events);
    let features = feature_strings(&result);

    assert!(!features.contains(&"SourceIp@10.2.3.4".to_string()));
    assert!(!features.contains(&"UserRaw@alice@example.com".to_string()));
    assert_eq!(metadata_values(&result, MetadataIdentityKindV1::SourceIp), vec!["10.2.3.4".to_string()]);
    assert_eq!(metadata_values(&result, MetadataIdentityKindV1::UserRaw), vec!["alice@example.com".to_string()]);
}

#[test]
fn canonical_userid_and_domain_are_extracted_for_upn_and_windows_forms() {
    let events = vec![
        TokenEventV1::Kv {
            key_norm: "user".to_string(),
            value_raw: "Alice@Example.com".to_string(),
        },
        TokenEventV1::Kv {
            key_norm: "user_name".to_string(),
            value_raw: "CONTOSO\\Bob".to_string(),
        },
    ];

    let result = emit_line_features_v1(&SyslogEnvelopeV1::default(), &events);
    assert_eq!(metadata_values(&result, MetadataIdentityKindV1::UserId), vec!["alice".to_string(), "bob".to_string()]);
    assert_eq!(metadata_values(&result, MetadataIdentityKindV1::Domain), vec!["example.com".to_string(), "contoso".to_string()]);
}

#[test]
fn plaintext_only_emits_limited_ip_shapes() {
    let events = vec![
        TokenEventV1::Word {
            token_raw: "failed".to_string(),
        },
        TokenEventV1::Word {
            token_raw: "10.2.3.4".to_string(),
        },
        TokenEventV1::Word {
            token_raw: "2001:db8::1".to_string(),
        },
        TokenEventV1::Word {
            token_raw: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        },
    ];

    let result = emit_line_features_v1(&SyslogEnvelopeV1::default(), &events);
    let features = feature_strings(&result);
    assert!(features.contains(&"w=failed".to_string()));
    assert!(features.contains(&"shape=<IPV4>".to_string()));
    assert!(features.contains(&"shape=<IPV6>".to_string()));
    assert!(!features.iter().any(|f| f.contains("UUID") || f.contains("HEX") || f.contains("B64")));
}

#[test]
fn feature_counts_accumulate_deterministically() {
    let events = vec![
        TokenEventV1::Word {
            token_raw: "failed".to_string(),
        },
        TokenEventV1::Word {
            token_raw: "failed".to_string(),
        },
    ];
    let result = emit_line_features_v1(&SyslogEnvelopeV1::default(), &events);
    assert_eq!(result.features.len(), 1);
    assert_eq!(result.features[0].feature.s, "w=failed");
    assert_eq!(result.features[0].family, FeatureFamilyV1::Word);
    assert_eq!(result.features[0].count, 2);
}
