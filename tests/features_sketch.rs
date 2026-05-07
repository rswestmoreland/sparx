use sparx::config::CapsSectionV1;
use sparx::db::keys::{
    key_tenant_window_row_ent_domain_v1, key_tenant_window_row_ent_dstip_v1,
    key_tenant_window_row_ent_host_v1, key_tenant_window_row_ent_srcip_v1,
    key_tenant_window_row_ent_userid_v1,
};
use sparx::db::open_window::{
    decode_win_row_ent_domain_v1, decode_win_row_ent_dstip_v1, decode_win_row_ent_host_v1,
    decode_win_row_ent_srcip_v1, decode_win_row_ent_userid_v1,
};
use sparx::features::{
    emit_line_features_v1, EntitySketchCapsV1, EntitySketchKindV1, EntitySketchesV1,
    MetadataIdentityKindV1,
};
use sparx::tokenize::{SyslogEnvelopeV1, TokenEventV1};

fn sample_caps() -> EntitySketchCapsV1 {
    EntitySketchCapsV1 {
        max_srcips: 2,
        max_dstips: 2,
        max_userids: 2,
        max_domains: 2,
        max_hosts: 2,
    }
}

#[test]
fn config_conversion_preserves_entity_caps() {
    let caps = CapsSectionV1 {
        max_features_per_window: 50000,
        max_word_features_per_window: 20000,
        max_shape_features_per_window: 20000,
        max_syslog_features_per_window: 2000,
        max_srcips: 64,
        max_dstips: 65,
        max_userids: 128,
        max_domains: 129,
        max_hosts: 130,
    };
    let sketch_caps = EntitySketchCapsV1::from(&caps);
    assert_eq!(sketch_caps.max_srcips, 64);
    assert_eq!(sketch_caps.max_dstips, 65);
    assert_eq!(sketch_caps.max_userids, 128);
    assert_eq!(sketch_caps.max_domains, 129);
    assert_eq!(sketch_caps.max_hosts, 130);
}

#[test]
fn ingest_line_counts_only_supported_metadata_kinds() {
    let mut sketches = EntitySketchesV1::new_v1(sample_caps());
    let line = emit_line_features_v1(
        &SyslogEnvelopeV1::default(),
        &[
            TokenEventV1::Kv {
                key_norm: "src_ip".to_string(),
                value_raw: "10.2.3.4".to_string(),
            },
            TokenEventV1::Kv {
                key_norm: "dst_ip".to_string(),
                value_raw: "10.9.8.7".to_string(),
            },
            TokenEventV1::Kv {
                key_norm: "user".to_string(),
                value_raw: "Alice@Example.com".to_string(),
            },
            TokenEventV1::Kv {
                key_norm: "dest_host".to_string(),
                value_raw: "DC01.example.com".to_string(),
            },
        ],
    );

    sketches.ingest_line_v1(&line);

    assert_eq!(
        sketches.counts_for_kind_v1(EntitySketchKindV1::SrcIp),
        vec![("10.2.3.4".to_string(), 1)]
    );
    assert_eq!(
        sketches.counts_for_kind_v1(EntitySketchKindV1::DstIp),
        vec![("10.9.8.7".to_string(), 1)]
    );
    assert_eq!(
        sketches.counts_for_kind_v1(EntitySketchKindV1::UserId),
        vec![("alice".to_string(), 1)]
    );
    assert_eq!(
        sketches.counts_for_kind_v1(EntitySketchKindV1::Domain),
        vec![("example.com".to_string(), 1)]
    );
    assert_eq!(
        sketches.counts_for_kind_v1(EntitySketchKindV1::Host),
        vec![("dc01.example.com".to_string(), 1)]
    );

    let user_raws: Vec<String> = line
        .metadata
        .iter()
        .filter(|m| m.kind == MetadataIdentityKindV1::UserRaw)
        .map(|m| m.value.clone())
        .collect();
    assert_eq!(user_raws, vec!["Alice@Example.com".to_string()]);
    assert_eq!(sketches.counts_for_kind_v1(EntitySketchKindV1::UserId).len(), 1);
}

#[test]
fn topk_snapshot_is_count_desc_then_lex_asc_and_capped() {
    let mut sketches = EntitySketchesV1::new_v1(sample_caps());
    sketches.ingest_identity_v1(MetadataIdentityKindV1::SourceIp, "10.0.0.9");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::SourceIp, "10.0.0.8");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::SourceIp, "10.0.0.8");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::SourceIp, "10.0.0.7");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::SourceIp, "10.0.0.7");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::SourceIp, "10.0.0.6");

    let snapshot = sketches.snapshot_v1();
    let got: Vec<(String, u32)> = snapshot
        .srcips
        .iter()
        .map(|e| (e.value.clone(), e.count))
        .collect();
    assert_eq!(
        got,
        vec![
            ("10.0.0.7".to_string(), 2),
            ("10.0.0.8".to_string(), 2),
        ]
    );
}

#[test]
fn checkpoint_writes_use_expected_keys_and_encodings() {
    let mut sketches = EntitySketchesV1::new_v1(sample_caps());
    sketches.ingest_identity_v1(MetadataIdentityKindV1::SourceIp, "10.2.3.4");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::DestIp, "10.9.8.7");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::UserId, "alice");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::Domain, "example.com");
    sketches.ingest_identity_v1(MetadataIdentityKindV1::Host, "dc01.example.com");

    let writes = sketches.checkpoint_writes_v1("dev123", 42).unwrap();
    assert_eq!(writes.len(), 5);
    assert_eq!(writes[0].key, key_tenant_window_row_ent_srcip_v1("dev123", 42));
    assert_eq!(writes[1].key, key_tenant_window_row_ent_dstip_v1("dev123", 42));
    assert_eq!(writes[2].key, key_tenant_window_row_ent_userid_v1("dev123", 42));
    assert_eq!(writes[3].key, key_tenant_window_row_ent_domain_v1("dev123", 42));
    assert_eq!(writes[4].key, key_tenant_window_row_ent_host_v1("dev123", 42));

    assert_eq!(
        decode_win_row_ent_srcip_v1(&writes[0].value).unwrap()[0].value,
        "10.2.3.4"
    );
    assert_eq!(
        decode_win_row_ent_dstip_v1(&writes[1].value).unwrap()[0].value,
        "10.9.8.7"
    );
    assert_eq!(
        decode_win_row_ent_userid_v1(&writes[2].value).unwrap()[0].value,
        "alice"
    );
    assert_eq!(
        decode_win_row_ent_domain_v1(&writes[3].value).unwrap()[0].value,
        "example.com"
    );
    assert_eq!(
        decode_win_row_ent_host_v1(&writes[4].value).unwrap()[0].value,
        "dc01.example.com"
    );
}

#[test]
fn empty_checkpoint_still_writes_all_entity_lists() {
    let sketches = EntitySketchesV1::new_v1(sample_caps());
    let writes = sketches.checkpoint_writes_v1("dev123", 7).unwrap();
    assert_eq!(writes.len(), 5);
    assert!(decode_win_row_ent_srcip_v1(&writes[0].value).unwrap().is_empty());
    assert!(decode_win_row_ent_dstip_v1(&writes[1].value).unwrap().is_empty());
    assert!(decode_win_row_ent_userid_v1(&writes[2].value).unwrap().is_empty());
    assert!(decode_win_row_ent_domain_v1(&writes[3].value).unwrap().is_empty());
    assert!(decode_win_row_ent_host_v1(&writes[4].value).unwrap().is_empty());
}
