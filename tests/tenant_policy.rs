use std::fs;

use sparx::cli::route::route_command_v1;
use sparx::cli::CommandV1;
use sparx::config::load::default_config_v1;

fn cfg_for_root_v1(root: &std::path::Path) -> sparx::config::ConfigV1 {
    let mut cfg = default_config_v1();
    cfg.sparx.tenant_root = root.join("tenants").display().to_string();
    cfg.sparx.data_root = root.join("data").display().to_string();
    cfg.sparx.global_db_path = format!("{}/global.db", cfg.sparx.data_root);
    cfg.sparx.tenant_db_root = format!("{}/tenants", cfg.sparx.data_root);
    cfg.sparx.alert_out_root = format!("{}/alerts", cfg.sparx.data_root);
    cfg
}

#[test]
fn tenant_policy_show_valid_renders_sorted_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(
        tenant_dir.join("policy.toml"),
        "policy_version = 1\nmin_identity_confidence = 3\nip_bucket = \"10.0.0.0/8\"\n\n[key_overrides]\nzkey = \"User\"\nakey = \"SourceIp\"\n",
    )
    .unwrap();

    let r = route_command_v1(
        &CommandV1::TenantPolicyShow {
            tenant_id: "acme".to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 0);
    let out = r.msg_stdout.unwrap();
    assert!(out.contains("tenant policy show"));
    assert!(out.contains("policy_version: 1"));
    assert!(out.contains("min_identity_confidence: 3"));
    let aidx = out.find("- akey => SourceIp").unwrap();
    let zidx = out.find("- zkey => User").unwrap();
    assert!(aidx < zidx);
}

#[test]
fn tenant_policy_check_valid_defaults_min_identity_confidence_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(
        tenant_dir.join("policy.toml"),
        "policy_version = 1\n\n[key_overrides]\nsrc = \"SourceIp\"\n",
    )
    .unwrap();

    let r = route_command_v1(
        &CommandV1::TenantPolicyCheck {
            tenant_id: "acme".to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 0);
    let out = r.msg_stdout.unwrap();
    assert!(out.contains("tenant policy ok"));
    assert!(out.contains("min_identity_confidence: 2"));
    assert!(out.contains("key_overrides_count: 1"));
}

#[test]
fn tenant_policy_check_invalid_policy_returns_exit_one_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(
        tenant_dir.join("policy.toml"),
        "policy_version = 2\nip_bucket = \"10.0.0.0/40\"\n\n[key_overrides]\nsrc = \"BadCat\"\n",
    )
    .unwrap();

    let r = route_command_v1(
        &CommandV1::TenantPolicyCheck {
            tenant_id: "acme".to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 1);
    let err = r.msg_stderr.unwrap();
    assert!(err.contains("tenant policy check failed"));
    assert!(err.contains("invalid policy_version: 2"));
    assert!(err.contains("invalid category for key_overrides.src: BadCat"));
    assert!(err.contains("invalid ip_bucket: 10.0.0.0/40"));
}

#[test]
fn tenant_policy_show_missing_tenant_returns_exit_one_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());

    let r = route_command_v1(
        &CommandV1::TenantPolicyShow {
            tenant_id: "missing".to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 1);
    assert!(r.msg_stderr.unwrap().contains("tenant directory not found"));
}

#[test]
fn tenant_policy_show_missing_policy_returns_exit_one_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());
    fs::create_dir_all(td.path().join("tenants").join("acme")).unwrap();

    let r = route_command_v1(
        &CommandV1::TenantPolicyShow {
            tenant_id: "acme".to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 1);
    assert!(r.msg_stderr.unwrap().contains("tenant policy not found"));
}
