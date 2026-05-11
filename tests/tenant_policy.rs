// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;

use sparx::cli::route::route_command_v1;
use sparx::cli::CommandV1;
use sparx::config::load::default_config_v1;
use sparx::policy::load_tenant_policy_v1;
use sparx::policy::resolve_vdrop_source_stream_enabled_v1;
use sparx::policy::tenant_policy_path_parts_v1;

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

#[test]
fn tenant_policy_show_and_check_include_vdrop_overrides_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(
        tenant_dir.join("policy.toml"),
        "policy_version = 1\nvdrop_enabled = false\nvdrop_device_enabled = true\nvdrop_tenant_enabled = false\nvdrop_source_stream_enabled = true\nvdrop_min_expected_windows_missed = 5\nvdrop_min_mature_windows = 9\nvdrop_min_expected_lines = 11\n",
    )
    .unwrap();

    let show = route_command_v1(
        &CommandV1::TenantPolicyShow {
            tenant_id: "acme".to_string(),
        },
        &cfg,
    );
    assert_eq!(show.exit_code, 0);
    let out = show.msg_stdout.unwrap();
    assert!(out.contains("vdrop_enabled: false"));
    assert!(out.contains("vdrop_device_enabled: true"));
    assert!(out.contains("vdrop_tenant_enabled: false"));
    assert!(out.contains("vdrop_source_stream_enabled: true"));
    assert!(out.contains("vdrop_min_expected_windows_missed: 5"));
    assert!(out.contains("vdrop_min_mature_windows: 9"));
    assert!(out.contains("vdrop_min_expected_lines: 11"));

    let check = route_command_v1(
        &CommandV1::TenantPolicyCheck {
            tenant_id: "acme".to_string(),
        },
        &cfg,
    );
    assert_eq!(check.exit_code, 0);
    let out = check.msg_stdout.unwrap();
    assert!(out.contains("vdrop_enabled: false"));
    assert!(out.contains("vdrop_device_enabled: true"));
    assert!(out.contains("vdrop_tenant_enabled: false"));
    assert!(out.contains("vdrop_source_stream_enabled: true"));
    assert!(out.contains("vdrop_min_expected_windows_missed: 5"));
    assert!(out.contains("vdrop_min_mature_windows: 9"));
    assert!(out.contains("vdrop_min_expected_lines: 11"));
}

#[test]
fn tenant_policy_defaults_vdrop_overrides_to_inherit_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(tenant_dir.join("policy.toml"), "policy_version = 1\n").unwrap();

    let r = route_command_v1(
        &CommandV1::TenantPolicyCheck {
            tenant_id: "acme".to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 0);
    let out = r.msg_stdout.unwrap();
    assert!(out.contains("vdrop_enabled: inherit"));
    assert!(out.contains("vdrop_device_enabled: inherit"));
    assert!(out.contains("vdrop_tenant_enabled: inherit"));
    assert!(out.contains("vdrop_source_stream_enabled: inherit"));
    assert!(out.contains("vdrop_min_expected_windows_missed: inherit"));
    assert!(out.contains("vdrop_min_mature_windows: inherit"));
    assert!(out.contains("vdrop_min_expected_lines: inherit"));
}

#[test]
fn tenant_policy_rejects_zero_vdrop_missed_window_threshold_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(
        tenant_dir.join("policy.toml"),
        "policy_version = 1\nvdrop_min_expected_windows_missed = 0\n",
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
    assert!(err.contains("invalid vdrop_min_expected_windows_missed: 0"));
}

#[test]
fn tenant_policy_source_stream_gate_resolves_default_off_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(
        tenant_dir.join("policy.toml"),
        "policy_version = 1
",
    )
    .unwrap();

    let (tenant_base, policy_path) =
        tenant_policy_path_parts_v1(std::path::Path::new(&cfg.sparx.tenant_root), "acme");
    let policy = load_tenant_policy_v1(&tenant_base, &policy_path).unwrap();

    assert!(!cfg.vdrop.source_stream_enabled);
    assert!(!resolve_vdrop_source_stream_enabled_v1(
        cfg.vdrop.enabled,
        cfg.vdrop.source_stream_enabled,
        Some(&policy),
    ));
}

#[test]
fn tenant_policy_source_stream_gate_respects_tenant_override_v1() {
    let td = tempfile::tempdir().unwrap();
    let mut cfg = cfg_for_root_v1(td.path());
    cfg.vdrop.source_stream_enabled = false;
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(
        tenant_dir.join("policy.toml"),
        "policy_version = 1
vdrop_source_stream_enabled = true
",
    )
    .unwrap();

    let (tenant_base, policy_path) =
        tenant_policy_path_parts_v1(std::path::Path::new(&cfg.sparx.tenant_root), "acme");
    let policy = load_tenant_policy_v1(&tenant_base, &policy_path).unwrap();

    assert!(resolve_vdrop_source_stream_enabled_v1(
        cfg.vdrop.enabled,
        cfg.vdrop.source_stream_enabled,
        Some(&policy),
    ));
}

#[test]
fn tenant_policy_source_stream_gate_disabled_by_global_vdrop_off_v1() {
    let td = tempfile::tempdir().unwrap();
    let mut cfg = cfg_for_root_v1(td.path());
    cfg.vdrop.enabled = false;
    cfg.vdrop.source_stream_enabled = true;
    let tenant_dir = td.path().join("tenants").join("acme").join(".sparx");
    fs::create_dir_all(&tenant_dir).unwrap();
    fs::write(
        tenant_dir.join("policy.toml"),
        "policy_version = 1
vdrop_source_stream_enabled = true
",
    )
    .unwrap();

    let (tenant_base, policy_path) =
        tenant_policy_path_parts_v1(std::path::Path::new(&cfg.sparx.tenant_root), "acme");
    let policy = load_tenant_policy_v1(&tenant_base, &policy_path).unwrap();

    assert!(!resolve_vdrop_source_stream_enabled_v1(
        cfg.vdrop.enabled,
        cfg.vdrop.source_stream_enabled,
        Some(&policy),
    ));
}

#[test]
fn tenant_cli_commands_reject_unsafe_tenant_components_v1() {
    let td = tempfile::tempdir().unwrap();
    let cfg = cfg_for_root_v1(td.path());

    let show = route_command_v1(
        &CommandV1::TenantPolicyShow {
            tenant_id: "../acme".to_string(),
        },
        &cfg,
    );
    assert_eq!(show.exit_code, 2);
    assert!(show
        .msg_stderr
        .unwrap()
        .contains("invalid tenant_id filesystem component"));

    let replay = route_command_v1(
        &CommandV1::ReplaySpool {
            tenant_id: Some("tenant/a".to_string()),
        },
        &cfg,
    );
    assert_eq!(replay.exit_code, 2);
    assert!(replay
        .msg_stderr
        .unwrap()
        .contains("invalid tenant_id filesystem component"));
}
