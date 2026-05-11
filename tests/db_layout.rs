// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use sparx::config::load::default_config_v1;
use sparx::db::layout::{filesystem_layout_v1, FilesystemLayoutV1};

fn s(path: std::path::PathBuf) -> String {
    path.to_string_lossy().replace("\\", "/")
}

#[test]
fn default_layout_matches_contract_paths() {
    let cfg = default_config_v1();
    let layout = filesystem_layout_v1(&cfg);

    assert_eq!(s(layout.data_root_v1()), "/var/lib/sparx");
    assert_eq!(s(layout.tenant_root_v1()), "/var/log/tenants");
    assert_eq!(s(layout.global_db_path_v1()), "/var/lib/sparx/global.db");
    assert_eq!(s(layout.tenant_db_root_v1()), "/var/lib/sparx/tenants");
    assert_eq!(s(layout.alert_out_root_v1()), "/var/lib/sparx/alerts");
    assert_eq!(s(layout.spool_root_v1()), "/var/lib/sparx/spool/alerts");

    assert_eq!(
        s(layout.tenant_db_dir_v1("acme")),
        "/var/lib/sparx/tenants/tenant=acme/tenant.db"
    );
    assert_eq!(
        s(layout.tenant_alert_dir_v1("acme")),
        "/var/lib/sparx/alerts/tenant=acme"
    );
    assert_eq!(
        s(layout.tenant_spool_dir_v1("acme")),
        "/var/lib/sparx/spool/alerts/tenant=acme"
    );
    assert_eq!(
        s(layout.tenant_policy_path_v1("acme")),
        "/var/log/tenants/acme/.sparx/policy.toml"
    );
}

#[test]
fn overridden_layout_uses_effective_config_values() {
    let mut cfg = default_config_v1();
    cfg.sparx.data_root = "/srv/sparx-state".to_string();
    cfg.sparx.tenant_root = "/srv/watch".to_string();
    cfg.sparx.global_db_path = "/srv/custom/global-kv".to_string();
    cfg.sparx.tenant_db_root = "/srv/custom/tenants-kv".to_string();
    cfg.sparx.alert_out_root = "/srv/custom/alerts-out".to_string();

    let layout = FilesystemLayoutV1::from_config_v1(&cfg);

    assert_eq!(s(layout.data_root_v1()), "/srv/sparx-state");
    assert_eq!(s(layout.tenant_root_v1()), "/srv/watch");
    assert_eq!(s(layout.global_db_path_v1()), "/srv/custom/global-kv");
    assert_eq!(s(layout.tenant_db_root_v1()), "/srv/custom/tenants-kv");
    assert_eq!(s(layout.alert_out_root_v1()), "/srv/custom/alerts-out");
    assert_eq!(s(layout.spool_root_v1()), "/srv/sparx-state/spool/alerts");

    assert_eq!(
        s(layout.tenant_db_dir_v1("tenant-01")),
        "/srv/custom/tenants-kv/tenant=tenant-01/tenant.db"
    );
    assert_eq!(
        s(layout.tenant_alert_dir_v1("tenant-01")),
        "/srv/custom/alerts-out/tenant=tenant-01"
    );
    assert_eq!(
        s(layout.tenant_spool_dir_v1("tenant-01")),
        "/srv/sparx-state/spool/alerts/tenant=tenant-01"
    );
    assert_eq!(
        s(layout.tenant_policy_path_v1("tenant-01")),
        "/srv/watch/tenant-01/.sparx/policy.toml"
    );
}

#[test]
fn tenant_path_derivation_is_deterministic() {
    let cfg = default_config_v1();
    let layout = filesystem_layout_v1(&cfg);

    let first = layout.tenant_db_dir_v1("blue");
    let second = layout.tenant_db_dir_v1("blue");
    let third = layout.tenant_db_dir_v1("green");

    assert_eq!(first, second);
    assert_ne!(first, third);
    assert_eq!(
        s(layout.tenant_spool_dir_v1("blue")),
        "/var/lib/sparx/spool/alerts/tenant=blue"
    );
}
