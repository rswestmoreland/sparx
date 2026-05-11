// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// CLI parsing tests with no filesystem IO.

use sparx::cli::parse::parse_args_v1;
use sparx::cli::{AlertCategoryFilterV1, AlertEntityKindFilterV1, CommandV1, MigrateModeV1};

fn v(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

#[test]
fn parse_run() {
    let (cmd, ov) = parse_args_v1(&v(&["sparx", "run"])).unwrap();
    assert_eq!(
        cmd,
        CommandV1::Run {
            migrate: MigrateModeV1::Auto
        }
    );
    assert!(ov.config_path.is_none());
}

#[test]
fn parse_run_migrate_require() {
    let (cmd, _) = parse_args_v1(&v(&["sparx", "run", "--migrate", "require"])).unwrap();
    assert_eq!(
        cmd,
        CommandV1::Run {
            migrate: MigrateModeV1::Require
        }
    );
}

#[test]
fn parse_oneshot() {
    let (cmd, _) = parse_args_v1(&v(&["sparx", "oneshot", "--tenant", "t1"])).unwrap();
    assert_eq!(
        cmd,
        CommandV1::OneShot {
            tenant_id: "t1".to_string(),
            since: None,
            until: None,
            device_path: None,
            migrate: MigrateModeV1::Auto,
        }
    );
}

#[test]
fn parse_oneshot_full_args() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "oneshot",
        "--tenant",
        "t1",
        "--since",
        "10",
        "--until",
        "20",
        "--device",
        "edge01",
        "--migrate",
        "require",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::OneShot {
            tenant_id: "t1".to_string(),
            since: Some(10),
            until: Some(20),
            device_path: Some("edge01".to_string()),
            migrate: MigrateModeV1::Require,
        }
    );
}

#[test]
fn parse_oneshot_requires_tenant() {
    let err = parse_args_v1(&v(&["sparx", "oneshot"])).unwrap_err();
    assert_eq!(err.msg, "oneshot: missing --tenant");
}

#[test]
fn parse_oneshot_rejects_inverted_time_range() {
    let err = parse_args_v1(&v(&[
        "sparx", "oneshot", "--tenant", "t1", "--since", "20", "--until", "10",
    ]))
    .unwrap_err();
    assert_eq!(err.msg, "oneshot: --since must be <= --until");
}

#[test]
fn parse_status_json() {
    let (cmd, _) = parse_args_v1(&v(&["sparx", "status", "--json"])).unwrap();
    assert_eq!(cmd, CommandV1::Status { json: true });
}

#[test]
fn parse_tenant_purge_force() {
    let (cmd, _) = parse_args_v1(&v(&["sparx", "tenant", "purge", "t1", "--force"])).unwrap();
    assert_eq!(
        cmd,
        CommandV1::TenantPurge {
            tenant_id: "t1".to_string(),
            force: true
        }
    );
}

#[test]
fn parse_alerts_show() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "alerts",
        "show",
        "--tenant",
        "t1",
        "--alert-id",
        "abc",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::AlertsShow {
            tenant_id: "t1".to_string(),
            alert_id: "abc".to_string(),
            json: false,
        }
    );
}

#[test]
fn parse_validate_fixtures() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "validate-fixtures",
        "--fixture-root",
        "/tmp/fx",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::ValidateFixtures {
            fixture_root: "/tmp/fx".to_string(),
        }
    );
}

#[test]
fn parse_tenant_policy_show() {
    let (cmd, _) = parse_args_v1(&v(&["sparx", "tenant", "policy", "show", "t1"])).unwrap();
    assert_eq!(
        cmd,
        CommandV1::TenantPolicyShow {
            tenant_id: "t1".to_string(),
        }
    );
}

#[test]
fn parse_tenant_policy_check() {
    let (cmd, _) = parse_args_v1(&v(&["sparx", "tenant", "policy", "check", "t1"])).unwrap();
    assert_eq!(
        cmd,
        CommandV1::TenantPolicyCheck {
            tenant_id: "t1".to_string(),
        }
    );
}

#[test]
fn parse_alerts_list_json() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx", "alerts", "list", "--tenant", "t1", "--since", "10", "--until", "20", "--json",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::AlertsList {
            tenant_id: "t1".to_string(),
            since: Some(10),
            until: Some(20),
            category: None,
            entity_kind: None,
            entity_value: None,
            json: true,
        }
    );
}

#[test]
fn parse_alerts_list_structured_filters_v1() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "alerts",
        "list",
        "--tenant",
        "t1",
        "--category",
        "outlier",
        "--entity-kind",
        "userid",
        "--entity-value",
        "alice",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::AlertsList {
            tenant_id: "t1".to_string(),
            since: None,
            until: None,
            category: Some(AlertCategoryFilterV1::Outlier),
            entity_kind: Some(AlertEntityKindFilterV1::UserId),
            entity_value: Some("alice".to_string()),
            json: false,
        }
    );
}

#[test]
fn parse_alerts_show_json() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "alerts",
        "show",
        "--tenant",
        "t1",
        "--alert-id",
        "abc",
        "--json",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::AlertsShow {
            tenant_id: "t1".to_string(),
            alert_id: "abc".to_string(),
            json: true,
        }
    );
}

#[test]
fn parse_alerts_search_with_time_bounds() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "alerts",
        "search",
        "--tenant",
        "t1",
        "--since",
        "10",
        "--until",
        "20",
        "--contains",
        "alice",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::AlertsSearch {
            tenant_id: "t1".to_string(),
            since: Some(10),
            until: Some(20),
            category: None,
            entity_kind: None,
            entity_value: None,
            contains: "alice".to_string(),
        }
    );
}

#[test]
fn parse_alerts_search_rejects_missing_entity_value_v1() {
    let err = parse_args_v1(&v(&[
        "sparx",
        "alerts",
        "search",
        "--tenant",
        "t1",
        "--entity-kind",
        "srcip",
        "--contains",
        "alice",
    ]))
    .unwrap_err();
    assert_eq!(err.msg, "alerts search: missing --entity-value");
}

#[test]
fn parse_alerts_list_rejects_invalid_category_v1() {
    let err = parse_args_v1(&v(&[
        "sparx",
        "alerts",
        "list",
        "--tenant",
        "t1",
        "--category",
        "weird",
    ]))
    .unwrap_err();
    assert_eq!(err.msg, "invalid alert category: weird");
}

#[test]
fn parse_alerts_export_gzip() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "alerts",
        "export",
        "--tenant",
        "t1",
        "--out",
        "/tmp/out.jsonl.gz",
        "--gzip",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::AlertsExport {
            tenant_id: "t1".to_string(),
            category: None,
            entity_kind: None,
            entity_value: None,
            out_path: "/tmp/out.jsonl.gz".to_string(),
            gzip: true,
        }
    );
}

#[test]
fn parse_alert_extract_with_caps() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "alert",
        "extract",
        "--tenant",
        "t1",
        "--alert-id",
        "abc",
        "--out",
        "/tmp/out.log",
        "--max-bytes",
        "128",
        "--max-lines",
        "4",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::AlertExtract {
            tenant_id: "t1".to_string(),
            alert_id: "abc".to_string(),
            out_path: "/tmp/out.log".to_string(),
            max_bytes: Some(128),
            max_lines: Some(4),
        }
    );
}

#[test]
fn parse_alert_drill_with_caps() {
    let (cmd, _) = parse_args_v1(&v(&[
        "sparx",
        "alert",
        "drill",
        "--tenant",
        "t1",
        "--alert-id",
        "abc",
        "--max-bytes",
        "64",
        "--max-lines",
        "2",
    ]))
    .unwrap();
    assert_eq!(
        cmd,
        CommandV1::AlertDrill {
            tenant_id: "t1".to_string(),
            alert_id: "abc".to_string(),
            max_bytes: Some(64),
            max_lines: Some(2),
        }
    );
}
