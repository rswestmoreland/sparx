// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::write::GzEncoder;
use flate2::Compression;

use sparx::cli::route::route_command_v1;
use sparx::cli::CommandV1;
use sparx::config::load::default_config_v1;

fn unique_temp_dir(name: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("sparx_{}_{}_{}", name, std::process::id(), ts));
    let _ = fs::remove_dir_all(&path);
    path
}

fn write_text(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, body).unwrap();
}

fn write_gzip(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let file = fs::File::create(path).unwrap();
    let mut enc = GzEncoder::new(file, Compression::default());
    enc.write_all(body.as_bytes()).unwrap();
    enc.finish().unwrap();
}

fn build_valid_fixture_root(root: &Path) {
    write_text(
        &root.join("tenants/acme/devices/linux.log"),
        "Jan  1 00:00:01 host sshd[1]: Accepted password for alice\n",
    );
    write_text(
        &root.join("tenants/acme/devices/cloudtrail.jsonl"),
        "{\"eventName\":\"ConsoleLogin\"}\n{\"eventName\":\"CreateUser\"}\n",
    );
    write_text(
        &root.join("tenants/acme/devices/pan.csv"),
        "time,src,dst\n1,10.0.0.1,10.0.0.2\n",
    );
    write_text(
        &root.join("tenants/acme/devices/event.cef"),
        "<134>Feb 10 10:00:00 host CEF:0|Vendor|Product|1|100|Example|5|src=10.0.0.1\n",
    );
    write_gzip(
        &root.join("tenants/acme/devices/linux.gz"),
        "Jan  1 00:00:02 host sudo: pam_unix\n",
    );
    write_text(
        &root.join("golden/alerts_subset.json"),
        "{\"alerts\":[{\"alert_id\":\"a1\"}]}",
    );
    write_text(&root.join("golden/status.jsonl"), "{\"status\":\"ok\"}\n");
    write_text(
        &root.join("gen/scenario.toml"),
        "seed = 7\nscenario = \"mixed\"\n",
    );
    write_text(&root.join("gen/manifest.json"), "{\"ground_truth\":[]}");
}

#[test]
fn validate_fixtures_route_accepts_valid_corpus() {
    let root = unique_temp_dir("fixture_ok");
    build_valid_fixture_root(&root);

    let cfg = default_config_v1();
    let r = route_command_v1(
        &CommandV1::ValidateFixtures {
            fixture_root: root.to_string_lossy().to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 0);
    let out = r.msg_stdout.unwrap();
    assert!(out.contains("fixture validation ok"));
    assert!(out.contains("tenants: 1"));
    assert!(out.contains("device_files: 5"));
    assert!(out.contains("golden_files: 2"));
    assert!(out.contains("gen_files: 2"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn validate_fixtures_route_reports_validation_failures() {
    let root = unique_temp_dir("fixture_bad");
    fs::create_dir_all(root.join("tenants/acme/devices")).unwrap();
    fs::create_dir_all(root.join("golden")).unwrap();
    fs::create_dir_all(root.join("gen")).unwrap();
    write_text(&root.join("tenants/acme/devices/bad.txt"), "x\n");
    write_text(&root.join("golden/expected.json"), "not-json\n");

    let cfg = default_config_v1();
    let r = route_command_v1(
        &CommandV1::ValidateFixtures {
            fixture_root: root.to_string_lossy().to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 1);
    let err = r.msg_stderr.unwrap();
    assert!(err.contains("fixture validation failed"));
    assert!(err.contains("unsupported fixture extension .txt"));
    assert!(err.contains("invalid golden json file"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn validate_fixtures_route_reports_io_errors() {
    let root = unique_temp_dir("fixture_missing");
    let cfg = default_config_v1();
    let r = route_command_v1(
        &CommandV1::ValidateFixtures {
            fixture_root: root.to_string_lossy().to_string(),
        },
        &cfg,
    );
    assert_eq!(r.exit_code, 3);
    assert!(r
        .msg_stderr
        .unwrap()
        .contains("fixture validation IO error"));
}
