// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sparx::ingest::{
    device_key_v1, discover_device_files_at_v1, discover_device_inventory_v1, discover_tenant_devices_v1,
    file_key_v1, has_allowed_suffix_v1, is_gzip_name_v1,
};
use sparx::stable_hash::STABLE_HASH_HEX128_LEN_V1;

fn temp_case_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!("sparx_{}_{}_{}", name, std::process::id(), nanos));
    p
}

fn write_file(path: &Path, body: &str) {
    fs::write(path, body.as_bytes()).unwrap();
}

#[test]
fn stable_device_and_file_keys_are_lowercase_hex_and_deterministic() {
    let dk1 = device_key_v1("acme", "router01");
    let dk2 = device_key_v1("acme", "router01");
    let dk3 = device_key_v1("acme", "router02");
    let fk1 = file_key_v1("events.log");
    let fk2 = file_key_v1("events.log");
    let fk3 = file_key_v1("events.csv");

    for v in [&dk1, &fk1] {
        assert_eq!(v.len(), STABLE_HASH_HEX128_LEN_V1);
        assert!(v.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    }

    assert_eq!(dk1, dk2);
    assert_ne!(dk1, dk3);
    assert_eq!(fk1, fk2);
    assert_ne!(fk1, fk3);
}

#[test]
fn discover_tenant_devices_returns_deterministic_sorted_devices() {
    let root = temp_case_dir("discover_devices");
    fs::create_dir_all(root.join("tenant-b").join("z-last")).unwrap();
    fs::create_dir_all(root.join("tenant-a").join("m-mid")).unwrap();
    fs::create_dir_all(root.join("tenant-a").join("a-first")).unwrap();
    write_file(&root.join("tenant-a").join("README.txt"), "not a device dir");

    let got = discover_tenant_devices_v1(&root, false).unwrap();
    let triples: Vec<(String, String, String)> = got
        .iter()
        .map(|d| (d.tenant_id.clone(), d.device_dir_rel.clone(), d.device_key.clone()))
        .collect();

    assert_eq!(
        triples,
        vec![
            (
                "tenant-a".to_string(),
                "a-first".to_string(),
                device_key_v1("tenant-a", "a-first"),
            ),
            (
                "tenant-a".to_string(),
                "m-mid".to_string(),
                device_key_v1("tenant-a", "m-mid"),
            ),
            (
                "tenant-b".to_string(),
                "z-last".to_string(),
                device_key_v1("tenant-b", "z-last"),
            ),
        ]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn discover_device_files_filters_hidden_unsupported_and_non_files() {
    let root = temp_case_dir("discover_files");
    fs::create_dir_all(&root).unwrap();
    write_file(&root.join("b.csv"), "a,b");
    write_file(&root.join("a.log"), "hello");
    write_file(&root.join("c.gz"), "compressed-name-only");
    write_file(&root.join(".hidden.log"), "ignore");
    write_file(&root.join("notes.md"), "ignore");
    fs::create_dir_all(root.join("nested")).unwrap();

    let got = discover_device_files_at_v1(&root, false).unwrap();
    let pairs: Vec<(String, bool, String)> = got
        .iter()
        .map(|f| (f.file_rel.clone(), f.is_gzip, f.file_key.clone()))
        .collect();

    assert_eq!(
        pairs,
        vec![
            (
                "a.log".to_string(),
                false,
                file_key_v1("a.log"),
            ),
            (
                "b.csv".to_string(),
                false,
                file_key_v1("b.csv"),
            ),
            (
                "c.gz".to_string(),
                true,
                file_key_v1("c.gz"),
            ),
        ]
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn discovery_inventory_joins_devices_with_files() {
    let root = temp_case_dir("discover_inventory");
    let dev = root.join("acme").join("router01");
    fs::create_dir_all(&dev).unwrap();
    write_file(&dev.join("events.log"), "one");
    write_file(&dev.join("events.csv"), "two");

    let inv = discover_device_inventory_v1(&root, false).unwrap();
    assert_eq!(inv.len(), 1);
    assert_eq!(inv[0].device.tenant_id, "acme");
    assert_eq!(inv[0].device.device_dir_name, "router01");
    assert_eq!(inv[0].files.len(), 2);
    assert_eq!(inv[0].files[0].file_rel, "events.csv");
    assert_eq!(inv[0].files[1].file_rel, "events.log");

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn suffix_helpers_match_contract_allowlist() {
    assert!(has_allowed_suffix_v1("a.log"));
    assert!(has_allowed_suffix_v1("a.txt"));
    assert!(has_allowed_suffix_v1("a.json"));
    assert!(has_allowed_suffix_v1("a.csv"));
    assert!(has_allowed_suffix_v1("a.cef"));
    assert!(has_allowed_suffix_v1("a.gz"));
    assert!(!has_allowed_suffix_v1("a.jsonl"));
    assert!(!has_allowed_suffix_v1("a.gzip"));
    assert!(is_gzip_name_v1("a.gz"));
    assert!(!is_gzip_name_v1("a.log"));
}

#[cfg(unix)]
#[test]
fn symlinks_are_skipped_by_default_and_followed_when_enabled() {
    use std::os::unix::fs as unix_fs;

    let root = temp_case_dir("discover_symlinks");
    let tenant = root.join("acme");
    let real_device = tenant.join("router01");
    let link_device = tenant.join("router02-link");
    fs::create_dir_all(&real_device).unwrap();
    write_file(&real_device.join("events.log"), "one");
    unix_fs::symlink(&real_device, &link_device).unwrap();
    unix_fs::symlink(real_device.join("events.log"), real_device.join("events-link.log")).unwrap();

    let devices_default = discover_tenant_devices_v1(&root, false).unwrap();
    assert_eq!(devices_default.len(), 1);
    assert_eq!(devices_default[0].device_dir_rel, "router01");

    let devices_follow = discover_tenant_devices_v1(&root, true).unwrap();
    assert_eq!(devices_follow.len(), 2);

    let files_default = discover_device_files_at_v1(&real_device, false).unwrap();
    assert_eq!(files_default.len(), 1);
    assert_eq!(files_default[0].file_rel, "events.log");

    let files_follow = discover_device_files_at_v1(&real_device, true).unwrap();
    assert_eq!(files_follow.len(), 2);
    assert_eq!(files_follow[0].file_rel, "events-link.log");
    assert_eq!(files_follow[1].file_rel, "events.log");

    fs::remove_dir_all(root).unwrap();
}
