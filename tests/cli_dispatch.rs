use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_path(name: &str, ext: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("sparx_{}_{}_{}.{}", name, std::process::id(), ts, ext))
}

#[test]
fn version_bypasses_invalid_config_file() {
    let cfg = unique_temp_path("bad_cfg_version", "toml");
    fs::write(&cfg, "[sparx\nmode = \"daemon\"\n").unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_sparx"))
        .arg("--config")
        .arg(&cfg)
        .arg("version")
        .output()
        .unwrap();

    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stdout.contains("sparx 0.0.0"));
    assert!(stderr.trim().is_empty());

    let _ = fs::remove_file(cfg);
}

#[test]
fn validate_fixtures_bypasses_invalid_config_file() {
    let cfg = unique_temp_path("bad_cfg_fixture", "toml");
    fs::write(&cfg, "[sparx\nmode = \"daemon\"\n").unwrap();

    let root = unique_temp_path("fixture_root", "dir");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("tenants/acme/devices")).unwrap();
    fs::create_dir_all(root.join("golden")).unwrap();
    fs::create_dir_all(root.join("gen")).unwrap();
    fs::write(
        root.join("tenants/acme/devices/linux.log"),
        "Jan  1 00:00:01 host sshd[1]: Accepted password for alice\n",
    )
    .unwrap();
    fs::write(root.join("golden/alerts_subset.json"), "{\"alerts\":[]}").unwrap();
    fs::write(root.join("gen/scenario.toml"), "seed = 1\n").unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_sparx"))
        .arg("--config")
        .arg(&cfg)
        .arg("validate-fixtures")
        .arg("--fixture-root")
        .arg(&root)
        .output()
        .unwrap();

    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stdout.contains("fixture validation ok"));
    assert!(stderr.trim().is_empty());

    let _ = fs::remove_file(cfg);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn run_runtime_failure_is_nonzero_and_fail_closed() {
    let cfg = unique_temp_path("run_cfg_missing_watch", "toml");
    let root = unique_temp_path("run_missing_watch_root", "dir");
    let missing_watch = root.join("watch_missing");
    let state_root = root.join("state");
    let tenant_db_root = state_root.join("tenants");
    let global_db_path = state_root.join("global.db");
    let alert_out_root = root.join("alerts");
    fs::create_dir_all(&state_root).unwrap();
    fs::write(
        &cfg,
        format!(
            concat!(
                "[sparx]\n",
                "mode = \"daemon\"\n",
                "tenant_root = '{}'\n",
                "data_root = '{}'\n",
                "global_db_path = '{}'\n",
                "tenant_db_root = '{}'\n",
                "alert_out_root = '{}'\n",
            ),
            missing_watch.display(),
            state_root.display(),
            global_db_path.display(),
            tenant_db_root.display(),
            alert_out_root.display(),
        ),
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_sparx"))
        .arg("--config")
        .arg(&cfg)
        .arg("run")
        .output()
        .unwrap();

    assert_eq!(out.status.code(), Some(3));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("run discovery error"));

    let _ = fs::remove_file(cfg);
    let _ = fs::remove_dir_all(root);
}

