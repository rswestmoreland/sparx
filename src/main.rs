// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// sparx CLI entrypoint.
// Keeps process setup minimal and delegates parsing/routing to library modules.

fn main() {
    let argv: Vec<String> = std::env::args().collect();

    let (cmd, cli_overrides) = match sparx::cli::parse::parse_args_v1(&argv) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("cli error: {}", e.msg);
            eprintln!("{}", usage());
            std::process::exit(1);
        }
    };

    let r = if sparx::cli::route::command_requires_config_v1(&cmd) {
        let cfg = match sparx::config::load::load_config_v1(&cli_overrides) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("config load failed: {}", e.msg);
                std::process::exit(2);
            }
        };
        if let Err(e) = sparx::config::validate::validate_config_v1(&cfg) {
            eprintln!("config invalid: {}", e.msg);
            std::process::exit(2);
        }
        sparx::cli::route::route_command_v1(&cmd, &cfg)
    } else {
        sparx::cli::route::route_command_no_config_v1(&cmd)
    };

    if let Some(s) = r.msg_stdout {
        println!("{}", s);
    }
    if let Some(s) = r.msg_stderr {
        eprintln!("{}", s);
    }
    std::process::exit(r.exit_code);
}

fn usage() -> &'static str {
    "usage: sparx [--config PATH] [--watch-root PATH] [--state-root PATH] [--log-level L] [--log-format F] <command> ...
commands:
  run [--migrate auto|off|require]
  oneshot --tenant <tenant_id> [--since <ts>] [--until <ts>] [--device <device_path>] [--migrate auto|off|require]
  status [--json]
  version
  config check
  tenant purge <tenant_id> [--force]
  tenant policy show <tenant_id>
  tenant policy check <tenant_id>
  migrate --tenant <tenant_id> | --all
  replay-spool [--tenant <tenant_id>]
  validate-fixtures --fixture-root <path>
  alerts list --tenant <tenant_id> [--since <ts>] [--until <ts>]
  alerts show --tenant <tenant_id> --alert-id <id>
  alerts search --tenant <tenant_id> --contains <text>
  alerts export --tenant <tenant_id> --out <path>
  alert extract --tenant <tenant_id> --alert-id <id> --out <path>
  alert drill --tenant <tenant_id> --alert-id <id>
"
}
