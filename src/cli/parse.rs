// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// CLI parsing (manual, std-only).
// See: contracts/07_cli_contract_v0_1.md
// Parses argv into deterministic command and override structures.
// ASCII-only.

use crate::config::CliOverridesV1;
use super::{
    AlertCategoryFilterV1, AlertEntityKindFilterV1, CommandV1, MigrateModeV1,
};

#[derive(Clone, Debug)]
pub struct CliErrorV1 {
    pub msg: String,
}

fn take_value(args: &[String], i: &mut usize, flag: &str) -> Result<String, CliErrorV1> {
    if *i + 1 >= args.len() {
        return Err(CliErrorV1 {
            msg: format!("missing value for {}", flag),
        });
    }
    *i += 1;
    Ok(args[*i].clone())
}

fn parse_i64(s: &str, name: &str) -> Result<i64, CliErrorV1> {
    s.parse::<i64>().map_err(|_| CliErrorV1 {
        msg: format!("invalid {}: {}", name, s),
    })
}

fn parse_u64(s: &str, name: &str) -> Result<u64, CliErrorV1> {
    s.parse::<u64>().map_err(|_| CliErrorV1 {
        msg: format!("invalid {}: {}", name, s),
    })
}

fn parse_migrate_mode_v1(s: &str) -> Result<MigrateModeV1, CliErrorV1> {
    match s {
        "auto" => Ok(MigrateModeV1::Auto),
        "off" => Ok(MigrateModeV1::Off),
        "require" => Ok(MigrateModeV1::Require),
        _ => Err(CliErrorV1 {
            msg: format!("invalid migrate mode: {}", s),
        }),
    }
}

fn parse_alert_category_filter_v1(s: &str) -> Result<AlertCategoryFilterV1, CliErrorV1> {
    match s {
        "outlier" => Ok(AlertCategoryFilterV1::Outlier),
        "noise_suspect" => Ok(AlertCategoryFilterV1::NoiseSuspect),
        "info" => Ok(AlertCategoryFilterV1::Info),
        _ => Err(CliErrorV1 {
            msg: format!("invalid alert category: {}", s),
        }),
    }
}

fn parse_alert_entity_kind_filter_v1(s: &str) -> Result<AlertEntityKindFilterV1, CliErrorV1> {
    match s {
        "srcip" => Ok(AlertEntityKindFilterV1::SrcIp),
        "dstip" => Ok(AlertEntityKindFilterV1::DstIp),
        "userid" => Ok(AlertEntityKindFilterV1::UserId),
        "domain" => Ok(AlertEntityKindFilterV1::Domain),
        "host" => Ok(AlertEntityKindFilterV1::Host),
        _ => Err(CliErrorV1 {
            msg: format!("invalid alert entity kind: {}", s),
        }),
    }
}

fn validate_alert_entity_filter_pair_v1(
    command_label: &str,
    entity_kind: &Option<AlertEntityKindFilterV1>,
    entity_value: &Option<String>,
) -> Result<(), CliErrorV1> {
    match (entity_kind, entity_value) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => Err(CliErrorV1 {
            msg: format!("{}: missing --entity-value", command_label),
        }),
        (None, Some(_)) => Err(CliErrorV1 {
            msg: format!("{}: missing --entity-kind", command_label),
        }),
    }
}

pub fn parse_args_v1(argv: &[String]) -> Result<(CommandV1, CliOverridesV1), CliErrorV1> {
    // argv includes program name at index 0.
    let mut cli = CliOverridesV1::default();
    let mut i: usize = 1;

    // Parse global flags until first non-flag token.
    while i < argv.len() {
        let a = argv[i].as_str();
        if !a.starts_with("--") {
            break;
        }

        match a {
            "--config" => {
                let v = take_value(argv, &mut i, "--config")?;
                cli.config_path = Some(v);
            }
            "--watch-root" => {
                let v = take_value(argv, &mut i, "--watch-root")?;
                cli.watch_root = Some(v);
            }
            "--state-root" => {
                let v = take_value(argv, &mut i, "--state-root")?;
                cli.state_root = Some(v);
            }
            "--log-level" => {
                let v = take_value(argv, &mut i, "--log-level")?;
                cli.log_level = Some(v);
            }
            "--log-format" => {
                let v = take_value(argv, &mut i, "--log-format")?;
                cli.log_format = Some(v);
            }
            _ => {
                return Err(CliErrorV1 {
                    msg: format!("unknown flag: {}", a),
                });
            }
        }

        i += 1;
    }

    if i >= argv.len() {
        return Err(CliErrorV1 {
            msg: "missing command".to_string(),
        });
    }

    let cmd = argv[i].clone();
    i += 1;

    match cmd.as_str() {
        "run" => {
            let mut migrate = MigrateModeV1::Auto;
            while i < argv.len() {
                match argv[i].as_str() {
                    "--migrate" => {
                        let v = take_value(argv, &mut i, "--migrate")?;
                        migrate = parse_migrate_mode_v1(&v)?;
                    }
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown run arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }
            Ok((CommandV1::Run { migrate }, cli))
        }
        "oneshot" => {
            let mut tenant_id: Option<String> = None;
            let mut since: Option<i64> = None;
            let mut until: Option<i64> = None;
            let mut device_path: Option<String> = None;
            let mut migrate = MigrateModeV1::Auto;

            while i < argv.len() {
                match argv[i].as_str() {
                    "--tenant" => {
                        let v = take_value(argv, &mut i, "--tenant")?;
                        tenant_id = Some(v);
                    }
                    "--since" => {
                        let v = take_value(argv, &mut i, "--since")?;
                        since = Some(parse_i64(&v, "since")?);
                    }
                    "--until" => {
                        let v = take_value(argv, &mut i, "--until")?;
                        until = Some(parse_i64(&v, "until")?);
                    }
                    "--device" => {
                        let v = take_value(argv, &mut i, "--device")?;
                        device_path = Some(v);
                    }
                    "--migrate" => {
                        let v = take_value(argv, &mut i, "--migrate")?;
                        migrate = parse_migrate_mode_v1(&v)?;
                    }
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown oneshot arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }

            let tenant_id = tenant_id.ok_or_else(|| CliErrorV1 {
                msg: "oneshot: missing --tenant".to_string(),
            })?;
            if let (Some(since_v), Some(until_v)) = (since, until) {
                if since_v > until_v {
                    return Err(CliErrorV1 {
                        msg: "oneshot: --since must be <= --until".to_string(),
                    });
                }
            }

            Ok((CommandV1::OneShot { tenant_id, since, until, device_path, migrate }, cli))
        }
        "status" => {
            // status [--json]
            let mut json = false;
            while i < argv.len() {
                match argv[i].as_str() {
                    "--json" => json = true,
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown status arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }
            Ok((CommandV1::Status { json }, cli))
        }
        "version" => Ok((CommandV1::Version, cli)),
        "config" => {
            // config check
            if i >= argv.len() {
                return Err(CliErrorV1 {
                    msg: "missing config subcommand".to_string(),
                });
            }
            let sub = argv[i].as_str();
            match sub {
                "check" => Ok((CommandV1::ConfigCheck, cli)),
                _ => Err(CliErrorV1 {
                    msg: format!("unknown config subcommand: {}", sub),
                }),
            }
        }
        "tenant" => parse_tenant(argv, i, cli),
        "replay-spool" => {
            // replay-spool [--tenant <id>]
            let mut tenant_id: Option<String> = None;
            while i < argv.len() {
                match argv[i].as_str() {
                    "--tenant" => {
                        let v = take_value(argv, &mut i, "--tenant")?;
                        tenant_id = Some(v);
                    }
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown replay-spool arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }
            Ok((CommandV1::ReplaySpool { tenant_id }, cli))
        }
        "validate-fixtures" => {
            // validate-fixtures --fixture-root <path>
            let mut fixture_root: Option<String> = None;
            while i < argv.len() {
                match argv[i].as_str() {
                    "--fixture-root" => {
                        let v = take_value(argv, &mut i, "--fixture-root")?;
                        fixture_root = Some(v);
                    }
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown validate-fixtures arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }
            let fr = fixture_root.ok_or_else(|| CliErrorV1 {
                msg: "missing --fixture-root".to_string(),
            })?;
            Ok((CommandV1::ValidateFixtures { fixture_root: fr }, cli))
        }
        "migrate" => parse_migrate(argv, i, cli),
        "alerts" => parse_alerts(argv, i, cli),
        "alert" => parse_alert(argv, i, cli),
        _ => Err(CliErrorV1 {
            msg: format!("unknown command: {}", cmd),
        }),
    }
}

fn parse_tenant(argv: &[String], mut i: usize, cli: CliOverridesV1) -> Result<(CommandV1, CliOverridesV1), CliErrorV1> {
    if i >= argv.len() {
        return Err(CliErrorV1 {
            msg: "missing tenant subcommand".to_string(),
        });
    }
    let sub = argv[i].as_str();
    i += 1;

    match sub {
        "purge" => {
            if i >= argv.len() {
                return Err(CliErrorV1 {
                    msg: "missing tenant_id".to_string(),
                });
            }
            let tenant_id = argv[i].clone();
            i += 1;
            let mut force = false;
            while i < argv.len() {
                match argv[i].as_str() {
                    "--force" => force = true,
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown tenant purge arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }
            Ok((CommandV1::TenantPurge { tenant_id, force }, cli))
        }
        "policy" => {
            if i >= argv.len() {
                return Err(CliErrorV1 {
                    msg: "missing tenant policy subcommand".to_string(),
                });
            }
            let sub2 = argv[i].as_str();
            i += 1;
            if i >= argv.len() {
                return Err(CliErrorV1 {
                    msg: "missing tenant_id".to_string(),
                });
            }
            let tenant_id = argv[i].clone();
            i += 1;
            if i != argv.len() {
                return Err(CliErrorV1 {
                    msg: "unexpected extra args for tenant policy".to_string(),
                });
            }
            match sub2 {
                "show" => Ok((CommandV1::TenantPolicyShow { tenant_id }, cli)),
                "check" => Ok((CommandV1::TenantPolicyCheck { tenant_id }, cli)),
                _ => Err(CliErrorV1 {
                    msg: format!("unknown tenant policy subcommand: {}", sub2),
                }),
            }
        }
        _ => Err(CliErrorV1 {
            msg: format!("unknown tenant subcommand: {}", sub),
        }),
    }
}

fn parse_migrate(argv: &[String], mut i: usize, cli: CliOverridesV1) -> Result<(CommandV1, CliOverridesV1), CliErrorV1> {
    // migrate --tenant <id> | --all
    let mut tenant: Option<String> = None;
    let mut all = false;

    while i < argv.len() {
        match argv[i].as_str() {
            "--tenant" => {
                let v = take_value(argv, &mut i, "--tenant")?;
                tenant = Some(v);
            }
            "--all" => all = true,
            _ => {
                return Err(CliErrorV1 {
                    msg: format!("unknown migrate arg: {}", argv[i]),
                });
            }
        }
        i += 1;
    }

    if all && tenant.is_some() {
        return Err(CliErrorV1 {
            msg: "migrate: cannot use --all with --tenant".to_string(),
        });
    }
    if all {
        return Ok((CommandV1::MigrateAll, cli));
    }
    if let Some(tid) = tenant {
        return Ok((CommandV1::MigrateTenant { tenant_id: tid }, cli));
    }

    Err(CliErrorV1 {
        msg: "migrate: missing --all or --tenant".to_string(),
    })
}

fn parse_alerts(argv: &[String], mut i: usize, cli: CliOverridesV1) -> Result<(CommandV1, CliOverridesV1), CliErrorV1> {
    if i >= argv.len() {
        return Err(CliErrorV1 {
            msg: "missing alerts subcommand".to_string(),
        });
    }
    let sub = argv[i].as_str();
    i += 1;

    match sub {
        "list" => {
            let mut tenant_id: Option<String> = None;
            let mut since: Option<i64> = None;
            let mut until: Option<i64> = None;
            let mut category: Option<AlertCategoryFilterV1> = None;
            let mut entity_kind: Option<AlertEntityKindFilterV1> = None;
            let mut entity_value: Option<String> = None;
            let mut json = false;

            while i < argv.len() {
                match argv[i].as_str() {
                    "--tenant" => {
                        let v = take_value(argv, &mut i, "--tenant")?;
                        tenant_id = Some(v);
                    }
                    "--since" => {
                        let v = take_value(argv, &mut i, "--since")?;
                        since = Some(parse_i64(&v, "since")?);
                    }
                    "--until" => {
                        let v = take_value(argv, &mut i, "--until")?;
                        until = Some(parse_i64(&v, "until")?);
                    }
                    "--category" => {
                        let v = take_value(argv, &mut i, "--category")?;
                        category = Some(parse_alert_category_filter_v1(&v)?);
                    }
                    "--entity-kind" => {
                        let v = take_value(argv, &mut i, "--entity-kind")?;
                        entity_kind = Some(parse_alert_entity_kind_filter_v1(&v)?);
                    }
                    "--entity-value" => {
                        let v = take_value(argv, &mut i, "--entity-value")?;
                        entity_value = Some(v);
                    }
                    "--json" => json = true,
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown alerts list arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }

            let tid = tenant_id.ok_or_else(|| CliErrorV1 {
                msg: "alerts list: missing --tenant".to_string(),
            })?;
            validate_alert_entity_filter_pair_v1("alerts list", &entity_kind, &entity_value)?;
            Ok((
                CommandV1::AlertsList {
                    tenant_id: tid,
                    since,
                    until,
                    category,
                    entity_kind,
                    entity_value,
                    json,
                },
                cli,
            ))
        }
        "show" => {
            let mut tenant_id: Option<String> = None;
            let mut alert_id: Option<String> = None;
            let mut json = false;

            while i < argv.len() {
                match argv[i].as_str() {
                    "--tenant" => {
                        let v = take_value(argv, &mut i, "--tenant")?;
                        tenant_id = Some(v);
                    }
                    "--alert-id" => {
                        let v = take_value(argv, &mut i, "--alert-id")?;
                        alert_id = Some(v);
                    }
                    "--json" => json = true,
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown alerts show arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }

            let tid = tenant_id.ok_or_else(|| CliErrorV1 {
                msg: "alerts show: missing --tenant".to_string(),
            })?;
            let aid = alert_id.ok_or_else(|| CliErrorV1 {
                msg: "alerts show: missing --alert-id".to_string(),
            })?;
            Ok((CommandV1::AlertsShow { tenant_id: tid, alert_id: aid, json }, cli))
        }
        "search" => {
            let mut tenant_id: Option<String> = None;
            let mut since: Option<i64> = None;
            let mut until: Option<i64> = None;
            let mut category: Option<AlertCategoryFilterV1> = None;
            let mut entity_kind: Option<AlertEntityKindFilterV1> = None;
            let mut entity_value: Option<String> = None;
            let mut contains: Option<String> = None;

            while i < argv.len() {
                match argv[i].as_str() {
                    "--tenant" => {
                        let v = take_value(argv, &mut i, "--tenant")?;
                        tenant_id = Some(v);
                    }
                    "--since" => {
                        let v = take_value(argv, &mut i, "--since")?;
                        since = Some(parse_i64(&v, "since")?);
                    }
                    "--until" => {
                        let v = take_value(argv, &mut i, "--until")?;
                        until = Some(parse_i64(&v, "until")?);
                    }
                    "--category" => {
                        let v = take_value(argv, &mut i, "--category")?;
                        category = Some(parse_alert_category_filter_v1(&v)?);
                    }
                    "--entity-kind" => {
                        let v = take_value(argv, &mut i, "--entity-kind")?;
                        entity_kind = Some(parse_alert_entity_kind_filter_v1(&v)?);
                    }
                    "--entity-value" => {
                        let v = take_value(argv, &mut i, "--entity-value")?;
                        entity_value = Some(v);
                    }
                    "--contains" => {
                        let v = take_value(argv, &mut i, "--contains")?;
                        contains = Some(v);
                    }
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown alerts search arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }

            let tid = tenant_id.ok_or_else(|| CliErrorV1 {
                msg: "alerts search: missing --tenant".to_string(),
            })?;
            let c = contains.ok_or_else(|| CliErrorV1 {
                msg: "alerts search: missing --contains".to_string(),
            })?;
            validate_alert_entity_filter_pair_v1("alerts search", &entity_kind, &entity_value)?;
            Ok((
                CommandV1::AlertsSearch {
                    tenant_id: tid,
                    since,
                    until,
                    category,
                    entity_kind,
                    entity_value,
                    contains: c,
                },
                cli,
            ))
        }
        "export" => {
            let mut tenant_id: Option<String> = None;
            let mut category: Option<AlertCategoryFilterV1> = None;
            let mut entity_kind: Option<AlertEntityKindFilterV1> = None;
            let mut entity_value: Option<String> = None;
            let mut out_path: Option<String> = None;
            let mut gzip = false;

            while i < argv.len() {
                match argv[i].as_str() {
                    "--tenant" => {
                        let v = take_value(argv, &mut i, "--tenant")?;
                        tenant_id = Some(v);
                    }
                    "--out" => {
                        let v = take_value(argv, &mut i, "--out")?;
                        out_path = Some(v);
                    }
                    "--category" => {
                        let v = take_value(argv, &mut i, "--category")?;
                        category = Some(parse_alert_category_filter_v1(&v)?);
                    }
                    "--entity-kind" => {
                        let v = take_value(argv, &mut i, "--entity-kind")?;
                        entity_kind = Some(parse_alert_entity_kind_filter_v1(&v)?);
                    }
                    "--entity-value" => {
                        let v = take_value(argv, &mut i, "--entity-value")?;
                        entity_value = Some(v);
                    }
                    "--gzip" => gzip = true,
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown alerts export arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }

            let tid = tenant_id.ok_or_else(|| CliErrorV1 {
                msg: "alerts export: missing --tenant".to_string(),
            })?;
            let out = out_path.ok_or_else(|| CliErrorV1 {
                msg: "alerts export: missing --out".to_string(),
            })?;
            validate_alert_entity_filter_pair_v1("alerts export", &entity_kind, &entity_value)?;
            Ok((
                CommandV1::AlertsExport {
                    tenant_id: tid,
                    category,
                    entity_kind,
                    entity_value,
                    out_path: out,
                    gzip,
                },
                cli,
            ))
        }
        _ => Err(CliErrorV1 {
            msg: format!("unknown alerts subcommand: {}", sub),
        }),
    }
}

fn parse_alert(argv: &[String], mut i: usize, cli: CliOverridesV1) -> Result<(CommandV1, CliOverridesV1), CliErrorV1> {
    if i >= argv.len() {
        return Err(CliErrorV1 {
            msg: "missing alert subcommand".to_string(),
        });
    }
    let sub = argv[i].as_str();
    i += 1;

    match sub {
        "extract" => {
            let mut tenant_id: Option<String> = None;
            let mut alert_id: Option<String> = None;
            let mut out_path: Option<String> = None;
            let mut max_bytes: Option<u64> = None;
            let mut max_lines: Option<u64> = None;

            while i < argv.len() {
                match argv[i].as_str() {
                    "--tenant" => {
                        let v = take_value(argv, &mut i, "--tenant")?;
                        tenant_id = Some(v);
                    }
                    "--alert-id" => {
                        let v = take_value(argv, &mut i, "--alert-id")?;
                        alert_id = Some(v);
                    }
                    "--out" => {
                        let v = take_value(argv, &mut i, "--out")?;
                        out_path = Some(v);
                    }
                    "--max-bytes" => {
                        let v = take_value(argv, &mut i, "--max-bytes")?;
                        max_bytes = Some(parse_u64(&v, "max-bytes")?);
                    }
                    "--max-lines" => {
                        let v = take_value(argv, &mut i, "--max-lines")?;
                        max_lines = Some(parse_u64(&v, "max-lines")?);
                    }
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown alert extract arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }

            let tid = tenant_id.ok_or_else(|| CliErrorV1 {
                msg: "alert extract: missing --tenant".to_string(),
            })?;
            let aid = alert_id.ok_or_else(|| CliErrorV1 {
                msg: "alert extract: missing --alert-id".to_string(),
            })?;
            let out = out_path.ok_or_else(|| CliErrorV1 {
                msg: "alert extract: missing --out".to_string(),
            })?;
            Ok((CommandV1::AlertExtract { tenant_id: tid, alert_id: aid, out_path: out, max_bytes, max_lines }, cli))
        }
        "drill" => {
            let mut tenant_id: Option<String> = None;
            let mut alert_id: Option<String> = None;
            let mut max_bytes: Option<u64> = None;
            let mut max_lines: Option<u64> = None;

            while i < argv.len() {
                match argv[i].as_str() {
                    "--tenant" => {
                        let v = take_value(argv, &mut i, "--tenant")?;
                        tenant_id = Some(v);
                    }
                    "--alert-id" => {
                        let v = take_value(argv, &mut i, "--alert-id")?;
                        alert_id = Some(v);
                    }
                    "--max-bytes" => {
                        let v = take_value(argv, &mut i, "--max-bytes")?;
                        max_bytes = Some(parse_u64(&v, "max-bytes")?);
                    }
                    "--max-lines" => {
                        let v = take_value(argv, &mut i, "--max-lines")?;
                        max_lines = Some(parse_u64(&v, "max-lines")?);
                    }
                    _ => {
                        return Err(CliErrorV1 {
                            msg: format!("unknown alert drill arg: {}", argv[i]),
                        });
                    }
                }
                i += 1;
            }

            let tid = tenant_id.ok_or_else(|| CliErrorV1 {
                msg: "alert drill: missing --tenant".to_string(),
            })?;
            let aid = alert_id.ok_or_else(|| CliErrorV1 {
                msg: "alert drill: missing --alert-id".to_string(),
            })?;
            Ok((CommandV1::AlertDrill { tenant_id: tid, alert_id: aid, max_bytes, max_lines }, cli))
        }
        _ => Err(CliErrorV1 {
            msg: format!("unknown alert subcommand: {}", sub),
        }),
    }
}
