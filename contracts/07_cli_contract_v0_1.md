# CLI Contract v0.1

This contract defines `sparx` CLI commands, arguments, exit codes, and observable behavior.
It is the reference for operators and integration tests.

## Binary
- `sparx`

## Default config
- Default path: `/etc/sparx/sparx.toml`
- Config path override: `--config <path>`
- Override precedence (highest to lowest):
  1) CLI flags
  2) Environment variables
  3) Config file
  4) Built-in defaults

Notes:
- The config surface is defined by Config Schema Contract v0.1.

## Required inputs
These are required only if not provided by config.

- `--watch-root <path>`
  - overrides the configured `tenant_root` (root directory containing tenant directories)
- `--state-root <path>`
  - overrides the configured `data_root` (embedded DB + spool + alerts output roots)

## Commands
Common flags (all commands):
- `--config <path>` (default `/etc/sparx/sparx.toml`)
- `--log-level <error|warn|info|debug|trace>` overrides config
- `--log-format <text|json>` overrides config

Exit codes:
- `0` success
- `1` generic error
- `2` config error (parse/validation)
- `3` IO error (filesystem)
- `4` DB error (embedded DB open/lock/read/write)
- `5` invariant violation (bug; should be rare)
- `6` partial success (used only where noted)

## General command rules
- Config-free commands must bypass config load/validation.
- DB-backed commands must fail closed if the embedded DB path is already owned by another sparx process.
- Partial checkpoints or stubbed command routes must never return exit `0` for unimplemented operational behavior.

### `sparx run`
Daemon mode (poll + window + score + emit).

Usage:
- `sparx run [--config <path>] [--watch-root <path>] [--state-root <path>] [--migrate auto|off|require]`

Behavior:
- loads config
- opens global DB
- discovers tenants and starts ingest workers
- runs until SIGINT/SIGTERM
- emits alerts via configured sink

Exit:
- 0 on clean shutdown
- 1/3/4 on failures, depending on error category

### `sparx oneshot`
One pass then exit (backfill/test mode).

Usage:
- `sparx oneshot --tenant <tenant_id> [--since <ts>] [--until <ts>] [--device <device_path>] [--migrate auto|off|require]`

Behavior:
- requires `--tenant <tenant_id>`
- processes available data once and exits after reaching EOF for selected files
- supports optional unix-second UTC filters via `--since` and `--until`
- supports optional device scoping via `--device <device_path>`
- advances cursors and window state as in daemon mode
- emits alerts via configured sink
- returns deterministic mixed-outcome status across devices

Exit:
- 0 success
- 6 partial success if some device files failed but others succeeded (must log failures)

Notes:
- `ts` values are unix seconds (UTC) in v0.1.

### `sparx status`
Operational status summary.

Usage:
- `sparx status [--json]`

Output (text or JSON):
- process version
- config summary (effective window size, roots, sink)
- known tenants count
- active tenants count
- last run timestamps (if available)
- selected process/runtime state already persisted in the global DB (for example last exit code, last host, global schema state)

Exit:
- 0 success
- 4 DB error

Notes:
- This command may read global DB for tenant registry.

### `sparx tenant purge <tenant_id>`
Purge tenant DB directory and associated outputs.

Usage:
- `sparx tenant purge <tenant_id> [--force]`

Behavior:
- requires tenant status to be terminating (2) unless `--force`
- deletes tenant DB + alerts + spool directories (Service and Deployment Contract v0.1)
- writes purge journal entries in global DB
- sets status to terminated (3)

Exit:
- 0 success
- 1 tenant not found
- 3 IO error (delete failures)
- 4 DB error
- 6 partial success if some directories deleted and others failed (must log details)

### `sparx config check`
Validate layout and config.

Usage:
- `sparx config check [--config <path>] [--watch-root <path>] [--state-root <path>]`

Behavior:
- loads config and applies overrides
- validates required directories are present or creatable
- validates embedded DB directories are writable/creatable
- validates window_size whitelist/bounds

Exit:
- 0 success
- 2 config error
- 3 IO error

### `sparx version`
Print version and exit.

Usage:
- `sparx version`

Behavior:
- config-free command
- must not require config parsing or DB access

Exit:
- 0 success

---

## Additional operational commands (v0.1)

### `sparx replay-spool`
Replay spooled alerts into the configured replay-compatible file sink.

Usage:
- `sparx replay-spool [--tenant <tenant_id>]`

Behavior:
- filesystem/config command only; does not open the embedded DB
- requires `output.sink=jsonl` in v0.1
- `output.sink=stdout` is not replay-compatible and must fail non-zero
- replays spooled alerts into the configured JSONL destination
- deletes spool files on success
- deterministic ordering by filename

Exit:
- 0 success
- 6 partial success if some spool files could not be replayed

### `sparx validate-fixtures`
Validate fixture corpus layout and expected outputs.

Usage:
- `sparx validate-fixtures --fixture-root <path>`

Behavior:
- config-free command
- validates fixture corpus directory structure per Fixture Corpus Contract v0.1
- validates sample logs and expected outputs where provided
- does not modify DB state

Exit:
- 0 success
- 1 validation failure
- 3 IO error

---

## Additional CLI subcommands (v0.1)

These commands are defined as part of v0.1 and are required for operational workflows.
The detailed behavior is specified in their respective contracts.

### `sparx tenant policy show|check`
Reference: Overrides and Tenant Policy Contract v0.1 (`13_overrides_tenant_policy_v0_1.md`)

Usage:
- `sparx tenant policy show <tenant_id>`
- `sparx tenant policy check <tenant_id>`

### `sparx migrate`
Reference: Schema Versioning + Migration Contract v0.1 (`14_schema_migrations_v0_1.md`)

Usage:
- `sparx migrate --tenant <tenant_id>`
- `sparx migrate --all`
- Controls: `--migrate auto|off|require` (also supported by `run` and `oneshot`)

### `sparx alerts list|show|search|export`
Reference: Alert Query CLI Contract v0.1 (`15_alert_query_cli_v0_1.md`)

Usage:
- `sparx alerts list --tenant <tenant_id> [--since <ts>] [--until <ts>] [--category <outlier|noise_suspect|info>] [--entity-kind <srcip|dstip|userid|domain|host> --entity-value <value>] [--json]`
- `sparx alerts show --tenant <tenant_id> --alert-id <id>`
- `sparx alerts search --tenant <tenant_id> [--since <ts>] [--until <ts>] [--category <outlier|noise_suspect|info>] [--entity-kind <srcip|dstip|userid|domain|host> --entity-value <value>] --contains <text>`
- `sparx alerts export --tenant <tenant_id> [--category <outlier|noise_suspect|info>] [--entity-kind <srcip|dstip|userid|domain|host> --entity-value <value>] --out <path> [--gzip]`

### `sparx alert extract|drill`
Reference: Raw Log Drilldown Contract v0.1 (`16_raw_log_drilldown_v0_1.md`)

Usage:
- `sparx alert extract --tenant <tenant_id> --alert-id <id> --out <path> [--max-bytes <n>] [--max-lines <n>]`
- `sparx alert drill --tenant <tenant_id> --alert-id <id> [--max-bytes <n>] [--max-lines <n>]`

## Output format guarantees

### Human-readable text
- stable column ordering where applicable
- timestamps printed as unix seconds (UTC) in v0.1

### JSON
- snake_case field names
- command outputs are objects or arrays (not NDJSON), except alert sinks (Output Sink Contract v0.1)

## Window time basis
- UTC minute boundaries (v0.1)
- If `window_size_s` is not 60, windows still align to UTC epoch boundaries of that size.
