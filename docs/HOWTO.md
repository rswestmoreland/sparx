# sparx HOWTO

This guide shows how to build sparx, prepare the required log directory layout,
run a single ingest pass, and inspect emitted alerts.

## Requirements

Minimum practical requirements for a small deployment or validation host:

- Enterprise Linux or another Linux host with local filesystem access
- Rust toolchain compatible with `rust-toolchain.toml` for source builds
- read access to the tenant log root
- write access to the sparx state root
- local storage for Fjall state, alert output, and replay spool data

The current benchmark planning estimates in the README assume a release/bench
build on a modest single-node Linux validation environment with local storage.
Actual throughput depends on CPU, storage, log shape, gzip/zlg share, row width,
output sink, and tenant/device mix.

## Build from source

From the repository root:

```bash
cargo build --release
./target/release/sparx version
```

For release validation, run the full validation flow separately:

```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo bench --bench tenant_device_eps
```

## Directory layout for logs

sparx reads a tenant root containing tenant directories. Each tenant directory
contains device directories. Each device directory contains regular log files.

Default production-style layout:

```text
/var/log/tenants/
  acme-customer/
    primary-fw-a01/
      messages.log
      traffic.cef
      archive.log.gz
      archive.zlg
    domain-controller-01/
      security.json
```

Rules:

- tenant id is the first directory under the tenant root
- device path is the path under the tenant directory
- regular files only
- symlinks are ignored by default
- hidden files are ignored
- default accepted suffixes include `.log`, `.txt`, `.json`, `.csv`, `.cef`, `.gz`, and `.zlg`
- gzip handling is enabled by default for configured gzip suffixes
- `.zlg` archives are read as finalized zstd-backed log archives and should be replaced atomically rather than edited in place

A small local test layout can be created without root privileges:

```bash
mkdir -p /tmp/sparx-demo/tenants/acme-customer/primary-fw-a01
mkdir -p /tmp/sparx-demo/state
cat > /tmp/sparx-demo/tenants/acme-customer/primary-fw-a01/messages.log <<'LOG'
2026-05-11T10:00:00Z primary-fw-a01 action=allow src=10.1.2.3 dst=10.2.3.4 user=alice service=https
2026-05-11T10:00:01Z primary-fw-a01 action=deny src=10.9.9.9 dst=10.2.3.4 user=bob service=ssh reason=policy
2026-05-11T10:00:02Z primary-fw-a01 action=deny src=10.9.9.9 dst=10.2.3.4 user=bob service=ssh reason=policy
LOG
```

## Configuration

The default config path is `/etc/sparx/sparx.toml`. CLI options can override
important paths for testing:

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  config check
```

A minimal config file can also be used:

```toml
[sparx]
data_root = "/var/lib/sparx"
tenant_root = "/var/log/tenants"
log_level = "info"
log_format = "text"

[output]
sink = "jsonl"

[ingest]
window_size_s = 60
max_emit_latency_s = 600
```

For a tiny demo corpus, default scoring may build baseline state without
emitting an alert. For test-only exploration, lower the scoring floors in a
separate local config rather than changing production defaults:

```toml
[scoring]
cold_start_days = 0
min_lines_per_window = 1
outlier_threshold = 0.10
noise_threshold = 0.05
```

## Run one ingest pass

Use `oneshot` for a bounded single pass:

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  oneshot --tenant acme-customer --migrate auto
```

Use `run` for daemon-style polling:

```bash
./target/release/sparx \
  --watch-root /var/log/tenants \
  --state-root /var/lib/sparx \
  run --migrate auto
```

## Check status and health

Text status:

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  status
```

JSON status:

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  status --json
```

When enabled, runtime health and metrics endpoints use the configured bind
addresses. Defaults are documented in `contracts/28_config_schema_v0_1.md`.

## Inspect alerts

List alerts for a tenant:

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  alerts list --tenant acme-customer
```

Show one alert:

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  alerts show --tenant acme-customer --alert-id <alert_id>
```

Export alerts deterministically:

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  alerts export --tenant acme-customer --out /tmp/sparx-demo/acme-alerts.jsonl
```

Drill into or extract source evidence for an alert:

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  alert drill --tenant acme-customer --alert-id <alert_id>

./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  alert extract --tenant acme-customer --alert-id <alert_id> --out /tmp/sparx-demo/evidence
```

## Replay spooled alert output

`replay-spool` is filesystem/config based and does not open Fjall. It is valid
only for replay-compatible file sinks. Stdout replay fails closed.

```bash
./target/release/sparx \
  --watch-root /tmp/sparx-demo/tenants \
  --state-root /tmp/sparx-demo/state \
  replay-spool --tenant acme-customer
```

## Benchmark locally

Default benchmark:

```bash
cargo bench --bench tenant_device_eps
```

Larger workload:

```bash
SPARX_BENCH_TENANTS=2 \
SPARX_BENCH_DEVICES_PER_TENANT=10 \
SPARX_BENCH_FILES_PER_DEVICE=5 \
SPARX_BENCH_EVENTS_PER_FILE=1000 \
cargo bench --bench tenant_device_eps
```

Durable oneshot timing:

```bash
SPARX_BENCH_DURABLE_ONESHOT=1 cargo bench --bench tenant_device_eps
```

## Troubleshooting

- `config check` should pass before `run` or `oneshot`.
- Ensure the tenant root exists and contains tenant/device directories.
- Ensure the state root is writable by the sparx process.
- If no alert is emitted from a tiny demo corpus, confirm that rows finalized and
  remember that default scoring suppresses cold-start and very low-volume noise.
- Use `status --json` for machine-readable diagnostics.
- Keep one sparx process owning a state root at a time.

## License and author

sparx is open source under the MIT License.

Author: Richard S. Westmoreland  
Contact: dev@rswestmore.land  
Copyright (c) 2026 Richard S. Westmoreland.

See `../LICENSE` for the full license text.
