# Service and Deployment Contract v0.1

This contract defines how `sparx` is deployed and operated on Enterprise Linux (systemd-first).

## Goals
- Predictable filesystem layout and permissions.
- Safe defaults for high-volume tailing (nofile, memory, CPU).
- Deterministic service behavior on restart.
- Tenant lifecycle support (enable/disable/purge).

## Non-goals (v0.1)
- Kubernetes manifests.
- Multi-node clustering.
- Secrets management beyond OS facilities.
- Live multi-process DB administration.

---

## 1) Service identity

### 1.1 Binary
- Service executable: `/usr/bin/sparx`

### 1.2 User and group
- Recommended service user: `sparx`
- Recommended group: `sparx`

The service must run as non-root.

---

## 2) Filesystem layout

Paths are derived from Config Schema v0.1 defaults unless overridden.

### 2.1 Config
- `/etc/sparx/sparx.toml` (0640, owner root:sparx)

### 2.2 Data
- `/var/lib/sparx/` (0750, owner sparx:sparx)
  - `global.db/` (embedded DB directory)
  - `tenants/` (tenant DB directories)
  - `alerts/` (JSONL output)
  - `spool/alerts/` (spooled alerts)

### 2.3 Tenant log input root
- `/var/log/tenants/` (readable by sparx user)
  - `<tenant_id>/<device_dir>/...`

If input logs are not readable, deployment must grant access via group membership or ACLs.

### 2.4 PID file (optional)
- `/run/sparx.pid` (created by service user)

---

## 3) systemd unit expectations

### 3.1 Unit naming
- Unit name: `sparx.service`

### 3.2 Exec
- `ExecStart=/usr/bin/sparx run --config /etc/sparx/sparx.toml`

### 3.3 Restart behavior
- `Restart=on-failure`
- `RestartSec=2`

### 3.4 Resource limits
- `LimitNOFILE=65536` (minimum recommended)
- `Nice=0` (default)
- `MemoryMax` optional; default is unset in v0.1.
- CPU affinity is optional; default is unset in v0.1.

Notes:
- High volume + many devices require high `nofile` and conservative tenant DB open-handle limits.

### 3.5 Capabilities and sandboxing
v0.1 recommendation: minimal restrictions to avoid breaking file tailing.

Optional hardening (not required in v0.1):
- `NoNewPrivileges=true`
- `ProtectSystem=strict`
- `ProtectHome=true`
- `PrivateTmp=true`
- `ProtectKernelTunables=true`
- `ProtectKernelModules=true`

These may require explicit read/write allowances:
- `ReadWritePaths=/var/lib/sparx /run`
- `ReadOnlyPaths=/var/log/tenants /etc/sparx`

---

## 4) Operational behaviors

### 4.1 DB ownership model
- In the current Fjall-based design, a given global or tenant DB path has a single active sparx process owner.
- DB-backed CLI commands must fail with a DB error if another sparx process already owns the DB path.
- v0.1 does not include a live admin/query control plane.

### 4.2 Startup
On startup, the service must:
- open or create `global.db`
- discover tenants under `tenant_root`
- for each active tenant:
  - open tenant DB (LRU handle pool)
  - reconcile cursors and active windows
  - begin polling ingest

### 4.3 Restart safety
- At-least-once alert emission is allowed.
- Open-window checkpoint recovery follows Open-Window Checkpoint Encoding v0.1.
- If an active window references missing win_row keys, worker must safely reinitialize a new empty active window and increment a counter.

### 4.4 Shutdown
On SIGTERM:
- flush active window checkpoints
- flush JSONL writers
- attempt a final spool replay pass (best-effort)
- exit 0

---

## 5) Tenant lifecycle operations

### 5.1 Disable tenant (stop ingest/emit)
Mechanisms:
- set global DB key: `tenant/v1/<tenant_id>/status = 1 (disabled)` (Global DB Key Prefix Map v0.1)
- service must stop ingesting that tenant within `max_emit_latency_s`

### 5.2 Terminate tenant (full purge)
Mechanisms:
- set status to `2 (terminating)` and run `sparx tenant purge <tenant_id>`

Purge deletes:
- tenant DB directory: `{tenant_db_root}/tenant=<tenant_id>/tenant.db`
- tenant alert output directory: `{alert_out_root}/tenant=<tenant_id>/`
- tenant spool directory: `{data_root}/spool/alerts/tenant=<tenant_id>/`

Global DB behavior:
- append purge journal entries under `tenant_purge/v1/<tenant_id>/...`
- set status to `3 (terminated)` on success

Notes:
- Purge is destructive and must require explicit CLI invocation.

---

## 6) Log rotation expectations
- Input logs are managed externally (rsyslog/logrotate/vendor).
- Output JSONL rotation is internal per Output Sink Contract v0.1.
- System logs: `sparx` logs to stdout/stderr; systemd journal captures it.

---

## 7) Required tests (integration)
- systemd-style graceful shutdown triggers flush paths (simulated)
- tenant termination deletes expected directories (in temp sandbox)
- disabled tenant stops ingest without process restart
- DB-backed command fails cleanly when the DB path is already owned by another sparx process
