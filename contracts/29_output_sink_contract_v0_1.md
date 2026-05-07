# Output Sink Contract v0.1

This contract defines how `sparx` emits alerts to external consumers and how delivery semantics work.

## Goals
- Simple, robust sinks that work well on Enterprise Linux.
- At-least-once delivery semantics with deterministic IDs for de-duplication.
- Bounded disk usage and predictable rotation.
- Minimal dependencies and operational simplicity.

## Non-goals (v0.1)
- Exactly-once delivery end-to-end.
- Remote queueing systems (Kafka/SQS/etc) or webhooks.
- Encrypted sinks (leave to filesystem/OS controls in v0.1).

---

## 1) Sinks supported (v0.1)

Configured by `[output].sink` (Config Schema v0.1):

- `jsonl` (default)
- `stdout`

All sinks emit the same logical `AlertV1` object (Alert Object Schema v0.1).

---

## 2) Delivery semantics

### 2.1 At-least-once
An alert may be emitted more than once (restarts, retries, sink failures).

De-duplication key:
- `alert_id` (stable) from Alert Object Schema v0.1.

### 2.2 Ordering
- Ordering is best-effort by `window_start_ts` within a device.
- No global ordering guarantees across devices or tenants.

### 2.3 Failure behavior
- For `stdout`: failures are fatal only if the write call fails (rare).
- For `jsonl`: active runtime writes fail closed on open/write/flush errors.
- Replay/spool helper behavior is defined in section 4, but automatic fallback from the active `run`/`oneshot` sink path is deferred in v0.1.

---

## 3) JSONL sink

### 3.1 Output layout
Base directory:
- `{alert_out_root}/tenant=<tenant_id>/device=<device_key>/YYYY/MM/DD/`

File naming:
- `alerts_<device_key>_<YYYYMMDD>_<seq>.jsonl`
- `seq` starts at 0000 and increments on rotation.

Each line:
- one JSON object for `AlertV1`.

### 3.2 JSON encoding
- UTF-8 JSON text
- One object per line, newline terminated.
- Field names match the AlertV1 struct (snake_case).
- `include_debug_fields` controls optional fields.

### 3.3 Rotation
Rotation triggers:
- file size exceeds `jsonl_rotate_mb` (default 256 MB), OR
- date changes (new day directory)

Rotation behavior:
- close current file
- open a new file with incremented `seq`

### 3.4 Flush
- Flush interval: `jsonl_flush_interval_s` (default 5s).
- Always flush on:
  - process shutdown
  - rotation
  - after writing an alert if `sink=stdout` (stdout is line buffered by default; still write newline)

### 3.5 Permissions
- Directory mode: 0750
- File mode: 0640
Ownership is determined by service user.

---

## 4) Spool helpers and replay-spool CLI

### 4.1 Spool directory
- `{data_root}/spool/alerts/`

Per-tenant spool:
- `{data_root}/spool/alerts/tenant=<tenant_id>/`

### 4.2 Spool write shape
The spool directory stores each alert as a single JSON file:
- `spool_<alert_id>.json`

This helper shape is covered by sink tests and is now used by the active
`run`/`oneshot` jsonl sink path in v0.1 when a live jsonl write fails.

### 4.3 Replay-spool CLI
The CLI command `sparx replay-spool` is the active replay surface in v0.1:
- filesystem/config command only; no embedded DB open
- valid only when `output.sink=jsonl`
- `stdout` is not replay-compatible in v0.1 and must fail non-zero
- deterministic replay ordering is by spool filename lex order
- successful replay emits to jsonl, flushes, and then deletes the spool file
- failed replay leaves the spool file in place and returns non-zero on partial
  failure

### 4.4 Automated runtime recovery
For `output.sink=jsonl`, the active runtime sink path now uses automatic
recovery behavior in v0.1:
- if a live jsonl emit fails during `run` or `oneshot`, Sparx writes the alert to
  the spool directory instead of dropping it
- if the spool write succeeds, processing continues and the alert remains queued
  for later replay
- if both the live jsonl write and the spool write fail, the active command still
  fails the affected device/operation rather than pretending delivery succeeded

Automated replay is deterministic and bounded:
- replay order is still spool filename lex order
- `run` attempts one bounded replay pass at the start of each cycle and one more
  bounded pass before shutdown
- `oneshot` attempts one bounded replay pass before device processing and one more
  bounded pass before shutdown
- the bounded replay pass size is controlled by
  `output.automated_replay_max_files_per_pass`
- default replay pass size: 128 spooled alerts per pass
- replay failures leave the spool files in place
- replay warnings do not hide live processing failures

### 4.5 Spool caps
The helper-backed spooling sink includes deterministic spool-cap enforcement.
Spool tuning is still not part of the active config contract in v0.1.

Current helper/runtime default:
- spool cap default constant: 2048 MB

Cap behavior:
- delete deterministic oldest files by lex path order until total spool bytes are
  within cap
- helper counters track spool writes, replay successes, replay failures, and cap
  drops when the helper-backed spooling sink is used

### 4.6 Recovery visibility
The active observability surface in v0.1 now exposes recovery state through
`status`, `/metrics`, and `/healthz`, including:
- current spool backlog file count
- current spool backlog total bytes
- configured automated replay max-files-per-pass value

### 4.7 Deferred spool behavior
The following remain deferred beyond v0.1's active runtime/config surface:
- config-exposed spool cap tuning fields
- configurable replay cadence
- per-tenant recovery backlog breakdowns

---

## 5) stdout sink

### 5.1 Behavior
- Emit JSONL to stdout, one alert per line, newline terminated.
- Intended for debugging, piping, and oneshot mode.

### 5.2 Error behavior
- If stdout write fails: process exits non-zero.

---

## 6) Optional future sinks (deferred)
- `webhook` (HTTP POST)
- `kafka`
- `syslog` (re-emission)

These are not part of v0.1.

---

## 7) Required tests
- jsonl line is valid JSON and newline terminated
- rotation triggers on size and on day boundary
- deterministic output path mapping for tenant/device/date
- spool helper write on simulated jsonl failure
- replay-spool-compatible helper replay succeeds and deletes file
- spool caps delete oldest and are deterministic
- replay-spool fails closed for `stdout`
- stdout emits one line per alert
