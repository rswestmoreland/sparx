# Global DB Key Prefix Map Contract v0.1

This contract defines the authoritative keys and prefixes stored in the global embedded DB instance (`global.db`).
The global DB is used for tenant lifecycle, discovery, shared runtime metadata, and persisted observability values that must outlive a single process run.

## Goals
- Minimal global state.
- Support discovery of active tenants and last-seen tracking.
- Store global schema version and migration journal.
- Store the small active observability surface used by `status`, `/metrics`, and `/healthz`.
- Keep global DB safe to delete/rebuild without losing tenant-local baselines (tenant DBs remain authoritative for scoring).

## Non-goals (v0.1)
- Storing global feature dictionaries (tenant-scoped dicts are required).
- Cross-tenant baselines or correlation.
- Storing dense sparse-matrix rows in the global DB.

---

## 1) Conventions
- Keys are ASCII UTF-8 bytes.
- Prefix components are `/` separated.
- All groups include `/v1/` marker.
- Fixed-size integers are little-endian; varints use unsigned LEB128.

---

## 2) Meta / schema

### 2.1 Schema version
- `meta/schema/v1/version` -> u32
- `meta/schema/v1/created_ts` -> i64 unix seconds
- `meta/schema/v1/last_migrate_ts` -> i64 unix seconds

Rules:
- Missing schema version in an existing DB is an error after the current release.
- `migrate` is the only command allowed to advance schema version.

### 2.2 Process state
- `process/v1/last_run_start_ts` -> i64
- `process/v1/last_run_end_ts` -> i64
- `process/v1/last_run_exit_code` -> i32
- `process/v1/last_run_host` -> string

Notes:
- These fields are best-effort and may be absent.

---

## 3) Tenant registry

Tenant IDs are derived from directory names and are stable.

### 3.1 Tenant record
- `tenant/v1/<tenant_id>/created_ts` -> i64
- `tenant/v1/<tenant_id>/last_seen_ts` -> i64
- `tenant/v1/<tenant_id>/status` -> u8
  - 0 = active
  - 1 = disabled (stop ingest/emit)
  - 2 = terminating (pending purge)
  - 3 = terminated (purged; record may remain for audit)

### 3.2 Tenant paths (optional)
- `tenant/v1/<tenant_id>/tenant_root_rel` -> string (relative under configured tenant_root)
- `tenant/v1/<tenant_id>/tenant_db_path` -> string
- `tenant/v1/<tenant_id>/alert_out_root` -> string

Notes:
- Paths are optional convenience; canonical paths are derived from Config Schema v0.1 plus tenant_id.

### 3.3 Tenant purge journal
- `tenant_purge/v1/<tenant_id>/<ts>` -> string (status message)

Example status values:
- `requested`
- `db_deleted`
- `alerts_deleted`
- `spool_deleted`
- `complete`

---

## 4) Global indexes (minimal)

### 4.1 Active tenants index
- `tenant_idx_active/v1/<tenant_id>` -> empty

Semantics:
- The worker updates this key whenever it observes a tenant directory and status is active.
- Consumers may iterate this prefix to list known active tenants.

### 4.2 Last seen index (optional; OFF by default)
- `tenant_idx_seen/v1/<last_seen_ts>/<tenant_id>` -> empty

Used for:
- discovery of stale tenants
- operational dashboards

---

## 5) Global metrics persistence

The active runtime persists a bounded observability surface so `status`, `/metrics`, and `/healthz` can remain useful across restarts.

### 5.1 Generic metric key forms
- `metrics/v1/counter/<name>` -> u64
- `metrics/v1/gauge/<name>` -> f64

All recovery counters and snapshots listed below are stored by name under `metrics/v1/counter/<name>`.

### 5.2 Run-cycle and recovery counters
- `metrics/v1/counter/run_cycles_completed_total` -> u64
- `metrics/v1/counter/run_tenants_total` -> u64
- `metrics/v1/counter/run_tenants_processed_total` -> u64
- `metrics/v1/counter/run_tenants_skipped_total` -> u64
- `metrics/v1/counter/run_devices_processed_total` -> u64
- `metrics/v1/counter/run_devices_failed_total` -> u64
- `metrics/v1/counter/run_alerts_emitted_total` -> u64
- `metrics/v1/counter/run_last_cycle_completed_ts` -> u64
- `metrics/v1/counter/run_last_cycle_tenants_total` -> u64
- `metrics/v1/counter/run_last_cycle_tenants_processed` -> u64
- `metrics/v1/counter/run_last_cycle_tenants_skipped` -> u64
- `metrics/v1/counter/run_last_cycle_devices_processed` -> u64
- `metrics/v1/counter/run_last_cycle_devices_failed` -> u64
- `metrics/v1/counter/run_last_cycle_alerts_emitted` -> u64
- `metrics/v1/counter/recovery_spool_writes_total` -> u64
- `metrics/v1/counter/recovery_spool_replayed_total` -> u64
- `metrics/v1/counter/recovery_spool_replay_fail_total` -> u64
- `metrics/v1/counter/recovery_spool_drop_total` -> u64
- `metrics/v1/counter/recovery_automated_replay_attempts_total` -> u64
- `metrics/v1/counter/recovery_last_automated_replay_attempt_ts` -> u64
- `metrics/v1/counter/recovery_last_automated_replay_replayed` -> u64
- `metrics/v1/counter/recovery_last_automated_replay_failed` -> u64

### 5.3 Recovery backlog trend snapshots
- `metrics/v1/counter/recovery_previous_snapshot_ts` -> u64
- `metrics/v1/counter/recovery_previous_snapshot_backlog_files` -> u64
- `metrics/v1/counter/recovery_previous_snapshot_backlog_bytes` -> u64
- `metrics/v1/counter/recovery_last_snapshot_ts` -> u64
- `metrics/v1/counter/recovery_last_snapshot_backlog_files` -> u64
- `metrics/v1/counter/recovery_last_snapshot_backlog_bytes` -> u64
- `metrics/v1/counter/recovery_tenant_previous_snapshot_ts__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_previous_snapshot_backlog_files__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_previous_snapshot_backlog_bytes__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_last_snapshot_ts__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_last_snapshot_backlog_files__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_last_snapshot_backlog_bytes__<tenant_id>` -> u64

### 5.4 Global recovery rate analytics snapshots
- `metrics/v1/counter/recovery_previous_counter_snapshot_ts` -> u64
- `metrics/v1/counter/recovery_previous_counter_snapshot_spool_writes_total` -> u64
- `metrics/v1/counter/recovery_previous_counter_snapshot_spool_replayed_total` -> u64
- `metrics/v1/counter/recovery_previous_counter_snapshot_spool_replay_fail_total` -> u64
- `metrics/v1/counter/recovery_previous_counter_snapshot_automated_replay_attempts_total` -> u64
- `metrics/v1/counter/recovery_last_counter_snapshot_ts` -> u64
- `metrics/v1/counter/recovery_last_counter_snapshot_spool_writes_total` -> u64
- `metrics/v1/counter/recovery_last_counter_snapshot_spool_replayed_total` -> u64
- `metrics/v1/counter/recovery_last_counter_snapshot_spool_replay_fail_total` -> u64
- `metrics/v1/counter/recovery_last_counter_snapshot_automated_replay_attempts_total` -> u64

Short-window global replay-rate values are derived from these previous/last counter snapshots; the derived rates are not persisted as independent keys.

### 5.5 Per-tenant recovery rate analytics snapshots
- `metrics/v1/counter/recovery_tenant_previous_counter_snapshot_ts__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_previous_counter_snapshot_spool_writes_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_previous_counter_snapshot_spool_replayed_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_previous_counter_snapshot_spool_replay_fail_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_previous_counter_snapshot_automated_replay_attempts_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_last_counter_snapshot_ts__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_last_counter_snapshot_spool_writes_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_last_counter_snapshot_spool_replayed_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_last_counter_snapshot_spool_replay_fail_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_last_counter_snapshot_automated_replay_attempts_total__<tenant_id>` -> u64

Short-window per-tenant replay-rate values are derived from these previous/last counter snapshots; the derived rates are not persisted as independent keys.

### 5.6 Global recovery history-start rate analytics snapshot
- `metrics/v1/counter/recovery_history_start_counter_snapshot_ts` -> u64
- `metrics/v1/counter/recovery_history_start_counter_snapshot_spool_writes_total` -> u64
- `metrics/v1/counter/recovery_history_start_counter_snapshot_spool_replayed_total` -> u64
- `metrics/v1/counter/recovery_history_start_counter_snapshot_spool_replay_fail_total` -> u64
- `metrics/v1/counter/recovery_history_start_counter_snapshot_automated_replay_attempts_total` -> u64

The global history-start snapshot is initialized once for the global recovery counter surface. Long-window global replay-rate values are derived from this persisted anchor and the current global last counter snapshot; the derived rates are not persisted as independent keys.

### 5.7 Per-tenant recovery history-start rate analytics snapshots
- `metrics/v1/counter/recovery_tenant_history_start_counter_snapshot_ts__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_history_start_counter_snapshot_spool_writes_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_history_start_counter_snapshot_spool_replayed_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_history_start_counter_snapshot_spool_replay_fail_total__<tenant_id>` -> u64
- `metrics/v1/counter/recovery_tenant_history_start_counter_snapshot_automated_replay_attempts_total__<tenant_id>` -> u64

Per-tenant history-start snapshots are initialized once per tenant when that tenant appears in the active recovery snapshot path. Per-tenant long-window replay-rate values are derived from this persisted anchor and the current matching tenant last counter snapshot; the derived rates are not persisted as independent keys.

### 5.8 Recovery replay-rate derivation rule

the current release adds no new global DB key prefixes. It reconciles the recovery observability derivation rules for the already-active global and per-tenant short-window and long-window replay-rate views.

All replay-rate views are analytics-only. Both endpoint snapshots must exist, the timestamp interval must be positive, and the relevant counter delta must be nonnegative. Otherwise the derived rate is null or omitted rather than negative or misleading. These analytics do not change replay ordering, delivery semantics, replay cadence, spool cap behavior, or recovery control decisions.

---

## 6) Migrations

### 6.1 Migration journal
- `migrate/v1/journal/<ts>/<name>` -> bytes (result/status)

Rules:
- Any change to this map requires bumping `meta/schema/v1/version`.
- Migrations must be idempotent.

---

## 7) Required tests
- schema version read/write roundtrip
- tenant record upsert and last_seen updates
- active index updates deterministically
- purge journal append and prefix scan
- migration journal append and prefix scan
- global metric counter/gauge key shape and value roundtrip
- recovery snapshot key names match the `metrics/v1/counter/<name>` prefix form


## Current release/25d V_DROP diagnostic metrics

the current release persists bounded V_DROP diagnostics through the existing global metrics store. the current release hardens open-silence gauge semantics to represent post-evaluation current open dedup state.
The canonical key shape remains:

- `metrics/v1/counter/<name>` for counters and timestamp counters
- `metrics/v1/gauge/<name>` for gauges

Active global metric names are:

- `vdrop_evaluated_subjects_total`
- `vdrop_candidates_total`
- `vdrop_suppressed_candidates_total`
- `vdrop_alerts_emitted_total`
- `vdrop_last_evaluation_ts`

Active per-tenant metric names append the tenant suffix using the existing
`__<tenant_id>` convention:

- `vdrop_tracked_subjects__<tenant_id>`
- `vdrop_open_silence_subjects__<tenant_id>`
- `vdrop_evaluated_subjects_total__<tenant_id>`
- `vdrop_candidates_total__<tenant_id>`
- `vdrop_suppressed_candidates_total__<tenant_id>`
- `vdrop_alerts_emitted_total__<tenant_id>`
- `vdrop_last_evaluation_ts__<tenant_id>`

Top-level tracked/open subject values exposed in status and Prometheus are derived from
per-tenant gauges and are not required to be persisted as independent global gauges. Open-silence gauges are written after each tenant evaluation pass has written any newly emitted open-silence dedup records.
