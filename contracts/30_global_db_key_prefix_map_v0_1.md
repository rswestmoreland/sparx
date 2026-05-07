# Global DB Key Prefix Map Contract v0.1

This contract defines the authoritative keys and prefixes stored in the global embedded DB instance (`global.db`).
The global DB is used for tenant lifecycle, discovery, and shared metadata that must outlive a single process run.

## Goals
- Minimal global state.
- Support discovery of active tenants and last-seen tracking.
- Store global schema version and migration journal.
- Keep global DB safe to delete/rebuild without losing tenant-local baselines (tenant DBs remain authoritative for scoring).

## Non-goals (v0.1)
- Storing global feature dictionaries (tenant-scoped dicts are required).
- Cross-tenant baselines or correlation.

---

## 1) Conventions
- Keys are ASCII UTF-8 bytes.
- Prefix components are `/` separated.
- All groups include `/v1/` marker.
- Fixed-size integers are little-endian; varints use unsigned LEB128.

---

## 2) Meta / schema

### 2.1 Global schema
- `meta/schema/v1/version` -> u32
- `meta/schema/v1/created_ts` -> i64
- `meta/schema/v1/last_migrate_ts` -> i64

### 2.2 Global process state (optional)
- `meta/process/v1/last_run_start_ts` -> i64
- `meta/process/v1/last_run_end_ts` -> i64
- `meta/process/v1/last_run_exit_code` -> i32
- `meta/process/v1/last_run_host` -> string (optional)

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

Phase 13a persists a small set of observability counters and gauges so status
and enabled endpoints can survive restarts.

- `metrics/v1/counter/<name>` -> u64
- `metrics/v1/gauge/<name>` -> f64

Active examples:
- `run_cycles_completed_total`
- `run_tenants_total`
- `run_tenants_processed_total`
- `run_tenants_skipped_total`
- `run_devices_processed_total`
- `run_devices_failed_total`
- `run_alerts_emitted_total`
- `run_last_cycle_completed_ts`
- `run_last_cycle_tenants_total`
- `run_last_cycle_tenants_processed`
- `run_last_cycle_tenants_skipped`
- `run_last_cycle_devices_processed`
- `run_last_cycle_devices_failed`
- `run_last_cycle_alerts_emitted`

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
