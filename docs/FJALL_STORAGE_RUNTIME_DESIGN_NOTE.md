# Fjall Storage + Runtime Design Note

Status: implemented design note reconciled through Phase 12.5f. This document describes the current Fjall-backed storage/runtime shape used by sparx v0.1.

## 1) Purpose

This note defines the storage/runtime design direction for Phase 10 after the decision to use Fjall for the real embedded DB/runtime layer.

Goals:
- keep the current key and value contracts stable
- avoid direct engine lock-in outside `src/db/`
- keep the adapter thin enough that it does not add meaningful complexity or runtime overhead
- make it practical to benchmark Sparx later and swap engines if benches justify it

Non-goals:
- no generic multi-engine framework
- no code generation
- no new control plane in this design note
- no contract changes to key/value encodings

## 2) Current engine decision

Engine in use for the Phase 10+ runtime path: Fjall.

Current dependency pin in `Cargo.toml`:
- `fjall = "=3.1.2"`

The code and tests in this repository are written against that exact pin. Any future engine or version change must stay behind `src/db/` and preserve the locked Sparx key/value contracts.

## 3) Sparx-specific fit

For Sparx, the embedded DB is not the ingest source of concurrency. The runtime tails files, tokenizes lines, aggregates windows, and periodically persists ordered key/value state. That means the DB layer mostly needs:
- deterministic ordered keys
- efficient prefix/range iteration
- batched writes
- per-tenant isolation by path
- compact local persistence
- safe restart recovery

Fjall covers the storage primitives we need for the current roadmap:
- embedded LSM engine
- multiple keyspaces with atomic semantics
- prefix/range iteration
- built-in compression
- thread-safe in-process access
- explicit persist modes

## 4) Important engine constraint

Fjall does not allow the same database to be loaded in parallel from separate processes.

Implication for Sparx v0.1:
- a given global or tenant DB path has a single active sparx process owner
- DB-backed CLI commands must fail with a DB error if the live daemon already owns the DB path
- live multi-process admin/query access is out of scope for the current roadmap

This is acceptable for v0.1 as long as we are explicit about it in the contracts and command behavior.

## 5) Topology decision

Keep the current per-path topology:
- one global embedded DB directory
- one embedded DB directory per tenant

Within each database:
- use a single primary keyspace named `kv` in v0.1
- keep all current ASCII key-prefix contracts authoritative inside that keyspace

Why single keyspace:
- it preserves the current prefix-map contracts exactly
- it keeps the adapter smaller
- it keeps future engine swaps simpler
- it avoids premature repartitioning into engine-specific keyspaces

## 6) Thin adapter boundary

Do not spread Fjall types across the codebase.

Only `src/db/` should know about Fjall directly.

Current boundary shape:
- `src/db/layout.rs`
  - canonical path derivation only
- `src/db/fjall.rs`
  - Fjall-specific open/get/put/delete/batch/scan/persist wiring
- `src/db/global.rs`
  - global repository helpers over raw keys
- `src/db/tenant.rs`
  - tenant repository helpers over raw keys
- `src/db/tenant_cache.rs`
  - deterministic tenant DB handle cache and lifecycle helpers
- `src/runtime.rs`
  - runtime context holding config, layout, global DB, and tenant cache

The boundary should expose only operations Sparx actually needs.

## 7) Adapter API principles

Keep the API narrow.

At minimum the rest of Sparx needs:
- open global DB
- open tenant DB
- close tenant DB
- get raw value by key
- put raw value by key
- delete raw key
- apply deterministic write batch
- iterate by prefix in sorted order
- iterate by bounded range in sorted order
- explicit persist at selected sync points

Avoid trait explosion.

A small internal concrete wrapper is preferred over a large trait hierarchy.
If a tiny trait is useful for tests, keep it private to `src/db/`.

## 8) Transaction strategy

Initial plan:
- use Fjall `Database` with non-transactional writes plus deterministic write batching
- do not start with optimistic or single-writer transactional mode unless a concrete Phase 10 or Phase 11 need requires it

Reason:
- the current Sparx contracts are already prefix-keyed and batch-friendly
- most current operations are append/update/delete by known keys
- adding serializable transaction machinery before we need it would add complexity without clear benefit

If a later subphase needs read-modify-write semantics that must be serialized, revisit transactional mode then.

## 9) Persist strategy

Use explicit persist points rather than syncing every write.

Planned persist moments:
- after open-window checkpoint flushes
- after finalize-window commits
- after purge journal and status updates for destructive operations
- during graceful shutdown after final checkpoint flush

This keeps the design aligned with Sparx's checkpoint-oriented runtime instead of forcing maximum durability on every small write.

## 10) Handle cache strategy

The runtime should not keep every tenant DB open forever.

Phase 10e introduced:
- max-open cap
- deterministic eviction order
- idle-close timeout
- explicit close before tenant purge
- safe reopen when a tenant becomes active again

The cache stores opened tenant DB wrappers plus the single `kv` keyspace handle. The current API uses closure-based access so DB ownership stays inside the cache and purge/close behavior remains explicit.

## 11) Command behavior impact

This design directly affects operational command behavior.

Required behavior:
- config-free commands (`version`, `validate-fixtures`) bypass DB entirely
- DB-backed commands fail closed if the DB path is owned by another sparx process
- no stubbed operational command may return exit 0

Current DB-backed command groups in the implemented runtime path:
- `status`
- `tenant purge`
- `migrate`
- `tenant policy show/check`
- `alerts list/show/search/export`
- `alert drill/extract`

Current non-DB replay command:
- `replay-spool` remains filesystem/config based and does not open Fjall

## 12) What does not change

This design does not change:
- stable hash contract
- key prefix contracts
- open-window encodings
- baseline encodings
- AlertV1 schema
- provenance model
- deterministic ordering rules

## 13) Open risks to carry forward

- Fjall single-process ownership means live admin/query workflows remain limited without a future control channel.
- The storage config surface still includes reserved continuity fields that are intentionally not active runtime tuning knobs in v0.1; Phase 12.5c narrowed the contract but did not introduce new engine tuning behavior.
- Phase 15b persists secondary alert indexes alongside the primary alert object, but current alert query/export commands still use primary-alert scans so older tenants and mixed-history DBs remain correct.

## 14) Implementation sequence and current state

Implemented sequence:
1. 10b layout helper (done; `src/db/layout.rs`)
2. 10c global DB wrapper (Fjall) (done)
3. 10d tenant DB wrapper (Fjall) (done)
4. 10e tenant handle cache (done; `src/db/tenant_cache.rs`)
5. 10f runtime context + repositories (done; `src/runtime.rs`)
6. Phase 11 operational commands (done for the scoped v0.1 command set)
7. Phase 12 tenant lifecycle runtime reconciliation (done for the scoped v0.1 daemon path)

Current implementation notes:
- tenant purge is implemented on top of `SparxRuntimeV1`, closes cached tenant handles before deletion, deletes tenant DB + alert + spool directories in deterministic order, records purge journal entries, and sets terminated status on full success
- `sparx migrate --tenant <id>` and `sparx migrate --all` are implemented on top of `SparxRuntimeV1`, use the Fjall-backed global/tenant DB adapters, initialize missing schema state to version 1, upgrade older schema state in place, disable tenants whose schema version is newer than the binary, and keep migration journal writes deterministic in both the global DB and tenant DB
- tenant policy show/check uses the contract-defined watch-root path, validates policy_version, key_overrides category names, optional CIDR ip_bucket, and deterministic text rendering without requiring live DB access
- alert drill/extract operate against `AlertV1.provenance` with plain-file extraction and gzip drill-skip behavior while preserving the Fjall-backed runtime and repository boundary unchanged
- `replay-spool` remains outside the Fjall runtime path by design
