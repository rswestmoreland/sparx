# Fjall Storage and Runtime Design Note

Status: implemented design note for the current Fjall-backed storage/runtime
shape used by sparx v0.1.

## Purpose

This note records the storage/runtime design direction after the project selected
Fjall for the real embedded DB/runtime layer.

Goals:

- keep key and value contracts stable
- avoid direct engine lock-in outside `src/db/`
- keep the adapter thin enough that it does not add meaningful complexity or
  runtime overhead
- keep future benchmarking and engine replacement possible behind the adapter

Non-goals:

- no generic multi-engine framework
- no code generation
- no new control plane in this note
- no contract changes to key/value encodings

## Current engine decision

Fjall is the active runtime engine.

Current dependency pin in `Cargo.toml`:

- `fjall = "=3.1.2"`

The code and tests in this repository are written against that exact pin. Any
future engine or version change must stay behind `src/db/` and preserve the
locked sparx key/value contracts.

## sparx-specific fit

For sparx, the embedded DB is not the ingest source of concurrency. The runtime
tails files, tokenizes lines, aggregates windows, and periodically persists
ordered key/value state. The DB layer mostly needs:

- deterministic ordered keys
- efficient prefix/range iteration
- batched writes
- per-tenant isolation by path
- compact local persistence
- safe restart recovery

Fjall covers the storage primitives needed by the current design:

- embedded LSM engine
- multiple keyspaces with atomic semantics
- prefix/range iteration
- built-in compression
- thread-safe in-process access
- explicit persist modes

## Important engine constraint

Fjall does not allow the same database to be loaded in parallel from separate
processes.

Implication for sparx v0.1:

- a given global or tenant DB path has a single active sparx process owner
- DB-backed CLI commands fail with a DB error if the live daemon already owns the
  DB path
- live multi-process admin/query access is outside the current v0.1 scope

## Topology decision

Keep the current per-path topology:

- one global embedded DB directory
- one embedded DB directory per tenant

Within each database:

- use a single primary keyspace named `kv` in v0.1
- keep all current ASCII key-prefix contracts authoritative inside that keyspace

Why a single keyspace:

- preserves the current prefix-map contracts exactly
- keeps the adapter smaller
- keeps future engine swaps simpler
- avoids premature repartitioning into engine-specific keyspaces

## Thin adapter boundary

Do not spread Fjall types across the codebase. Only `src/db/` should know about
Fjall directly.

Current boundary shape:

- `src/db/layout.rs`: canonical path derivation only
- `src/db/fjall.rs`: Fjall-specific open/get/put/delete/batch/scan/persist wiring
- `src/db/global.rs`: global repository helpers over raw keys
- `src/db/tenant.rs`: tenant repository helpers over raw keys
- `src/db/tenant_cache.rs`: deterministic tenant DB handle cache and lifecycle
  helpers
- `src/runtime.rs`: runtime context holding config, layout, global DB, and tenant
  cache

The boundary should expose only operations sparx actually needs.

## Adapter API principles

Keep the API narrow. At minimum the rest of sparx needs:

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

A small internal concrete wrapper is preferred over a large trait hierarchy. If a
tiny trait is useful for tests, keep it private to `src/db/`.

## Transaction strategy

Current strategy:

- use Fjall `Database` with non-transactional writes plus deterministic write
  batching
- do not add optimistic or single-writer transactional mode unless a concrete
  runtime need requires it

Reason:

- current contracts are prefix-keyed and batch-friendly
- most operations are append/update/delete by known keys
- serializable transaction machinery would add complexity without clear current
  benefit

If a later step needs serialized read-modify-write semantics, revisit
transactional mode then.

## Persist strategy

Use explicit persist points rather than syncing every write.

Persist moments:

- after open-window checkpoint flushes
- after finalize-window commits
- after purge journal and status updates for destructive operations
- during graceful shutdown after final checkpoint flush

This keeps the design aligned with checkpoint-oriented runtime behavior instead
of forcing maximum durability on every small write.

## Handle cache strategy

The runtime should not keep every tenant DB open forever. The handle cache uses:

- max-open cap
- deterministic eviction order
- idle-close timeout
- explicit close before tenant purge
- safe reopen when a tenant becomes active again

The cache stores opened tenant DB wrappers plus the single `kv` keyspace handle.
The current API uses closure-based access so DB ownership stays inside the cache
and purge/close behavior remains explicit.

## Command behavior impact

Required behavior:

- config-free commands such as `version` and `validate-fixtures` bypass DB
  entirely
- DB-backed commands fail closed if the DB path is owned by another sparx process
- no stubbed operational command may return exit 0

Current DB-backed command groups:

- `status`
- `tenant purge`
- `migrate`
- `tenant policy show/check`
- `alerts list/show/search/export`
- `alert drill/extract`

Current non-DB replay command:

- `replay-spool` remains filesystem/config based and does not open Fjall

## What does not change

This design does not change:

- stable hash contract
- key prefix contracts
- open-window encodings
- baseline encodings
- AlertV1 schema
- provenance model
- deterministic ordering rules

## Open risks

- Fjall single-process ownership limits live admin/query workflows without a
  future control channel.
- The storage config surface still includes reserved continuity fields that are
  intentionally not active runtime tuning knobs in v0.1.
- Secondary alert indexes are current truth, but query/export paths must remain
  backward-safe for older or mixed-history tenant DBs.

## Current implementation notes

- tenant purge closes cached tenant handles before deletion, deletes tenant DB,
  alert, and spool directories in deterministic order, records purge journal
  entries, and sets terminated status on full success
- `sparx migrate --tenant <id>` and `sparx migrate --all` use the Fjall-backed
  global/tenant DB adapters, initialize missing schema state to version 1,
  upgrade older schema state in place, disable tenants whose schema version is
  newer than the binary, and keep migration journal writes deterministic
- tenant policy show/check validates policy version, key override category names,
  optional CIDR ip bucket, and deterministic text rendering
- alert drill/extract operate against `AlertV1.provenance` with plain-file
  extraction and gzip drill-skip behavior while preserving the Fjall boundary
- `replay-spool` remains outside the Fjall runtime path by design
