# Embedded DB Topology Contract v0.1 (Fjall target)

Note: this file name is retained for numbering continuity, but the v0.1 engine target for the real DB/runtime layer is Fjall.

## Topology
- One global embedded DB for shared metadata and registry.
- One embedded DB per tenant for all tenant-specific state.
- No per-device DBs.

## Internal layout
- Each global or tenant DB uses one primary keyspace named `kv` in v0.1.
- All existing ASCII key-prefix contracts remain authoritative inside that keyspace.
- No per-prefix keyspace split is introduced in v0.1.

## Paths
- Global DB: `{data_root}/global.db/`
- Tenant DBs: `{tenant_db_root}/tenant=<tenant_id>/tenant.db/`

## Purge
- Tenant purge = close tenant DB handle and delete `{tenant_db_root}/tenant=<tenant_id>/tenant.db/`.

## Handle management
- LRU cache of open tenant DB handles (e.g., 64).
- Open/close on demand to avoid too many concurrent open DBs.

## Single-process ownership
- A given DB path may be opened by only one sparx process at a time in v0.1.
- DB-backed CLI commands must fail with a DB error if another sparx process already owns the path.
- Live multi-process admin/query access is deferred.

## Where offsets live
- In tenant DB (so purge is complete).

Notes:
- `{data_root}` and `{tenant_db_root}` are configured via Config Schema Contract v0.1.
