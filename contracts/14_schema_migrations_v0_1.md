# Schema Versioning + Migration Contract v0.1

This contract defines schema versioning and forward migrations for both the global DB and tenant DBs.

## Versions
- Global DB schema version: `meta/schema/v1/version` (u32)
- Tenant DB schema version: `meta/schema/v1/version` (u32)

Global and tenant schemas evolve independently (version numbers are not required to match).

## Storage keys

Global (Global DB Key Prefix Map v0.1):
- `meta/schema/v1/version` (u32)
- `meta/schema/v1/created_ts` (i64)
- `meta/schema/v1/last_migrate_ts` (i64)
- `migrate/v1/journal/<ts>/<name>` (status payload)

Tenant (Tenant DB Key Prefix Map v0.1):
- `meta/schema/v1/version` (u32)
- `meta/schema/v1/created_ts` (i64)
- `meta/schema/v1/last_migrate_ts` (i64)
- `migrate/v1/journal/<ts>/<name>` (status payload)

Value encodings:
- See Global DB Key Prefix Map v0.1 and Tenant DB Simple Value Encodings v0.1.

## Startup behavior
- Migrate forward if DB schema < required (in-order, resumable).
- Refuse to run if DB schema > binary schema (downgrade protection).
  - For tenant DBs, mark tenant disabled in global DB.

## Principles
- Prefer additive migrations.
- Steps must be idempotent.
- Migrations must be resumable (progress markers in journal).
- Tenant migrations are isolated per tenant.

## Controls
- `--migrate auto|off|require`
- `sparx migrate --tenant <id>|--all`

## Tests
- upgrade global and tenant schema roundtrips
- downgrade refusal
- resumable migration simulation
