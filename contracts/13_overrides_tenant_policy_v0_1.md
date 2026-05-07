# Overrides + Tenant Policy Contract v0.1

## Purpose
Map specific normalized keys to canonical categories with strong confidence per tenant.

## Policy file (authoritative)
- `<watch-root>/<tenant_id>/.sparx/policy.toml`

Optionally cache last-good policy in tenant DB.

## Schema
- `policy_version = 1`
- `key_overrides` map: `norm_key -> Category`
- optional `ip_bucket` (ipv4/ipv6 CIDR)
- optional `min_identity_confidence` (default 2)

## Reload
- refresh on mtime change, at most once per 60s (default)
- invalid policy => keep last-good, increment counter, report in status

## Precedence
- override wins; sets confidence=3
- resolves default ambiguity

## CLI helpers
- `sparx tenant policy show <tenant>`
- `sparx tenant policy check <tenant>`
