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
- optional `vdrop_enabled`
- optional `vdrop_device_enabled`
- optional `vdrop_tenant_enabled`
- optional `vdrop_source_stream_enabled`
- optional `vdrop_min_expected_windows_missed`
- optional `vdrop_min_mature_windows`
- optional `vdrop_min_expected_lines`

`V_DROP` tenant policy fields are active as validated override surface. Missing
values mean `inherit` from global config/defaults. the current release runtime policy
resolution applies these overrides before evaluating hard-silence candidates. the current release adds
`vdrop_source_stream_enabled` as a source-stream-specific override with inherit semantics.
It defaults off through global config and does not activate source-stream runtime behavior
until the later runtime-integration stage. `vdrop_min_expected_windows_missed` must be
greater than zero when present.

## Reload
- refresh on mtime change, at most once per 60s (default)
- invalid policy => keep last-good, increment counter, report in status

## Precedence
- override wins; sets confidence=3
- resolves default ambiguity

## CLI helpers
- `sparx tenant policy show <tenant>`
- `sparx tenant policy check <tenant>`

`tenant policy show` and `tenant policy check` render `V_DROP` override values.
Unset override values render as `inherit`.
