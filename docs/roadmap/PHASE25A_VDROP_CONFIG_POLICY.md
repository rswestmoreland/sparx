# Phase 25a - V_DROP Configuration and Tenant Policy Implementation

Status: complete.

Phase 25a activates the configuration and tenant-policy surfaces for the existing
hard-silence `V_DROP` path. Phase 25b now routes runtime evaluation through those
controls; this document remains the Phase 25a surface record.

## Implemented config surface

A new top-level `[vdrop]` config section is parseable, has deterministic defaults, and is
validated by `validate_config_v1`.

Fields:

- `enabled: bool` default `true`
- `device_enabled: bool` default `true`
- `tenant_enabled: bool` default `true`
- `min_expected_windows_missed: u32` default `3`
- `min_mature_windows: optional u64` default unset
- `min_expected_lines: optional u64` default unset

Environment overrides:

- `SPARX_VDROP_ENABLED`
- `SPARX_VDROP_DEVICE_ENABLED`
- `SPARX_VDROP_TENANT_ENABLED`
- `SPARX_VDROP_MIN_EXPECTED_WINDOWS_MISSED`
- `SPARX_VDROP_MIN_MATURE_WINDOWS`
- `SPARX_VDROP_MIN_EXPECTED_LINES`

Validation:

- `vdrop.min_expected_windows_missed` must be greater than zero.
- `min_mature_windows` and `min_expected_lines` are optional floors and may be zero if
  explicitly set.

Default behavior remains equivalent to the Phase 23f hard-silence path because all
controls default to enabled and unset optional floors inherit the current scoring-derived
behavior under the Phase 25b runtime resolver.

## Implemented tenant policy surface

Tenant policy TOML now accepts optional per-tenant overrides:

- `vdrop_enabled`
- `vdrop_device_enabled`
- `vdrop_tenant_enabled`
- `vdrop_min_expected_windows_missed`
- `vdrop_min_mature_windows`
- `vdrop_min_expected_lines`

Missing tenant-policy values render as `inherit` in `tenant policy show` and
`tenant policy check` output.

Validation:

- `vdrop_min_expected_windows_missed = 0` is invalid and fails closed through tenant
  policy validation.

## Tests added

- config defaults preserve Phase 23f behavior
- config file and environment overrides load deterministically
- zero global missed-window threshold is rejected
- tenant policy show/check includes concrete V_DROP overrides
- missing tenant overrides render as `inherit`
- zero tenant-policy missed-window threshold is rejected

## Boundaries

Phase 25a does not add:

- diagnostics counters
- status, metrics, or health fields
- sharp-drop detection
- richer silence subject kinds
- recovery or replay changes

Next recommended phase: Phase 25b V_DROP policy resolution and runtime evaluator
integration.
