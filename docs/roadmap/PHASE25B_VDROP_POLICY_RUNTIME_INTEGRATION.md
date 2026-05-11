# Phase 25b - V_DROP policy resolution and runtime evaluator integration

Status: complete.

Phase 25b routes the active hard-silence `V_DROP` runtime path through the
configuration and tenant-policy controls introduced in Phase 25a.

## Implemented behavior

- Runtime `run` and `oneshot` now resolve a per-tenant `V_DROP` policy before
  evaluating hard-silence candidates.
- Global `[vdrop]` config values provide the default policy.
- Tenant policy fields override the global values for that tenant.
- Missing tenant policy means `inherit` from global config/defaults.
- Malformed, invalid, or unreadable tenant policy fails closed for that tenant's
  `V_DROP` runtime pass.
- `vdrop.enabled = false` suppresses all `V_DROP` candidate evaluation and alert
  emission for the resolved tenant policy.
- `vdrop.device_enabled = false` suppresses device-subject `V_DROP` alerts while
  leaving expected-source state updates active.
- `vdrop.tenant_enabled = false` suppresses tenant-aggregate `V_DROP` alerts while
  leaving expected-source state updates active.
- `vdrop.min_expected_windows_missed` is routed into the evaluator.
- `vdrop.min_mature_windows` is routed into the evaluator when set; otherwise it
  inherits the scoring-derived cold-start maturity floor.
- `vdrop.min_expected_lines` is routed into the evaluator when set; otherwise it
  inherits the scoring-derived minimum-lines floor.

## Tests added

- `oneshot_global_vdrop_disable_suppresses_runtime_alerts_v1`
- `oneshot_tenant_policy_can_disable_device_vdrop_subjects_v1`
- `oneshot_tenant_policy_overrides_global_vdrop_threshold_v1`
- `run_tenant_policy_can_disable_all_vdrop_subjects_v1`

## Boundaries

Phase 25b does not add:

- diagnostics counters
- status, metrics, or health `V_DROP` diagnostics
- sharp-drop detection
- per-file/source-path silence subjects
- parser-class or vendor-event-family silence subjects
- external heartbeat checks
- maintenance-window calendars
- recovery or replay behavior changes

## Next recommended phase

Phase 25c: V_DROP diagnostics counters and operator surfacing.
