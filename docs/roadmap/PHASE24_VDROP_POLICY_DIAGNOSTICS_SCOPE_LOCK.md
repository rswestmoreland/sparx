# Phase 24 - V_DROP Policy Controls and Diagnostics Scope Lock

Status: complete as a planning and contract-scoping phase.

Phase 24 locks the next expansion boundary for the active hard-silence `V_DROP` path.
It does not change runtime behavior, source code, tests, persisted keys, metrics, health
output, recovery behavior, replay behavior, or active scoring semantics.

## Current active baseline

As of Phase 23f, `V_DROP` is active for the first hard-silence runtime path:

- subjects: device and tenant aggregate
- runtime surfaces: `run` and `oneshot`
- output path: existing `AlertV1` primary alert object and configured alert sink
- dedup state: `silence_open/*`
- learning state: `silence_subject/*`
- closure behavior: later finalized-window observations close matching open-silence state

The active runtime still uses conservative internal defaults derived from existing scoring
configuration where appropriate. There is no public silence-policy section yet, and there
are no dedicated `V_DROP` diagnostics in `status`, `/metrics`, or `/healthz` yet.

## Phase 24 scope decision

The next implementation work should add operator controls and diagnostics for the current
hard-silence path before expanding detection scope.

Planned first policy-control scope:

- global enable/disable for `V_DROP`
- per-subject enable/disable for device and tenant aggregate hard-silence detection
- configurable missed-window threshold
- configurable minimum mature-window floor, with current scoring-derived behavior as the
  default when unset
- configurable minimum expected-line floor, with current scoring-derived behavior as the
  default when unset
- tenant policy overrides for the same bounded controls
- fail-closed validation for invalid values

Planned first diagnostics scope:

- number of tracked silence subjects
- number of open silence intervals
- number of evaluated subjects
- number of candidates produced
- number of candidates suppressed
- number of `V_DROP` alerts emitted
- last evaluation timestamp
- deterministic per-tenant and global views where feasible

Diagnostics should help operators answer these questions:

- Is `V_DROP` enabled for this deployment or tenant?
- How many subjects are being tracked?
- Did the evaluator run recently?
- How many candidates were suppressed and why?
- How many active open-silence intervals currently exist?
- Are alerts being deduplicated rather than missing?

## Locked boundaries

Phase 24 does not authorize these changes:

- sharp-drop detection
- per-file/source-path silence detection
- parser-class or vendor-event-family silence detection
- external heartbeat or reachability checks
- maintenance-window calendars
- cross-tenant outage correlation
- new alert schema fields
- changes to `AlertV1.provenance`
- recovery or replay behavior changes

Those items remain future work and require separate scope locks before implementation.

## Planned policy resolution order

Future implementation should resolve silence policy in this order:

1. built-in defaults
2. global config file or environment overrides
3. tenant policy overrides

CLI one-off overrides are not part of the first planned implementation unless a later
scope lock explicitly adds them.

## Planned default behavior

The future public controls must preserve the current active behavior by default:

- `V_DROP` remains enabled by default for the currently active hard-silence path
- device and tenant aggregate subjects remain enabled by default
- current scoring-derived maturity and line-floor behavior remains the default unless an
  explicit silence policy override is configured
- invalid policy values fail closed during config/policy validation rather than being
  silently coerced

## Planned implementation sequence

Recommended Phase 25 sequence:

1. Phase 25a - configuration and tenant-policy contract implementation for silence policy - complete
2. Phase 25b - policy resolution and runtime evaluator integration
3. Phase 25c - diagnostics counters and operator surfacing through status, metrics, and health
4. Phase 25d - validation hardening and closeout

Phase 25 should not expand beyond hard-silence policy controls and diagnostics unless the
contracts are explicitly updated first.

## Acceptance gates for the future implementation

The Phase 25 implementation should include tests proving:

- default behavior remains equivalent to the Phase 23f hard-silence path
- global disable suppresses `V_DROP` alert emission without corrupting expected-source state
- device-only and tenant-only controls work independently
- tenant policy overrides take precedence over global config
- invalid policy values fail closed through validation
- diagnostics counters are deterministic and do not require replay/recovery behavior changes
- metric/status/health fields are absent or null when disabled or unavailable rather than
  misleading

## Handoff

Phase 24 locks the scope and documents it in
`contracts/36_vdrop_policy_diagnostics_scope_v0_1.md`.

Phase 25a later implemented the V_DROP config and tenant-policy surfaces.

Next recommended phase: Phase 25b policy resolution and runtime evaluator integration for
`V_DROP` policy controls.
