# V_DROP Policy and Diagnostics Scope Contract v0.1

This contract defines the active policy and diagnostic boundaries for `V_DROP`.

## Active policy controls

Global `[vdrop]` configuration controls:

- global V_DROP enablement
- device subject enablement
- tenant aggregate subject enablement
- source-stream subject enablement, default disabled
- missed-window thresholds
- maturity and minimum expected-volume gates
- sharp-drop ratio and variance gates where configured

Tenant policy may override supported V_DROP fields. Missing tenant-policy values
inherit from global config/defaults. Invalid tenant policy fails closed for that
tenant's V_DROP pass.

## Active diagnostic concepts

Diagnostics may surface bounded counts for:

- subjects tracked
- subjects evaluated
- candidates found
- candidates suppressed
- alerts emitted
- open hard-silence intervals
- open sharp-drop intervals
- last evaluation timestamp

Source-stream diagnostics may include aggregate and per-tenant counts. Per-tenant
metrics may use `tenant_id` only.

## Prometheus label restrictions

The following label fanout remains prohibited:

- device
- source path
- source-stream id
- parser class
- vendor family
- per-subject state
- suppression reason

## Behavior boundaries

Policy and diagnostics must not change:

- AlertV1 schema
- AlertV1 provenance semantics
- DeviceStatsV1 layout
- SourceStreamStatsV1 layout
- replay behavior
- recovery behavior
- hard-silence priority over sharp drop
- source-stream default-off gate
