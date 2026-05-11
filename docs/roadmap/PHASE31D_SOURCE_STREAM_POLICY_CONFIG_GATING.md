# Phase 31d Source-Stream Policy and Config Gating

Status: complete as a policy/config gating checkpoint. Source-stream runtime V_DROP
remains inactive.

## Goal

Add the default-off policy and configuration gate required before source-stream V_DROP
runtime activation.

Phase 31d builds on the Phase 31a source-stream identity/catalog/stats-state primitives,
Phase 31b evaluator primitives, and Phase 31c AlertV1/open-state primitives. It exposes the
source-stream subject-family gate without wiring source-stream evaluation into `run` or
`oneshot`.

## Implemented

- added global config field `[vdrop].source_stream_enabled`
- defaulted `[vdrop].source_stream_enabled` to `false`
- added environment override `SPARX_VDROP_SOURCE_STREAM_ENABLED`
- added tenant-policy override field `vdrop_source_stream_enabled`
- kept missing tenant-policy value semantics as `inherit`
- added a small policy helper to resolve the effective source-stream gate from global config
  and optional tenant policy
- rendered `vdrop_source_stream_enabled` in `tenant policy show` and `tenant policy check`
- added deterministic config and tenant-policy tests for the default-off gate, file/env
  overrides, inherited values, and tenant override behavior

## Preserved boundaries

Phase 31d does not add:

- source-stream runtime evaluation
- source-stream `run` or `oneshot` integration
- source-stream AlertV1 emission from runtime paths
- source-stream metrics
- source-stream status output
- source-stream health output
- replay behavior changes
- recovery behavior changes
- AlertV1 schema changes
- DeviceStatsV1 layout changes
- hashed-fallback FeatureId behavior
- parser-class or vendor-event-family subject behavior

## Gate semantics

Global config:

```toml
[vdrop]
source_stream_enabled = false
```

Tenant policy:

```toml
vdrop_source_stream_enabled = true
```

Missing tenant-policy values inherit from global config/defaults. The first implementation
keeps the global default disabled so source-stream runtime activation requires an explicit
operator decision.

The Phase 31d helper resolves only the source-stream gate. Phase 31e later wired runtime
source-stream evaluator, AlertV1, and open-state helpers behind this default-off gate.

## Validation performed in this sandbox

- ASCII-only scan
- path-length scan
- stale-marker scan for common unfinished-code markers
- checkpoint zip integrity check

No local cargo/build/test/rustfmt run was performed in this sandbox.

## Next phase

Phase 31e source-stream runtime integration behind the default-off source-stream gate is complete.

Diagnostics, validation, and closeout should were completed in Phase 31f.
