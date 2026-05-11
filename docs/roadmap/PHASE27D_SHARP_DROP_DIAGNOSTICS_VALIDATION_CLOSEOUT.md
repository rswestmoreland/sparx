# Phase 27d - Sharp-drop diagnostics, validation, and closeout

Status: implementation checkpoint.

Phase 27d closes the first sharp-drop implementation sequence by adding the
smallest useful bounded diagnostic surface for the new `drop_open/*` state and
recording the validation/closeout boundary. The runtime behavior activated in
Phase 27c remains the active sharp-drop behavior.

## Scope

Implemented in Phase 27d:

- add `open_drop_subjects` to the in-memory V_DROP diagnostic delta
- persist per-tenant `vdrop_open_drop_subjects__<tenant_id>` gauges after each
  V_DROP tenant evaluation pass
- surface aggregate and per-tenant open-drop counts in status JSON
- surface aggregate and per-tenant open-drop counts in text status
- surface aggregate and per-tenant open-drop counts in Prometheus metrics
- surface the aggregate open-drop count in health output
- add deterministic checks for the new status/metrics fields
- update docs/contracts/history for Phase 27 closeout

Not implemented in Phase 27d:

- new config schema fields
- new tenant-policy fields
- new AlertV1 fields
- new stats encodings
- new replay semantics
- new recovery semantics
- per-device Prometheus labels
- per-subject metric series
- per-file/source-path, parser-class, vendor-family, heartbeat, or maintenance
  window drop detection

## Diagnostic model

The new diagnostic is intentionally narrow:

- `open_drop_subjects` counts currently open sharp-drop dedup intervals after the
  tenant V_DROP evaluation pass completes
- the aggregate status value is derived by summing per-tenant gauges
- Prometheus output uses one aggregate gauge and one bounded per-tenant gauge
- no device key, source path, parser class, vendor family, suppression reason, or
  alert id is used as a metric label

The metric names are:

- `sparx_vdrop_open_drop_subjects`
- `sparx_vdrop_open_drop_subjects_by_tenant{tenant_id="..."}`

The persisted per-tenant gauge key is:

- `vdrop_open_drop_subjects__<tenant_id>`

This mirrors the existing open-silence diagnostic shape while keeping sharp-drop
state separate from hard-silence state.

## Validation added

Phase 27d updates deterministic tests so that:

- status JSON reports aggregate and per-tenant open-drop counts
- text status includes `vdrop.open_drop_subjects`
- Prometheus output includes the open-drop metric family
- oneshot sharp-drop runtime integration writes the per-tenant open-drop gauge
  after opening device `drop_open/*` state

## Preserved behavior

Phase 27d preserves:

- DeviceStatsV1 68-byte layout
- AlertV1 schema
- AlertV1.provenance authority
- hard-silence V_DROP behavior and priority
- silence_open/* behavior
- drop_open/* key semantics from Phase 27b and Phase 27c
- replay-spool behavior
- recovery behavior
- existing config and tenant-policy schemas

No local cargo/build/test/rustfmt run was performed in this sandbox.
