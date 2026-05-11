# Sharp-Drop Detection Scope Contract v0.1

Sharp drop is a `V_DROP` subcase for reduced-but-nonzero activity.

## Active scope

Active sharp-drop subjects:

- device
- tenant aggregate
- source stream when the source-stream gate is enabled

Sharp-drop alerts reuse the existing `AlertV1` schema and use reason code
`V_DROP` with deterministic detail `drop_kind=sharp_drop`.

## Ratio semantics

```text
observed_expected_ratio = observed_lines / expected_lines
drop_ratio = 1.0 - observed_expected_ratio
```

Sharp drop requires nonzero observed lines. Zero observed lines belongs to the
hard-silence path. Hard silence has priority over sharp drop.

## Expected-volume model

Device and tenant aggregate sharp drop use `DeviceStatsV1` line-count bucket
baselines as the primary expected-volume signal. Byte-count baselines are
explanation-only unless a later contract approves using them as a gate.

Source-stream sharp drop uses `SourceStreamStatsV1` and separate source-stream
state. It must not change `DeviceStatsV1`.

## State and dedup

Sharp-drop open state uses `drop_open/*` only:

```text
drop_open/v1/device/<device_key>
drop_open/v1/tenant
drop_open/v1/source_stream/<device_key>/<source_stream_id>
```

`drop_open/*` must not be used for hard-silence intervals. Hard-silence state
remains under `silence_open/*`.

## Alert details

Sharp-drop alerts must include deterministic reason details for expected lines,
observed lines, observed/expected ratio, drop ratio, bucket, subject kind, and
subject identity. Device and source-stream sharp-drop alerts should include
current provenance when available. Tenant aggregate sharp-drop alerts may have
empty provenance.

## Diagnostics

Sharp-drop diagnostics must remain bounded and low-cardinality. Device labels,
source-path labels, source-stream-id labels, per-subject labels, and suppression-
reason label fanout remain prohibited.
