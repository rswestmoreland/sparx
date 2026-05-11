# Expected-Source State and V_DROP Storage Contract v0.1

This contract defines expected-source state used by `V_DROP` hard-silence,
sharp-drop, and source-stream volume-loss detection.

## Purpose

Expected-source state records that a subject has historically produced logs and
is mature enough to evaluate for missing or sharply reduced activity.

## Active subject kinds

- device
- tenant aggregate
- source stream

## ExpectedSourceStateV1

`ExpectedSourceStateV1` is the common expected-source state value. It records the
subject kind, last observed window, maturity counters, and state flags required
for fail-closed volume-loss evaluation.

Rules:

- reject unknown schema versions
- reject unknown subject kinds
- reject malformed timestamps or counters
- do not derive negative missed-window counts
- do not panic on malformed stored values
- suppress candidate evaluation when state is invalid or immature

## Device and tenant aggregate keys

```text
silence_subject/v1/device/<device_key>/state
silence_subject/v1/tenant/state
silence_open/v1/device/<device_key>
silence_open/v1/tenant
drop_open/v1/device/<device_key>
drop_open/v1/tenant
```

Device and tenant aggregate expected volume uses the locked 68-byte
`DeviceStatsV1` bucket layout. This layout must not change.

## Source-stream keys

```text
source_stream/v1/<device_key>/<source_stream_id>/catalog
source_stats/v1/<device_key>/<source_stream_id>/<bucket>
source_prov/v1/<device_key>/<source_stream_id>/<window_start>
silence_subject/v1/source_stream/<device_key>/<source_stream_id>/state
silence_open/v1/source_stream/<device_key>/<source_stream_id>
drop_open/v1/source_stream/<device_key>/<source_stream_id>
```

Source-stream expected volume uses `SourceStreamStatsV1`, not `DeviceStatsV1`.
The source-stream subject id is not a `FeatureId`.

## Hard-silence evaluation

Hard silence is the full-drop case. A candidate may be produced when a mature
subject has expected activity, misses the configured number of windows, and has
no equivalent open hard-silence interval.

Open hard-silence state is stored under `silence_open/*`.

## Sharp-drop evaluation

Sharp drop is the reduced-but-nonzero case. A candidate may be produced when a
mature subject has expected activity, current observed lines are nonzero, the
observed/expected ratio is below the configured gate, and no equivalent open
sharp-drop interval exists.

Open sharp-drop state is stored under `drop_open/*`.

Ratio semantics:

```text
observed_expected_ratio = observed_lines / expected_lines
drop_ratio = 1.0 - observed_expected_ratio
```

Hard silence has priority over sharp drop.

## Alert construction

`V_DROP` alerts reuse the existing `AlertV1` schema. Device and source-stream
sharp-drop alerts should include capped current provenance when available.
Tenant aggregate and hard-silence absence-of-data alerts may use empty
provenance.

## Diagnostics

Diagnostics must remain bounded and low-cardinality. Source-stream diagnostics
may use aggregate and per-tenant counters only. Device labels, source-path labels,
source-stream-id labels, parser-class labels, vendor-family labels, per-subject
labels, and suppression-reason labels remain prohibited.

## Preserved behavior

This contract does not change:

- `DeviceStatsV1` layout
- `AlertV1` schema
- `AlertV1.provenance` authority
- replay behavior
- recovery behavior
- hashed-fallback FeatureId absence
