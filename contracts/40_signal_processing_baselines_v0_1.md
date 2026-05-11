# Contract 40: Signal Processing Baselines v0.1

## Purpose

sparx treats finalized sparse rows as sampled signal frames. This contract locks
in the lean signal-processing baseline direction for improving volume-based
alert quality without changing the sparse matrix model.

## Signal model

For a finalized window `n`:

```text
x_feature[n] = count of a canonical FeatureId in window n
x_volume[n] = total line count in window n
x_source[n] = source-stream line count in window n
```

The sparse row remains the high-dimensional feature representation. Signal
processing state is compact auxiliary baseline state.

## MVP baseline additions

The MVP adds two baseline families:

1. EWMA volume smoothing
2. Hour-of-week periodic volume baselines

Both operate on volume signals for:

- device subjects
- tenant aggregate subjects
- source-stream subjects when the source-stream gate is enabled

## EWMA contract

EWMA state must be deterministic and compact.

The update equation is:

```text
ewma[n] = alpha * observed[n] + (1 - alpha) * ewma[n - 1]
```

The implementation must define:

- alpha source and default
- maturity rule
- line-count smoothing
- byte-count smoothing
- deterministic encoding
- invalid-state handling

EWMA is a supporting expected-volume signal. It is not a new alert type.

## Periodic volume baseline contract

The MVP periodic slot is hour of week:

```text
hour_of_week = day_of_week * 24 + hour
```

There are 168 slots per subject. Each mature slot can provide expected volume
for the matching window time.

The periodic baseline must track compact volume stats per slot:

- sample count
- mean line count
- line-count variance accumulator
- mean byte count
- byte-count variance accumulator
- last updated bucket

The periodic baseline must use separate key prefixes and must not change
existing fixed-layout records.

## Detection integration contract

Existing volume-based detection may consume periodic expected volume only when
the matching slot is mature. Otherwise, detection must fall back to the existing
general expected-volume baseline.

The first integration targets:

- spike scoring
- extreme volume scoring
- sharp-drop detection
- hard-silence maturity checks where expected volume is relevant
- source-stream volume-loss detection behind the source-stream gate

Hard silence remains higher priority than sharp-drop.

The existing ratio semantics remain unchanged:

```text
observed_expected_ratio = observed_lines / expected_lines
drop_ratio = 1.0 - observed_expected_ratio
```

## Storage boundaries

The implementation must not change:

- `DeviceStatsV1` layout
- `SourceStreamStatsV1` layout
- sparse row encoding
- AlertV1 schema
- AlertV1 provenance semantics

New signal baseline state must live under separate tenant DB key prefixes behind
`src/db/`.

Example prefix families:

```text
periodic_stats/v1/device/<device_key>/<slot>
periodic_stats/v1/tenant/<slot>
periodic_stats/v1/source_stream/<source_stream_id>/<slot>
```

Final names must be locked before implementation.

## Observability boundaries

Diagnostics must remain bounded and low-cardinality.

Do not add Prometheus labels for:

- device labels
- source paths
- source stream ids
- parser classes
- vendor families
- individual subjects
- suppression reasons
- per-file identity

Aggregate counters and gauges are acceptable.

## Performance boundaries

Updates must remain O(1) per subject/window. The MVP must not add dense
per-feature seasonal state. Any new hot-path allocation must be justified by
benchmark evidence.

