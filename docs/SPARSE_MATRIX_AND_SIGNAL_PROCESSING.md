# Sparse Matrix and Signal Processing Model

sparx is centered on sparse matrix log analysis, but the same data model also
has a natural signal-processing interpretation. This guide explains that model
for operators and maintainers without adding a separate heavy analytics system.

## Sparse rows as signal frames

Each finalized time window is a sparse matrix row:

- row: tenant/device/window slice
- column: canonical `FeatureId`
- value: count observed for that feature in the window

The row is also a signal frame. It is the current sampled state of many log
signals at the same time index.

For one feature, the sequence of counts across windows is a discrete-time
signal:

```text
x_feature[n] = count of FeatureId in finalized window n
```

For volume, the signal is the number of lines observed per window:

```text
x_volume[n] = total lines observed in finalized window n
```

For a source stream, the signal is the per-source line count:

```text
x_source[n] = lines observed for source stream in finalized window n
```

This lets sparx use sparse matrix methods for high-dimensional feature state
and signal-processing ideas for behavior over time.

## Existing signal-like behavior

The current alert model already applies several signal-processing concepts:

- rarity scoring highlights uncommon feature presence in the current sparse row
- drift scoring compares the current sparse row to a baseline vector
- spike scoring detects elevated volume relative to expected volume
- extreme volume scoring detects unusually large windows
- hard-silence detection detects a signal dropping to zero
- sharp-drop detection detects a reduced-but-nonzero negative edge
- source-stream volume-loss detection applies the same volume-loss idea to a
  specific canonical log source path behind the source-stream gate

These behaviors operate on finalized windows and persisted baseline state. They
avoid dense matrix expansion and keep per-subject state compact.

## Lean signal-processing MVP

The lean signal-processing extension for sparx should complement the existing
sparse matrix model. It should not replace the current baselines, alert schema,
or storage contracts.

The MVP has two useful baseline additions:

1. EWMA volume smoothing
2. Periodicity-aware volume baselines

Both additions operate on subject volume signals rather than per-feature
seasonality. This keeps the design bounded and avoids multiplying state by the
full feature dictionary.

## EWMA volume smoothing

EWMA is an exponentially weighted moving average. It smooths noisy volume while
remaining cheap and deterministic:

```text
ewma[n] = alpha * observed[n] + (1 - alpha) * ewma[n - 1]
```

For sparx, EWMA should be maintained as compact state for volume signals:

- device volume
- tenant aggregate volume
- source-stream volume when the source-stream gate is enabled

EWMA can help spike and drop evaluation by providing a responsive smoothed
expected value. It should be introduced as a supporting expected-volume signal,
not as a new alert family.

A minimal EWMA state can track:

- smoothed line count
- smoothed byte count
- number of mature windows
- last updated bucket

The implementation should use separate storage keys and must not alter
`DeviceStatsV1` or `SourceStreamStatsV1` layouts.

## Periodicity-aware volume baselines

Many log sources are periodic. Authentication bursts, backups, scheduled jobs,
batch pipelines, and business-hour traffic often repeat by hour of day or day
of week. A general baseline can misclassify those normal rhythms as spikes or
drops.

The MVP periodic model should track volume by hour of week:

```text
hour_of_week = day_of_week * 24 + hour
```

This creates 168 deterministic slots per subject. For each subject and slot,
sparx can maintain compact Welford-style volume stats:

- sample count
- mean line count
- line-count variance accumulator
- mean byte count
- byte-count variance accumulator
- last updated bucket

When a slot is mature, spike and drop evaluation can compare the current window
against the expected volume for that hour-of-week slot. When the slot is not
mature, evaluation falls back to the current general baseline.

## Storage shape

The periodic baseline should use new narrow Fjall prefixes behind the existing
`src/db/` adapter boundary. It should not change existing sparse row keys,
alert keys, or fixed-layout stats records.

Example key families:

```text
periodic_stats/v1/device/<device_key>/<slot>
periodic_stats/v1/tenant/<slot>
periodic_stats/v1/source_stream/<source_stream_id>/<slot>
```

This preserves the current schema while adding bounded signal state.

## Detection use

The first integration should reduce false positives in existing volume-based
alerts:

- spike scoring
- extreme volume scoring
- sharp-drop detection
- hard-silence maturity checks where expected volume is relevant
- source-stream volume-loss detection behind the source-stream gate

The detection model should remain conservative:

```text
if periodic slot is mature:
    use periodic expected volume for this time slot
else:
    use existing general expected volume
```

A blended model can be evaluated later, but the MVP should start with a clear
mature-slot fallback rule.

## Boundaries

The lean MVP must preserve these boundaries:

- do not change `AlertV1` schema
- do not change `AlertV1.provenance`
- do not change `DeviceStatsV1` layout
- do not change `SourceStreamStatsV1` layout
- do not add dense per-feature seasonal storage
- do not add high-cardinality metric labels
- keep Fjall access behind `src/db/`
- keep behavior deterministic

