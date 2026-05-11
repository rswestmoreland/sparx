# Benchmarking

sparx includes a dependency-free tenant/device EPS benchmark for measuring two
separate throughput paths:

- ingestion EPS: file scan, line read, syslog parsing, tokenization, feature
  emission, dictionary resolution, and sparse row/window population
- detection EPS: alert scoring, alert construction, and alert encoding over the
  finalized sparse rows produced by the ingestion probe

The benchmark creates a deterministic temporary corpus before timing starts.
Corpus generation is not included in the reported EPS values.

## Command

Run the benchmark with:

```bash
cargo bench --bench tenant_device_eps
```

The benchmark prints a compact summary:

```text
sparx tenant/device EPS benchmark
tenants=2
devices_per_tenant=5
files_per_device=2
events_per_file=500
total_events=10000
events_per_timestamp=100
approx_event_time_span_s_per_file=5
read_chunk_bytes=262144
source_stream_enabled=false
durable_oneshot_enabled=false
ingest_events=10000
ingest_bytes=2200000
ingest_sparse_rows=10
ingest_elapsed_s=0.123456
ingest_eps=81000.00
detection_events=10000
detection_sparse_rows=10
detection_alerts_emitted=10
detection_encoded_alert_bytes=12000
detection_elapsed_s=0.012345
detection_event_eps=810000.00
detection_row_eps=810.00
detection_alert_eps=810.00
```

`ingest_eps` is the primary ingestion throughput metric. It measures how fast
sparx can poll/read generated files, parse lines, tokenize, emit canonical
features, resolve feature ids, and populate sparse rows in memory.

`detection_event_eps` is the primary detection throughput metric. It measures
how fast alert scoring/build/encoding can use the finalized sparse rows produced
by the ingestion probe. `detection_row_eps` and `detection_alert_eps` are also
reported because detections operate per finalized sparse row rather than per raw
line.

## Workload shape

The default workload is intentionally small enough for normal validation runs:

- 2 tenants
- 5 devices per tenant
- 2 files per device
- 500 events per file
- 10000 total events
- 100 events per event timestamp
- about 5 seconds of event time per file
- source-stream V_DROP disabled, matching the default product gate
- durable oneshot timing disabled

Generated events use deterministic RFC5424-style syslog lines with common
key/value fields such as source IP, destination IP, user, action, result, bytes,
path, and status.

The default workload is shaped as a high-EPS logging scenario. Many events share
the same event timestamp so the benchmark measures dense ingestion instead of
mostly measuring repeated window-finalization overhead. Set
`SPARX_BENCH_EVENTS_PER_TIMESTAMP=1` to reproduce a sparse event-time shape
where every event advances by one second.

## Environment controls

Use environment variables to scale or alter the workload:

- `SPARX_BENCH_TENANTS`
- `SPARX_BENCH_DEVICES_PER_TENANT`
- `SPARX_BENCH_FILES_PER_DEVICE`
- `SPARX_BENCH_EVENTS_PER_FILE`
- `SPARX_BENCH_READ_CHUNK_BYTES`
- `SPARX_BENCH_EVENTS_PER_TIMESTAMP`
- `SPARX_BENCH_SOURCE_STREAM`
- `SPARX_BENCH_DURABLE_ONESHOT`
- `SPARX_BENCH_KEEP_ROOT`

Example 100000-event validation run:

```bash
SPARX_BENCH_TENANTS=2 \
SPARX_BENCH_DEVICES_PER_TENANT=10 \
SPARX_BENCH_FILES_PER_DEVICE=5 \
SPARX_BENCH_EVENTS_PER_FILE=1000 \
cargo bench --bench tenant_device_eps
```

To include source-stream runtime overhead in the optional durable oneshot path,
enable:

```bash
SPARX_BENCH_SOURCE_STREAM=1 SPARX_BENCH_DURABLE_ONESHOT=1 cargo bench --bench tenant_device_eps
```

To run the storage-inclusive durable `oneshot` path in addition to the default
in-memory ingestion and detection probes, enable:

```bash
SPARX_BENCH_DURABLE_ONESHOT=1 cargo bench --bench tenant_device_eps
```

The optional durable oneshot metric is useful for storage-inclusive regression
tracking, but it is not the primary ingestion or detection EPS metric because it
also includes embedded DB writes, cursor updates, recovery bookkeeping, sink
handling, and other runtime durability costs.

To retain the generated temporary corpus for inspection, enable:

```bash
SPARX_BENCH_KEEP_ROOT=1 cargo bench --bench tenant_device_eps
```

## Safety bounds

The benchmark rejects a generated corpus larger than 5000000 total events by
default. This prevents accidental oversized runs from consuming excessive CPU,
memory, or disk on development machines.

## Interpreting results

Use the benchmark for relative throughput comparisons between changes. Absolute
EPS depends on CPU, storage, filesystem, OS, build profile, and whether optional
source-stream or durable oneshot checks are enabled.

For release notes, retain the command, environment values, hardware summary,
Rust version, event timestamp density, approximate event-time span, and the
reported ingestion and detection EPS values.

If ingestion EPS is unexpectedly low, first check `events_per_timestamp`. A
value of 1 intentionally stresses sparse event-time behavior and is not
representative of dense high-EPS logging.
