# Benchmarking

sparx includes a dependency-free tenant/device EPS benchmark for measuring a
representative end-to-end ingestion path.

The benchmark creates a deterministic temporary corpus, runs the existing
`oneshot` runtime path for every generated tenant, and prints total events per
second. The measured interval starts after fixture generation and covers runtime
processing, tokenization, sparse window updates, baseline/storage activity, and
alert sink behavior.

## Command

Run the benchmark with:

```bash
cargo bench --bench tenant_device_eps
```

The benchmark prints a compact summary:

```text
sparx tenant/device EPS benchmark
tenants=2
devices_per_tenant=8
files_per_device=2
events_per_file=2000
total_events=64000
elapsed_s=1.234567
total_eps=51840.03
read_chunk_bytes=262144
source_stream_enabled=false
```

`total_eps` is the primary throughput metric.

## Workload shape

The default workload is intentionally moderate so it can run on development
machines:

- 2 tenants
- 8 devices per tenant
- 2 files per device
- 2000 events per file
- 64000 total events
- source-stream V_DROP disabled, matching the default product gate

Generated events use deterministic RFC5424-style syslog lines with common
key/value fields such as source IP, destination IP, user, action, result, bytes,
path, and status.

## Environment controls

Use environment variables to scale or alter the workload:

- `SPARX_BENCH_TENANTS`
- `SPARX_BENCH_DEVICES_PER_TENANT`
- `SPARX_BENCH_FILES_PER_DEVICE`
- `SPARX_BENCH_EVENTS_PER_FILE`
- `SPARX_BENCH_READ_CHUNK_BYTES`
- `SPARX_BENCH_SOURCE_STREAM`
- `SPARX_BENCH_KEEP_ROOT`

Example larger run:

```bash
SPARX_BENCH_TENANTS=4 \
SPARX_BENCH_DEVICES_PER_TENANT=25 \
SPARX_BENCH_FILES_PER_DEVICE=2 \
SPARX_BENCH_EVENTS_PER_FILE=5000 \
cargo bench --bench tenant_device_eps
```

To include source-stream runtime overhead, enable:

```bash
SPARX_BENCH_SOURCE_STREAM=1 cargo bench --bench tenant_device_eps
```

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
EPS depends on CPU, storage, filesystem, OS, build profile, and whether the
source-stream gate is enabled.

For release notes, retain the command, environment values, hardware summary,
Rust version, and final `total_eps` value.
