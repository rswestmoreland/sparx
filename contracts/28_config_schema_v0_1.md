# Config Schema Contract v0.1

This contract defines the canonical configuration model for `sparx` (Sparse Matrix Log Analyzer) and how configuration is loaded and overridden.

## Goals
- Single, stable config surface for Enterprise Linux deployment.
- Deterministic defaults that match v0.1 contracts.
- Clear override precedence (CLI > env > config file).
- Safe caps to prevent memory blowups at high volume.

## Non-goals (v0.1)
- Hot reload of config.
- Multi-profile config with includes.
- Remote config management.

---

## 1) Config sources and precedence

### 1.1 Config file
- Default path: `/etc/sparx/sparx.toml`
- CLI may override path: `--config <path>`

### 1.2 Environment variables
- Prefix: `SPARX_`
- Names use uppercase with `_` separators.
- Example: `SPARX_WINDOW_SIZE_S=60`

### 1.3 CLI flags
- CLI flags override env and config file.

Precedence order (highest to lowest):
1) CLI
2) Environment
3) Config file
4) Built-in defaults

---

## 2) Top-level config object

TOML root: `[sparx]`

### 2.1 Identity and paths
- `data_root: string` (default: `/var/lib/sparx`)
  - contains tenant DBs and spool directories
- `tenant_root: string` (default: `/var/log/tenants`)
  - root directory containing tenant directories
- `global_db_path: string` (default: `{data_root}/global.db`)
- `tenant_db_root: string` (default: `{data_root}/tenants`)
- `alert_out_root: string` (default: `{data_root}/alerts`)
- `pid_file: string` (default: `/run/sparx.pid`) (optional)

Env:
- `SPARX_DATA_ROOT`
- `SPARX_TENANT_ROOT`
- `SPARX_GLOBAL_DB_PATH`
- `SPARX_TENANT_DB_ROOT`
- `SPARX_ALERT_OUT_ROOT`
- `SPARX_PID_FILE`

### 2.2 Runtime mode
- `mode: string` in `{ "daemon", "oneshot" }` (default: `daemon`)
- `log_level: string` in `{ "error", "warn", "info", "debug", "trace" }` (default: `info`)
- `log_format: string` in `{ "text", "json" }` (default: `text`)

Status:
- `mode`, `log_level`, and `log_format` are active v0.1 fields and are validator-enforced.

Env:
- `SPARX_MODE`
- `SPARX_LOG_LEVEL`
- `SPARX_LOG_FORMAT`

---

## 3) Ingestion and windowing

TOML: `[ingest]`

### 3.1 Windowing
- `window_size_s: u32` (default: 60)
- `max_emit_latency_s: u32` (default: 600)

Rules:
- `window_size_s` must be in { 60, 120, 300, 600 } for v0.1 (whitelist).
- `max_emit_latency_s` must be >= window_size_s.

Env:
- `SPARX_WINDOW_SIZE_S`
- `SPARX_MAX_EMIT_LATENCY_S`

### 3.2 File polling
- `poll_interval_ms: u32` (default: 1000)
- `max_open_files: u32` (default: 4096)
- `follow_symlinks: bool` (default: false)
- `read_chunk_bytes: u32` (default: 262144) (256 KiB)

Rules:
- `read_chunk_bytes` must be 1 through 16777216.

Env:
- `SPARX_POLL_INTERVAL_MS`
- `SPARX_MAX_OPEN_FILES`
- `SPARX_FOLLOW_SYMLINKS`
- `SPARX_READ_CHUNK_BYTES`

### 3.3 Compression handling
- `gzip_enabled: bool` (default: true)
- `gzip_suffixes: [string]` (default: [".gz", ".gzip"])
- `prefer_plain_when_both: bool` (default: true)

Env:
- `SPARX_GZIP_ENABLED`
- `SPARX_GZIP_SUFFIXES` (comma-separated)
- `SPARX_PREFER_PLAIN_WHEN_BOTH`

### 3.4 Line processing caps (align with Tokenizer Details v0.1)
- `max_line_len: u32` (default: 16384)
- `max_tokens_per_line: u32` (default: 256)
- `max_kv_per_line: u32` (default: 64)
- `max_words_from_quoted_value: u32` (default: 32)

Rules:
- `max_line_len` must be 1 through 1048576.
- `max_tokens_per_line` must be 1 through 4096.
- `max_kv_per_line` must be 1 through 1024.
- `max_words_from_quoted_value` must be 1 through 1024.
- runtime line buffering must stay bounded by `max_line_len`.

Env:
- `SPARX_MAX_LINE_LEN`
- `SPARX_MAX_TOKENS_PER_LINE`
- `SPARX_MAX_KV_PER_LINE`
- `SPARX_MAX_WORDS_FROM_QUOTED_VALUE`

---

## 4) Feature dictionary

TOML: `[features]`

- `dict_enabled: bool` (default: true)
- `dict_max_entries: u32` (default: 2_000_000)
- `hash_space_bits: u8` (default: 26)
- `dict_gc_interval_s: u32` (default: 3600)

Env:
- `SPARX_DICT_ENABLED`
- `SPARX_DICT_MAX_ENTRIES`
- `SPARX_HASH_SPACE_BITS`
- `SPARX_DICT_GC_INTERVAL_S`

Rules:
- v0.1 feature IDs are dictionary-only; no hashed fallback FeatureId behavior exists.
- If `dict_enabled=false`, new feature insertion fails closed.
- If the dictionary reaches `dict_max_entries`, new feature insertion fails closed.
- `hash_space_bits` is retained as a reserved config field for compatibility; it does not enable hashed fallback behavior in v0.1.
- `dict_gc_interval_s` is retained as a reserved metadata/compatibility field; dictionary GC/eviction is not implemented in v0.1.

---

## 5) Baselines and scoring

TOML: `[baseline]` and `[scoring]`

### 5.1 Baseline retention
- `baseline_days: u32` (default: 7)
- `baseline_min_days: u32` (default: 1)
- `df_bucket_count: u32` (default: 48) (weekday/weekend x hour)

Env:
- `SPARX_BASELINE_DAYS`
- `SPARX_BASELINE_MIN_DAYS`

### 5.2 DF ring sizing
- `df_ring_slots: u32` (default: 7) (one per day)
- `df_buckets_per_slot: u32` (default: 48)

Env:
- `SPARX_DF_RING_SLOTS`
- `SPARX_DF_BUCKETS_PER_SLOT`

### 5.3 Scoring thresholds
- `outlier_threshold: f32` (default: 0.85)
- `noise_threshold: f32` (default: 0.65)

- `cold_start_days: u32` (default: 2)
- `min_lines_per_window: u32` (default: 10)

Env:
- `SPARX_OUTLIER_THRESHOLD`
- `SPARX_NOISE_THRESHOLD`
- `SPARX_COLD_START_DAYS`
- `SPARX_MIN_LINES_PER_WINDOW`

Notes:
- Threshold interpretation is defined by Scoring Math + Thresholding contract v0.1.
- `outlier_threshold` and `noise_threshold` are active scoring inputs in v0.1.
- `cold_start_days` is active in the current release. It defines the day-based maturity floor for bucket scoring as `cold_start_days * (3600 / window_size_s)`. A value of `0` disables the count-based cold-start floor and leaves only the empty-centroid cold-start rule.
- `min_lines_per_window` is active in the current release. If a finalized window has fewer lines than this value, alert emission is suppressed for that window while scoring preview and baseline updates still proceed. A value of `0` disables the line floor.

---

## 6) Window caps (align with Feature Emission Catalog v0.1)

TOML: `[caps]`

- `max_features_per_window: u32` (default: 50000)
- `max_word_features_per_window: u32` (default: 20000)
- `max_shape_features_per_window: u32` (default: 20000)
- `max_syslog_features_per_window: u32` (default: 2000)

- `max_srcips: u32` (default: 64)
- `max_dstips: u32` (default: 64)
- `max_userids: u32` (default: 128)
- `max_domains: u32` (default: 128)
- `max_hosts: u32` (default: 128)

Env:
- `SPARX_MAX_FEATURES_PER_WINDOW`
- `SPARX_MAX_WORD_FEATURES_PER_WINDOW`
- `SPARX_MAX_SHAPE_FEATURES_PER_WINDOW`
- `SPARX_MAX_SYSLOG_FEATURES_PER_WINDOW`
- `SPARX_MAX_SRCIPS`
- `SPARX_MAX_DSTIPS`
- `SPARX_MAX_USERIDS`
- `SPARX_MAX_DOMAINS`
- `SPARX_MAX_HOSTS`

---

## 7) Storage (embedded DB; Fjall target for 10c+)

TOML: `[storage]`

Notes:
- v0.1 runtime work targets Fjall for the real DB/runtime layer.
- The field names below are retained for config continuity while the exact Fjall wiring is locked in the current release.
- `tenant_db_max_open` and `tenant_db_idle_close_s` are the active lifecycle controls in v0.1.
- `global_db_open_files`, `global_db_write_buffer_mb`, `tenant_db_open_files`, `tenant_db_write_buffer_mb`, and `tenant_db_max_background_jobs` remain reserved continuity fields in v0.1; they stay parseable but are not mapped to current runtime behavior.

### 7.1 Global DB
- `global_db_open_files: i32` (default: 256)
- `global_db_write_buffer_mb: u32` (default: 64)

### 7.2 Tenant DB
- `tenant_db_open_files: i32` (default: 512)
- `tenant_db_write_buffer_mb: u32` (default: 128)
- `tenant_db_max_background_jobs: i32` (default: 4)

### 7.3 Tenant DB lifecycle
- `tenant_db_max_open: u32` (default: 64) (LRU open handles)
- `tenant_db_idle_close_s: u32` (default: 60)

Env:
- `SPARX_TENANT_DB_MAX_OPEN`
- `SPARX_TENANT_DB_IDLE_CLOSE_S`

---

## 8) Outputs (alerts)

TOML: `[output]`

- `sink: string` in `{ "jsonl", "stdout" }` (default: `jsonl`)
- `jsonl_rotate_mb: u32` (default: 256)
- `jsonl_flush_interval_s: u32` (default: 5)
- `include_debug_fields: bool` (default: false)
- `automated_replay_max_files_per_pass: u32` (default: 128)
- `automated_replay_interval_s: u32` (default: 1)
- `spool_max_mb: u32` (default: 2048)

Status:
- `sink`, `jsonl_rotate_mb`, `jsonl_flush_interval_s`, `include_debug_fields`,
  `automated_replay_max_files_per_pass`, `automated_replay_interval_s`, and
  `spool_max_mb` are the active v0.1 output config surface.
- Automatic jsonl-failure-to-spool fallback and bounded automated replay are
  active runtime behaviors in v0.1 for `output.sink=jsonl`.
- `automated_replay_max_files_per_pass` controls the deterministic per-pass
  replay bound used by `run` and `oneshot` automated recovery.
- `automated_replay_interval_s` controls the minimum seconds between daemon
  automated replay attempts at the start of successive `run` cycles. Final
  shutdown replay remains unconditional.
- `spool_max_mb` controls the deterministic spool cap enforced by the helper-
  backed jsonl recovery path in `run` and `oneshot`.

Env:
- `SPARX_OUTPUT_SINK`
- `SPARX_JSONL_ROTATE_MB`
- `SPARX_JSONL_FLUSH_INTERVAL_S`
- `SPARX_INCLUDE_DEBUG_FIELDS`
- `SPARX_AUTOMATED_REPLAY_MAX_FILES_PER_PASS`
- `SPARX_AUTOMATED_REPLAY_INTERVAL_S`
- `SPARX_SPOOL_MAX_MB`

---

## 9) V_DROP hard-silence policy controls

TOML: `[vdrop]`

These fields are active as parseable and validator-enforced config surface.
the current release routes runtime hard-silence V_DROP evaluation through the resolved
per-tenant policy.

- `enabled: bool` (default: true)
- `device_enabled: bool` (default: true)
- `tenant_enabled: bool` (default: true)
- `source_stream_enabled: bool` (default: false)
- `min_expected_windows_missed: u32` (default: 3)
- `min_mature_windows: optional u64` (default: unset; inherit scoring-derived floor)
- `min_expected_lines: optional u64` (default: unset; inherit scoring-derived floor)

Rules:
- `min_expected_windows_missed` must be greater than zero.
- `min_mature_windows` and `min_expected_lines` may be unset. If set, they must parse as
  `u64` values.
- Defaults preserve the the current release hard-silence behavior when no tenant-policy override is present.
- `source_stream_enabled` defaults to `false` and does not activate source-stream runtime
  evaluation until the later runtime-integration stage wires the gate into `run` and
  `oneshot`.

Env:
- `SPARX_VDROP_ENABLED`
- `SPARX_VDROP_DEVICE_ENABLED`
- `SPARX_VDROP_TENANT_ENABLED`
- `SPARX_VDROP_SOURCE_STREAM_ENABLED`
- `SPARX_VDROP_MIN_EXPECTED_WINDOWS_MISSED`
- `SPARX_VDROP_MIN_MATURE_WINDOWS`
- `SPARX_VDROP_MIN_EXPECTED_LINES`

---

## 10) Metrics and health

TOML: `[metrics]`

These fields are active in the current release.

- `prometheus_enabled: bool` (default: true)
- `prometheus_bind: string` (default: `127.0.0.1:9898`)
- `health_enabled: bool` (default: true)
- `health_bind: string` (default: `127.0.0.1:9899`)

Status:
- when `prometheus_enabled=true`, `run` binds `prometheus_bind` and serves
  Prometheus text on `/metrics`
- when `health_enabled=true`, `run` binds `health_bind` and serves `/healthz`
- bind parsing is validator-enforced
- when both endpoints are enabled, the binds must differ
- endpoint startup failures fail closed at `run` startup
- disabled endpoints are not bound

Env:
- `SPARX_PROMETHEUS_ENABLED`
- `SPARX_PROMETHEUS_BIND`
- `SPARX_HEALTH_ENABLED`
- `SPARX_HEALTH_BIND`

The current the current release observability surface is defined by Contract 10.

---

## 11) Example config (sparx.toml)

```toml
[sparx]
data_root = "/var/lib/sparx"
tenant_root = "/var/log/tenants"
mode = "daemon"
log_level = "info"
log_format = "text"

[ingest]
window_size_s = 60
max_emit_latency_s = 600
poll_interval_ms = 1000
max_open_files = 4096
gzip_enabled = true
max_line_len = 16384

[features]
dict_enabled = true
dict_max_entries = 2000000
hash_space_bits = 26

[baseline]
baseline_days = 7
df_ring_slots = 7
df_buckets_per_slot = 48

[scoring]
outlier_threshold = 0.85
noise_threshold = 0.65
cold_start_days = 2
min_lines_per_window = 10

[caps]
max_features_per_window = 50000
max_word_features_per_window = 20000
max_shape_features_per_window = 20000

[storage]
tenant_db_max_open = 64
tenant_db_idle_close_s = 60

[output]
sink = "jsonl"
jsonl_rotate_mb = 256
jsonl_flush_interval_s = 5

[metrics]
prometheus_enabled = true
prometheus_bind = "127.0.0.1:9898"
health_enabled = true
health_bind = "127.0.0.1:9899"

[vdrop]
enabled = true
device_enabled = true
tenant_enabled = true
source_stream_enabled = false
min_expected_windows_missed = 3
```

---

## 12) Required tests
- precedence: CLI > env > config > defaults
- window_size whitelist enforced
- numeric bounds enforced for active numeric fields and reserved compatibility fields that still validate (for example `hash_space_bits`)
- config serialization roundtrip (TOML) preserves fields
- V_DROP policy defaults, source-stream default-off gate, file/env overrides, and invalid missed-window threshold validation
