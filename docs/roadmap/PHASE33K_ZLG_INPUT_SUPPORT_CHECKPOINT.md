# Phase 33K zlg input support checkpoint

## Scope

This checkpoint expands the supported input model so sparx can read `.zlg`
archives in addition to plain text and gzip files.

## Implemented changes

- Added `.zlg` to the deterministic input suffix allowlist.
- Added a `ZlgFileReaderV1` reader variant behind the existing file-reader
  abstraction.
- Added zlg archive parsing for the current public zlg v1 layout:
  - global header validation
  - chunk header parsing
  - summary skipping
  - stored payload support
  - zstd payload decode
  - decoded-length and CRC checks
  - directory/footer stop handling
- Kept `AlertV1` and storage layouts unchanged.
- Kept `FileSpanV1.is_gzip` gzip-specific; zlg spans use `is_gzip=false` and
  archive byte ranges for offsets.
- Updated alert drill/extract helpers so `.zlg` provenance can be decoded through
  the reader abstraction rather than treated as plain archive bytes.
- Added reader, discovery, alert drill, and alert extract tests for `.zlg` inputs.
- Updated README, HOWTO, active docs, and contracts to include zlg input support.

## Notes and limitations

- zlg files are treated as finalized archives and should be replaced atomically
  rather than edited in place.
- zlg provenance offsets identify archive chunk byte ranges, not exact decoded
  line byte positions.
- This checkpoint does not implement zlg mesh-bigram search acceleration inside
  sparx ingestion; sparx decodes archive chunks and tokenizes lines normally.

## Validation

No Rust toolchain validation is claimed for this ChatGPT-produced checkpoint.
Codex or another Rust 1.90 environment should run:

```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Phase33k validation fix 1

- Reviewed zlg reader, ingest discovery, cursor, provenance, drill, extract, and documentation paths.
- Fixed partial zlg record magic handling so a truncated record marker after the global header fails closed with `UnexpectedEof` instead of being treated as clean archive termination.
- Added narrow runtime-generated zlg reader coverage for final lines with and without newlines, malformed headers and payloads, decoded length mismatch, CRC mismatch, partial record magic, and directory/footer-only termination.
- Confirmed zlg cursor semantics remain archive-byte offsets; gzip-specific span fields remain gzip-only and zlg spans keep `is_gzip = false` while dispatching by `.zlg` suffix.
- Confirmed cross-project compatibility by building the public zlg CLI in a temporary directory, creating a `.zlg` archive from plaintext input, and ingesting it successfully with `sparx oneshot`.
- Full Rust validation and EPS benchmark commands passed on Rust 1.90.0; benchmark results are recorded in `validation_results/codex_validation_zlg_support_rust190_phase33k_fix1.txt`.
