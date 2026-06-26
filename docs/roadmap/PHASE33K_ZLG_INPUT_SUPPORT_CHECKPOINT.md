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
