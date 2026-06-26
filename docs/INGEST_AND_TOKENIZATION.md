# Ingest and Tokenization

sparx reads logs from a multi-tenant directory layout. Each tenant has watch
roots and per-device log directories. The runtime processes files in deterministic
order and tracks cursors so repeated processing is stable.

## Supported file handling

- plain text input
- gzip input where applicable
- zlg archive input for `.zlg` files
- deterministic cursor restore and advancement
- restart-safe open-window checkpointing
- fail-closed behavior for invalid runtime state

## Supported format handling

sparx handles heterogeneous logs through explicit tokenizers and deterministic
fallback behavior:

- syslog envelope variants
- key/value logs
- JSON logs
- CSV logs
- CEF with reverse parsing rules
- plaintext fallback

## Normalization model

Tokenizers emit canonical features and entity sketches. Known fields can become
strong semantic features. Unknown or partially parsed content can still emit
bounded token and shape features so messy logs contribute signal without requiring
every vendor dialect to be modeled first.

## Safety boundaries

- feature and sketch counts are capped
- tokenization is deterministic
- malformed lines should not panic the runtime
- high-cardinality values are controlled through feature caps and sketches
- source provenance is retained separately from sparse feature storage
