# Directory Discovery Contract v0.1

## Default layout
```
<watch-root>/
  <tenant_id>/
    <device_dir>/
      *.log, *.gz, *.json, *.csv, ...
```

- tenant_id = first level under watch-root
- device_dir = second level under watch-root/tenant_id

## Device identity
- keep `device_dir_name` (directory name)
- internal `device_key = hash(tenant_id + "/" + device_dir_relative_path)`

Stable hash rule for v0.1:
- algorithm: BLAKE3
- persisted width: first 16 digest bytes (128 bits)
- encoding: lowercase hex
- input bytes: exact UTF-8 bytes of the canonical input string

## File inclusion
- regular files only
- no symlinks by default
- default extension allowlist: `.log .txt .json .csv .cef .gz`
- ignore hidden files (leading dot)

## Tenant lifecycle
- tenant appears when directory exists
- purge deletes tenant DB directory (state), raw logs may remain per retention policy
