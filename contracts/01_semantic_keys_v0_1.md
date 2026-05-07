# Semantic Key Classification Contract v0.1

## Goals
- Normalize key names across casing/separators/camelCase.
- Classify keys into canonical semantic categories.
- Return confidence so identity retention is safe.
- Allow per-tenant overrides without vendor parsers.

## 1) Key normalization
Given a raw key string (KV key / JSON field / CSV header / CEF key):

1. Trim whitespace.
2. Split CamelCase/PascalCase into word boundaries (insert `_` before upper→lower transitions).
3. Lowercase.
4. Replace non-alphanumeric with `_`.
5. Collapse multiple `_` into one.
6. Strip leading/trailing `_`.
7. Split on `_` into `parts`.
8. Keep `norm_key` as the joined parts with `_`.

Examples:
- `SourceIpAddress` -> `source_ip_address` parts `["source","ip","address"]`
- `src-address` -> `src_address` parts `["src","address"]`
- `source.address` -> `source_address` parts `["source","address"]`

## 2) Canonical categories (MVP)
- Network endpoints:
  - `SourceIp`, `DestIp`
  - `SourcePort`, `DestPort`
  - `SourceHost`, `DestHost`
- Identity:
  - `User`
- Execution:
  - `Process`, `Command`, `Path`
- Web:
  - `Url`, `Domain`
- Integrity:
  - `FileHash`
- Time:
  - `Timestamp` (optional)

Unmatched -> `None`.

## 3) Token groups
Direction tokens:
- `SRC_DIR = { src, source, client, remote, raddr, peer, origin, from, caller }`
- `DST_DIR = { dst, dest, destination, server, local, laddr, target, to, callee }`

Other token sets:
- `IP_TOK = { ip, ipaddr, addr, address }`
- `PORT_TOK = { port, sport, dport }`
- `HOST_TOK = { host, hostname, computer, device, node, machine }`
- `USER_TOK = { user, username, account, acct, principal, subject, actor, login, logon, uid }`
- `PROC_TOK = { process, proc, image, exe, program, binary, app }`
- `CMD_TOK = { cmd, command, commandline, argv, args }`
- `PATH_TOK = { path, filepath, file, filename, directory, dir }`
- `URL_TOK = { url, uri, request, resource }`
- `DOM_TOK = { domain, fqdn, hostdomain, dns, servername, sni }`
- `HASH_TOK = { hash, sha256, sha1, md5, checksum, fingerprint }`

## 4) Match rules (ordered precedence)
Compute:
- `has_src_dir = any(parts in SRC_DIR)`
- `has_dst_dir = any(parts in DST_DIR)`
If both present: ambiguous -> `None` unless overridden.

Precedence:
1. SourcePort / DestPort:
   - `has_src_dir && any(parts in PORT_TOK)` -> SourcePort
   - `has_dst_dir && any(parts in PORT_TOK)` -> DestPort
   - `sport` => SourcePort (strong)
   - `dport` => DestPort (strong)

2. SourceIp / DestIp:
   - `has_src_dir && (contains ip OR contains addr/address)` -> SourceIp
   - `has_dst_dir && (contains ip OR contains addr/address)` -> DestIp
   - `srcip/src_ip/source_ip/...` => SourceIp (strong)
   - `dstip/dst_ip/destination_ip/...` => DestIp (strong)

3. SourceHost / DestHost:
   - `has_src_dir && any(parts in HOST_TOK)` -> SourceHost
   - `has_dst_dir && any(parts in HOST_TOK)` -> DestHost

4. User:
   - `any(parts in USER_TOK)` -> User

5. Process:
   - `any(parts in PROC_TOK)` -> Process

6. Command:
   - `any(parts in CMD_TOK)` -> Command

7. Path:
   - `any(parts in PATH_TOK)` -> Path

8. Url / Domain:
   - `any(parts in URL_TOK)` -> Url
   - else if `any(parts in DOM_TOK)` -> Domain

9. FileHash:
   - `any(parts in HASH_TOK)` -> FileHash

Else `None`.

## 5) Confidence (0..3)
- 3 (Strong): compact canonical forms (`srcip`, `dstip`, `sport`, `dport`), explicit ip+direction.
- 2 (Medium): direction + addr/address (no explicit ip), good user tokens, etc.
- 1 (Weak): generic `src`, `dst`, `host` alone.
- 0: none/ambiguous.

Identity retention default: confidence >= 2.

## 6) Overrides (per tenant)
Overrides map `norm_key` -> `(Category, confidence=3)` and win over default rules.

## 7) Feature emission ties
Always emit `k=<norm_key>`.
If classified: `canon=<Category>` and `<Category>=<SHAPE>` when shape matches.
Identity metadata capture uses confidence threshold and allowlist.

## 8) Must-have tests
- normalization examples (camelCase, separators)
- classification of src/dst ip, ports, users, paths
- ambiguity -> None unless override
