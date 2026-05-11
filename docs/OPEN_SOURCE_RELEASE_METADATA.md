# Open Source Release Metadata

sparx is distributed as open-source software under the MIT License.

## Author

Richard S. Westmoreland  
dev@rswestmore.land

## Copyright

Copyright (c) 2026 Richard S. Westmoreland.

## License files

- `../LICENSE` contains the full MIT License text.
- `../NOTICE.md` contains the project notice, author, contact, and copyright.
- `../Cargo.toml` declares `license = "MIT"` and the author contact.

## Source-file metadata

Rust source and test files include a standard SPDX header:

```text
// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT
```

This keeps the license machine-readable without adding large license blocks to
every source file.

## Release validation

Before release, confirm that license and author metadata remain consistent in:

- `Cargo.toml`
- `LICENSE`
- `NOTICE.md`
- `README.md`
- `docs/README.md`
- `contracts/README.md`
- Rust source and test file headers
