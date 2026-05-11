// Copyright (c) 2026 Richard S. Westmoreland
// SPDX-License-Identifier: MIT

// Stable hash helpers for persistent identifiers.
//
// Locked v0.1 rule:
// - algorithm: BLAKE3
// - persisted width: 128 bits (first 16 digest bytes)
// - encoding: lowercase ASCII hex
// - input: exact UTF-8 bytes of the canonical contract string

pub const STABLE_HASH_HEX128_LEN_V1: usize = 32;

pub fn stable_hash_hex128_v1(input: &str) -> String {
    let digest = blake3::hash(input.as_bytes());
    let bytes = &digest.as_bytes()[0..16];
    let mut out = String::with_capacity(STABLE_HASH_HEX128_LEN_V1);
    for b in bytes {
        out.push(hex_nibble((*b >> 4) & 0x0f));
        out.push(hex_nibble(*b & 0x0f));
    }
    out
}

fn hex_nibble(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        10..=15 => (b'a' + (v - 10)) as char,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_hash_hex128_is_lowercase_ascii_hex() {
        let h = stable_hash_hex128_v1("tenant01/device01");
        assert_eq!(h.len(), STABLE_HASH_HEX128_LEN_V1);
        assert!(h.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    }

    #[test]
    fn stable_hash_hex128_is_deterministic_and_input_sensitive() {
        let a = stable_hash_hex128_v1("acme/router01");
        let b = stable_hash_hex128_v1("acme/router01");
        let c = stable_hash_hex128_v1("acme/router02");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
