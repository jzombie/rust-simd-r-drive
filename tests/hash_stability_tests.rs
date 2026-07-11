//! Hash stability tests.
//!
//! These tests enforce hardcoded xxh3 hash values to guard against silent
//! regressions if the `xxhash-rust` dependency is updated, replaced, or its
//! feature flags change. Every assertion locks down a specific input→output
//! mapping so that any hash change will cause a test failure.

#[cfg(test)]
mod hash_stability {
    use simd_r_drive::utils::NamespaceHasher;
    use simd_r_drive::{compute_hash, compute_hash_batch};

    // ── compute_hash: single-key stability ──────────────────────────────

    #[test]
    fn compute_hash_empty() {
        assert_eq!(compute_hash(b""), 0x2d06800538d394c2);
    }

    #[test]
    fn compute_hash_single_null_byte() {
        assert_eq!(compute_hash(b"\x00"), 0xc44bdff4074eecdb);
    }

    #[test]
    fn compute_hash_alice() {
        assert_eq!(compute_hash(b"alice"), 0x4da10dd61a0116b0);
    }

    #[test]
    fn compute_hash_bob() {
        assert_eq!(compute_hash(b"bob"), 0x1403c0c40f49b8e5);
    }

    #[test]
    fn compute_hash_carol() {
        assert_eq!(compute_hash(b"carol"), 0xe2fdb994ad3fcba4);
    }

    #[test]
    fn compute_hash_key1() {
        assert_eq!(compute_hash(b"key1"), 0x384d070cd5d829e2);
    }

    #[test]
    fn compute_hash_test_key() {
        assert_eq!(compute_hash(b"test_key"), 0xe0614cc5ecbeed92);
    }

    #[test]
    fn compute_hash_longer_key_name() {
        assert_eq!(compute_hash(b"longer_key_name"), 0x4c21bc57c3b572ee);
    }

    // ── compute_hash_batch: batch stability ─────────────────────────────

    #[test]
    fn compute_hash_batch_matches_individual() {
        let keys: &[&[u8]] = &[b"alice", b"bob", b"carol"];
        let hashes = compute_hash_batch(keys);

        assert_eq!(hashes[0], 0x4da10dd61a0116b0);
        assert_eq!(hashes[1], 0x1403c0c40f49b8e5);
        assert_eq!(hashes[2], 0xe2fdb994ad3fcba4);
    }

    #[test]
    fn compute_hash_batch_length() {
        let keys: &[&[u8]] = &[b"a", b"b", b"c", b"d", b"e"];
        let hashes = compute_hash_batch(keys);
        assert_eq!(hashes.len(), 5);
    }

    // ── NamespaceHasher: 16-byte namespaced key stability ───────────────

    #[test]
    fn namespace_hasher_ns1_key1() {
        let hasher = NamespaceHasher::new(b"namespace1");
        let key = hasher.namespace(b"key1");
        assert_eq!(
            key,
            vec![
                0x7c, 0x06, 0x6c, 0x9d, 0xf2, 0xe6, 0xec, 0xcb, 0xe2, 0x29, 0xd8, 0xd5, 0x0c, 0x07,
                0x4d, 0x38
            ]
        );
    }

    #[test]
    fn namespace_hasher_ns2_key1() {
        let hasher = NamespaceHasher::new(b"namespace2");
        let key = hasher.namespace(b"key1");
        assert_eq!(
            key,
            vec![
                0x8d, 0x56, 0x3a, 0x5c, 0x3c, 0x35, 0x16, 0x6c, 0xe2, 0x29, 0xd8, 0xd5, 0x0c, 0x07,
                0x4d, 0x38
            ]
        );
    }

    #[test]
    fn namespace_hasher_output_length() {
        let hasher = NamespaceHasher::new(b"ns");
        let key = hasher.namespace(b"k");
        assert_eq!(key.len(), 16, "Namespaced key must always be 16 bytes");
    }
}
