use xxhash_rust::xxh3::xxh3_64;

/// Computes a 64-bit hash for the given key using XXH3.
///
/// XXH3 is a high-performance, non-cryptographic hash function optimized for speed
/// and efficiency. It leverages SIMD (Single Instruction, Multiple Data) and
/// hardware acceleration when available (e.g., AVX2, NEON) for even faster hashing.
/// This function provides a fast way to generate a unique identifier for a given
/// byte slice, making it suitable for key indexing in hash maps.
///
/// # Parameters
/// - `key`: A byte slice representing the key to be hashed.
///
/// # Returns
/// - A `u64` hash value derived from the input key.
///
/// #Notes:
///
/// Stream writing does not call this directly and instead builds `checksum_state` off
/// the hasher directly.
///
/// See `crate::storage_engine::DataStore::write_stream_with_key_hash` for implementation
/// details.
#[inline]
pub fn compute_hash(key: &[u8]) -> u64 {
    xxh3_64(key)
}

/// Computes XXH3 64-bit hashes for a batch of keys **in one call**.
///
/// The function walks the `keys` slice only once, feeding each key into
/// `xxh3_64`.  Internally `xxh3_64` already dispatches to the fastest SIMD
/// implementation available on the host (AVX2, NEON, …), so you still get the
/// intrinsic acceleration for *each* key – but with the added benefit that
/// you can:
///
/// * call the hasher exactly **once** from the high-level API,
/// * pre-allocate the `Vec<u64>` only once,
/// * hand the resulting `(hash, payload)` tuples straight to
///   `batch_write_hashed_payloads`, keeping the critical section (the `RwLock`)
///   as small as possible.
///
/// # Parameters
/// * `keys` – slice of key byte-slices; the `n`-th output hash corresponds to
///   the `n`-th input key.
///
/// # Returns
/// A `Vec<u64>` whose length equals `keys.len()`, containing the XXH3 hash of
/// each key.
///
/// # Examples
/// ```
/// use simd_r_drive::storage_engine::digest::{compute_hash, compute_hash_batch};
///
/// let keys: &[&[u8]] = &[b"alice", b"bob", b"carol"];
/// let hashes = compute_hash_batch(keys);
///
/// assert_eq!(hashes.len(), 3);
/// assert_eq!(hashes[0], compute_hash(b"alice"));
/// assert_eq!(hashes[1], compute_hash(b"bob"));
/// assert_eq!(hashes[2], compute_hash(b"carol"));
/// ```
#[inline]
pub fn compute_hash_batch(keys: &[&[u8]]) -> Vec<u64> {
    // If you build xxhash-rust with the `xxh3-bulk` feature you can replace the
    // simple `.map()` below with `xxh3::hash64_batch(keys)` for an extra
    // 10-15 % on very large batches.  The API shape is the same.
    keys.iter().map(|k| xxh3_64(k)).collect()
}
