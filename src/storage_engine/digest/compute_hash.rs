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
