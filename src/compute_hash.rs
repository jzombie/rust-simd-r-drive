use xxhash_rust::xxh3::xxh3_64;
 
 /// Simple key hash function
 #[inline]
pub fn compute_hash(key: &[u8]) -> u64 {
  xxh3_64(key)
}
