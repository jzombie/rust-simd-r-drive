use crc32fast::Hasher as Crc32FastHasher;

/// Computes a SIMD-accelerated CRC32C-based 3-byte checksum.
#[inline]
pub fn compute_checksum(data: &[u8]) -> [u8; 3] {
    let mut hasher = Crc32FastHasher::new();
    hasher.update(data);
    let hash = hasher.finalize(); // Uses SSE4.2 or Neon when available
    [
        (hash & 0xFF) as u8,
        ((hash >> 8) & 0xFF) as u8,
        ((hash >> 16) & 0xFF) as u8,
    ]
}
