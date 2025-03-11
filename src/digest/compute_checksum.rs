use crc32fast::Hasher as Crc32FastHasher;

/// Computes a 3-byte CRC32C checksum using hardware-accelerated SIMD instructions.
///
/// This function calculates a CRC32C (Cyclic Redundancy Check) checksum over the given data.
/// When available, it utilizes SSE4.2 on x86_64 and NEON on ARM architectures for optimized
/// performance. The result is a truncated 3-byte checksum, suitable for lightweight integrity
/// checks while reducing storage overhead.
///
/// # Parameters
/// - `data`: A byte slice representing the input data for checksum calculation.
///
/// # Returns
/// - A 3-byte array representing the truncated CRC32C checksum.
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
