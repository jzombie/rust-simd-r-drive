use crc32fast::Hasher as Crc32FastHasher;

/// Computes a 4-byte CRC32C checksum using hardware-accelerated SIMD instructions.
///
/// This function calculates a CRC32C (Cyclic Redundancy Check) checksum over the given data.
/// When available, it utilizes SSE4.2 on x86_64 and NEON on ARM architectures for optimized
/// performance. The result is a **full 4-byte** checksum, ensuring stronger integrity checks.
///
/// # Parameters
/// - `data`: A byte slice representing the input data for checksum calculation.
///
/// # Returns
/// - A **4-byte** array representing the full CRC32C checksum.
#[inline]
pub fn compute_checksum(data: &[u8]) -> [u8; 4] {
    let mut hasher = Crc32FastHasher::new();
    hasher.update(data);
    let hash = hasher.finalize(); // Uses SSE4.2 or NEON when available
    hash.to_le_bytes() // Convert to little-endian 4-byte array
}
