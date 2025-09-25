use std::ops::Range;

// Metadata structure (fixed 20 bytes at the end of each entry)
pub const METADATA_SIZE: usize = 20;

pub const KEY_HASH_RANGE: Range<usize> = 0..8;
pub const PREV_OFFSET_RANGE: Range<usize> = 8..16;
pub const CHECKSUM_RANGE: Range<usize> = 16..20;

// Define checksum length explicitly since `CHECKSUM_RANGE.len()` isn't `const`
pub const CHECKSUM_LEN: usize = CHECKSUM_RANGE.end - CHECKSUM_RANGE.start;

/// Fixed alignment (power of two) for the start of every payload.
/// 64 bytes matches cache-line size and SIMD-friendly alignment.
/// This improves chances of staying zero-copy in vector kernels.
/// Max pre-pad per entry is `PAYLOAD_ALIGNMENT - 1` bytes.
pub const PAYLOAD_ALIGN_LOG2: u8 = 6; // 2^6 = 64
pub const PAYLOAD_ALIGNMENT: u64 = 1 << PAYLOAD_ALIGN_LOG2;
