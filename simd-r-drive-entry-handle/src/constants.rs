use std::ops::Range;

// Metadata structure (fixed 20 bytes at the end of each entry)
pub const METADATA_SIZE: usize = 20;

pub const KEY_HASH_RANGE: Range<usize> = 0..8;
pub const PREV_OFFSET_RANGE: Range<usize> = 8..16;
pub const CHECKSUM_RANGE: Range<usize> = 16..20;

// Define checksum length explicitly since `CHECKSUM_RANGE.len()` isn't `const`
pub const CHECKSUM_LEN: usize = CHECKSUM_RANGE.end - CHECKSUM_RANGE.start;
