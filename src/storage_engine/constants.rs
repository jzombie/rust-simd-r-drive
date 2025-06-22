// Metadata structure (fixed 20 bytes at the end of each entry)
pub const METADATA_SIZE: usize = 20;
pub const KEY_HASH_RANGE: std::ops::Range<usize> = 0..8;
pub const PREV_OFFSET_RANGE: std::ops::Range<usize> = 8..16;
pub const CHECKSUM_RANGE: std::ops::Range<usize> = 16..20;

// Marker indicating a logically deleted entry in the storage
pub const NULL_BYTE: [u8; 1] = [0];

// Define checksum length explicitly since `CHECKSUM_RANGE.len()` isn't `const`
pub const CHECKSUM_LEN: usize = CHECKSUM_RANGE.end - CHECKSUM_RANGE.start;

pub const WRITE_STREAM_BUFFER_SIZE: usize = 64 * 1024; // 64 KB chunks
