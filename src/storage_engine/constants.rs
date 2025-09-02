pub use simd_r_drive_entry_handle::constants::*;

// Marker indicating a logically deleted entry in the storage
pub const NULL_BYTE: [u8; 1] = [0];

pub const WRITE_STREAM_BUFFER_SIZE: usize = 64 * 1024; // 64 KB chunks
