pub use simd_r_drive_entry_handle::constants::*;

/// Marker indicating a logically deleted entry in the storage
pub const NULL_BYTE: [u8; 1] = [0];

/// Stream copy chunk size.
pub const WRITE_STREAM_BUFFER_SIZE: usize = 64 * 1024; // 64 KB

/// Fixed alignment (power of two) for the start of every payload.
/// 16 bytes covers u8/u16/u32/u64/u128 on mainstream targets.
pub const PAYLOAD_ALIGN_LOG2: u8 = 4;
pub const PAYLOAD_ALIGNMENT: u64 = 1 << PAYLOAD_ALIGN_LOG2;
