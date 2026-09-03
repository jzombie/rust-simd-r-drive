pub use simd_r_drive_entry_handle::constants::*;

/// Marker indicating a logically deleted entry in the storage
pub const NULL_BYTE: [u8; 1] = [0];

/// Stream copy chunk size.
pub const WRITE_STREAM_BUFFER_SIZE: usize = 64 * 1024; // 64 KB

/// Initial baseline capacity for the key index HashMap during store open.
/// Starts small and lets HashMap grow dynamically via amortized O(1)
/// doublings — avoids OOM from file-length-based heuristics on stores
/// with large payloads.
pub const INDEX_BASELINE_CAPACITY: usize = 10_000;
