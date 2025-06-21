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

//  ──────────────────────────────────────────────────────────────────────────
//  Buffered-write soft-limit
//
//  The `StageWriterBuffer` keeps recent `stage_write()` payloads in RAM and ships them
//  to disk in one go once the *aggregate* size passes a threshold.  The
//  `DEFAULT_WRITE_BUF_LIMIT`constant encodes that **soft limit**:
//
//  • **Why “soft”?**
//      – When the total buffered bytes ≥ LIMIT we *trigger a flush*, but we
//        never reject or block an incoming write; the buffer can temporarily
//        grow past the threshold while the flush is in flight.
//
//  • **Dual value (cfg(test) vs. real world)**
//      – During `cargo test` we keep the limit tiny (4 MiB) so every unit-test
//        exercises the flush path deterministically and the suite stays fast.
//      – In normal builds the default is 64 MiB – large enough to amortize I/O
//        on typical SSDs without hogging memory.
//
//  • **Tuning**
//      – The limit is *per DataStore instance*.
//      – Future work may make this adaptive (percentage of RAM) or overrideable
//        via an environment variable (`SIMD_R_DRIVE_BUF_LIMIT`).
//  ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) const DEFAULT_WRITE_BUF_LIMIT: usize = 4 * 1024 * 1024; // 4 MiB

// Note: This is for suggestive purposes only and will vary across real-world scenarios
#[cfg(not(test))]
pub(crate) const DEFAULT_WRITE_BUF_LIMIT: usize = 64 * 1024 * 1024; // 64 MiB
