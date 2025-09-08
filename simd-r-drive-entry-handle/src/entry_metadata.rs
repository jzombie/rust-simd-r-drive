use crate::constants::*;

/// Metadata structure for an append-only storage entry.
///
/// This structure stores metadata associated with each entry in the append-only storage.
/// It includes a hash of the key for quick lookups, an offset pointing to the previous
/// entry in the chain, and a checksum for integrity verification.
///
/// ## Entry Storage Layout
///
/// Aligned entry (non-tombstone):
///
/// | Offset Range   | Field              | Size (Bytes) | Description                       |
/// |----------------|--------------------|--------------|-----------------------------------|
/// | `P .. P+pad`   | Pre-Pad (optional) | `pad`        | Zero bytes to align payload start |
/// | `P+pad .. N`   | Payload            | `N-(P+pad)`  | Variable-length data              |
/// | `N .. N+8`     | Key Hash           | `8`          | 64-bit XXH3 key hash              |
/// | `N+8 .. N+16`  | Prev Offset        | `8`          | Absolute offset of previous tail  |
/// | `N+16 .. N+20` | Checksum           | `4`          | CRC32C of payload                 |
///
/// Where:
/// - `pad = (A - (prev_tail % A)) & (A - 1)`, `A = PAYLOAD_ALIGNMENT`.
/// - The next entry starts at `N + 20`.
///
/// Tombstone (deletion marker):
///
/// | Offset Range  | Field    | Size (Bytes) | Description            |
/// |---------------|----------|--------------|------------------------|
/// | `T .. T+1`    | Payload  | `1`          | Single byte `0x00`     |
/// | `T+1 .. T+21` | Metadata | `20`         | Key hash, prev, crc32c |
///
/// Notes:
/// - Using the previous tail in `Prev Offset` lets us insert pre-pad while
///   keeping chain traversal unambiguous.
/// - Readers compute `payload_start = prev_offset + prepad_len(prev_offset)`
///   and use the current metadata position as `payload_end`.
///
/// <img src="https://github.com/jzombie/rust-simd-r-drive/blob/main/assets/storage-layout.png" alt="Storage Layout" />
///
/// ## Notes
/// - The `prev_offset` forms a **backward-linked chain** for each key.
/// - The checksum is **not cryptographically secure** but serves as a quick integrity check.
/// - The first entry for a key has `prev_offset = 0`, indicating no previous version.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct EntryMetadata {
    pub key_hash: u64,     // 8 bytes (hashed key for lookup)
    pub prev_offset: u64,  // 8 bytes (absolute offset of previous entry)
    pub checksum: [u8; 4], // 4 bytes (checksum for integrity)
}

impl EntryMetadata {
    // TODO: Document
    pub fn new(key_hash: u64, prev_offset: u64, checksum: [u8; 4]) -> Self {
        Self {
            key_hash,
            prev_offset,
            checksum,
        }
    }

    /// Serializes the metadata into a byte array.
    ///
    /// Converts the `EntryMetadata` structure into a fixed-size array
    /// for efficient storage. The serialized format ensures compatibility
    /// with disk storage and memory-mapped access.
    ///
    /// # Format:
    /// - Encodes the key hash, previous offset, and checksum into their respective byte ranges.
    /// - Uses little-endian encoding for numeric values.
    ///
    /// # Returns:
    /// - A byte array containing the serialized metadata.
    #[inline]
    pub fn serialize(&self) -> [u8; METADATA_SIZE] {
        let mut buf = [0u8; METADATA_SIZE];

        buf[KEY_HASH_RANGE].copy_from_slice(&self.key_hash.to_le_bytes());
        buf[PREV_OFFSET_RANGE].copy_from_slice(&self.prev_offset.to_le_bytes());
        buf[CHECKSUM_RANGE].copy_from_slice(&self.checksum);

        buf
    }

    /// Deserializes a byte slice into an `EntryMetadata` instance.
    ///
    /// Reconstructs an `EntryMetadata` structure from a byte slice,
    /// following the predefined binary format. Extracts the key hash,
    /// previous offset, and checksum while ensuring correctness through
    /// explicit range-based indexing.
    ///
    /// # Parameters:
    /// - `data`: A byte slice containing the serialized metadata.
    ///
    /// # Returns:
    /// - A reconstructed `EntryMetadata` instance.
    ///
    /// # Panics:
    /// - If the provided `data` slice is too small.
    #[inline]
    pub fn deserialize(data: &[u8]) -> Self {
        Self {
            key_hash: u64::from_le_bytes(data[KEY_HASH_RANGE].try_into().unwrap()),
            prev_offset: u64::from_le_bytes(data[PREV_OFFSET_RANGE].try_into().unwrap()),
            // Use a `const`-safe way to construct a fixed-size array
            checksum: {
                let mut checksum = [0u8; CHECKSUM_LEN];
                checksum.copy_from_slice(&data[CHECKSUM_RANGE]);
                checksum
            },
        }
    }
}
