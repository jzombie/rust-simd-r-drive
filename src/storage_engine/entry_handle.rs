use crate::storage_engine::*;
use memmap2::Mmap;
use std::ops::Range;
use std::sync::Arc;

/// Zero-copy owner of a sub-slice in an `Arc<Mmap>`.
/// Provides access to the bytes of an entry as long as this struct is alive.
#[derive(Debug)]
pub struct EntryHandle {
    /// The underlying memory map.
    pub(in crate::storage_engine) mmap_arc: Arc<Mmap>,

    /// The range of bytes within the memory-mapped file corresponding to the payload.
    pub(in crate::storage_engine) range: Range<usize>,

    /// Metadata associated with the entry, including key hash and checksum.
    pub(in crate::storage_engine) metadata: EntryMetadata,
}

impl EntryHandle {
    /// Provides access to the raw pointer of the memory-mapped file for testing.
    ///
    /// This method allows unit tests to verify that multiple `EntryHandle` instances
    /// share the same underlying memory map, ensuring zero-copy behavior.
    ///
    /// # Returns
    /// - A raw pointer to the underlying `Mmap`.
    #[cfg(test)]
    pub fn arc_ptr(&self) -> *const Mmap {
        Arc::as_ptr(&self.mmap_arc)
    }
}

/// Enable `*entry_handle` to act like a `&[u8]`
impl std::ops::Deref for EntryHandle {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

/// Let us do: `assert_eq!(entry_handle, b"some bytes")`
impl PartialEq<[u8]> for EntryHandle {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

/// Allow comparisons with `&[u8]`
impl PartialEq<&[u8]> for EntryHandle {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

/// Allow comparisons with `Vec<u8>`
impl PartialEq<Vec<u8>> for EntryHandle {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl EntryHandle {
    /// Returns a zero-copy reference to the sub-slice of bytes corresponding to the entry.
    ///
    /// This method ensures **no additional allocations** occur by referencing the memory-mapped
    /// region instead of copying data.
    ///
    /// # Returns
    /// - A byte slice (`&[u8]`) referencing the original data.
    ///
    /// # Zero-Copy Guarantee
    /// - The returned slice directly references the **underlying memory-mapped file**.
    pub fn as_slice(&self) -> &[u8] {
        // Returning a *cloned reference* to the memory-mapped data rather than
        // cloning the values. This is expected behavior for zero-copy access.
        &self.mmap_arc[self.range.clone()]
    }

    /// Creates a new `EntryHandle` with the same memory-mapped reference.
    ///
    /// This method provides a way to duplicate an `EntryHandle` **without cloning the underlying data**.
    /// Instead, it increments the reference count on the `Arc<Mmap>`, ensuring that the same memory-mapped
    /// file remains accessible across multiple handles.
    ///
    /// # Usage
    ///
    /// - This is useful when multiple parts of the system need to access the same entry
    ///   without creating redundant copies.
    /// - Unlike `Clone`, which is not implemented for `EntryHandle`, this method allows controlled
    ///   duplication without unnecessary allocations.
    ///
    /// # Returns
    /// - A new `EntryHandle` referencing the same underlying data and metadata.
    ///
    /// # Zero-Copy Guarantee
    /// - Both the original and cloned handle will refer to the same memory-mapped region.
    /// - The `Arc<Mmap>` ensures the mapped file stays valid as long as any handle is in scope.
    ///
    /// # Safety Considerations
    /// - Do **not** use this method if you need to modify data, as all handles share the same immutable mapping.
    pub fn clone_arc(&self) -> Self {
        Self {
            mmap_arc: Arc::clone(&self.mmap_arc), // Keeps same mmap reference
            range: self.range.clone(),
            metadata: self.metadata.clone(),
        }
    }

    /// Returns a reference to the entry’s parsed metadata.
    ///
    /// This metadata includes:
    /// - `key_hash`: The hash of the key.
    /// - `prev_offset`: The offset of the previous entry.
    /// - `checksum`: A checksum for verifying data integrity.
    ///
    /// # Returns
    /// - A reference to the `EntryMetadata` struct.
    pub fn metadata(&self) -> &EntryMetadata {
        &self.metadata
    }

    /// Returns the payload size of the entry.
    ///
    /// # Returns
    /// - The size of the payload in bytes.
    pub fn size(&self) -> usize {
        self.range.len()
    }

    /// Returns the total size of the entry, including metadata.
    ///
    /// # Returns
    /// - The size of the payload plus metadata in bytes.
    pub fn size_with_metadata(&self) -> usize {
        self.range.len() + METADATA_SIZE
    }

    /// Returns the computed hash of the entry's key.
    ///
    /// This value is derived from `compute_hash()` and is used for fast lookups.
    ///
    /// # Returns
    /// - A 64-bit unsigned integer representing the key hash.
    pub fn key_hash(&self) -> u64 {
        self.metadata.key_hash
    }

    /// Returns the checksum of the entry's payload.
    ///
    /// The checksum is a 32-bit value used for data integrity verification.
    ///
    /// # Returns
    /// - A 32-bit unsigned integer representing the checksum.
    pub fn checksum(&self) -> u32 {
        u32::from_le_bytes(self.metadata.checksum)
    }

    /// Returns the raw checksum bytes of the entry.
    ///
    /// This method provides direct access to the checksum bytes for additional processing.
    ///
    /// # Returns
    /// - A `[u8; 4]` array containing the raw checksum.
    pub fn raw_checksum(&self) -> [u8; 4] {
        self.metadata.checksum
    }

    /// Validates the integrity of the entry using its stored checksum.
    ///
    /// This method computes the checksum of the payload **in chunks** (streaming)
    /// to match how it was originally computed during writes. This ensures that
    /// large entries and small entries are handled consistently.
    ///
    /// # Returns
    /// - `true` if the computed checksum matches the stored value.
    /// - `false` if the data has been corrupted.
    pub fn is_valid_checksum(&self) -> bool {
        let mut hasher = crc32fast::Hasher::new();
        let chunk_size = 4096; // Process in 4KB chunks
        let data = self.as_slice();

        // Compute checksum in a streaming manner
        let mut offset = 0;
        while offset < data.len() {
            let end = std::cmp::min(offset + chunk_size, data.len());
            hasher.update(&data[offset..end]);
            offset = end;
        }

        let computed = hasher.finalize().to_le_bytes();
        self.metadata.checksum == computed
    }

    /// Returns the absolute start byte offset within the mapped file.
    ///
    /// This offset represents where the payload begins in the memory-mapped storage.
    ///
    /// # Returns
    /// - A `usize` representing the start offset.
    pub fn start_offset(&self) -> usize {
        self.range.start
    }

    /// Returns the absolute end byte offset within the mapped file.
    ///
    /// This offset represents where the payload ends in the memory-mapped storage.
    ///
    /// # Returns
    /// - A `usize` representing the end offset.
    pub fn end_offset(&self) -> usize {
        self.range.end
    }

    /// Returns the byte offset range for the entry within the mapped file.
    ///
    /// This provides a structured way to access the start and end offsets.
    ///
    /// # Returns
    /// - A `Range<usize>` representing the byte range of the entry.
    pub fn offset_range(&self) -> Range<usize> {
        self.range.clone()
    }

    /// Returns the pointer range in the current process's memory.
    ///
    /// This is the actual *virtual address* space that the entry occupies.
    /// - The `start_ptr` points to the beginning of the payload in memory.
    /// - The `end_ptr` is `start_ptr + payload_length`.
    ///
    /// **Note**: These addresses are valid only in this process and can become
    /// invalid if the memory map is remapped or unmapped.
    pub fn address_range(&self) -> std::ops::Range<*const u8> {
        let slice = self.as_slice();
        let start_ptr = slice.as_ptr();
        let end_ptr = unsafe { start_ptr.add(slice.len()) };
        start_ptr..end_ptr
    }

    /// Returns a reference to the shared memory-mapped file.
    ///
    /// This exposes the underlying `Arc<Mmap>` used to back the entry's data.
    ///
    /// # Returns
    /// - A reference to the `Arc<Mmap>` instance holding the memory-mapped file.
    ///
    /// # Use Cases
    /// - Verifying that two `EntryHandle`s share the same `Mmap` backing.
    /// - Providing foreign-language bindings (e.g., Python) access to shared memory.
    /// - Internal testing or diagnostics (e.g., checking refcounts).
    ///
    /// # Safety Considerations
    /// - Do **not** attempt to unmap, remap, or modify the memory manually.
    /// - The returned mapping is shared and valid only as long as an `Arc` exists.
    ///
    /// # Feature Flag
    /// This method is gated behind the `expose-internal-api` Cargo feature:
    ///
    /// ```toml
    /// [features]
    /// expose-internal-api = []
    /// ```
    ///
    /// It is **not part of the stable public API** and may be changed or removed
    /// in future versions. It is intended for internal or FFI-bound use only.
    #[cfg(feature = "expose-internal-api")]
    pub fn mmap_arc(&self) -> &Arc<Mmap> {
        &self.mmap_arc
    }
}
