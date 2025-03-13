use crate::storage_engine::digest::*;
use crate::storage_engine::*;
use memmap2::Mmap;
use std::ops::Range;
use std::sync::Arc;

// TODO: Rename to `read-handle`?

/// Zero-copy owner of a sub-slice in an `Arc<Mmap>`.
/// Lets you access the bytes of the entry as long as this struct is alive.
#[derive(Debug)]
pub struct EntryHandle {
    pub(in crate::storage_engine) mmap_arc: Arc<Mmap>,

    /// The payload range
    pub(in crate::storage_engine) range: Range<usize>,

    pub(in crate::storage_engine) metadata: EntryMetadata,
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
    pub fn as_slice(&self) -> &[u8] {
        // Returning a *cloned reference* to the memory-mapped data rather than
        // cloning the values. This is expected behavior for zero-copy access.
        &self.mmap_arc[self.range.clone()]
    }

    /// Returns a reference to the entry’s parsed metadata.
    pub fn metadata(&self) -> &EntryMetadata {
        &self.metadata
    }

    /// Returns the payload size.
    pub fn size(&self) -> usize {
        self.range.len()
    }

    /// Returns the payload size plus metadata.
    pub fn size_with_metadata(&self) -> usize {
        self.range.len() + METADATA_SIZE
    }

    pub fn key_hash(&self) -> u64 {
        self.metadata.key_hash
    }

    pub fn checksum(&self) -> u32 {
        u32::from_le_bytes(self.metadata.checksum)
    }

    pub fn raw_checksum(&self) -> [u8; 4] {
        self.metadata.checksum
    }

    // TODO: This needs slight reworking if the data came from a large stream
    pub fn is_valid_checksum(&self) -> bool {
        let data = self.as_slice();
        let computed = compute_checksum(data);
        self.metadata.checksum == computed
    }

    /// Returns the absolute start byte offset within the mapped file.
    pub fn start_offset(&self) -> usize {
        self.range.start
    }

    /// Returns the absolute end byte offset within the mapped file.
    pub fn end_offset(&self) -> usize {
        self.range.end
    }

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
}
