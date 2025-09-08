use crate::storage_engine::constants::*;
use crate::storage_engine::digest::*;
use memmap2::Mmap;
use simd_r_drive_entry_handle::{EntryHandle, EntryMetadata};
use std::collections::HashSet;
use std::sync::Arc;

/// Iterator for traversing entries in the append-only storage.
///
/// This iterator scans entries stored in the memory-mapped file (`mmap`),
/// reading each entry's metadata and returning unique key-value pairs.
/// The iteration proceeds **backward**, following the chain of previous
/// offsets stored in each entry.
///
/// ## Behavior:
/// - **Starts at `tail_offset`** and moves backward using the
///   `prev_offset` field.
/// - **Ensures unique keys** by tracking seen hashes in a `HashSet`.
/// - **Skips deleted entries**, which are represented by empty data.
/// - **Stops when reaching an invalid or out-of-bounds offset.**
pub struct EntryIterator {
    mmap: Arc<Mmap>, // Borrow from Arc<Mmap> (zero-copy)
    cursor: u64,
    seen_keys: HashSet<u64, Xxh3BuildHasher>,
}

impl EntryIterator {
    /// Creates a new iterator for scanning storage entries.
    ///
    /// Initializes an iterator starting at the provided `tail_offset`
    /// and moving backward through the storage file. The iterator
    /// ensures that only the most recent version of each key is
    /// returned.
    ///
    /// # Parameters:
    /// - `mmap`: A reference to the memory-mapped file.
    /// - `tail_offset`: The file offset where iteration starts.
    ///
    /// # Returns:
    /// - A new `EntryIterator` instance.
    pub fn new(mmap: Arc<Mmap>, tail_offset: u64) -> Self {
        Self {
            mmap,
            cursor: tail_offset,
            seen_keys: HashSet::with_hasher(Xxh3BuildHasher),
        }
    }

    #[inline]
    fn prepad_len(offset: u64) -> usize {
        let a = PAYLOAD_ALIGNMENT;
        ((a - (offset % a)) & (a - 1)) as usize
    }
}

impl Iterator for EntryIterator {
    type Item = EntryHandle;

    /// Advances the iterator to the next valid entry.
    ///
    /// Reads and parses the metadata for the current entry, determines
    /// its boundaries, and extracts its data. If the key has already
    /// been seen, the iterator skips it to ensure that only the latest
    /// version is returned.
    ///
    /// # Returns:
    /// - `Some(&[u8])` containing the entry data if valid.
    /// - `None` when no more valid entries are available.
    fn next(&mut self) -> Option<Self::Item> {
        // Stop iteration if cursor is out of valid range
        if self.cursor < METADATA_SIZE as u64 || self.mmap.is_empty() {
            return None;
        }

        // Locate metadata at the current cursor position
        let metadata_offset = (self.cursor - METADATA_SIZE as u64) as usize;
        if metadata_offset + METADATA_SIZE > self.mmap.len() {
            return None;
        }
        let metadata_bytes = &self.mmap[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        // Stored `prev_offset` is the **previous tail**. Derive the
        // aligned payload start for regular values. For tombstones
        // (single NULL byte), also support the no-prepad case.
        let prev_tail = metadata.prev_offset as u64;
        let derived = prev_tail + Self::prepad_len(prev_tail) as u64;

        let entry_end = metadata_offset;
        let mut entry_start = derived as usize;

        // Tombstone (legacy, no-prepad).
        if entry_end > prev_tail as usize
            && entry_end - prev_tail as usize == 1
            && self.mmap[prev_tail as usize..entry_end] == NULL_BYTE
        {
            entry_start = prev_tail as usize;
        }

        // Ensure valid entry bounds before reading
        if entry_start >= entry_end || entry_end > self.mmap.len() {
            return None;
        }

        // Move cursor backward to follow the chain (by tails)
        self.cursor = metadata.prev_offset;

        // Skip duplicate keys (ensuring only the latest value is
        // returned)
        if !self.seen_keys.insert(metadata.key_hash) {
            return self.next(); // Skip if already seen
        }

        let entry_data = &self.mmap[entry_start..entry_end];

        // Skip deleted entries (denoted by single null byte)
        if entry_end - entry_start == 1 && entry_data == NULL_BYTE {
            return self.next();
        }

        Some(EntryHandle {
            mmap_arc: Arc::clone(&self.mmap),
            range: entry_start..entry_end,
            metadata,
        })
    }
}
