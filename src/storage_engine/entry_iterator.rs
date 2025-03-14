use crate::storage_engine::digest::*;
use crate::storage_engine::*;
use memmap2::Mmap;
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
/// - **Starts at `last_offset`** and moves backward using the `prev_offset` field.
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
    /// Initializes an iterator starting at the provided `last_offset` and
    /// moving backward through the storage file. The iterator ensures that
    /// only the most recent version of each key is returned.
    ///
    /// # Parameters:
    /// - `mmap`: A reference to the memory-mapped file.
    /// - `last_offset`: The file offset where iteration starts.
    ///
    /// # Returns:
    /// - A new `EntryIterator` instance.
    pub fn new(mmap: Arc<Mmap>, last_offset: u64) -> Self {
        Self {
            mmap,
            cursor: last_offset,
            seen_keys: HashSet::with_hasher(Xxh3BuildHasher),
        }
    }
}

impl Iterator for EntryIterator {
    type Item = EntryHandle;

    /// Advances the iterator to the next valid entry.
    ///
    /// Reads and parses the metadata for the current entry, determines its
    /// boundaries, and extracts its data. If the key has already been seen,
    /// the iterator skips it to ensure that only the latest version is returned.
    ///
    /// # Returns:
    /// - `Some(&[u8])` containing the entry data if valid.
    /// - `None` when no more valid entries are available.
    fn next(&mut self) -> Option<Self::Item> {
        // Stop iteration if cursor is out of valid range
        if self.cursor < METADATA_SIZE as u64 || self.mmap.len() == 0 {
            return None;
        }

        // Locate metadata at the current cursor position
        let metadata_offset = (self.cursor - METADATA_SIZE as u64) as usize;
        let metadata_bytes = &self.mmap[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        let entry_start = metadata.prev_offset as usize;
        let entry_end = metadata_offset;

        // Ensure valid entry bounds before reading
        if entry_start >= entry_end || entry_end > self.mmap.len() {
            return None;
        }

        // Move cursor backward to follow the chain
        self.cursor = metadata.prev_offset; // Move cursor backward

        // Skip duplicate keys (ensuring only the latest value is returned)
        if !self.seen_keys.insert(metadata.key_hash) {
            return self.next(); // Skip if already seen
        }

        let entry_data = &self.mmap[entry_start..entry_end];

        // Skip deleted entries (denoted by empty data)
        if entry_data == NULL_BYTE {
            return self.next();
        }

        Some(EntryHandle {
            mmap_arc: Arc::clone(&self.mmap),
            range: entry_start..entry_end,
            metadata,
        })
    }
}
