use crate::storage_engine::constants::*;
use crate::storage_engine::digest::Xxh3BuildHasher;
use crate::storage_engine::EntryMetadata;
use memmap2::Mmap;
use std::collections::{HashMap, HashSet};

pub struct KeyIndexer {
    index: HashMap<u64, u64, Xxh3BuildHasher>,
}

impl KeyIndexer {
    /// Builds an in-memory index for **fast key lookups**.
    ///
    /// This function **scans the storage file** and constructs a **hashmap**
    /// mapping each key's hash to its **latest** entry's file offset.
    ///
    /// # How It Works:
    /// - Iterates **backward** from the latest offset to find the most recent version of each key.
    /// - Skips duplicate keys to keep only the **most recent** entry.
    /// - Stores the **latest offset** of each unique key in the index.
    ///
    /// # Parameters:
    /// - `mmap`: A reference to the **memory-mapped file**.
    /// - `tail_offset`: The **final byte offset** in the file (starting point for scanning).
    ///
    /// # Returns:
    /// - A `HashMap<u64, u64>` mapping `key_hash` → `latest offset`.
    pub fn build(mmap: &Mmap, tail_offset: u64) -> Self {
        let mut index = HashMap::with_hasher(Xxh3BuildHasher);
        let mut seen_keys = HashSet::with_hasher(Xxh3BuildHasher);
        let mut cursor = tail_offset;

        while cursor >= METADATA_SIZE as u64 {
            let metadata_offset = cursor as usize - METADATA_SIZE;
            let metadata_bytes = &mmap[metadata_offset..metadata_offset + METADATA_SIZE];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            // If this key is already seen, skip it (to keep the latest entry only)
            if seen_keys.contains(&metadata.key_hash) {
                cursor = metadata.prev_offset;
                continue;
            }

            // Mark key as seen and store its latest offset
            seen_keys.insert(metadata.key_hash);
            index.insert(metadata.key_hash, metadata_offset as u64);

            // Stop when reaching the first valid entry
            if metadata.prev_offset == 0 {
                break;
            }

            cursor = metadata.prev_offset;
        }

        Self { index }
    }

    #[inline]
    pub fn insert(&mut self, key_hash: u64, new_offset: u64) -> Option<u64> {
        self.index.insert(key_hash, new_offset)
    }

    #[inline]
    pub fn get(&self, key_hash: &u64) -> Option<&u64> {
        self.index.get(key_hash)
    }
}
