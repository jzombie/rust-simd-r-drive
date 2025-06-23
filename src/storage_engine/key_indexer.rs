use crate::storage_engine::IndexLayer;
use crate::storage_engine::constants::*;
use crate::storage_engine::digest::Xxh3BuildHasher;
use crate::storage_engine::entry_metadata::EntryMetadata;
use crate::storage_engine::{IndexFooter, StaticHashIndex};
use log::info;
use memmap2::Mmap;
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct KeyIndexer<'a> {
    index: IndexLayer<'a>,
}

impl<'a> KeyIndexer<'a> {
    /// Builds an in-memory index for fast key lookups.
    ///
    /// This function first attempts to load a persistent, memory-mapped static index.
    /// If a valid static index is not found at the end of the file, it falls back
    /// to the legacy method of scanning the file to build a HashMap in memory.
    pub fn build(mmap: &'a Mmap, tail_offset: u64) -> Self {
        // Attempt to read the static index footer from the end of the mmap'd file.
        if let Some(footer) = IndexFooter::read_from(mmap) {
            // A static index exists. Check if it's up-to-date with the data segment.
            if footer.indexed_up_to == tail_offset {
                info!("Loaded persistent static index from file. Startup will be fast.");
                let static_index = StaticHashIndex::new(mmap, footer);
                return Self {
                    index: IndexLayer::new_static(static_index),
                };
            } else {
                info!(
                    "Static index is outdated (data has been added since last compaction). Falling back to legacy index build."
                );
            }
        } else {
            info!(
                "No static index found. Building index from scratch (this may be slow for large files)."
            );
        }

        // --- Fallback: Build the index using the legacy scan method ---
        let mut index_map = HashMap::with_hasher(Xxh3BuildHasher);
        let mut seen_keys = HashSet::with_hasher(Xxh3BuildHasher);
        let mut cursor = tail_offset;

        while cursor >= METADATA_SIZE as u64 {
            let metadata_offset = cursor as usize - METADATA_SIZE;
            let metadata_bytes = &mmap[metadata_offset..metadata_offset + METADATA_SIZE];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            // If this key has already been seen, skip it to keep only the latest entry
            if seen_keys.contains(&metadata.key_hash) {
                cursor = metadata.prev_offset;
                continue;
            }

            seen_keys.insert(metadata.key_hash);
            // The value is the offset of the *metadata*, not the payload.
            index_map.insert(metadata.key_hash, metadata_offset as u64);

            if metadata.prev_offset == 0 {
                break;
            }
            cursor = metadata.prev_offset;
        }

        Self {
            index: IndexLayer::new_legacy(index_map),
        }
    }

    #[inline]
    pub fn insert(&mut self, key_hash: u64, new_offset: u64) -> Option<u64> {
        self.index.insert(key_hash, new_offset)
    }

    #[inline]
    pub fn get(&self, key_hash: &u64) -> Option<u64> {
        self.index.get(key_hash)
    }
}
