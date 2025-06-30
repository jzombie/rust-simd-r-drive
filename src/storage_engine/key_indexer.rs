use crate::storage_engine::EntryMetadata;
use crate::storage_engine::constants::*;
use crate::storage_engine::digest::{Xxh3BuildHasher, compute_hash};
use memmap2::Mmap;
use std::collections::{HashMap, HashSet};

/// How many high bits we reserve for the collision‑detection tag.
const TAG_BITS: u64 = 16;
/// Low‑bit mask that keeps only the true file offset (≤ 2⁴⁸ bytes ≈ 256 TiB).
const OFFSET_MASK: u64 = (1u64 << (64 - TAG_BITS)) - 1;

/// In‑memory key → offset index with 16‑bit fingerprints to detect
/// hash collisions without storing the raw key.
pub struct KeyIndexer {
    /// `u64` value layout: `[16‑bit tag | 48‑bit offset]`
    index: HashMap<u64, u64, Xxh3BuildHasher>,
}

impl KeyIndexer {
    /// Returns the 16‑bit tag carried in the upper bits of `key_hash`.
    #[inline]
    pub fn tag_from_hash(key_hash: u64) -> u16 {
        (key_hash >> (64 - TAG_BITS)) as u16
    }

    /// Computes the tag directly from a raw key.
    #[inline]
    pub fn tag_from_key(key: &[u8]) -> u16 {
        Self::tag_from_hash(compute_hash(key))
    }

    /// Packs a 16‑bit tag together with a ≤ 48‑bit file offset into one `u64`.
    #[inline]
    pub fn pack(tag: u16, offset: u64) -> u64 {
        debug_assert!(offset <= OFFSET_MASK, "offset exceeds 48-bit limit");
        ((tag as u64) << (64 - TAG_BITS)) | offset
    }

    /// Unpacks a `u64` produced by `pack` back into `(tag, offset)`.
    #[inline]
    pub fn unpack(packed: u64) -> (u16, u64) {
        let tag = (packed >> (64 - TAG_BITS)) as u16;
        let offset = packed & OFFSET_MASK;
        (tag, offset)
    }

    /// Scans the file backwards and records only the latest entry per key.
    pub fn build(mmap: &Mmap, tail_offset: u64) -> Self {
        let mut index = HashMap::with_hasher(Xxh3BuildHasher);
        let mut seen = HashSet::with_hasher(Xxh3BuildHasher);
        let mut cursor = tail_offset;

        while cursor >= METADATA_SIZE as u64 {
            let meta_off = cursor as usize - METADATA_SIZE;
            let meta_bytes = &mmap[meta_off..meta_off + METADATA_SIZE];
            let meta = EntryMetadata::deserialize(meta_bytes);

            // Skip if we already recorded a newer version of this key.
            if seen.contains(&meta.key_hash) {
                cursor = meta.prev_offset;
                continue;
            }

            seen.insert(meta.key_hash);

            let tag = Self::tag_from_hash(meta.key_hash);
            index.insert(meta.key_hash, Self::pack(tag, meta_off as u64));

            if meta.prev_offset == 0 {
                break;
            }
            cursor = meta.prev_offset;
        }

        Self { index }
    }

    /// Inserts/updates and returns the **previous raw offset**, if any.
    #[inline]
    pub fn insert(&mut self, key_hash: u64, new_offset: u64) -> Option<u64> {
        let packed = Self::pack(Self::tag_from_hash(key_hash), new_offset);
        self.index
            .insert(key_hash, packed)
            .map(|prev| Self::unpack(prev).1)
    }

    /// Retrieves the packed value (`u64`) for advanced callers.
    #[inline]
    pub fn get_packed(&self, key_hash: &u64) -> Option<&u64> {
        self.index.get(key_hash)
    }

    /// Retrieves the **raw offset** (already unpacked).
    #[inline]
    pub fn get_offset(&self, key_hash: &u64) -> Option<u64> {
        self.index.get(key_hash).map(|&v| Self::unpack(v).1)
    }

    #[inline]
    pub fn remove(&mut self, key_hash: &u64) -> Option<u64> {
        self.index.remove(key_hash).map(|v| Self::unpack(v).1)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}
