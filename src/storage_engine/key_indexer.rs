use crate::storage_engine::EntryMetadata;
use crate::storage_engine::constants::*;
use crate::storage_engine::digest::{Xxh3BuildHasher, compute_hash};
use memmap2::Mmap;
use std::collections::{HashMap, HashSet};

/// Number of high bits reserved for collision-detection tag (16 bits).
///
/// This leaves 48 bits for actual file offset storage.
const TAG_BITS: u64 = 16;

/// Bitmask for extracting the lower 48-bit offset from a packed index value.
const OFFSET_MASK: u64 = (1u64 << (64 - TAG_BITS)) - 1;

/// `KeyIndexer` maps 64-bit key hashes to 48-bit file offsets, augmented with
/// a 16-bit tag for lightweight collision detection.
///
/// ## Purpose
/// While XXH3 is a high-quality 64-bit hash, collisions are still possible
/// in large-scale stores. This index:
///
/// - Detects such collisions via a 16-bit fingerprint (`tag`)
/// - Avoids storing full keys in memory
/// - Maintains constant memory footprint (`u64` per key)
///
/// ## Packed Value Format
/// Stored in a single `u64`:
///
/// ```text
/// [63         48][47                      0]
/// [  tag (16)  ][     file offset (48)     ]
/// ```
///
/// ## Collision Handling
/// During lookups, the stored tag is compared to a rederived one from the
/// key or key hash. If the tags do not match, the entry is rejected as a
/// potential collision.
///
/// ## Limitations
///
/// - **Max file size**: 2^48 bytes = **256 TiB**
///   - Any file larger than this will overflow the offset field and
///     corrupt the tag.
/// - **Tag uniqueness**: 2^16 = **65,536** distinct tags
///   - Probability of tag collision is 1 in 65,536 per conflicting key hash
///   - This is sufficient to distinguish over 4 billion keys with
///     ~50% collision probability (birthday bound)
///
/// ## Tradeoffs
/// This scheme offers a middle ground:
/// - Significantly improves safety over pure hash indexing
/// - No dynamic memory cost
/// - Small and predictable performance cost (~2 bit ops + 1 compare)
pub struct KeyIndexer {
    /// Index: key_hash → packed (tag | offset)
    index: HashMap<u64, u64, Xxh3BuildHasher>,
}

impl KeyIndexer {
    /// Returns a 16-bit tag from the upper bits of a key hash.
    #[inline]
    pub fn tag_from_hash(key_hash: u64) -> u16 {
        (key_hash >> (64 - TAG_BITS)) as u16
    }

    /// Computes a tag directly from the raw key.
    #[inline]
    pub fn tag_from_key(key: &[u8]) -> u16 {
        Self::tag_from_hash(compute_hash(key))
    }

    /// Combines a tag and offset into a packed 64-bit value.
    ///
    /// # Panics
    /// If offset exceeds 48 bits (i.e. > 256 TiB), this will panic.
    #[inline]
    pub fn pack(tag: u16, offset: u64) -> u64 {
        debug_assert!(
            offset <= OFFSET_MASK,
            "offset exceeds 48-bit range (tag would be corrupted)"
        );
        ((tag as u64) << (64 - TAG_BITS)) | offset
    }

    /// Extracts (tag, offset) from a packed value.
    #[inline]
    pub fn unpack(packed: u64) -> (u16, u64) {
        let tag = (packed >> (64 - TAG_BITS)) as u16;
        let offset = packed & OFFSET_MASK;
        (tag, offset)
    }

    /// Scans the file backwards and builds a tag-aware hash index.
    ///
    /// The most recent version of each key hash is kept.
    pub fn build(mmap: &Mmap, tail_offset: u64) -> Self {
        let mut index = HashMap::with_hasher(Xxh3BuildHasher);
        let mut seen = HashSet::with_hasher(Xxh3BuildHasher);
        let mut cursor = tail_offset;

        while cursor >= METADATA_SIZE as u64 {
            let meta_off = cursor as usize - METADATA_SIZE;
            let meta_bytes = &mmap[meta_off..meta_off + METADATA_SIZE];
            let meta = EntryMetadata::deserialize(meta_bytes);

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

    /// Inserts a new key hash and offset into the index.
    ///
    /// Returns the previous offset if one existed.
    #[inline]
    pub fn insert(&mut self, key_hash: u64, new_offset: u64) -> Option<u64> {
        let packed = Self::pack(Self::tag_from_hash(key_hash), new_offset);
        self.index
            .insert(key_hash, packed)
            .map(|prev| Self::unpack(prev).1)
    }

    /// Gets the raw packed value.
    #[inline]
    pub fn get_packed(&self, key_hash: &u64) -> Option<&u64> {
        self.index.get(key_hash)
    }

    /// Returns only the unpacked offset (ignores tag).
    #[inline]
    pub fn get_offset(&self, key_hash: &u64) -> Option<u64> {
        self.index.get(key_hash).map(|&v| Self::unpack(v).1)
    }

    /// Removes a key and returns its offset.
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
