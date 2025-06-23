//! Thin wrapper: Static index + in-memory delta for recent writes.

use super::static_hash_index::StaticHashIndex;
use crate::storage_engine::digest::Xxh3BuildHasher;
use std::{collections::HashMap, sync::RwLock};

#[derive(Debug)]
pub enum IndexLayer<'a> {
    Static {
        table: StaticHashIndex<'a>,
        delta: RwLock<HashMap<u64, u64, Xxh3BuildHasher>>,
    },
    Legacy(RwLock<HashMap<u64, u64, Xxh3BuildHasher>>),
}

impl<'a> IndexLayer<'a> {
    /// Creates a new IndexLayer with a static table and an empty delta map for new writes.
    pub fn new_static(table: StaticHashIndex<'a>) -> Self {
        Self::Static {
            table,
            delta: RwLock::new(HashMap::with_hasher(Xxh3BuildHasher)),
        }
    }

    /// Creates a new IndexLayer that wraps a legacy HashMap.
    pub fn new_legacy(map: HashMap<u64, u64, Xxh3BuildHasher>) -> Self {
        Self::Legacy(RwLock::new(map))
    }

    #[inline]
    pub fn get(&self, h: &u64) -> Option<u64> {
        match self {
            Self::Static { table, delta } => {
                // Check the delta (recent writes) first. If not found, check the static table.
                delta
                    .read()
                    .ok()
                    .and_then(|d| d.get(h).copied())
                    .or_else(|| table.get(*h))
            }
            Self::Legacy(map) => map.read().ok().and_then(|d| d.get(h).copied()),
        }
    }

    #[inline]
    pub fn insert(&self, h: u64, off: u64) -> Option<u64> {
        match self {
            Self::Static { delta, .. } => delta.write().ok().and_then(|mut d| d.insert(h, off)),
            Self::Legacy(map) => map.write().ok().and_then(|mut d| d.insert(h, off)),
        }
    }
}
