use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use xxhash_rust::xxh3::xxh3_64;

/// Custom Hasher using XXH3
#[derive(Default)]
pub struct Xxh3Hasher {
    hash: u64,
}

impl Hasher for Xxh3Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.hash = xxh3_64(bytes);
    }

    fn finish(&self) -> u64 {
        self.hash
    }
}

/// Custom BuildHasher for `HashMap`/`HashSet`
#[derive(Default, Clone)]
pub struct Xxh3BuildHasher;

impl BuildHasher for Xxh3BuildHasher {
    type Hasher = Xxh3Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        Xxh3Hasher::default()
    }
}

/// Zero-cost passthrough hasher for pre-hashed `u64` keys.
///
/// Used in `KeyIndexer` where keys are already XXH3 hashes — running
/// them through another hasher is redundant.
#[derive(Default)]
pub struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("KeyIndexer only hashes u64 keys");
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

/// `BuildHasher` adapter for `IdentityHasher`.
pub type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;
