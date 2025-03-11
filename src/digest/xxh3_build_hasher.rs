use std::hash::{BuildHasher, Hasher};
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
