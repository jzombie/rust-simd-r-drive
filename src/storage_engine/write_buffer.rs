use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub type KeyHash = u64;
type Payload = Vec<u8>;

/// Simple, thread-safe write buffer (append-only semantics: newest wins).
pub struct WriteBuffer {
    map: DashMap<KeyHash, Payload>,
    bytes_in_mem: AtomicUsize,
    soft_limit: usize,
}

impl WriteBuffer {
    pub fn new(soft_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            map: DashMap::new(),
            bytes_in_mem: AtomicUsize::new(0),
            soft_limit,
        })
    }

    /// Insert/overwrite a payload; returns `true` if we crossed the
    /// configured soft limit and should flush.
    pub fn insert(&self, hash: KeyHash, payload: Vec<u8>) -> bool {
        let delta = payload.len();
        if let Some(old) = self.map.insert(hash, payload) {
            // overwrite: compensate the counter
            self.bytes_in_mem.fetch_sub(old.len(), Ordering::Relaxed);
        }
        self.bytes_in_mem.fetch_add(delta, Ordering::Relaxed);

        self.bytes_in_mem.load(Ordering::Relaxed) >= self.soft_limit
    }

    /// Move all buffered records out and reset the buffer.
    pub fn drain(&self) -> Vec<(KeyHash, Payload)> {
        let out: Vec<_> = self
            .map
            .iter()
            .map(|kv| (*kv.key(), kv.value().clone()))
            .collect();

        self.map.clear();
        self.bytes_in_mem.store(0, Ordering::Relaxed);
        out
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
