use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 64-bit hash of the key (we always pre-hash with XXH3 to avoid
/// keeping the original key bytes in the buffer).
pub type KeyHash = u64;

/// Owned payload stored in the buffer.  
/// We keep a `Vec<u8>` instead of a `&[u8]` so the buffer owns its
/// memory and survives past the caller’s stack frame.
type Payload = Vec<u8>;

/// **WriteBuffer**  
///
/// A tiny, thread-safe staging area that sits in front of the on-disk
/// [`DataStore`].  Writers call `insert()` **instead of** hitting the
/// file layer immediately; the buffer coalesces duplicates (newest
/// value for the same key wins) and flushes en masse once a
/// *soft* size limit is reached.
///
/// It does **not** try to provide transaction semantics or ordering
/// guarantees beyond “last insert wins”.  Its only job is to:
///
/// 1.  Absorb bursts of writes without hammering the file-lock.
/// 2.  Turn many tiny random writes into one big sequential
///     `batch_write()` when we flush.
///
/// ### Fields
/// * `map`   A lock-free [`DashMap`] keyed by `KeyHash`.
/// * `bytes_in_mem`   Approximate size of all payloads currently
///   buffered (atomic counter – no lock contention).
/// * `soft_limit`   Threshold in bytes.  When
///   `bytes_in_mem >= soft_limit`, the next `insert()` returns `true`
///   to signal the caller that it should flush.
///
/// ### Concurrency
/// * Reads & writes to the buffer are lock-free (courtesy of
///   `DashMap`).  
/// * A flush is expected to happen under an *external* lock in
///   `DataStore::buf_write_flush()`, so clearing the map is safe.
pub struct WriteBuffer {
    /// Latest payload for every hashed key.
    map: DashMap<KeyHash, Payload>,

    /// Running total of payload bytes currently resident in `map`.
    /// `Relaxed` ordering is fine – it is only a heuristic.
    bytes_in_mem: AtomicUsize,

    /// Soft cap (bytes).  *Soft* means we exceed it by at most one
    /// record before the buffer is drained.
    soft_limit: usize,
}

impl WriteBuffer {
    /// Construct a new `WriteBuffer` wrapped in an `Arc` so the same
    /// instance can be shared between `DataStore` clones.
    ///
    /// * `soft_limit` – Flush threshold in **bytes** (not records).
    pub fn new(soft_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            map: DashMap::new(),
            bytes_in_mem: AtomicUsize::new(0),
            soft_limit,
        })
    }

    /// Buffer (or overwrite) a single payload.
    ///
    /// * `hash`   A *pre-computed* 64-bit key hash.  
    /// * `payload`   The bytes to store (this function takes ownership).
    ///
    /// Returns **`true`** if, *after* inserting this record, the buffer
    /// size is **greater than or equal to** `soft_limit` – i.e. the
    /// caller *should* flush soon.  A flush is *not* performed
    /// automatically because the buffer does not own the on-disk
    /// writer; the higher-level `DataStore` decides when to act.
    pub fn insert(&self, hash: KeyHash, payload: Vec<u8>) -> bool {
        let delta = payload.len();
        if let Some(old) = self.map.insert(hash, payload) {
            // overwrite: compensate the counter
            self.bytes_in_mem.fetch_sub(old.len(), Ordering::Relaxed);
        }
        self.bytes_in_mem.fetch_add(delta, Ordering::Relaxed);

        self.bytes_in_mem.load(Ordering::Relaxed) >= self.soft_limit
    }

    /// Drain the entire buffer, returning an owning `Vec` of
    /// `(hash, payload)` pairs **in arbitrary order** (DashMap shards
    /// do not preserve insertion order and that’s irrelevant for the
    /// eventual `batch_write()`).
    ///
    /// After draining, the buffer is empty and `bytes_in_mem` is reset
    /// to zero.
    ///
    /// Cost is `O(n)` for cloning the `Vec<u8>` values; unavoidable
    /// because the `DataStore` needs ownership to pass slices to
    /// `batch_write_hashed_payloads`.
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

    /// Cheap helper used mainly by tests.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
