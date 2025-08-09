use crate::storage_engine::EntryMetadata;
use std::io::Result;

pub trait DataStoreReader {
    type EntryHandleType;

    /// Checks whether a key currently exists in the store.
    ///
    /// This is a **constant‑time** lookup against the in‑memory
    /// [`crate::storage_engine::KeyIndexer`] map.  
    /// A key is considered to *exist* only if it is present **and not marked
    /// as deleted**.
    ///
    /// # Parameters
    /// - `key`: The **binary key** to check.
    ///
    /// # Returns
    /// - `Ok(true)`: Key exists and is active.  
    /// - `Ok(false)`: Key is absent or has been deleted.  
    /// - `Err(std::io::Error)`: On I/O failure.
    fn exists(&self, key: &[u8]) -> Result<bool>;

    /// Checks whether a key with a pre-computed hash exists in the store.
    ///
    /// This is a more direct version of [`Self::exists`] that skips the hashing step,
    /// making it faster if the hash is already known. Because the original key is not
    /// provided, this check does not perform tag verification and relies solely on the
    /// hash's presence in the index.
    ///
    /// # Parameters
    /// - `prehashed_key`: The **pre-computed hash** of the key to check.
    ///
    /// # Returns
    /// - `Ok(true)` if the key hash exists in the index.
    /// - `Ok(false)` if the key hash is absent.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn exists_with_key_hash(&self, prehashed_key: u64) -> Result<bool>;

    /// Retrieves the most recent value associated with a given key.
    ///
    /// This method **efficiently looks up a key** using a fast in-memory index,
    /// and returns the latest corresponding value if found.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** whose latest value is to be retrieved.
    ///
    /// # Returns:
    /// - `Ok(Some(EntryHandle))`: Handle to the entry if found.
    /// - `Ok(None)`: If the key does not exist or is deleted.
    /// - `Err(std::io::Error)`: On I/O failure.
    ///
    /// # Notes:
    /// - The returned `EntryHandle` provides zero-copy access to the stored data.
    fn read(&self, key: &[u8]) -> Result<Option<Self::EntryHandleType>>;

    /// Retrieves the most recent value associated with a pre-computed key hash.
    ///
    /// This is a low-level alternative to [`Self::read`] that looks up an entry using
    /// only its hash, bypassing the hashing step.
    ///
    /// # Warning
    /// This method does **not** perform tag verification, as the original key is not
    /// provided. This means that in the rare event of a hash collision, this function
    /// could return the entry for a different key.
    ///
    /// # Parameters
    /// - `prehashed_key`: The **pre-computed hash** of the key to retrieve.
    ///
    /// # Returns
    /// - `Ok(Some(EntryHandle))`: Handle to the entry if found.
    /// - `Ok(None)`: If the key hash does not exist or is deleted.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn read_with_key_hash(&self, prehashed_key: u64) -> Result<Option<Self::EntryHandleType>>;

    /// Retrieves the last entry written to the file.
    ///
    /// # Returns:
    /// - `Ok(Some(EntryHandle))`: Handle to the last entry, if any.
    /// - `Ok(None)`: If the file is empty.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn read_last_entry(&self) -> Result<Option<Self::EntryHandleType>>;

    /// Reads many keys in one shot.
    ///
    /// This is the **vectorized** counterpart to [`crate::DataStore::read`].  
    /// It takes a slice of raw-byte keys and returns a `Vec` whose *i-th* element
    /// is the result of looking up the *i-th* key.
    ///
    /// *   **Zero-copy** – each `Some(EntryHandle)` points directly into the
    ///     shared `Arc<Mmap>`; no payload is copied.
    /// *   **Constant-time per key** – the in-memory [`crate::storage_engine::KeyIndexer`] map is used
    ///     for each lookup, so the complexity is *O(n)* where *n* is
    ///     `keys.len()`.
    /// *   **Thread-safe** – a read lock on the index is taken once for the whole
    ///     batch, so concurrent writers are still blocked only for the same short
    ///     critical section that a single `read` would need.
    ///
    /// # Returns:
    /// - `Ok(results)`: `Vec<Option<EntryHandle>>` in key order.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn batch_read(&self, keys: &[&[u8]]) -> Result<Vec<Option<Self::EntryHandleType>>>;

    /// Reads many keys in one shot using pre-computed hashes.
    ///
    /// This is a lower-level, high-performance version of [`Self::batch_read`].
    /// It is designed for scenarios where the caller has already computed the key
    /// hashes and wants to avoid the overhead of re-hashing. The method offers
    /// an optional verification step to safeguard against hash collisions.
    ///
    /// * **Zero-copy**: Each `Some(EntryHandle)` provides a direct, zero-copy view
    ///   into the memory-mapped file.
    /// * **High-performance**: Bypasses the key hashing step if hashes are already
    ///   available.
    /// * **Thread-safe**: Acquires a single read lock for the entire batch
    ///   operation, minimizing contention.
    ///
    /// # Parameters
    /// - `prehashed_keys`: A slice of `u64` key hashes to look up.
    /// - `non_hashed_keys`: An optional slice of the original, non-hashed keys
    ///   corresponding to `prehashed_keys`.
    ///     - If `Some(keys)`, the method performs a tag-based verification to ensure
    ///       that the found entry truly belongs to the original key, preventing
    ///       data retrieval from a hash collision. The length of this slice
    ///       **must** match the length of `prehashed_keys`.
    ///     - If `None`, this verification is skipped. The lookup relies solely
    ///       on the hash, which is faster but carries a theoretical risk of
    ///       returning incorrect data in the event of a hash collision.
    ///
    /// # Returns
    /// - `Ok(results)`: A `Vec<Option<Self::EntryHandleType>>` where each element
    ///   corresponds to the result of looking up the key at the same index.
    /// - `Err(std::io::Error)`: On I/O failure or if the lengths of `prehashed_keys`
    ///   and `non_hashed_keys` (when `Some`) do not match.
    fn batch_read_hashed_keys(
        &self,
        prehashed_keys: &[u64],
        non_hashed_keys: Option<&[&[u8]]>,
    ) -> Result<Vec<Option<Self::EntryHandleType>>>;

    /// Retrieves metadata for a given key.
    ///
    /// This method looks up a key in the storage and returns its associated metadata.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** whose metadata is to be retrieved.
    ///
    /// # Returns:
    /// - `Ok(Some(metadata))`: Metadata if the key exists.
    /// - `Ok(None)`: If the key is absent.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn read_metadata(&self, key: &[u8]) -> Result<Option<EntryMetadata>>;

    /// Counts **active** (non-deleted) key-value pairs in the storage.
    ///
    /// # Returns:
    /// - `Ok(active_count)`: Total active entries.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn len(&self) -> Result<usize>;

    /// Determines if the store is empty or has no active keys.
    ///
    /// # Returns:
    /// - `Ok(bool)`: Whether or not the store has any active keys.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn is_empty(&self) -> Result<bool>;

    /// Returns the current file size on disk (including those of deleted entries).
    ///
    /// # Returns:
    /// - `Ok(bytes)`: File size in bytes.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn file_size(&self) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDataStoreReader {
    type EntryHandleType;

    /// Checks whether a key currently exists in the store.
    ///
    /// This is a **constant‑time** lookup against the in‑memory
    /// [`crate::storage_engine::KeyIndexer`] map.  
    /// A key is considered to *exist* only if it is present **and not marked
    /// as deleted**.
    ///
    /// # Parameters
    /// - `key`: The **binary key** to check.
    ///
    /// # Returns
    /// - `Ok(true)`: Key exists and is active.  
    /// - `Ok(false)`: Key is absent or has been deleted.  
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn exists(&self, key: &[u8]) -> Result<bool>;

    /// Checks whether a key with a pre-computed hash exists in the store.
    ///
    /// This is a more direct version of [`Self::exists`] that skips the hashing step,
    /// making it faster if the hash is already known. Because the original key is not
    /// provided, this check does not perform tag verification and relies solely on the
    /// hash's presence in the index.
    ///
    /// # Parameters
    /// - `prehashed_key`: The **pre-computed hash** of the key to check.
    ///
    /// # Returns
    /// - `Ok(true)` if the key hash exists in the index.
    /// - `Ok(false)` if the key hash is absent.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn exists_with_key_hash(&self, prehashed_key: u64) -> Result<bool>;

    /// Retrieves the most recent value associated with a given key.
    ///
    /// This method **efficiently looks up a key** using a fast in-memory index,
    /// and returns the latest corresponding value if found.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** whose latest value is to be retrieved.
    ///
    /// # Returns:
    /// - `Ok(Some(EntryHandle))`: Handle to the entry if found.
    /// - `Ok(None)`: If the key does not exist or is deleted.
    /// - `Err(std::io::Error)`: On I/O failure.
    ///
    /// # Notes:
    /// - The returned `EntryHandle` provides zero-copy access to the stored data.
    async fn read(&self, key: &[u8]) -> Result<Option<Self::EntryHandleType>>;

    /// Retrieves the most recent value associated with a pre-computed key hash.
    ///
    /// This is a low-level alternative to [`Self::read`] that looks up an entry using
    /// only its hash, bypassing the hashing step.
    ///
    /// # Warning
    /// This method does **not** perform tag verification, as the original key is not
    /// provided. This means that in the rare event of a hash collision, this function
    /// could return the entry for a different key.
    ///
    /// # Parameters
    /// - `prehashed_key`: The **pre-computed hash** of the key to retrieve.
    ///
    /// # Returns
    /// - `Ok(Some(EntryHandle))`: Handle to the entry if found.
    /// - `Ok(None)`: If the key hash does not exist or is deleted.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn read_with_key_hash(&self, prehashed_key: u64)
    -> Result<Option<Self::EntryHandleType>>;

    /// Retrieves the last entry written to the file.
    ///
    /// # Returns:
    /// - `Ok(Some(EntryHandle))`: Handle to the last entry, if any.
    /// - `Ok(None)`: If the file is empty.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn read_last_entry(&self) -> Result<Option<Self::EntryHandleType>>;

    /// Reads many keys in one shot.
    ///
    /// This is the **vectorized** counterpart to [`crate::DataStore::read`].  
    /// It takes a slice of raw-byte keys and returns a `Vec` whose *i-th* element
    /// is the result of looking up the *i-th* key.
    ///
    /// *   **Zero-copy** – each `Some(EntryHandle)` points directly into the
    ///     shared `Arc<Mmap>`; no payload is copied.
    /// *   **Constant-time per key** – the in-memory [`crate::storage_engine::KeyIndexer`] map is used
    ///     for each lookup, so the complexity is *O(n)* where *n* is
    ///     `keys.len()`.
    /// *   **Thread-safe** – a read lock on the index is taken once for the whole
    ///     batch, so concurrent writers are still blocked only for the same short
    ///     critical section that a single `read` would need.
    ///
    /// # Returns:
    /// - `Ok(results)`: `Vec<Option<EntryHandle>>` in key order.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn batch_read(&self, keys: &[&[u8]]) -> Result<Vec<Option<Self::EntryHandleType>>>;

    /// Reads many keys in one shot using pre-computed hashes.
    ///
    /// This is a lower-level, high-performance version of [`Self::batch_read`].
    /// It is designed for scenarios where the caller has already computed the key
    /// hashes and wants to avoid the overhead of re-hashing. The method offers
    /// an optional verification step to safeguard against hash collisions.
    ///
    /// * **Zero-copy**: Each `Some(EntryHandle)` provides a direct, zero-copy view
    ///     into the memory-mapped file.
    /// * **High-performance**: Bypasses the key hashing step if hashes are already
    ///     available.
    /// * **Thread-safe**: Acquires a single read lock for the entire batch
    ///     operation, minimizing contention.
    ///
    /// # Parameters
    /// - `prehashed_keys`: A slice of `u64` key hashes to look up.
    /// - `non_hashed_keys`: An optional slice of the original, non-hashed keys
    ///   corresponding to `prehashed_keys`.
    ///     - If `Some(keys)`, the method performs a tag-based verification to ensure
    ///       that the found entry truly belongs to the original key, preventing
    ///       data retrieval from a hash collision. The length of this slice
    ///       **must** match the length of `prehashed_keys`.
    ///     - If `None`, this verification is skipped. The lookup relies solely
    ///       on the hash, which is faster but carries a theoretical risk of
    ///       returning incorrect data in the event of a hash collision.
    ///
    /// # Returns
    /// - `Ok(results)`: A `Vec<Option<Self::EntryHandleType>>` where each element
    ///   corresponds to the result of looking up the key at the same index.
    /// - `Err(std::io::Error)`: On I/O failure or if the lengths of `prehashed_keys`
    ///   and `non_hashed_keys` (when `Some`) do not match.
    async fn batch_read_hashed_keys(
        &self,
        prehashed_keys: &[u64],
        non_hashed_keys: Option<&[&[u8]]>,
    ) -> Result<Vec<Option<Self::EntryHandleType>>>;

    /// Retrieves metadata for a given key.
    ///
    /// This method looks up a key in the storage and returns its associated metadata.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** whose metadata is to be retrieved.
    ///
    /// # Returns:
    /// - `Ok(Some(metadata))`: Metadata if the key exists.
    /// - `Ok(None)`: If the key is absent.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn read_metadata(&self, key: &[u8]) -> Result<Option<EntryMetadata>>;

    /// Counts **active** (non-deleted) key-value pairs in the storage.
    ///
    /// # Returns:
    /// - `Ok(active_count)`: Total active entries.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn len(&self) -> Result<usize>;

    /// Determines if the store is empty or has no active keys.
    ///
    /// # Returns:
    /// - `Ok(bool)`: Whether or not the store has any active keys.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn is_empty(&self) -> Result<bool>;

    /// Returns the current file size on disk (including those of deleted entries).
    ///
    /// # Returns:
    /// - `Ok(bytes)`: File size in bytes.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn file_size(&self) -> Result<u64>;
}
