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
