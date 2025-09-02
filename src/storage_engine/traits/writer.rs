use crate::storage_engine::DataStore;
use std::io::{Read, Result};

pub trait DataStoreWriter {
    /// Writes an entry using a streaming `Read` source (e.g., file, network).
    ///
    /// This method is designed for **large entries** that may exceed available RAM,
    /// allowing them to be written in chunks without loading the full payload into memory.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** for the entry.
    /// - `reader`: A **streaming reader** (`Read` trait) supplying the entry's content.
    ///
    /// # Returns:
    /// - `Ok(offset)`: The file offset where the entry was written.
    /// - `Err(std::io::Error)`: If a write or I/O operation fails.
    ///
    /// # Notes:
    /// - Internally, this method delegates to `write_stream_with_key_hash`, computing
    ///   the key hash first.
    /// - If the entry is **small enough to fit in memory**, consider using `write()`
    ///   or `batch_write()` instead if you don't want to stream the data in.
    ///
    /// # Streaming Behavior:
    /// - The `reader` is **read incrementally in 64KB chunks** (`WRITE_STREAM_BUFFER_SIZE`).
    /// - Data is immediately **written to disk** as it is read.
    /// - **A checksum is computed incrementally** during the write.
    /// - Metadata is appended **after** the full entry is written.
    fn write_stream<R: Read>(&self, key: &[u8], reader: &mut R) -> Result<u64>;

    /// Writes an entry using a **precomputed key hash** and a streaming `Read` source.
    ///
    /// This is a **low-level** method that operates like `write_stream`, but requires
    /// the key to be hashed beforehand. It is primarily used internally to avoid
    /// redundant hash computations when writing multiple entries.
    ///
    /// # Parameters:
    /// - `key_hash`: The **precomputed hash** of the key.
    /// - `reader`: A **streaming reader** (`Read` trait) supplying the entry's content.
    ///
    /// # Returns:
    /// - `Ok(offset)`: The file offset where the entry was written.
    /// - `Err(std::io::Error)`: If a write or I/O operation fails.
    fn write_stream_with_key_hash<R: Read>(&self, key_hash: u64, reader: &mut R) -> Result<u64>;

    /// Writes an entry with a given key and payload.
    ///
    /// This method computes the hash of the key and delegates to `write_with_key_hash()`.
    /// It is a **high-level API** for adding new entries to the storage.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** associated with the entry.
    /// - `payload`: The **data payload** to be stored.
    ///
    /// # Returns:
    /// - `Ok(offset)`: The file offset where the entry was written.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - If you need streaming support, use `write_stream` instead.
    /// - If multiple entries with the **same key** are written, the most recent
    ///   entry will be retrieved when reading.
    /// - This method **locks the file for writing** to ensure consistency.
    /// - For writing **multiple entries at once**, use `batch_write()`.
    fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64>;

    /// Writes an entry using a **precomputed key hash** and a payload.
    ///
    /// This method is a **low-level** alternative to `write()`, allowing direct
    /// specification of the key hash. It is mainly used for optimized workflows
    /// where the key hash is already known, avoiding redundant computations.
    ///
    /// # Parameters:
    /// - `key_hash`: The **precomputed hash** of the key.
    /// - `payload`: The **data payload** to be stored.
    ///
    /// # Returns:
    /// - `Ok(offset)`: The file offset where the entry was written.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - The caller is responsible for ensuring that `key_hash` is correctly computed.
    /// - This method **locks the file for writing** to maintain consistency.
    /// - If writing **multiple entries**, consider using `batch_write_with_key_hashes()`.
    fn write_with_key_hash(&self, key_hash: u64, payload: &[u8]) -> Result<u64>;

    /// Writes multiple key-value pairs as a **single transaction**.
    ///
    /// This method computes the hashes of the provided keys and delegates to
    /// `batch_write_with_key_hashes()`, ensuring all writes occur in a single
    /// locked operation for efficiency.
    ///
    /// # Parameters:
    /// - `entries`: A **slice of key-value pairs**, where:
    ///   - `key`: The **binary key** for the entry.
    ///   - `payload`: The **data payload** to be stored.
    ///
    /// # Returns:
    /// - `Ok(final_offset)`: The file offset after all writes.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - This method improves efficiency by **minimizing file lock contention**.
    /// - If a large number of entries are written, **batching reduces overhead**.
    /// - If the key hashes are already computed, use `batch_write_with_key_hashes()`.
    fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64>;

    /// Writes multiple key-value pairs as a **single transaction**, using precomputed key hashes.
    ///
    /// This method efficiently appends multiple entries in a **batch operation**,
    /// reducing lock contention and improving performance for bulk writes.
    ///
    /// # Parameters:
    /// - `prehashed_keys`: A **vector of precomputed key hashes and payloads**, where:
    ///   - `key_hash`: The **precomputed hash** of the key.
    ///   - `payload`: The **data payload** to be stored.
    ///
    /// # Returns:
    /// - `Ok(final_offset)`: The file offset after all writes.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - **File locking is performed only once** for all writes, improving efficiency.
    /// - If an entry's `payload` is empty, an error is returned.
    /// - This method uses **SIMD-accelerated memory copy (`simd_copy`)** to optimize write
    ///   performance.
    /// - **Metadata (checksums, offsets) is written after payloads** to ensure data integrity.
    /// - After writing, the memory-mapped file (`mmap`) is **remapped** to reflect updates.
    ///
    /// # Efficiency Considerations:
    /// - **Faster than multiple `write()` calls**, since it reduces lock contention.
    /// - Suitable for **bulk insertions** where key hashes are known beforehand.
    /// - If keys are available but not hashed, use `batch_write()` instead.
    fn batch_write_with_key_hashes(
        &self,
        prehashed_keys: Vec<(u64, &[u8])>,
        allow_null_bytes: bool,
    ) -> Result<u64>;

    /// Renames an existing entry by copying it under a new key and marking the old key as deleted.
    ///
    /// This function:
    /// - Reads the existing entry associated with `old_key`.
    /// - Writes the same data under `new_key`.
    /// - Deletes the `old_key` by appending a tombstone entry.
    ///
    /// # Parameters:
    /// - `old_key`: The **original key** of the entry to be renamed.
    /// - `new_key`: The **new key** under which the entry will be stored.
    ///
    /// # Returns:
    /// - `Ok(new_offset)`: The file offset where the new entry was written.
    /// - `Err(std::io::Error)`: If the old key is not found or if a write operation fails.
    ///
    /// # Notes:
    /// - This operation **does not modify** the original entry but instead appends a new copy.
    /// - The old key is **logically deleted** via an append-only tombstone.
    /// - Attempting to rename a key to itself will return an error.
    fn rename(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64>;

    /// Copies an entry to a **different storage container**.
    ///
    /// This function:
    /// - Reads the entry associated with `key` in the current storage.
    /// - Writes it to the `target` storage.
    ///
    /// # Parameters:
    /// - `key`: The **key** of the entry to be copied.
    /// - `target`: The **destination storage** where the entry should be copied.
    ///
    /// # Returns:
    /// - `Ok(target_offset)`: The file offset where the copied entry was written in the target storage.
    /// - `Err(std::io::Error)`: If the key is not found, if the write operation fails,  
    ///   or if attempting to copy to the same storage.
    ///
    /// # Notes:
    /// - Copying within the **same** storage is unnecessary; use `rename` instead.
    /// - This operation does **not** delete the original entry.
    fn copy(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    /// Moves an entry from the current storage to a **different storage container**.
    ///
    /// This function:
    /// - Copies the entry from the current storage to `target`.
    /// - Marks the original entry as deleted.
    ///
    /// # Parameters:
    /// - `key`: The **key** of the entry to be moved.
    /// - `target`: The **destination storage** where the entry should be moved.
    ///
    /// # Returns:
    /// - `Ok(target_offset)`: The file offset where the entry was written in the target storage.
    /// - `Err(std::io::Error)`: If the key is not found, or if the copy/delete operation fails.
    ///
    /// # Notes:
    /// - Moving an entry within the **same** storage is unnecessary; use `rename` instead.
    /// - The original entry is **logically deleted** by appending a tombstone, maintaining
    ///   the append-only structure.
    fn transfer(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    /// Logically deletes an entry by its key.
    ///
    /// The storage engine is **append-only**, so entries are not removed directly.
    /// Instead, this method appends a **tombstone marker** to logically delete the key.
    ///
    /// This operation first **verifies that the key exists** before appending a tombstone.
    /// If the key is not found, no data is written to the file, and the operation
    /// succeeds without changing the store's state.
    ///
    /// # Parameters
    /// - `key`: The **binary key** to mark as deleted.
    ///
    /// # Returns
    /// - `Ok(tail_offset)`: The file's tail offset after the operation completes.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn delete(&self, key: &[u8]) -> Result<u64>;

    /// Deletes a batch of entries from the storage by their keys.
    ///
    /// This method computes the hash for each key and then calls the underlying
    /// `batch_delete_key_hashes` method. It will only write deletion markers
    /// (tombstones) for keys that currently exist in the store.
    ///
    /// # Parameters
    /// - `keys`: A slice of keys to be deleted.
    ///
    /// # Returns
    /// - `Ok(tail_offset)`: The new tail offset of the file after the operation.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn batch_delete(&self, keys: &[&[u8]]) -> Result<u64>;

    /// Deletes a batch of entries from the storage using pre-computed key hashes.
    ///
    /// This is the lowest-level batch deletion method. It checks for the existence
    /// of each key hash in the in-memory index before writing a deletion marker.
    /// This prevents the store from being filled with unnecessary tombstones for
    /// keys that were never present.
    ///
    /// # Parameters
    /// - `prehashed_keys`: A slice of `u64` key hashes to be deleted.
    ///
    /// # Returns
    /// - `Ok(tail_offset)`: The new tail offset of the file after the operation.
    /// - `Err(std::io::Error)`: On I/O failure.
    fn batch_delete_key_hashes(&self, prehashed_keys: &[u64]) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDataStoreWriter {
    /// Writes an entry using a streaming `Read` source (e.g., file, network).
    ///
    /// This method is designed for **large entries** that may exceed available RAM,
    /// allowing them to be written in chunks without loading the full payload into memory.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** for the entry.
    /// - `reader`: A **streaming reader** (`Read` trait) supplying the entry's content.
    ///
    /// # Returns:
    /// - `Ok(offset)`: The file offset where the entry was written.
    /// - `Err(std::io::Error)`: If a write or I/O operation fails.
    ///
    /// # Notes:
    /// - Internally, this method delegates to `write_stream_with_key_hash`, computing
    ///   the key hash first.
    /// - If the entry is **small enough to fit in memory**, consider using `write()`
    ///   or `batch_write()` instead if you don't want to stream the data in.
    ///
    /// # Streaming Behavior:
    /// - The `reader` is **read incrementally in 64KB chunks** (`WRITE_STREAM_BUFFER_SIZE`).
    /// - Data is immediately **written to disk** as it is read.
    /// - **A checksum is computed incrementally** during the write.
    /// - Metadata is appended **after** the full entry is written.
    async fn write_stream<R: Read>(&self, key: &[u8], reader: &mut R) -> Result<u64>;

    /// Writes an entry using a **precomputed key hash** and a streaming `Read` source.
    ///
    /// This is a **low-level** method that operates like `write_stream`, but requires
    /// the key to be hashed beforehand. It is primarily used internally to avoid
    /// redundant hash computations when writing multiple entries.
    ///
    /// # Parameters:
    /// - `key_hash`: The **precomputed hash** of the key.
    /// - `reader`: A **streaming reader** (`Read` trait) supplying the entry's content.
    ///
    /// # Returns:
    /// - `Ok(offset)`: The file offset where the entry was written.
    /// - `Err(std::io::Error)`: If a write or I/O operation fails.
    async fn write_stream_with_key_hash<R: Read>(
        &self,
        key_hash: u64,
        reader: &mut R,
    ) -> Result<u64>;

    /// Writes an entry with a given key and payload.
    ///
    /// This method computes the hash of the key and delegates to `write_with_key_hash()`.
    /// It is a **high-level API** for adding new entries to the storage.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** associated with the entry.
    /// - `payload`: The **data payload** to be stored.
    ///
    /// # Returns:
    /// - `Ok(offset)`: The file offset where the entry was written.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - If you need streaming support, use `write_stream` instead.
    /// - If multiple entries with the **same key** are written, the most recent
    ///   entry will be retrieved when reading.
    /// - This method **locks the file for writing** to ensure consistency.
    /// - For writing **multiple entries at once**, use `batch_write()`.
    async fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64>;

    /// Writes an entry using a **precomputed key hash** and a payload.
    ///
    /// This method is a **low-level** alternative to `write()`, allowing direct
    /// specification of the key hash. It is mainly used for optimized workflows
    /// where the key hash is already known, avoiding redundant computations.
    ///
    /// # Parameters:
    /// - `key_hash`: The **precomputed hash** of the key.
    /// - `payload`: The **data payload** to be stored.
    ///
    /// # Returns:
    /// - `Ok(offset)`: The file offset where the entry was written.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - The caller is responsible for ensuring that `key_hash` is correctly computed.
    /// - This method **locks the file for writing** to maintain consistency.
    /// - If writing **multiple entries**, consider using `batch_write_with_key_hashes()`.
    async fn write_with_key_hash(&self, key_hash: u64, payload: &[u8]) -> Result<u64>;

    /// Writes multiple key-value pairs as a **single transaction**.
    ///
    /// This method computes the hashes of the provided keys and delegates to
    /// `batch_write_with_key_hashes()`, ensuring all writes occur in a single
    /// locked operation for efficiency.
    ///
    /// # Parameters:
    /// - `entries`: A **slice of key-value pairs**, where:
    ///   - `key`: The **binary key** for the entry.
    ///   - `payload`: The **data payload** to be stored.
    ///
    /// # Returns:
    /// - `Ok(final_offset)`: The file offset after all writes.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - This method improves efficiency by **minimizing file lock contention**.
    /// - If a large number of entries are written, **batching reduces overhead**.
    /// - If the key hashes are already computed, use `batch_write_with_key_hashes()`.
    async fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64>;

    /// Writes multiple key-value pairs as a **single transaction**, using precomputed key hashes.
    ///
    /// This method efficiently appends multiple entries in a **batch operation**,
    /// reducing lock contention and improving performance for bulk writes.
    ///
    /// # Parameters:
    /// - `prehashed_keys`: A **vector of precomputed key hashes and payloads**, where:
    ///   - `key_hash`: The **precomputed hash** of the key.
    ///   - `payload`: The **data payload** to be stored.
    ///
    /// # Returns:
    /// - `Ok(final_offset)`: The file offset after all writes.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - **File locking is performed only once** for all writes, improving efficiency.
    /// - If an entry's `payload` is empty, an error is returned.
    /// - This method uses **SIMD-accelerated memory copy (`simd_copy`)** to optimize write
    ///   performance.
    /// - **Metadata (checksums, offsets) is written after payloads** to ensure data integrity.
    /// - After writing, the memory-mapped file (`mmap`) is **remapped** to reflect updates.
    ///
    /// # Efficiency Considerations:
    /// - **Faster than multiple `write()` calls**, since it reduces lock contention.
    /// - Suitable for **bulk insertions** where key hashes are known beforehand.
    /// - If keys are available but not hashed, use `batch_write()` instead.
    async fn batch_write_with_key_hashes(
        &self,
        prehashed_keys: Vec<(u64, &[u8])>,
        allow_null_bytes: bool,
    ) -> Result<u64>;

    /// Renames an existing entry by copying it under a new key and marking the old key as deleted.
    ///
    /// This function:
    /// - Reads the existing entry associated with `old_key`.
    /// - Writes the same data under `new_key`.
    /// - Deletes the `old_key` by appending a tombstone entry.
    ///
    /// # Parameters:
    /// - `old_key`: The **original key** of the entry to be renamed.
    /// - `new_key`: The **new key** under which the entry will be stored.
    ///
    /// # Returns:
    /// - `Ok(new_offset)`: The file offset where the new entry was written.
    /// - `Err(std::io::Error)`: If the old key is not found or if a write operation fails.
    ///
    /// # Notes:
    /// - This operation **does not modify** the original entry but instead appends a new copy.
    /// - The old key is **logically deleted** via an append-only tombstone.
    /// - Attempting to rename a key to itself will return an error.
    async fn rename(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64>;

    /// Copies an entry to a **different storage container**.
    ///
    /// This function:
    /// - Reads the entry associated with `key` in the current storage.
    /// - Writes it to the `target` storage.
    ///
    /// # Parameters:
    /// - `key`: The **key** of the entry to be copied.
    /// - `target`: The **destination storage** where the entry should be copied.
    ///
    /// # Returns:
    /// - `Ok(target_offset)`: The file offset where the copied entry was written in the target storage.
    /// - `Err(std::io::Error)`: If the key is not found, if the write operation fails,  
    ///   or if attempting to copy to the same storage.
    ///
    /// # Notes:
    /// - Copying within the **same** storage is unnecessary; use `rename` instead.
    /// - This operation does **not** delete the original entry.
    async fn copy(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    /// Moves an entry from the current storage to a **different storage container**.
    ///
    /// This function:
    /// - Copies the entry from the current storage to `target`.
    /// - Marks the original entry as deleted.
    ///
    /// # Parameters:
    /// - `key`: The **key** of the entry to be moved.
    /// - `target`: The **destination storage** where the entry should be moved.
    ///
    /// # Returns:
    /// - `Ok(target_offset)`: The file offset where the entry was written in the target storage.
    /// - `Err(std::io::Error)`: If the key is not found, or if the copy/delete operation fails.
    ///
    /// # Notes:
    /// - Moving an entry within the **same** storage is unnecessary; use `rename` instead.
    /// - The original entry is **logically deleted** by appending a tombstone, maintaining
    ///   the append-only structure.
    async fn transfer(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    /// Logically deletes an entry by its key.
    ///
    /// The storage engine is **append-only**, so entries are not removed directly.
    /// Instead, this method appends a **tombstone marker** to logically delete the key.
    ///
    /// This operation first **verifies that the key exists** before appending a tombstone.
    /// If the key is not found, no data is written to the file, and the operation
    /// succeeds without changing the store's state.
    ///
    /// # Parameters
    /// - `key`: The **binary key** to mark as deleted.
    ///
    /// # Returns
    /// - `Ok(tail_offset)`: The file's tail offset after the operation completes.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn delete(&self, key: &[u8]) -> Result<u64>;

    /// Deletes a batch of entries from the storage by their keys.
    ///
    /// This method computes the hash for each key and then calls the underlying
    /// `batch_delete_key_hashes` method. It will only write deletion markers
    /// (tombstones) for keys that currently exist in the store.
    ///
    /// # Parameters
    /// - `keys`: A slice of keys to be deleted.
    ///
    /// # Returns
    /// - `Ok(tail_offset)`: The new tail offset of the file after the operation.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn batch_delete(&self, keys: &[&[u8]]) -> Result<u64>;

    /// Deletes a batch of entries from the storage using pre-computed key hashes.
    ///
    /// This is the lowest-level batch deletion method. It checks for the existence
    /// of each key hash in the in-memory index before writing a deletion marker.
    /// This prevents the store from being filled with unnecessary tombstones for
    /// keys that were never present.
    ///
    /// # Parameters
    /// - `prehashed_keys`: A slice of `u64` key hashes to be deleted.
    ///
    /// # Returns
    /// - `Ok(tail_offset)`: The new tail offset of the file after the operation.
    /// - `Err(std::io::Error)`: On I/O failure.
    async fn batch_delete_key_hashes(&self, prehashed_keys: &[u64]) -> Result<u64>;
}
