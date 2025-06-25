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

    /// Writes multiple key-value pairs as a **single transaction**.
    ///
    /// This method computes the hashes of the provided keys and delegates to
    /// `batch_write_hashed_payloads()`, ensuring all writes occur in a single
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
    /// - If the key hashes are already computed, use `batch_write_hashed_payloads()`.
    fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64>;

    // TODO: Rename to `rename`
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
    fn rename_entry(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64>;

    // TODO: Rename to `copy`
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
    /// - Copying within the **same** storage is unnecessary; use `rename_entry` instead.
    /// - This operation does **not** delete the original entry.
    fn copy_entry(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    // TODO: Rename to `move`
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
    /// - Moving an entry within the **same** storage is unnecessary; use `rename_entry` instead.
    /// - The original entry is **logically deleted** by appending a tombstone, maintaining
    ///   the append-only structure.
    fn move_entry(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    // TODO: Rename to `delete`
    /// Deletes a key by appending a **null byte marker**.
    ///
    /// The storage engine is **append-only**, so keys cannot be removed directly.
    /// Instead, a **null byte is appended** as a tombstone entry to mark the key as deleted.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** to mark as deleted.
    ///
    /// # Returns:
    /// - The **new file offset** where the delete marker was appended.
    fn delete_entry(&self, key: &[u8]) -> Result<u64>;
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

    /// Writes multiple key-value pairs as a **single transaction**.
    ///
    /// This method computes the hashes of the provided keys and delegates to
    /// `batch_write_hashed_payloads()`, ensuring all writes occur in a single
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
    /// - If the key hashes are already computed, use `batch_write_hashed_payloads()`.
    async fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64>;

    // TODO: Rename to `rename`
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
    async fn rename_entry(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64>;

    // TODO: Rename to `copy`
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
    /// - Copying within the **same** storage is unnecessary; use `rename_entry` instead.
    /// - This operation does **not** delete the original entry.
    async fn copy_entry(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    // TODO: Rename to `move`
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
    /// - Moving an entry within the **same** storage is unnecessary; use `rename_entry` instead.
    /// - The original entry is **logically deleted** by appending a tombstone, maintaining
    ///   the append-only structure.
    async fn move_entry(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    // TODO: Rename to `delete`
    /// Deletes a key by appending a **null byte marker**.
    ///
    /// The storage engine is **append-only**, so keys cannot be removed directly.
    /// Instead, a **null byte is appended** as a tombstone entry to mark the key as deleted.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** to mark as deleted.
    ///
    /// # Returns:
    /// - The **new file offset** where the delete marker was appended.
    async fn delete_entry(&self, key: &[u8]) -> Result<u64>;
}
