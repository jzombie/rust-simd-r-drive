use crate::storage_engine::constants::*;
use crate::storage_engine::digest::{
    Xxh3BuildHasher, compute_checksum, compute_hash, compute_hash_batch,
};
use crate::storage_engine::simd_copy;
use crate::storage_engine::stage_writer_buffer::{KeyHash, StageWriterBuffer};
use crate::storage_engine::{EntryHandle, EntryIterator, EntryMetadata, EntryStream, KeyIndexer};
use crate::traits::{DataStoreReader, DataStoreWriter};
use crate::utils::verify_file_existence;
use log::{debug, info, warn};
use memmap2::Mmap;
use std::collections::HashSet;
use std::convert::From;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// Experimented with using a feature flag to enable `tokio::sync::Mutex`
// and `tokio::sync::RwLock` for async compatibility but decided to hold off for now.
// The current implementation remains on `std::sync::{Mutex, RwLock}` because:
// - The existing code is **blocking**, and there is no immediate need for async locks.
// - Switching to `tokio::sync` would require `.await` at locking points, leading
//   to refactoring without clear performance benefits at this stage.
// - Lock contention has not yet been identified as a bottleneck, so there's no
//   strong reason to introduce async synchronization primitives.
// This decision may be revisited if future profiling shows tangible benefits.
use std::sync::{
    Arc,
    Mutex,
    // TODO: Investigate using `parking_lot::RwLock;`
    RwLock,
};

use std::sync::atomic::{AtomicU64, Ordering};

/// Append-Only Storage Engine
pub struct DataStore {
    file: Arc<RwLock<BufWriter<File>>>,
    mmap: Arc<Mutex<Arc<Mmap>>>,
    tail_offset: AtomicU64,
    key_indexer: Arc<RwLock<KeyIndexer>>,
    path: PathBuf,
    write_buffer: Arc<StageWriterBuffer>,
}

impl IntoIterator for DataStore {
    type Item = EntryHandle;
    type IntoIter = EntryIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_entries()
    }
}

impl From<PathBuf> for DataStore {
    /// Creates an `DataStore` instance from a `PathBuf`.
    ///
    /// This allows creating a storage instance **directly from a file path**.
    ///
    /// # Panics:
    /// - If the file cannot be opened or mapped into memory.
    fn from(path: PathBuf) -> Self {
        DataStore::open(&path).expect("Failed to open storage file")
    }
}

impl DataStoreWriter for DataStore {
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
    fn write_stream<R: Read>(&self, key: &[u8], reader: &mut R) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.write_stream_with_key_hash(key_hash, reader)
    }

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
    fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.write_with_key_hash(key_hash, payload)
    }

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
    fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64> {
        // 1.  Split keys & payloads so we can hash the keys in one go.
        let (keys, payloads): (Vec<_>, Vec<_>) = entries.iter().cloned().unzip();

        // 2.  One call → many hashes (stays outside the write-lock).
        let hashes = compute_hash_batch(&keys);

        // 3.  Zip hashes back with payloads and hand off to the low-level routine.
        let hashed_entries = hashes
            .into_iter()
            .zip(payloads.into_iter())
            .collect::<Vec<_>>();

        self.batch_write_hashed_payloads(hashed_entries, false)
    }

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
    fn rename_entry(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64> {
        if old_key == new_key {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot rename a key to itself",
            ));
        }

        let old_entry = self.read(old_key).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Old key not found")
        })?;

        let mut old_entry_stream = EntryStream::from(old_entry);

        self.write_stream(new_key, &mut old_entry_stream)?;

        let new_offset = self.delete_entry(old_key)?;

        Ok(new_offset)
    }

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
    fn copy_entry(&self, key: &[u8], target: &DataStore) -> Result<u64> {
        if self.path == target.path {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Cannot copy entry to the same storage ({:?}). Use `rename_entry` instead.",
                    self.path
                ),
            ));
        }

        let entry_handle = self.read(key).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Key not found: {:?}", key),
            )
        })?;

        self.copy_entry_handle(&entry_handle, target)
    }

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
    fn move_entry(&self, key: &[u8], target: &DataStore) -> Result<u64> {
        self.copy_entry(key, target)?;

        self.delete_entry(key)
    }

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
    fn delete_entry(&self, key: &[u8]) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.batch_write_hashed_payloads(vec![(key_hash, &NULL_BYTE)], true)
    }
}

impl DataStoreReader for DataStore {
    type EntryHandleType = EntryHandle;

    /// Retrieves the most recent value associated with a given key.
    ///
    /// This method **efficiently looks up a key** using a fast in-memory index,
    /// and returns the latest corresponding value if found.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** whose latest value is to be retrieved.
    ///
    /// # Returns:
    /// - `Some(EntryHandle)`: A handle to the entry containing the data and metadata.
    /// - `None`: If the key does not exist or is deleted.
    ///
    /// # Notes:
    /// - The returned `EntryHandle` provides zero-copy access to the stored data.
    fn read(&self, key: &[u8]) -> Option<EntryHandle> {
        let mmap_arc = self.get_mmap_arc();
        let key_indexer_guard = self.key_indexer.read().ok()?;

        let key_hash = compute_hash(key);
        Self::read_hashed_with_ctx(key_hash, &mmap_arc, &key_indexer_guard)
    }

    /// Reads many keys in one shot.
    ///
    /// This is the **vectorized** counterpart to [`read`].  
    /// It takes a slice of raw-byte keys and returns a `Vec` whose *i-th* element
    /// is the result of looking up the *i-th* key.
    ///
    /// *   **Zero-copy** – each `Some(EntryHandle)` points directly into the
    ///     shared `Arc<Mmap>`; no payload is copied.
    /// *   **Constant-time per key** – the in-memory [`KeyIndexer`] map is used
    ///     for each lookup, so the complexity is *O(n)* where *n* is
    ///     `keys.len()`.
    /// *   **Thread-safe** – a read lock on the index is taken once for the whole
    ///     batch, so concurrent writers are still blocked only for the same short
    ///     critical section that a single `read` would need.
    ///
    /// #### Error handling
    /// *If* the index lock is poisoned the function falls back to “best-effort”
    /// semantics and returns a vector of `None`s (one per requested key).  
    /// This keeps the signature simple (`Vec<Option<…>>`) while still signalling
    /// that the read failed.
    fn batch_read(&self, keys: &[&[u8]]) -> Result<Vec<Option<EntryHandle>>> {
        use crate::storage_engine::digest::compute_hash_batch;

        // 1. Grab the mmap once ----------------------------------------------------
        let mmap_arc = self.get_mmap_arc();

        // 2. Read-lock the index.  On poisoning → bubble up an error.
        let key_indexer_guard = self.key_indexer.read().map_err(|_| {
            Error::new(
                ErrorKind::Other,
                "Key-index lock poisoned during batch_read",
            )
        })?;

        // 3. Hash all keys outside the critical section ---------------------------
        let hashes = compute_hash_batch(keys);

        // 4. Probe the index -------------------------------------------------------
        let results = hashes
            .into_iter()
            .map(|h| Self::read_hashed_with_ctx(h, &mmap_arc, &key_indexer_guard))
            .collect();

        Ok(results)
    }

    /// Retrieves metadata for a given key.
    ///
    /// This method looks up a key in the storage and returns its associated metadata.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** whose metadata is to be retrieved.
    ///
    /// # Returns:
    /// - `Some(&EntryMetadata)`: Metadata for the key if it exists.
    /// - `None`: If the key does not exist in the storage.
    fn read_metadata(&self, key: &[u8]) -> Result<Option<EntryMetadata>> {
        Ok(self.read(key).map(|entry| entry.metadata().clone()))
    }

    /// Counts the number of **active** entries in the storage.
    ///
    /// This method iterates through the storage file and counts **only the latest versions**
    /// of keys, skipping deleted or outdated entries.
    ///
    /// # Returns:
    /// - The **total count** of active key-value pairs in the database.
    fn count(&self) -> Result<usize> {
        Ok(self.iter_entries().count())
    }

    /// Retrieves the **total size** of the storage file.
    ///
    /// This method queries the **current file size** of the storage file on disk.
    ///
    /// # Returns:
    /// - `Ok(file_size_in_bytes)` if successful.
    /// - `Err(std::io::Error)` if the file cannot be accessed.
    fn get_storage_size(&self) -> Result<u64> {
        std::fs::metadata(&self.path).map(|meta| meta.len())
    }
}

impl DataStore {
    /// Opens an **existing** or **new** append-only storage file.
    ///
    /// This function:
    /// 1. **Opens the file** in read/write mode (creating it if necessary).
    /// 2. **Maps the file** into memory using `mmap` for fast access.
    /// 3. **Recovers the valid chain**, ensuring **data integrity**.
    /// 4. **Re-maps** the file after recovery to reflect the correct state.
    /// 5. **Builds an in-memory index** for **fast key lookups**.
    ///
    /// # Parameters:
    /// - `path`: The **file path** where the storage is located.
    ///
    /// # Returns:
    /// - `Ok(DataStore)`: A **new storage instance**.
    /// - `Err(std::io::Error)`: If any file operation fails.
    pub fn open(path: &Path) -> Result<Self> {
        let file = Self::open_file_in_append_mode(path)?;
        let file_len = file.get_ref().metadata()?.len();

        // First mmap the file
        let mmap = Self::init_mmap(&file)?;

        // Recover valid chain using mmap, not file
        let final_len = Self::recover_valid_chain(&mmap, file_len)?;

        // TODO: Don't automatically do this; make it configurable with parameter
        if final_len < file_len {
            // TODO: If open in read-only mode, reject the call
            warn!(
                "Truncating corrupted data in {} from offset {} to {}.",
                path.display(),
                final_len,
                file_len
            );

            // Close the file before truncation
            drop(mmap);
            drop(file);

            // Reopen the file in read-write mode and truncate it
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            file.set_len(final_len)?;
            file.sync_all()?; // Ensure OS writes take effect

            // Now reopen everything fresh
            return Self::open(path);
        }

        let key_indexer = KeyIndexer::build(&mmap, final_len);

        Ok(Self {
            file: Arc::new(RwLock::new(file)), // Wrap in RwLock
            mmap: Arc::new(Mutex::new(Arc::new(mmap))),
            tail_offset: final_len.into(),
            key_indexer: Arc::new(RwLock::new(key_indexer)),
            path: path.to_path_buf(),
            write_buffer: StageWriterBuffer::new(DEFAULT_WRITE_BUF_LIMIT),
        })
    }

    /// Opens an **existing** append-only storage file.
    ///
    /// This function verifies that the file exists before attempting to open it.
    /// If the file does not exist or is not a valid file, an error is returned.
    ///
    /// # Parameters:
    /// - `path`: The **file path** of the storage file.
    ///
    /// # Returns:
    /// - `Ok(DataStore)`: A **new storage instance** if the file exists and can be opened.
    /// - `Err(std::io::Error)`: If the file does not exist or is invalid.
    ///
    /// # Notes:
    /// - Unlike `open()`, this function **does not create** a new storage file if the
    ///   specified file does not exist.
    /// - If the file is **missing** or is not a regular file, an error is returned.
    /// - This is useful in scenarios where the caller needs to **ensure** that they are
    ///   working with an already existing storage file.
    pub fn open_existing(path: &Path) -> Result<Self> {
        // Errors if the file does not exist or is not a valid file
        verify_file_existence(&path)?;

        Self::open(path)
    }

    /// Workaround for directly opening in **append mode** causing permissions issues on Windows
    ///
    /// The file is opened normally and the **cursor is moved to the end.
    ///
    /// Unix family unaffected by this issue, but this standardizes their handling.
    ///
    /// # Parameters:
    /// - `path`: The **file path** of the storage file.
    ///
    /// # Returns:
    /// - `Ok(BufWriter<File>)`: A buffered writer pointing to the file.
    /// - `Err(std::io::Error)`: If the file could not be opened.
    fn open_file_in_append_mode(path: &Path) -> Result<BufWriter<File>> {
        // Note: If using `append` here, Windows may throw an error with the message:
        // "Failed to open storage". A workaround is to open the file normally, then
        // move the cursor to the end of the file.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        file.seek(SeekFrom::End(0))?; // Move cursor to end to prevent overwriting

        Ok(BufWriter::new(file))
    }

    /// Initializes a memory-mapped file for fast access.
    ///
    /// This function creates a memory-mapped file (`mmap`) from a `BufWriter<File>`.
    /// It provides a read-only view of the file, allowing efficient direct access to
    /// stored data without unnecessary copies.
    ///
    /// # Parameters:
    /// - `file`: A reference to a `BufWriter<File>`, which must be flushed before
    ///   mapping to ensure all written data is visible.
    ///
    /// # Returns:
    /// - `Ok(Mmap)`: A memory-mapped view of the file.
    /// - `Err(std::io::Error)`: If the mapping fails.
    ///
    /// # Notes:
    /// - The `BufWriter<File>` should be flushed before calling this function to
    ///   ensure that all pending writes are persisted.
    /// - The memory mapping remains valid as long as the underlying file is not truncated
    ///   or modified in ways that invalidate the mapping.
    ///
    /// # Safety:
    /// - This function uses an **unsafe** operation (`memmap2::MmapOptions::map`).
    ///   The caller must ensure that the mapped file is not resized or closed while
    ///   the mapping is in use, as this could lead to undefined behavior.
    fn init_mmap(file: &BufWriter<File>) -> Result<Mmap> {
        unsafe { memmap2::MmapOptions::new().map(file.get_ref()) }
    }

    /// Returns a cloned `Arc<Mmap>`, providing a shared reference to the memory-mapped file.
    ///
    /// # How It Works:
    /// - Acquires the `Mutex<Arc<Mmap>>` lock to access the current memory-mapped file.
    /// - Clones the `Arc<Mmap>` to create another reference to the **same underlying memory**.
    /// - Releases the lock immediately (`drop(guard)`) to allow other threads to proceed.
    ///
    /// # Important:
    /// - **Cloning the `Arc<Mmap>` does not duplicate the memory-mapped file.**  
    ///   Instead, it creates a new reference to the existing memory region,  
    ///   ensuring efficient, zero-copy access.
    /// - The returned `Arc<Mmap>` remains valid as long as at least one reference exists.
    ///
    /// # Returns:
    /// - A **shared reference** (`Arc<Mmap>`) to the current memory-mapped file.
    ///
    /// # Safety:
    /// - The returned `Arc<Mmap>` must not be used after a file truncation or remap.
    ///   Ensure proper synchronization when modifying the underlying storage.
    #[inline]
    fn get_mmap_arc(&self) -> Arc<Mmap> {
        // Briefly acquire the guard and release so that others can proceed
        let guard = self.mmap.lock().unwrap();
        let mmap_clone = guard.clone();
        drop(guard);

        mmap_clone
    }

    /// Re-maps the storage file and updates the key index after a write operation.
    ///
    /// This function performs two key tasks:
    /// 1. **Re-maps the file (`mmap`)**: Ensures that newly written data is visible
    ///    to readers by creating a fresh memory-mapped view of the storage file.
    /// 2. **Updates the key index**: Inserts new key hash-to-offset mappings into
    ///    the in-memory key index, ensuring efficient key lookups for future reads.
    ///
    /// # Parameters:
    /// - `write_guard`: A locked reference to the `BufWriter<File>`, ensuring that
    ///   writes are completed before remapping and indexing.
    /// - `key_hash_offsets`: A slice of `(key_hash, tail_offset)` tuples containing
    ///   the latest key mappings to be added to the index.
    /// - `tail_offset`: The **new absolute file offset** after the most recent write.
    ///   This represents the byte position where the next write operation should begin.
    ///   It is updated to reflect the latest valid data in the storage.
    ///
    /// # Returns:
    /// - `Ok(())` if the reindexing process completes successfully.
    /// - `Err(std::io::Error)` if file metadata retrieval, memory mapping, or
    ///   key index updates fail.
    ///
    /// # Important:
    /// - **The write operation must be flushed before calling `reindex`** to ensure
    ///   all pending writes are persisted and visible in the new memory-mapped file.
    ///   This prevents potential inconsistencies where written data is not reflected
    ///   in the remapped view.
    ///
    /// # Safety:
    /// - This function should be called **immediately after a write operation**
    ///   to ensure the file is in a consistent state before remapping.
    /// - The function acquires locks on both the `mmap` and `key_indexer`
    ///   to prevent race conditions while updating shared structures.
    ///
    /// # Locks Acquired:
    /// - `mmap` (`Mutex<Arc<Mmap>>`) is locked to update the memory-mapped file.
    /// - `key_indexer` (`RwLock<HashMap<u64, u64>>`) is locked to modify key mappings.
    fn reindex(
        &self,
        write_guard: &std::sync::RwLockWriteGuard<'_, BufWriter<File>>,
        key_hash_offsets: &[(u64, u64)],
        tail_offset: u64,
    ) -> std::io::Result<()> {
        // Create a new Mmap from the file
        let new_mmap = Self::init_mmap(write_guard)?;

        // Obtain the lock guards
        let mut mmap_guard = self.mmap.lock().unwrap();
        let mut key_indexer_guard = self.key_indexer.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire index lock")
        })?;

        // Update tail_offset (or any other fields)
        let new_offset = write_guard.get_ref().metadata()?.len();
        self.tail_offset
            .store(new_offset, std::sync::atomic::Ordering::Release);

        for (key_hash, tail_offset) in key_hash_offsets.iter() {
            key_indexer_guard.insert(*key_hash, *tail_offset);
        }

        // Overwrite the old Arc<Mmap> with the new one
        *mmap_guard = Arc::new(new_mmap);

        self.tail_offset.store(tail_offset, Ordering::Release);

        // These are automatically dropped by Rust once leaving scope, but calling them for good measure
        drop(mmap_guard);
        drop(key_indexer_guard);

        Ok(())
    }

    /// Returns the storage file path.
    ///
    /// # Returns:
    /// - A `PathBuf` containing the path to the storage file.
    pub fn get_path(&self) -> PathBuf {
        /*
        This function **does not** clone or duplicate the actual storage file.
        It only returns a clone of the in-memory `PathBuf` reference that
        represents the file path.

        `PathBuf::clone()` creates a shallow copy of the path, which is
        inexpensive since it only duplicates the internal path buffer.

        For more details, see:
        https://doc.rust-lang.org/std/path/struct.PathBuf.html
        */
        self.path.clone()
    }

    /// Retrieves an iterator over all valid entries in the storage.
    ///
    /// This iterator allows scanning the storage file and retrieving **only the most recent**
    /// versions of each key.
    ///
    /// # Returns:
    /// - An `EntryIterator` instance for iterating over valid entries.
    pub fn iter_entries(&self) -> EntryIterator {
        let mmap_clone = self.get_mmap_arc();

        let tail_offset = self.tail_offset.load(Ordering::Acquire);

        EntryIterator::new(mmap_clone, tail_offset)
    }

    /// Recovers the **latest valid chain** of entries from the storage file.
    ///
    /// This function **scans backward** through the file, verifying that each entry
    /// correctly references the previous offset. It determines the **last valid
    /// storage position** to ensure data integrity.
    ///
    /// # How It Works:
    /// - Scans from the last written offset **backward**.
    /// - Ensures each entry correctly points to its **previous offset**.
    /// - Stops at the **deepest valid chain** that reaches offset `0`.
    ///
    /// # Parameters:
    /// - `mmap`: A reference to the **memory-mapped file**.
    /// - `file_len`: The **current size** of the file in bytes.
    ///
    /// # Returns:
    /// - `Ok(final_valid_offset)`: The last **valid** byte offset.
    /// - `Err(std::io::Error)`: If a file read or integrity check fails
    fn recover_valid_chain(mmap: &Mmap, file_len: u64) -> Result<u64> {
        if file_len < METADATA_SIZE as u64 {
            return Ok(0);
        }

        let mut cursor = file_len;
        let mut best_valid_offset = None;

        while cursor >= METADATA_SIZE as u64 {
            let metadata_offset = cursor - METADATA_SIZE as u64;

            // Read metadata directly from `mmap`
            let metadata_bytes =
                &mmap[metadata_offset as usize..(metadata_offset as usize + METADATA_SIZE)];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            let entry_start = metadata.prev_offset;

            if entry_start >= metadata_offset {
                cursor -= 1;
                continue;
            }

            // Trace back the entire chain from this entry
            let mut chain_valid = true;
            let mut back_cursor = entry_start;
            let mut total_size = (metadata_offset - entry_start) + METADATA_SIZE as u64;
            let mut temp_chain = vec![metadata_offset];

            while back_cursor != 0 {
                if back_cursor < METADATA_SIZE as u64 {
                    chain_valid = false;
                    break;
                }

                let prev_metadata_offset = back_cursor - METADATA_SIZE as u64;

                // Read previous entry metadata directly from `mmap`
                let prev_metadata_bytes = &mmap[prev_metadata_offset as usize
                    ..(prev_metadata_offset as usize + METADATA_SIZE)];
                let prev_metadata = EntryMetadata::deserialize(prev_metadata_bytes);

                let entry_size = prev_metadata_offset.saturating_sub(prev_metadata.prev_offset);
                total_size += entry_size + METADATA_SIZE as u64;

                if prev_metadata.prev_offset >= prev_metadata_offset {
                    chain_valid = false;
                    break;
                }

                temp_chain.push(prev_metadata_offset);
                back_cursor = prev_metadata.prev_offset;
            }

            // Only accept the deepest valid chain that reaches `offset = 0`
            if chain_valid && back_cursor == 0 && total_size <= file_len {
                debug!(
                    "Found valid chain of {} entries. Ending at offset {}.",
                    temp_chain.len(),
                    metadata_offset + METADATA_SIZE as u64
                );
                best_valid_offset = Some(metadata_offset + METADATA_SIZE as u64);
                break; // Stop checking further offsets since we found the best chain
            }

            cursor -= 1;
        }

        let final_len = best_valid_offset.unwrap_or(0);
        Ok(final_len)
    }

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
    pub fn write_stream_with_key_hash<R: Read>(
        &self,
        key_hash: u64,
        reader: &mut R,
    ) -> Result<u64> {
        let mut file = self.file.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire file lock")
        })?;

        let prev_offset = self.tail_offset.load(Ordering::Acquire);

        let mut buffer = vec![0; WRITE_STREAM_BUFFER_SIZE]; // 64KB buffer
        let mut aligned_buffer = vec![0; WRITE_STREAM_BUFFER_SIZE]; // Aligned buffer for SIMD copy
        let mut total_written = 0;
        let mut checksum_state = crc32fast::Hasher::new();

        let mut is_null_only = true; // Flag to track NULL-byte-only payload

        while let Ok(bytes_read) = reader.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }

            // Check if all bytes in buffer match NULL_BYTE
            if buffer[..bytes_read].iter().any(|&b| b != NULL_BYTE[0]) {
                is_null_only = false;
            }

            // Use SIMD to optimize memory copy
            simd_copy(&mut aligned_buffer[..bytes_read], &buffer[..bytes_read]);

            file.write_all(&aligned_buffer[..bytes_read])?;
            checksum_state.update(&aligned_buffer[..bytes_read]); // Update checksum
            total_written += bytes_read;
        }

        // Reject if the entire stream was NULL bytes
        if total_written > 0 && is_null_only {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "NULL-byte-only streams cannot be written directly.",
            ));
        }

        let checksum_u32 = checksum_state.finalize();
        let checksum = checksum_u32.to_le_bytes();

        let metadata = EntryMetadata {
            key_hash,
            prev_offset,
            checksum,
        };

        // Write metadata **after** payload
        file.write_all(&metadata.serialize())?;
        file.flush()?; // Ensure data is written to disk

        let tail_offset = prev_offset + total_written as u64 + METADATA_SIZE as u64;

        self.reindex(
            &file,
            &vec![(key_hash, tail_offset - METADATA_SIZE as u64)],
            tail_offset,
        )?;

        Ok(tail_offset)
    }

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
    /// - If writing **multiple entries**, consider using `batch_write_hashed_payloads()`.
    pub fn write_with_key_hash(&self, key_hash: u64, payload: &[u8]) -> Result<u64> {
        self.batch_write_hashed_payloads(vec![(key_hash, payload)], false)
    }

    /// Writes multiple key-value pairs as a **single transaction**, using precomputed key hashes.
    ///
    /// This method efficiently appends multiple entries in a **batch operation**,
    /// reducing lock contention and improving performance for bulk writes.
    ///
    /// # Parameters:
    /// - `hashed_payloads`: A **vector of precomputed key hashes and payloads**, where:
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
    pub fn batch_write_hashed_payloads(
        &self,
        hashed_payloads: Vec<(u64, &[u8])>,
        allow_null_bytes: bool,
    ) -> Result<u64> {
        let mut file = self.file.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire file lock")
        })?; // Lock only the file, not the whole struct

        let mut buffer = Vec::new();
        let mut tail_offset = self.tail_offset.load(Ordering::Acquire);

        let mut key_hash_offsets: Vec<(u64, u64)> = Vec::with_capacity(hashed_payloads.len());

        for (key_hash, payload) in hashed_payloads {
            if !allow_null_bytes && payload == NULL_BYTE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "NULL-byte payloads cannot be written directly.",
                ));
            }

            if payload.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Payload cannot be empty.",
                ));
            }

            let prev_offset = tail_offset;
            let checksum = compute_checksum(payload);

            let metadata = EntryMetadata {
                key_hash,
                prev_offset,
                checksum,
            };

            let payload_len = payload.len();

            let mut entry: Vec<u8> = vec![0u8; payload_len + METADATA_SIZE];

            // Use SIMD to copy payload into buffer
            simd_copy(&mut entry[..payload.len()], payload);

            // Copy metadata normally (small size, not worth SIMD)
            entry[payload.len()..].copy_from_slice(&metadata.serialize());

            buffer.extend_from_slice(&entry);

            tail_offset += entry.len() as u64;

            key_hash_offsets.push((key_hash, tail_offset - METADATA_SIZE as u64));
        }

        file.write_all(&buffer)?;
        file.flush()?;

        self.reindex(&file, &key_hash_offsets, tail_offset)?; // Ensure mmap updates

        Ok(self.tail_offset.load(Ordering::Acquire))
    }

    /// Reads the last entry stored in the database.
    ///
    /// This method retrieves the **most recently appended** entry in the storage.
    /// It does not check for key uniqueness; it simply returns the last-written
    /// data segment from the memory-mapped file.
    ///
    /// # Returns:
    /// - `Some(EntryHandle)`: A handle to the last entry containing the data and metadata.
    /// - `None`: If the storage is empty or corrupted.
    ///
    /// # Notes:
    /// - The returned `EntryHandle` allows zero-copy access to the entry data.
    pub fn read_last_entry(&self) -> Option<EntryHandle> {
        let mmap_arc = self.get_mmap_arc();

        // 4) Use `mmap_arc` to find the last entry boundaries
        let tail_offset = self.tail_offset.load(std::sync::atomic::Ordering::Acquire);

        if tail_offset < METADATA_SIZE as u64 || mmap_arc.len() == 0 {
            return None;
        }

        let metadata_offset = (tail_offset - METADATA_SIZE as u64) as usize;
        if metadata_offset + METADATA_SIZE > mmap_arc.len() {
            return None;
        }

        // Read the last entry's metadata
        let metadata_bytes = &mmap_arc[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        let entry_start = metadata.prev_offset as usize;
        let entry_end = metadata_offset;
        if entry_start >= entry_end || entry_end > mmap_arc.len() {
            return None;
        }

        // 5) Create a handle that "owns" the Arc and the byte range
        Some(EntryHandle {
            mmap_arc,
            range: entry_start..entry_end,
            metadata,
        })
    }

    /// Internal helper that does the real work for `read`/`batch_read`.
    ///
    /// *   `key` – raw-byte key we are searching for.  
    /// *   `mmap_arc` – the current shared memory-map.  
    /// *   `key_indexer` – **already locked** read-only view of the index.  
    ///
    /// The function:
    /// 1.  Hashes `key` with XXH3 (same as writers do).
    /// 2.  Looks the hash up in the index; bails out early if absent.
    /// 3.  Validates that the stored offset and metadata still fit inside the
    ///     current `mmap` (guards against truncated / corrupted files).
    /// 4.  Creates and returns an `EntryHandle` that spans the payload slice in
    ///     the `mmap`.
    ///
    /// It deliberately **does not** take any locks itself – that must be done by
    /// the caller so that `batch_read` can reuse the same lock for many lookups.
    ///
    /// `None` is returned when:
    /// * the key is unknown,
    /// * the mapped file looks inconsistent (bounds checks fail), or
    /// * the latest record for the key is a tomb-stone (one-byte NULL payload).
    #[inline]
    pub fn read_hashed_with_ctx(
        key_hash: u64,
        mmap_arc: &Arc<Mmap>,
        key_indexer: &KeyIndexer, // already locked
    ) -> Option<EntryHandle> {
        let offset = *key_indexer.get(&key_hash)?;

        if offset as usize + METADATA_SIZE > mmap_arc.len() {
            return None;
        }

        let metadata_bytes = &mmap_arc[offset as usize..offset as usize + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        let entry_start = metadata.prev_offset as usize;
        let entry_end = offset as usize;

        if entry_start >= entry_end || entry_end > mmap_arc.len() {
            return None;
        }

        // Tomb-stone?  → treat as deleted.
        if entry_end - entry_start == 1 && &mmap_arc[entry_start..entry_end] == NULL_BYTE {
            return None;
        }

        Some(EntryHandle {
            mmap_arc: mmap_arc.clone(),
            range: entry_start..entry_end,
            metadata,
        })
    }

    /// Copies an entry handle to a **different storage container**.
    ///
    /// This function:
    /// - Extracts metadata and content from the given `EntryHandle`.
    /// - Writes the entry into the `target` storage.
    ///
    /// # Parameters:
    /// - `entry`: The **entry handle** to be copied.
    /// - `target`: The **destination storage** where the entry should be copied.
    ///
    /// # Returns:
    /// - `Ok(target_offset)`: The file offset where the copied entry was written.
    /// - `Err(std::io::Error)`: If a write operation fails.
    ///
    /// # Notes:
    /// - This is a **low-level function** used by `copy_entry` and related operations.
    /// - The `entry` remains **unchanged** in the original storage.
    fn copy_entry_handle(&self, entry: &EntryHandle, target: &DataStore) -> Result<u64> {
        let metadata = entry.metadata();

        let mut entry_stream = EntryStream::from(entry.clone_arc()); // Convert to Arc to keep ownership

        let target_offset =
            target.write_stream_with_key_hash(metadata.key_hash, &mut entry_stream)?;

        Ok(target_offset)
    }

    /// Compacts the storage by keeping only the latest version of each key.
    ///
    /// # ⚠️ WARNING:
    /// - **This function should only be used when a single thread is accessing the storage.**
    /// - While `&mut self` prevents concurrent **mutations**, it **does not** prevent
    ///   other threads from holding shared references (`&DataStore`) and performing reads.
    /// - If the `DataStore` instance is wrapped in `Arc<DataStore>`, multiple threads
    ///   may still hold **read** references while compaction is running, potentially
    ///   leading to inconsistent reads.
    /// - If stricter concurrency control is required, **manual synchronization should
    ///   be enforced externally.**
    ///
    /// # Behavior:
    /// - Creates a **temporary compacted file** containing only the latest versions
    ///   of stored keys.
    /// - Swaps the original file with the compacted version upon success.
    /// - Does **not** remove tombstone (deleted) entries due to the append-only model.
    ///
    /// # Returns:
    /// - `Ok(())` if compaction completes successfully.
    /// - `Err(std::io::Error)` if an I/O operation fails.
    pub fn compact(&mut self) -> std::io::Result<()> {
        let compacted_path = crate::utils::append_extension(&self.path, "bk");

        debug!("Starting compaction. Writing to: {:?}", compacted_path);

        // Create a new DataStore instance for the compacted file
        let mut compacted_storage = DataStore::open(&compacted_path)?;

        // Iterate over all valid entries using your iterator
        for entry in self.iter_entries() {
            self.copy_entry_handle(&entry, &mut compacted_storage)?;
        }

        // Flush the compacted file
        {
            let mut file_guard = compacted_storage.file.write().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("Lock poisoned: {}", e))
            })?;
            file_guard.flush()?;
        }

        drop(compacted_storage); // ensure all writes are flushed before swapping

        debug!("Reduced backup completed. Swapping files...");
        std::fs::rename(&compacted_path, &self.path)?;
        info!("Compaction successful.");
        Ok(())
    }

    /// Estimates the potential space savings from compaction.
    ///
    /// This method scans the storage file and calculates the difference
    /// between the total file size and the size required to keep only
    /// the latest versions of all keys.
    ///
    /// # How It Works:
    /// - Iterates through the entries, tracking the **latest version** of each key.
    /// - Ignores older versions of keys to estimate the **optimized** storage footprint.
    /// - Returns the **difference** between the total file size and the estimated compacted size.
    pub fn estimate_compaction_savings(&self) -> u64 {
        let total_size = self.get_storage_size().unwrap_or(0);
        let mut unique_entry_size: u64 = 0;
        let mut seen_keys = HashSet::with_hasher(Xxh3BuildHasher);

        let mmap_arc = self.get_mmap_arc();

        // Now we can safely iterate zero-copy
        for entry in self.iter_entries() {
            // Convert pointer offsets relative to `mmap_arc`
            let entry_start_offset = entry.as_ptr() as usize - mmap_arc.as_ptr() as usize;
            let metadata_offset = entry_start_offset + entry.len();

            if metadata_offset + METADATA_SIZE > mmap_arc.len() {
                warn!("Skipping corrupted entry at offset {}", entry_start_offset);
                continue;
            }

            let metadata_bytes = &mmap_arc[metadata_offset..metadata_offset + METADATA_SIZE];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            // Only count the latest version of each key
            if seen_keys.insert(metadata.key_hash) {
                unique_entry_size += entry.len() as u64 + METADATA_SIZE as u64;
            }
        }

        //  Return the difference between total size and the unique size
        total_size.saturating_sub(unique_entry_size)
    }

    /// Provides access to the shared memory-mapped file (`Arc<Mmap>`) for testing.
    ///
    /// This method returns a cloned `Arc<Mmap>`, allowing test cases to inspect
    /// the memory-mapped region while ensuring reference counting remains intact.
    ///
    /// # Notes:
    /// - The returned `Arc<Mmap>` ensures safe access without invalidating the mmap.
    /// - This function is only available in **test** and **debug** builds.
    #[cfg(any(test, debug_assertions))]
    pub fn get_mmap_arc_for_testing(&self) -> Arc<Mmap> {
        self.get_mmap_arc()
    }

    /// Provides direct access to the raw pointer of the underlying memory map for testing.
    ///
    /// This method retrieves a raw pointer (`*const u8`) to the start of the memory-mapped file.
    /// It is useful for validating zero-copy behavior and memory alignment in test cases.
    ///
    /// # Safety Considerations:
    /// - The pointer remains valid **as long as** the mmap is not remapped or dropped.
    /// - Dereferencing this pointer outside of controlled test environments **is unsafe**
    ///   and may result in undefined behavior.
    ///
    /// # Notes:
    /// - This function is only available in **test** and **debug** builds.
    #[cfg(any(test, debug_assertions))]
    pub fn arc_ptr(&self) -> *const u8 {
        let mmap_arc = self.get_mmap_arc();

        mmap_arc.as_ptr()
    }
}
