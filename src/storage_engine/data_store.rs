use crate::storage_engine::constants::*;
use crate::storage_engine::digest::{
    Xxh3BuildHasher, compute_checksum, compute_hash, compute_hash_batch,
};
use crate::storage_engine::simd_copy;
use crate::storage_engine::{EntryHandle, EntryIterator, EntryMetadata, EntryStream, KeyIndexer};
use crate::traits::{DataStoreReader, DataStoreWriter};
use crate::utils::verify_file_existence;
use memmap2::Mmap;
use std::collections::HashSet;
use std::convert::From;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Error, Read, Result, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use tracing::{debug, info, warn};

/// Append-Only Storage Engine
pub struct DataStore {
    file: Arc<RwLock<BufWriter<File>>>,
    mmap: Arc<Mutex<Arc<Mmap>>>,
    tail_offset: AtomicU64,
    key_indexer: Arc<RwLock<KeyIndexer>>,
    path: PathBuf,
}

impl IntoIterator for DataStore {
    type Item = EntryHandle;
    type IntoIter = EntryIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_entries()
    }
}

// TODO: Add feature to iterate via `rayon`
// pub struct ParallelEntryIter {
//     entries: Arc<[EntryHandle]>, // or offsets
// }
//
// impl ParallelIterator for ParallelEntryIter {
//     type Item = EntryHandle;
//
//     fn drive_unindexed<C>(self, consumer: C) -> C::Result
//     where
//         C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
//     {
//         rayon::slice::from_arc(&self.entries).into_par_iter().drive_unindexed(consumer)
//     }
// }

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

        let mmap = Self::init_mmap(&file)?;
        let final_len = Self::recover_valid_chain(&mmap, file_len)?;

        if final_len < file_len {
            warn!(
                "Truncating corrupted data in {} from offset {} to {}.",
                path.display(),
                final_len,
                file_len
            );
            drop(mmap);
            drop(file);
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            file.set_len(final_len)?;
            file.sync_all()?;
            return Self::open(path);
        }

        let mmap_arc = Arc::new(mmap);
        let mmap_for_indexer: &'static Mmap = unsafe { &*(Arc::as_ptr(&mmap_arc)) };
        let key_indexer = KeyIndexer::build(mmap_for_indexer, final_len);

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            mmap: Arc::new(Mutex::new(mmap_arc)),
            tail_offset: final_len.into(),
            key_indexer: Arc::new(RwLock::new(key_indexer)),
            path: path.to_path_buf(),
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
        verify_file_existence(path)?;
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
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(BufWriter::new(file))
    }

    fn init_mmap(file: &BufWriter<File>) -> Result<Mmap> {
        unsafe { memmap2::MmapOptions::new().map(file.get_ref()) }
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
        deleted_keys: Option<&HashSet<u64>>,
    ) -> std::io::Result<()> {
        let new_mmap = Self::init_mmap(write_guard)?;
        let mut mmap_guard = self.mmap.lock().unwrap();
        let mut key_indexer_guard = self
            .key_indexer
            .write()
            .map_err(|_| std::io::Error::other("Failed to acquire index lock"))?;

        for (key_hash, offset) in key_hash_offsets.iter() {
            if deleted_keys
                .as_ref()
                .is_some_and(|set| set.contains(key_hash))
            {
                key_indexer_guard.remove(key_hash);
            } else {
                // Handle the Result from the new insert method
                if let Err(e) = key_indexer_guard.insert(*key_hash, *offset) {
                    // A collision was detected on write. The entire batch operation
                    // should fail to prevent an inconsistent state.
                    warn!("Write operation aborted due to hash collision: {}", e);
                    return Err(std::io::Error::other(e));
                }
            }
        }

        *mmap_guard = Arc::new(new_mmap);
        self.tail_offset.store(tail_offset, Ordering::Release);

        Ok(())
    }

    /// Returns the storage file path.
    ///
    /// # Returns:
    /// - A `PathBuf` containing the path to the storage file.
    pub fn get_path(&self) -> PathBuf {
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
            let metadata_bytes =
                &mmap[metadata_offset as usize..(metadata_offset as usize + METADATA_SIZE)];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            let entry_start = metadata.prev_offset;

            if entry_start >= metadata_offset {
                cursor -= 1;
                continue;
            }

            let mut chain_valid = true;
            let mut back_cursor = entry_start;
            let mut total_size = (metadata_offset - entry_start) + METADATA_SIZE as u64;

            while back_cursor != 0 {
                if back_cursor < METADATA_SIZE as u64 {
                    chain_valid = false;
                    break;
                }

                let prev_metadata_offset = back_cursor - METADATA_SIZE as u64;
                let prev_metadata_bytes = &mmap[prev_metadata_offset as usize
                    ..(prev_metadata_offset as usize + METADATA_SIZE)];
                let prev_metadata = EntryMetadata::deserialize(prev_metadata_bytes);

                let entry_size = prev_metadata_offset.saturating_sub(prev_metadata.prev_offset);
                total_size += entry_size + METADATA_SIZE as u64;
                if prev_metadata.prev_offset >= prev_metadata_offset {
                    chain_valid = false;
                    break;
                }

                back_cursor = prev_metadata.prev_offset;
            }

            if chain_valid && back_cursor == 0 && total_size <= file_len {
                best_valid_offset = Some(metadata_offset + METADATA_SIZE as u64);
                break;
            }

            cursor -= 1;
        }

        Ok(best_valid_offset.unwrap_or(0))
    }

    // TODO: Move to writer trait
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
        let mut file = self
            .file
            .write()
            .map_err(|_| std::io::Error::other("Failed to acquire file lock"))?;
        let prev_offset = self.tail_offset.load(Ordering::Acquire);

        let mut buffer = vec![0; WRITE_STREAM_BUFFER_SIZE];
        let mut total_written = 0;
        let mut checksum_state = crc32fast::Hasher::new();
        let mut is_null_only = true;

        while let Ok(bytes_read) = reader.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }

            if buffer[..bytes_read].iter().any(|&b| b != NULL_BYTE[0]) {
                is_null_only = false;
            }

            file.write_all(&buffer[..bytes_read])?;
            checksum_state.update(&buffer[..bytes_read]);
            total_written += bytes_read;
        }

        if total_written > 0 && is_null_only {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "NULL-byte-only streams cannot be written directly.",
            ));
        }

        let checksum = checksum_state.finalize().to_le_bytes();
        let metadata = EntryMetadata {
            key_hash,
            prev_offset,
            checksum,
        };
        file.write_all(&metadata.serialize())?;
        file.flush()?;

        let tail_offset = prev_offset + total_written as u64 + METADATA_SIZE as u64;
        self.reindex(
            &file,
            &[(key_hash, tail_offset - METADATA_SIZE as u64)],
            tail_offset,
            None,
        )?;
        Ok(tail_offset)
    }

    // TODO: Move to writer trait
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

    // TODO: Move to writer trait
    // TODO: Change `hashed_payloads: Vec<(u64, &[u8])>` to `hashed_payloads: Vec<(u64, Vec<u8>)>`
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
        let mut file = self
            .file
            .write()
            .map_err(|_| std::io::Error::other("Failed to acquire file lock"))?;

        let mut buffer = Vec::new();
        let mut tail_offset = self.tail_offset.load(Ordering::Acquire);

        let mut key_hash_offsets: Vec<(u64, u64)> = Vec::with_capacity(hashed_payloads.len());
        let mut deleted_keys: HashSet<u64> = HashSet::new();

        for (key_hash, payload) in hashed_payloads {
            if payload == NULL_BYTE {
                if !allow_null_bytes {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "NULL-byte payloads cannot be written directly.",
                    ));
                }

                deleted_keys.insert(key_hash);
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
            simd_copy(&mut entry[..payload.len()], payload);
            entry[payload.len()..].copy_from_slice(&metadata.serialize());
            buffer.extend_from_slice(&entry);

            tail_offset += entry.len() as u64;
            key_hash_offsets.push((key_hash, tail_offset - METADATA_SIZE as u64));
        }

        file.write_all(&buffer)?;
        file.flush()?;

        self.reindex(&file, &key_hash_offsets, tail_offset, Some(&deleted_keys))?;

        Ok(self.tail_offset.load(Ordering::Acquire))
    }

    /// Performs the core logic of reading an entry from the store.
    ///
    /// This private helper centralizes the logic for both `read` and `batch_read`.
    /// It takes all necessary context to perform a safe lookup, including the key,
    /// its hash, the memory map, and a read guard for the key indexer.
    ///
    /// # Parameters
    /// - `key`: The original key bytes used for tag verification.
    /// - `key_hash`: The pre-computed hash of the key for index lookup.
    /// - `mmap_arc`: A reference to the active memory map.
    /// - `key_indexer_guard`: A read-lock guard for the key index.
    ///
    /// # Returns
    /// - `Some(EntryHandle)` if the key is found and all checks pass.
    /// - `None` if the key is not found, a tag mismatch occurs (collision/corruption),
    ///   or the entry is a tombstone.
    #[inline]
    fn read_entry_with_context<'a>(
        &self,
        key: &[u8],
        key_hash: u64,
        mmap_arc: &Arc<Mmap>,
        key_indexer_guard: &RwLockReadGuard<'a, KeyIndexer>,
    ) -> Option<EntryHandle> {
        let packed = *key_indexer_guard.get_packed(&key_hash)?;
        let (tag, offset) = KeyIndexer::unpack(packed);

        // The crucial verification check, now centralized.
        if tag != KeyIndexer::tag_from_key(key) {
            warn!("Tag mismatch detected for key, likely a hash collision or index corruption.");
            return None;
        }

        let offset = offset as usize;
        if offset + METADATA_SIZE > mmap_arc.len() {
            return None;
        }

        let metadata_bytes = &mmap_arc[offset..offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);
        let entry_start = metadata.prev_offset as usize;
        let entry_end = offset;

        if entry_start >= entry_end || entry_end > mmap_arc.len() {
            return None;
        }

        // Check for tombstone (deleted entry)
        if entry_end - entry_start == 1 && mmap_arc[entry_start..entry_end] == NULL_BYTE {
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
    /// - This is a **low-level function** used by `copy` and related operations.
    /// - The `entry` remains **unchanged** in the original storage.
    fn copy_handle(&self, entry: &EntryHandle, target: &DataStore) -> Result<u64> {
        let mut entry_stream = EntryStream::from(entry.clone_arc());
        target.write_stream_with_key_hash(entry.key_hash(), &mut entry_stream)
    }

    // TODO: Determine thread count *before* running this OR [somehow] make it thread safe.
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
    pub fn compact(&mut self) -> Result<()> {
        let compacted_path = crate::utils::append_extension(&self.path, "bk");
        info!("Starting compaction. Writing to: {:?}", compacted_path);

        let compacted_storage = DataStore::open(&compacted_path)?;
        let mut index_pairs: Vec<(u64, u64)> = Vec::new();
        let mut compacted_data_size: u64 = 0;

        for entry in self.iter_entries() {
            let new_tail_offset = self.copy_handle(&entry, &compacted_storage)?;
            let stored_metadata_offset = new_tail_offset - METADATA_SIZE as u64;
            index_pairs.push((entry.key_hash(), stored_metadata_offset));
            compacted_data_size += entry.file_size() as u64;
        }

        let size_before = self.file_size()?;

        // Note: The current implementation should never increase space, but if an additional indexer
        // is ever used, this may change.
        //
        // Only write the static index if it actually saves space
        if size_before > compacted_data_size {
            info!("Compaction will save space. Writing static index.");
            // let indexed_up_to = compacted_storage.tail_offset.load(Ordering::Acquire);

            let mut file_guard = compacted_storage
                .file
                .write()
                .map_err(|e| std::io::Error::other(format!("Lock poisoned: {e}")))?;
            file_guard.flush()?;
        } else {
            info!(
                "Compaction would increase file size (data w/ indexing: {compacted_data_size}). Skipping static index generation.",
            );
        }

        drop(compacted_storage);

        debug!("Compaction successful. Swapping files...");
        std::fs::rename(&compacted_path, &self.path)?;
        info!("Compaction file swap complete.");
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
        let total_size = self.file_size().unwrap_or(0);
        let mut unique_entry_size: u64 = 0;
        let mut seen_keys = HashSet::with_hasher(Xxh3BuildHasher);

        for entry in self.iter_entries() {
            if seen_keys.insert(entry.key_hash()) {
                unique_entry_size += entry.file_size() as u64;
            }
        }
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
        self.get_mmap_arc().as_ptr()
    }

    #[inline]
    fn get_mmap_arc(&self) -> Arc<Mmap> {
        let guard = self.mmap.lock().unwrap();
        let mmap_clone = guard.clone();
        drop(guard);
        mmap_clone
    }
}

impl DataStoreWriter for DataStore {
    fn write_stream<R: Read>(&self, key: &[u8], reader: &mut R) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.write_stream_with_key_hash(key_hash, reader)
    }

    fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.write_with_key_hash(key_hash, payload)
    }

    // TODO: Change signature to: fn batch_write(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<u64> {
    fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64> {
        let (keys, payloads): (Vec<_>, Vec<_>) = entries.iter().cloned().unzip();
        let hashes = compute_hash_batch(&keys);
        let hashed_entries = hashes.into_iter().zip(payloads).collect::<Vec<_>>();
        self.batch_write_hashed_payloads(hashed_entries, false)
    }

    fn rename(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64> {
        if old_key == new_key {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot rename a key to itself",
            ));
        }

        let old_entry = self.read(old_key)?.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "Old key not found")
        })?;
        let mut old_entry_stream = EntryStream::from(old_entry);

        self.write_stream(new_key, &mut old_entry_stream)?;

        let new_offset = self.delete(old_key)?;
        Ok(new_offset)
    }

    fn copy(&self, key: &[u8], target: &DataStore) -> Result<u64> {
        if self.path == target.path {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Cannot copy entry to the same storage ({:?}). Use `rename` instead.",
                    self.path
                ),
            ));
        }

        let entry_handle = self.read(key)?.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Key not found: {:?}", String::from_utf8_lossy(key)),
            )
        })?;
        self.copy_handle(&entry_handle, target)
    }

    fn transfer(&self, key: &[u8], target: &DataStore) -> Result<u64> {
        self.copy(key, target)?;
        self.delete(key)
    }

    fn delete(&self, key: &[u8]) -> Result<u64> {
        let key_hash = compute_hash(key);
        // TODO: Check prior exists before deletion
        self.batch_write_hashed_payloads(vec![(key_hash, &NULL_BYTE)], true)
    }

    // TODO: Implement batch_delete
}

impl DataStoreReader for DataStore {
    type EntryHandleType = EntryHandle;

    fn exists(&self, key: &[u8]) -> Result<bool> {
        Ok(self.read(key)?.is_some())
    }

    fn read(&self, key: &[u8]) -> Result<Option<EntryHandle>> {
        let key_hash = compute_hash(key);
        let key_indexer_guard = self
            .key_indexer
            .read()
            .map_err(|_| Error::other("key-index lock poisoned"))?;
        let mmap_arc = self.get_mmap_arc();

        Ok(self.read_entry_with_context(key, key_hash, &mmap_arc, &key_indexer_guard))
    }

    fn read_last_entry(&self) -> Result<Option<EntryHandle>> {
        let mmap_arc = self.get_mmap_arc();
        let tail_offset = self.tail_offset.load(std::sync::atomic::Ordering::Acquire);
        if tail_offset < METADATA_SIZE as u64 || mmap_arc.is_empty() {
            return Ok(None);
        }

        let metadata_offset = (tail_offset - METADATA_SIZE as u64) as usize;
        if metadata_offset + METADATA_SIZE > mmap_arc.len() {
            return Ok(None);
        }

        let metadata_bytes = &mmap_arc[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        let entry_start = metadata.prev_offset as usize;
        let entry_end = metadata_offset;
        if entry_start >= entry_end || entry_end > mmap_arc.len() {
            return Ok(None);
        }

        Ok(Some(EntryHandle {
            mmap_arc,
            range: entry_start..entry_end,
            metadata,
        }))
    }

    fn batch_read(&self, keys: &[&[u8]]) -> Result<Vec<Option<EntryHandle>>> {
        let mmap_arc = self.get_mmap_arc();
        let key_indexer_guard = self
            .key_indexer
            .read()
            .map_err(|_| Error::other("Key-index lock poisoned during `batch_read`"))?;

        let hashes = compute_hash_batch(keys);

        let results = hashes
            .into_iter()
            .zip(keys.iter())
            .map(|(key_hash, &key)| {
                self.read_entry_with_context(key, key_hash, &mmap_arc, &key_indexer_guard)
            })
            .collect();

        Ok(results)
    }

    fn read_metadata(&self, key: &[u8]) -> Result<Option<EntryMetadata>> {
        Ok(self.read(key)?.map(|entry| entry.metadata().clone()))
    }

    fn len(&self) -> Result<usize> {
        let read_guard = self
            .key_indexer
            .read()
            .map_err(|_| Error::other("Key-index lock poisoned during `len`"))?;

        Ok(read_guard.len())
    }

    fn is_empty(&self) -> Result<bool> {
        let len = self.len()?;

        Ok(len == 0)
    }

    fn file_size(&self) -> Result<u64> {
        std::fs::metadata(&self.path).map(|meta| meta.len())
    }
}
