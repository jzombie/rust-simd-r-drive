use crate::storage_engine::constants::*;
use crate::storage_engine::digest::{compute_checksum, compute_hash, Xxh3BuildHasher};
use crate::storage_engine::simd_copy;
use crate::storage_engine::{EntryHandle, EntryIterator, EntryMetadata, KeyIndexer};
use log::{debug, info, warn};
use memmap2::Mmap;
use std::collections::HashSet;
use std::convert::From;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Result, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use std::sync::atomic::{AtomicU64, Ordering};

/// Append-Only Storage Engine
pub struct DataStore {
    file: Arc<RwLock<BufWriter<File>>>,
    mmap: Arc<Mutex<Arc<Mmap>>>,
    last_offset: AtomicU64,
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

        // First mmap the file
        let mmap = Self::init_mmap(&file)?;

        // Recover valid chain using mmap, not file
        let final_len = Self::recover_valid_chain(&mmap, file_len)?;

        if final_len < file_len {
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
            last_offset: final_len.into(),
            key_indexer: Arc::new(RwLock::new(key_indexer)),
            path: path.to_path_buf(),
        })
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
    /// - `key_hash_offsets`: A slice of `(key_hash, last_offset)` tuples containing
    ///   the latest key mappings to be added to the index.
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
    ) -> std::io::Result<()> {
        // Create a new Mmap from the file
        let new_mmap = Self::init_mmap(write_guard)?;

        // Obtain the lock guards
        let mut mmap_guard = self.mmap.lock().unwrap();
        let mut key_indexer_guard = self.key_indexer.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire index lock")
        })?;

        // Update last_offset (or any other fields)
        let new_offset = write_guard.get_ref().metadata()?.len();
        self.last_offset
            .store(new_offset, std::sync::atomic::Ordering::Release);

        for (key_hash, last_offset) in key_hash_offsets.iter() {
            key_indexer_guard.insert(*key_hash, *last_offset);
        }

        // Overwrite the old Arc<Mmap> with the new one
        *mmap_guard = Arc::new(new_mmap);

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

    /// Opens the storage file in **append mode**.
    ///
    /// This function opens the file with both **read and write** access.
    /// If the file does not exist, it is created automatically.
    ///
    /// # Windows Note:
    /// - Directly opening in **append mode** can cause issues on Windows.
    /// - Instead, the file is opened normally and the **cursor is moved to the end**.
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

    /// Retrieves an iterator over all valid entries in the storage.
    ///
    /// This iterator allows scanning the storage file and retrieving **only the most recent**
    /// versions of each key.
    ///
    /// # Returns:
    /// - An `EntryIterator` instance for iterating over valid entries.
    pub fn iter_entries(&self) -> EntryIterator {
        // 1. Lock the mutex
        let guard = self.mmap.lock().unwrap();

        // 2. Clone the Arc<Mmap>
        let mmap_clone = guard.clone();

        // 3. Drop guard so others can proceed
        drop(guard);

        // 4. Get the actual last offset
        let last_offset = self.last_offset.load(Ordering::Acquire);

        EntryIterator::new(mmap_clone, last_offset)
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
    pub fn write_stream<R: Read>(&self, key: &[u8], reader: &mut R) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.write_stream_with_key_hash(key_hash, reader)
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
    fn write_stream_with_key_hash<R: Read>(&self, key_hash: u64, reader: &mut R) -> Result<u64> {
        let mut file: std::sync::RwLockWriteGuard<'_, BufWriter<File>> =
            self.file.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire file lock")
            })?;

        let prev_offset = self.last_offset.load(Ordering::Acquire);

        let mut buffer = vec![0; WRITE_STREAM_BUFFER_SIZE]; // 64KB chunks
        let mut total_written = 0;

        let mut checksum_state = crc32fast::Hasher::new(); // Use incremental checksum

        // Stream and write chunks directly to disk
        while let Ok(bytes_read) = reader.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }

            file.write_all(&buffer[..bytes_read])?;
            checksum_state.update(&buffer[..bytes_read]); // Update checksum incrementally
            total_written += bytes_read;
        }

        let checksum_u32 = checksum_state.finalize(); // Finalize checksum after writing
        let checksum = checksum_u32.to_le_bytes();

        // // Write metadata **after** payload
        let metadata = EntryMetadata {
            key_hash,
            prev_offset,
            checksum,
        };
        file.write_all(&metadata.serialize())?;
        file.flush()?; // **Ensure data is persisted to disk**

        let new_offset = prev_offset + total_written as u64 + METADATA_SIZE as u64;
        self.last_offset.store(new_offset, Ordering::Release);

        self.reindex(&file, &vec![(key_hash, new_offset - METADATA_SIZE as u64)])?; // Ensure mmap updates

        Ok(new_offset)
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
    pub fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.write_with_key_hash(key_hash, payload)
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
        self.batch_write_hashed_payloads(vec![(key_hash, payload)])
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
    pub fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64> {
        let hashed_entries: Vec<(u64, &[u8])> = entries
            .iter()
            .map(|(key, payload)| (compute_hash(key), *payload))
            .collect();
        self.batch_write_hashed_payloads(hashed_entries)
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
    pub fn batch_write_hashed_payloads(&self, hashed_payloads: Vec<(u64, &[u8])>) -> Result<u64> {
        let mut file = self.file.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire file lock")
        })?; // Lock only the file, not the whole struct

        let mut buffer = Vec::new();
        let mut last_offset = self.last_offset.load(Ordering::Acquire);

        let mut key_hash_offsets: Vec<(u64, u64)> = Vec::with_capacity(hashed_payloads.len());

        for (key_hash, payload) in hashed_payloads {
            if payload.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Payload cannot be empty.",
                ));
            }

            let prev_offset = last_offset;
            let checksum = compute_checksum(payload);

            let metadata = EntryMetadata {
                key_hash,
                prev_offset,
                checksum,
            };

            let mut entry: Vec<u8> = vec![0u8; payload.len() + METADATA_SIZE];

            // Use SIMD to copy payload into buffer
            simd_copy(&mut entry[..payload.len()], payload);

            // Copy metadata normally (small size, not worth SIMD)
            entry[payload.len()..].copy_from_slice(&metadata.serialize());

            buffer.extend_from_slice(&entry);

            last_offset += entry.len() as u64;

            key_hash_offsets.push((key_hash, last_offset - METADATA_SIZE as u64));
        }

        file.write_all(&buffer)?;
        file.flush()?;

        self.last_offset.store(last_offset, Ordering::Release);

        self.reindex(&file, &key_hash_offsets)?; // Ensure mmap updates

        Ok(self.last_offset.load(Ordering::Acquire))
    }

    /// Reads the last entry stored in the database.
    ///
    /// This method retrieves the **most recently appended** entry in the storage.
    /// It does not check for key uniqueness; it simply returns the last-written
    /// data segment from the memory-mapped file.
    ///
    /// # Returns:
    /// TODO: Update return type
    /// - `Some(&[u8])` containing the binary payload of the last entry.
    /// - `None` if the storage is empty or corrupted.
    /// Zero-copy: no bytes are duplicated, just reference-counted.
    pub fn read_last_entry(&self) -> Option<EntryHandle> {
        // 1) Lock the `Mutex<Arc<Mmap>>` to safely access the current map
        let guard = self.mmap.lock().unwrap();

        // 2) Clone the inner Arc<Mmap> so we can drop the lock quickly
        let mmap_arc = Arc::clone(&*guard);

        // 3) Release the lock (other threads can proceed)
        drop(guard);

        // 4) Use `mmap_arc` to find the last entry boundaries
        let last_offset = self.last_offset.load(std::sync::atomic::Ordering::Acquire);

        if last_offset < METADATA_SIZE as u64 || mmap_arc.len() == 0 {
            return None;
        }

        let metadata_offset = (last_offset - METADATA_SIZE as u64) as usize;
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

    /// Retrieves the most recent value associated with a given key.
    ///
    /// This method **efficiently looks up a key** using a fast in-memory index,
    /// and returns the latest corresponding value if found.
    ///
    /// # Parameters:
    /// - `key`: The **binary key** whose latest value is to be retrieved.
    ///
    /// # Returns:
    /// // TODO: Update return type
    /// - `Some(&[u8])` containing the latest value associated with the key.
    /// - `None` if the key does not exist.
    pub fn read(&self, key: &[u8]) -> Option<EntryHandle> {
        let key_hash = compute_hash(key);

        // 1) Lock the mutex to get our Arc<Mmap>
        let guard = self.mmap.lock().unwrap();
        let mmap_arc = Arc::clone(&*guard); // Clone so we can drop the lock quickly
        drop(guard);

        // 2) Re-check last_offset, ensure the file is big enough
        let last_offset = self.last_offset.load(std::sync::atomic::Ordering::Acquire);
        if last_offset < METADATA_SIZE as u64 || mmap_arc.len() == 0 {
            return None;
        }

        // 3) Look up the offset in the in-memory key index
        let offset = *self.key_indexer.read().ok()?.get(&key_hash)?;

        // 4) Grab the metadata from the mapped file
        if offset as usize + METADATA_SIZE > mmap_arc.len() {
            return None;
        }
        let metadata_bytes = &mmap_arc[offset as usize..offset as usize + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        // 5) Extract the actual entry range
        let entry_start = metadata.prev_offset as usize;
        let entry_end = offset as usize;
        if entry_start >= entry_end || entry_end > mmap_arc.len() {
            return None;
        }

        // Check for tombstone (NULL_BYTE)
        if entry_end - entry_start == 1 && &mmap_arc[entry_start..entry_end] == NULL_BYTE {
            return None;
        }

        // 6) Return a handle that *owns* the Arc and the slice range
        Some(EntryHandle {
            mmap_arc,
            range: entry_start..entry_end,
            metadata,
        })
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
    pub fn read_metadata(&self, key: &[u8]) -> Option<EntryMetadata> {
        self.read(key).map(|entry| entry.metadata().clone())
    }

    // TODO: Document
    pub fn copy_entry(&self, key: &[u8], target: &DataStore) -> Result<u64> {
        let entry_handle = self.read(key).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Key not found: {:?}", key),
            )
        })?;

        self.copy_entry_handle(&entry_handle, target)
    }

    // TODO: Document return type
    /// Low-level copy functionality.
    fn copy_entry_handle(&self, entry: &EntryHandle, target: &DataStore) -> Result<u64> {
        let metadata = entry.metadata();

        // Append to the compacted storage
        let result = target.write_with_key_hash(metadata.key_hash, entry)?;

        Ok(result)
    }

    // TODO: Document
    pub fn move_entry(&self, key: &[u8], target: &DataStore) -> Result<u64> {
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
    pub fn delete_entry(&self, key: &[u8]) -> Result<u64> {
        self.write(key, &NULL_BYTE)
    }

    // TODO: Return `Err` if more than one thread
    /// Compacts the storage by keeping only the latest version of each key.
    pub fn compact(&mut self) -> std::io::Result<()> {
        let compacted_path = self.path.with_extension("bk");
        debug!("Starting compaction. Writing to: {:?}", compacted_path);

        // 1) Create a new DataStore instance for the compacted file
        let mut compacted_storage = DataStore::open(&compacted_path)?;

        // 2) Iterate over all valid entries using your iterator
        for entry in self.iter_entries() {
            self.copy_entry_handle(&entry, &mut compacted_storage)?;
        }

        // 4) Flush the compacted file
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

    /// Counts the number of **active** entries in the storage.
    ///
    /// This method iterates through the storage file and counts **only the latest versions**
    /// of keys, skipping deleted or outdated entries.
    ///
    /// # Returns:
    /// - The **total count** of active key-value pairs in the database.
    pub fn count(&self) -> usize {
        self.iter_entries().count()
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

        // 1) Briefly lock the Mutex to clone the Arc<Mmap>
        let guard = self.mmap.lock().unwrap();
        let mmap_arc = Arc::clone(&*guard);
        drop(guard);

        // 2) Now we can safely iterate zero-copy
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

        // 3) Return the difference between total size and the unique size
        total_size.saturating_sub(unique_entry_size)
    }

    /// Retrieves the **total size** of the storage file.
    ///
    /// This method queries the **current file size** of the storage file on disk.
    ///
    /// # Returns:
    /// - `Ok(file_size_in_bytes)` if successful.
    /// - `Err(std::io::Error)` if the file cannot be accessed.
    pub fn get_storage_size(&self) -> Result<u64> {
        std::fs::metadata(&self.path).map(|meta| meta.len())
    }
}
