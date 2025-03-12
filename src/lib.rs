use memmap2::Mmap;
use std::collections::{HashMap, HashSet};
use std::convert::From;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Result, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
mod simd_copy;
use simd_copy::simd_copy;
mod digest;
use digest::{compute_checksum, compute_hash, Xxh3BuildHasher};
use log::{debug, info, warn};
use std::sync::atomic::{AtomicU64, Ordering};

// TODO: When creating new files, start off with initial metadata regarding the software version
// used to create the file, and ensure whatever key is used for that is protected from writing.
// Also, use a [semver] heuristic to determine compatibility (or simply see if it can be parsed).
//
// TODO: Use keys with a null byte for the leading byte to represent "hidden" entries?

/// Enable `*entry_handle` to act like a `&[u8]`
impl std::ops::Deref for EntryHandle {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

/// Let us do: `assert_eq!(entry_handle, b"some bytes")`
impl PartialEq<[u8]> for EntryHandle {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

/// Allow comparisons with `&[u8]`
impl PartialEq<&[u8]> for EntryHandle {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_slice() == *other
    }
}

/// Allow comparisons with `Vec<u8>`
impl PartialEq<Vec<u8>> for EntryHandle {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

/// Metadata structure (fixed 20 bytes at the end of each entry)
const METADATA_SIZE: usize = 20;
const KEY_HASH_RANGE: std::ops::Range<usize> = 0..8;
const PREV_OFFSET_RANGE: std::ops::Range<usize> = 8..16;
const CHECKSUM_RANGE: std::ops::Range<usize> = 16..20;

// Marker indicating a logically deleted entry in the storage
const NULL_BYTE: [u8; 1] = [0];

// Define checksum length explicitly since `CHECKSUM_RANGE.len()` isn't `const`
const CHECKSUM_LEN: usize = CHECKSUM_RANGE.end - CHECKSUM_RANGE.start;

/// Metadata structure for an append-only storage entry.
///
/// This structure stores metadata associated with each entry in the append-only storage.
/// It includes a hash of the key for quick lookups, an offset pointing to the previous
/// entry in the chain, and a checksum for integrity verification.
///
/// ## Entry Storage Layout
///
/// Each entry consists of a **variable-sized payload** followed by a **fixed-size metadata block**.
/// The metadata is stored **at the end** of the entry to simplify sequential writes and enable
/// efficient recovery.
///
/// - **Offset `0` → `N`**: **Payload** (variable-length data)
/// - **Offset `N` → `N + 8`**: **Key Hash** (64-bit XXH3 hash of the key, used for fast lookups)
/// - **Offset `N + 8` → `N + 16`**: **Prev Offset** (absolute file offset pointing to the previous version)
/// - **Offset `N + 16` → `N + 20`**: **Checksum** (full 32-bit CRC32C checksum for integrity verification)
///
/// **Total Size**: `N + 20` bytes, where `N` is the length of the payload.
///
/// ## Notes
/// - The `prev_offset` forms a **backward-linked chain** for each key.
/// - The checksum is **not cryptographically secure** but serves as a quick integrity check.
/// - The first entry for a key has `prev_offset = 0`, indicating no previous version.
#[repr(C)]
#[derive(Debug)]
pub struct EntryMetadata {
    key_hash: u64,     // 8 bytes (hashed key for lookup)
    prev_offset: u64,  // 8 bytes (absolute offset of previous entry)
    checksum: [u8; 4], // 4 bytes (checksum for integrity)
}

impl EntryMetadata {
    /// Serializes the metadata into a byte array.
    ///
    /// Converts the `EntryMetadata` structure into a fixed-size array
    /// for efficient storage. The serialized format ensures compatibility
    /// with disk storage and memory-mapped access.
    ///
    /// # Format:
    /// - Encodes the key hash, previous offset, and checksum into their respective byte ranges.
    /// - Uses little-endian encoding for numeric values.
    ///
    /// # Returns:
    /// - A byte array containing the serialized metadata.
    #[inline]
    fn serialize(&self) -> [u8; METADATA_SIZE] {
        let mut buf = [0u8; METADATA_SIZE];

        buf[KEY_HASH_RANGE].copy_from_slice(&self.key_hash.to_le_bytes());
        buf[PREV_OFFSET_RANGE].copy_from_slice(&self.prev_offset.to_le_bytes());
        buf[CHECKSUM_RANGE].copy_from_slice(&self.checksum);

        buf
    }

    /// Deserializes a byte slice into an `EntryMetadata` instance.
    ///
    /// Reconstructs an `EntryMetadata` structure from a byte slice,
    /// following the predefined binary format. Extracts the key hash,
    /// previous offset, and checksum while ensuring correctness through
    /// explicit range-based indexing.
    ///
    /// # Parameters:
    /// - `data`: A byte slice containing the serialized metadata.
    ///
    /// # Returns:
    /// - A reconstructed `EntryMetadata` instance.
    ///
    /// # Panics:
    /// - If the provided `data` slice is too small.
    #[inline]
    fn deserialize(data: &[u8]) -> Self {
        Self {
            key_hash: u64::from_le_bytes(data[KEY_HASH_RANGE].try_into().unwrap()),
            prev_offset: u64::from_le_bytes(data[PREV_OFFSET_RANGE].try_into().unwrap()),
            // Use a `const`-safe way to construct a fixed-size array
            checksum: {
                let mut checksum = [0u8; CHECKSUM_LEN];
                checksum.copy_from_slice(&data[CHECKSUM_RANGE]);
                checksum
            },
        }
    }
}

/// Iterator for traversing entries in the append-only storage.
///
/// This iterator scans entries stored in the memory-mapped file (`mmap`),
/// reading each entry's metadata and returning unique key-value pairs.
/// The iteration proceeds **backward**, following the chain of previous
/// offsets stored in each entry.
///
/// ## Behavior:
/// - **Starts at `last_offset`** and moves backward using the `prev_offset` field.
/// - **Ensures unique keys** by tracking seen hashes in a `HashSet`.
/// - **Skips deleted entries**, which are represented by empty data.
/// - **Stops when reaching an invalid or out-of-bounds offset.**
pub struct EntryIterator {
    mmap: Arc<Mmap>, // Borrow from Arc<Mmap> (zero-copy)
    cursor: u64,
    seen_keys: HashSet<u64, Xxh3BuildHasher>,
}

impl EntryIterator {
    /// Creates a new iterator for scanning storage entries.
    ///
    /// Initializes an iterator starting at the provided `last_offset` and
    /// moving backward through the storage file. The iterator ensures that
    /// only the most recent version of each key is returned.
    ///
    /// # Parameters:
    /// - `mmap`: A reference to the memory-mapped file.
    /// - `last_offset`: The file offset where iteration starts.
    ///
    /// # Returns:
    /// - A new `EntryIterator` instance.
    pub fn new(mmap: Arc<Mmap>, last_offset: u64) -> Self {
        Self {
            mmap,
            cursor: last_offset,
            seen_keys: HashSet::with_hasher(Xxh3BuildHasher),
        }
    }
}

impl Iterator for EntryIterator {
    type Item = EntryHandle;

    /// Advances the iterator to the next valid entry.
    ///
    /// Reads and parses the metadata for the current entry, determines its
    /// boundaries, and extracts its data. If the key has already been seen,
    /// the iterator skips it to ensure that only the latest version is returned.
    ///
    /// # Returns:
    /// - `Some(&[u8])` containing the entry data if valid.
    /// - `None` when no more valid entries are available.
    fn next(&mut self) -> Option<Self::Item> {
        // Stop iteration if cursor is out of valid range
        if self.cursor < METADATA_SIZE as u64 || self.mmap.len() == 0 {
            return None;
        }

        // Locate metadata at the current cursor position
        let metadata_offset = (self.cursor - METADATA_SIZE as u64) as usize;
        let metadata_bytes = &self.mmap[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        let entry_start = metadata.prev_offset as usize;
        let entry_end = metadata_offset;

        // Ensure valid entry bounds before reading
        if entry_start >= entry_end || entry_end > self.mmap.len() {
            return None;
        }

        // Move cursor backward to follow the chain
        self.cursor = metadata.prev_offset; // Move cursor backward

        // Skip duplicate keys (ensuring only the latest value is returned)
        if !self.seen_keys.insert(metadata.key_hash) {
            return self.next(); // Skip if already seen
        }

        let entry_data = &self.mmap[entry_start..entry_end];

        // Skip deleted entries (denoted by empty data)
        if entry_data == NULL_BYTE {
            return self.next();
        }

        // Some(entry_data.into())

        Some(EntryHandle {
            mmap_arc: Arc::clone(&self.mmap),
            range: entry_start..entry_end,
            metadata,
        })
    }
}

/// Zero-copy owner of a sub-slice in an `Arc<Mmap>`.
/// Lets you access the bytes of the entry as long as this struct is alive.
#[derive(Debug)]
pub struct EntryHandle {
    mmap_arc: Arc<Mmap>,

    /// The payload range
    range: Range<usize>,

    metadata: EntryMetadata,
}

impl EntryHandle {
    /// Returns the sub-slice of bytes corresponding to the entry.
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap_arc[self.range.clone()]
    }

    /// Returns a reference to the entry’s parsed metadata.
    pub fn metadata(&self) -> &EntryMetadata {
        &self.metadata
    }

    /// Returns the payload size.
    pub fn size(&self) -> usize {
        self.range.len()
    }

    /// Returns the payload size plus metadata.
    pub fn size_with_metadata(&self) -> usize {
        self.range.len() + METADATA_SIZE
    }

    pub fn key_hash(&self) -> u64 {
        self.metadata.key_hash
    }

    pub fn checksum(&self) -> u32 {
        u32::from_le_bytes(self.metadata.checksum)
    }

    pub fn is_valid_checksum(&self) -> bool {
        let data = self.as_slice();
        let computed = compute_checksum(data);
        self.metadata.checksum == computed
    }

    /// Returns the absolute start byte offset within the mapped file.
    pub fn start_offset(&self) -> usize {
        self.range.start
    }

    /// Returns the absolute end byte offset within the mapped file.
    pub fn end_offset(&self) -> usize {
        self.range.end
    }

    pub fn offset_range(&self) -> Range<usize> {
        self.range.clone()
    }

    /// Returns the pointer range in the current process's memory.
    ///
    /// This is the actual *virtual address* space that the entry occupies.
    /// - The `start_ptr` points to the beginning of the payload in memory.
    /// - The `end_ptr` is `start_ptr + payload_length`.
    ///
    /// **Note**: These addresses are valid only in this process and can become
    /// invalid if the memory map is remapped or unmapped.
    pub fn address_range(&self) -> std::ops::Range<*const u8> {
        let slice = self.as_slice();
        let start_ptr = slice.as_ptr();
        let end_ptr = unsafe { start_ptr.add(slice.len()) };
        start_ptr..end_ptr
    }
}

/// Append-Only Storage Engine
pub struct AppendStorage {
    file: Arc<RwLock<BufWriter<File>>>, // ✅ Wrap file in Arc<RwLock<>> for safe concurrent writes
    mmap: Arc<Mutex<Arc<Mmap>>>,        // Atomic pointer to an mmap for zero-copy reads
    last_offset: AtomicU64,
    key_index: Arc<RwLock<HashMap<u64, u64, Xxh3BuildHasher>>>, // ✅ Wrap in RwLock for safe writes
    path: PathBuf,
}

impl IntoIterator for AppendStorage {
    type Item = EntryHandle;
    type IntoIter = EntryIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_entries()
    }
}

impl From<PathBuf> for AppendStorage {
    /// Creates an `AppendStorage` instance from a `PathBuf`.
    ///
    /// This allows creating a storage instance **directly from a file path**.
    ///
    /// # Panics:
    /// - If the file cannot be opened or mapped into memory.
    fn from(path: PathBuf) -> Self {
        AppendStorage::open(&path).expect("Failed to open storage file")
    }
}

impl AppendStorage {
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
    /// - `Ok(AppendStorage)`: A **new storage instance**.
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

        let key_index = Self::build_key_index(&mmap, final_len);

        Ok(Self {
            file: Arc::new(RwLock::new(file)), // ✅ Wrap in RwLock
            // mmap: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(mmap)))), // ✅ Correct
            mmap: Arc::new(Mutex::new(Arc::new(mmap))),
            last_offset: final_len.into(),
            key_index: Arc::new(RwLock::new(key_index)), // ✅ Wrap HashMap in RwLock
            path: path.to_path_buf(),
        })
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

    /// Builds an in-memory index for **fast key lookups**.
    ///
    /// This function **scans the storage file** and constructs a **hashmap**
    /// mapping each key's hash to its **latest** entry's file offset.
    ///
    /// # How It Works:
    /// - Iterates **backward** from the latest offset to find the most recent version of each key.
    /// - Skips duplicate keys to keep only the **most recent** entry.
    /// - Stores the **latest offset** of each unique key in the index.
    ///
    /// # Parameters:
    /// - `mmap`: A reference to the **memory-mapped file**.
    /// - `last_offset`: The **final byte offset** in the file (starting point for scanning).
    ///
    /// # Returns:
    /// - A `HashMap<u64, u64>` mapping `key_hash` → `latest offset`.
    fn build_key_index(mmap: &Mmap, last_offset: u64) -> HashMap<u64, u64, Xxh3BuildHasher> {
        let mut index = HashMap::with_hasher(Xxh3BuildHasher);
        let mut seen_keys = HashSet::with_hasher(Xxh3BuildHasher);
        let mut cursor = last_offset;

        while cursor >= METADATA_SIZE as u64 {
            let metadata_offset = cursor as usize - METADATA_SIZE;
            let metadata_bytes = &mmap[metadata_offset..metadata_offset + METADATA_SIZE];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            // If this key is already seen, skip it (to keep the latest entry only)
            if seen_keys.contains(&metadata.key_hash) {
                cursor = metadata.prev_offset;
                continue;
            }

            // Mark key as seen and store its latest offset
            seen_keys.insert(metadata.key_hash);
            index.insert(metadata.key_hash, metadata_offset as u64);

            // Stop when reaching the first valid entry
            if metadata.prev_offset == 0 {
                break;
            }

            cursor = metadata.prev_offset;
        }

        index
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

    /// Re-maps the storage file to ensure that the latest updates are visible.
    ///
    /// This method is called **after a write operation** to reload the memory-mapped file
    /// and ensure that newly written data is accessible for reading.
    fn remap_file(&self) -> std::io::Result<()> {
        // 1) Acquire file read lock
        let file_guard = self.file.read().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to acquire file read lock",
            )
        })?;

        // 2) Create a new Mmap from the file
        let new_mmap = unsafe { memmap2::MmapOptions::new().map(file_guard.get_ref())? };

        // 3) Replace the old Arc<Mmap> with a new Arc<Mmap>
        {
            // Lock the mutex to get a mutable reference to the current Arc<Mmap>
            let mut guard = self.mmap.lock().unwrap();

            // Overwrite the old Arc<Mmap> with the new one
            *guard = Arc::new(new_mmap);
        } // Once the guard drops here, other threads can lock again

        // 4) Update last_offset (or any other fields)
        let new_offset = file_guard.get_ref().metadata()?.len();
        self.last_offset
            .store(new_offset, std::sync::atomic::Ordering::Release);

        Ok(())
    }

    // TODO: Document return type
    /// High-level method: Appends a single entry by key
    pub fn append_entry(&mut self, key: &[u8], payload: &[u8]) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.append_entry_with_key_hash(key_hash, payload)
    }

    // TODO: Document return type
    /// High-level method: Appends a single entry by key hash
    pub fn append_entry_with_key_hash(&mut self, key_hash: u64, payload: &[u8]) -> Result<u64> {
        self.batch_write(vec![(key_hash, payload)])
    }

    // TODO: Document return type
    /// Batch append multiple entries as a single transaction
    pub fn append_entries(&mut self, entries: &[(&[u8], &[u8])]) -> Result<u64> {
        let hashed_entries: Vec<(u64, &[u8])> = entries
            .iter()
            .map(|(key, payload)| (compute_hash(key), *payload))
            .collect();
        self.batch_write(hashed_entries)
    }

    // TODO: Document return type
    /// Batch append multiple entries with precomputed key hashes
    pub fn append_entries_with_key_hashes(&mut self, entries: &[(u64, &[u8])]) -> Result<u64> {
        self.batch_write(entries.to_vec())
    }

    /// Core transaction method (Handles locking, writing, flushing)
    fn batch_write(&mut self, entries: Vec<(u64, &[u8])>) -> Result<u64> {
        {
            let mut file = self.file.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire file lock")
            })?; // ✅ Lock only the file, not the whole struct

            let mut buffer = Vec::new();
            let mut last_offset = self.last_offset.load(Ordering::Acquire);

            for (key_hash, payload) in entries {
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

                let mut entry = vec![0u8; payload.len() + METADATA_SIZE];

                // Use SIMD to copy payload into buffer
                simd_copy(&mut entry[..payload.len()], payload);

                // Copy metadata normally (small size, not worth SIMD)
                entry[payload.len()..].copy_from_slice(&metadata.serialize());

                buffer.extend_from_slice(&entry);

                last_offset += entry.len() as u64;

                // ✅ Lock the key index before modifying
                {
                    let mut key_index = self.key_index.write().map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Failed to acquire index lock",
                        )
                    })?;
                    key_index.insert(key_hash, last_offset - METADATA_SIZE as u64);
                } // ✅ Unlocks automatically here
            }

            file.write_all(&buffer)?;
            file.flush()?;

            self.last_offset.store(last_offset, Ordering::Release);
        }

        self.remap_file()?;

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
    pub fn get_entry_by_key(&self, key: &[u8]) -> Option<EntryHandle> {
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
        let offset = *self.key_index.read().ok()?.get(&key_hash)?;

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

    // TODO: Document
    pub fn copy_entry(&self, key: &[u8], target: &mut AppendStorage) -> Result<u64> {
        let entry_handle = self.get_entry_by_key(key).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Key not found: {:?}", key),
            )
        })?;

        self.copy_entry_handle(&entry_handle, target)
    }

    // TODO: Document return type
    /// Low-level copy functionality.
    fn copy_entry_handle(&self, entry: &EntryHandle, target: &mut AppendStorage) -> Result<u64> {
        let metadata = entry.metadata();

        // Append to the compacted storage
        let result = target.append_entry_with_key_hash(metadata.key_hash, &entry)?;

        Ok(result)
    }

    // TODO: Document
    pub fn move_entry(&mut self, key: &[u8], target: &mut AppendStorage) -> Result<u64> {
        self.copy_entry(key, target)?;

        self.delete_entry(&key)
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
    pub fn delete_entry(&mut self, key: &[u8]) -> Result<u64> {
        self.append_entry(key, &NULL_BYTE)
    }

    /// Compacts the storage by keeping only the latest version of each key.
    pub fn compact(&mut self) -> std::io::Result<()> {
        let compacted_path = self.path.with_extension("bk");
        debug!("Starting compaction. Writing to: {:?}", compacted_path);

        // 1) Create a new AppendStorage instance for the compacted file
        let mut compacted_storage = AppendStorage::open(&compacted_path)?;

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
