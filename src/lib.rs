use crc32fast::Hasher as Crc32FastHasher;
use memmap2::Mmap;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::io::{BufWriter, Result, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use xxhash_rust::xxh3::xxh3_64;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

#[inline]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_copy_x86(dst: &mut [u8], src: &[u8]) {
    let len = dst.len().min(src.len());
    let chunks = len / 32; // AVX2 processes 32 bytes at a time

    let mut i = 0;
    while i < chunks * 32 {
        let data = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
        _mm256_storeu_si256(dst.as_mut_ptr().add(i) as *mut __m256i, data);
        i += 32;
    }

    // Copy remaining bytes normally
    dst[i..len].copy_from_slice(&src[i..len]);
}

#[inline]
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn simd_copy_arm(dst: &mut [u8], src: &[u8]) {
    let len = dst.len().min(src.len());
    let chunks = len / 16; // NEON processes 16 bytes at a time

    let mut i = 0;
    while i < chunks * 16 {
        let data = vld1q_u8(src.as_ptr().add(i));
        vst1q_u8(dst.as_mut_ptr().add(i), data);
        i += 16;
    }

    // Copy remaining bytes normally
    dst[i..len].copy_from_slice(&src[i..len]);
}

#[inline]
fn simd_copy(dst: &mut [u8], src: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        return simd_copy_x86(dst, src);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        return simd_copy_arm(dst, src);
    }

    // Fallback for unsupported architectures
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    dst.copy_from_slice(&src[..dst.len().min(src.len())]);
}

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawHandle;

/// Custom Hasher using XXH3
#[derive(Default)]
struct Xxh3Hasher {
    hash: u64,
}

impl Hasher for Xxh3Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.hash = xxh3_64(bytes);
    }

    fn finish(&self) -> u64 {
        self.hash
    }
}

/// Custom BuildHasher for HashMap
#[derive(Default, Clone)]
struct Xxh3BuildHasher;

impl BuildHasher for Xxh3BuildHasher {
    type Hasher = Xxh3Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        Xxh3Hasher::default()
    }
}

/// Metadata structure (fixed 19 bytes at the end of each entry)
const METADATA_SIZE: usize = 19;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct EntryMetadata {
    key_hash: u64,     // 8 bytes (hashed key for lookup)
    prev_offset: u64,  // 8 bytes (absolute offset of previous entry)
    checksum: [u8; 3], // 3 bytes (optional checksum for integrity)
}

impl EntryMetadata {
    fn serialize(&self) -> [u8; METADATA_SIZE] {
        let mut buf = [0u8; METADATA_SIZE];
        buf[0..8].copy_from_slice(&self.key_hash.to_le_bytes());
        buf[8..16].copy_from_slice(&self.prev_offset.to_le_bytes());
        buf[16..19].copy_from_slice(&self.checksum);
        buf
    }

    fn deserialize(data: &[u8]) -> Self {
        Self {
            key_hash: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            prev_offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            checksum: [data[16], data[17], data[18]],
        }
    }
}

/// Iterator for traversing entries in the append-only storage
pub struct EntryIterator<'a> {
    mmap: &'a Mmap,
    cursor: u64,
    seen_keys: HashSet<u64, Xxh3BuildHasher>,
}

impl<'a> EntryIterator<'a> {
    pub fn new(mmap: &'a Mmap, last_offset: u64) -> Self {
        Self {
            mmap,
            cursor: last_offset,
            seen_keys: HashSet::with_hasher(Xxh3BuildHasher),
        }
    }
}

impl<'a> Iterator for EntryIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < METADATA_SIZE as u64 || self.mmap.len() == 0 {
            return None;
        }

        let metadata_offset = (self.cursor - METADATA_SIZE as u64) as usize;
        let metadata_bytes = &self.mmap[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        let entry_start = metadata.prev_offset as usize;
        let entry_end = metadata_offset;

        if entry_start >= entry_end || entry_end > self.mmap.len() {
            return None;
        }

        self.cursor = metadata.prev_offset; // Move cursor backward

        // Check if key has been seen before
        if !self.seen_keys.insert(metadata.key_hash) {
            return self.next(); // Skip if already seen
        }

        let entry_data = &self.mmap[entry_start..entry_end];

        // Skip deleted entries (empty binary data)
        if entry_data == b"\0" {
            return self.next();
        }

        Some(entry_data)
    }
}

/// Append-Only Storage Engine
pub struct AppendStorage {
    file: BufWriter<File>,
    mmap: Arc<Mmap>,
    last_offset: u64,
    key_index: HashMap<u64, u64, Xxh3BuildHasher>, // Key → Offset map
    lock: Arc<RwLock<()>>,
    path: PathBuf,
}

impl<'a> IntoIterator for &'a AppendStorage {
    type Item = &'a [u8];
    type IntoIter = EntryIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_entries()
    }
}

impl AppendStorage {
    /// Returns an iterator over all stored entries (sequential read optimization)
    pub fn iter_entries(&self) -> EntryIterator {
        EntryIterator::new(&self.mmap, self.last_offset)
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = BufWriter::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?,
        );

        let file_len = file.get_ref().metadata()?.len();

        // First mmap the file
        let mmap = unsafe { memmap2::MmapOptions::new().map(file.get_ref())? };

        // Recover valid chain using mmap, not file
        let final_len = Self::recover_valid_chain(&mmap, file_len)?;
        file.get_ref().set_len(final_len)?; // Correct file size before remapping

        // Re-map the file after recovery
        let mmap = unsafe { memmap2::MmapOptions::new().map(file.get_ref())? };

        let key_index = Self::build_key_index(&mmap, final_len);

        Ok(Self {
            file,
            mmap: Arc::new(mmap),
            last_offset: final_len,
            key_index,
            lock: Arc::new(RwLock::new(())),
            path: path.to_path_buf(),
        })
    }

    fn build_key_index(mmap: &Mmap, last_offset: u64) -> HashMap<u64, u64, Xxh3BuildHasher> {
        let mut index = HashMap::with_hasher(Xxh3BuildHasher);
        let mut cursor = last_offset;

        while cursor >= METADATA_SIZE as u64 {
            let metadata_offset = cursor as usize - METADATA_SIZE;
            let metadata_bytes = &mmap[metadata_offset..metadata_offset + METADATA_SIZE];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            index.insert(metadata.key_hash, metadata_offset as u64); // Store offset

            if metadata.prev_offset == 0 {
                break;
            }

            cursor = metadata.prev_offset;
        }
        index
    }

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

                let entry_size = prev_metadata_offset - prev_metadata.prev_offset;
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
                eprintln!(
                    "✔ Found valid chain of {} entries. Ending at offset {}",
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

    /// Re-maps the file to ensure latest updates are visible
    fn remap_file(&mut self) -> Result<()> {
        self.mmap = Arc::new(unsafe { memmap2::MmapOptions::new().map(self.file.get_ref())? });
        Ok(())
    }

    /// High-level method: Appends a single entry by key
    pub fn append_entry(&mut self, key: &[u8], payload: &[u8]) -> Result<u64> {
        let key_hash = xxh3_64(key);
        self.append_entry_with_key_hash(key_hash, payload)
    }

    pub fn delete_entry(&mut self, key: &[u8]) -> Result<u64> {
        self.append_entry(key, b"\0")
    }

    /// High-level method: Appends a single entry by key hash
    pub fn append_entry_with_key_hash(&mut self, key_hash: u64, payload: &[u8]) -> Result<u64> {
        self.batch_write(vec![(key_hash, payload)])
    }

    /// Batch append multiple entries as a single transaction
    pub fn append_entries(&mut self, entries: &[(&[u8], &[u8])]) -> Result<u64> {
        let hashed_entries: Vec<(u64, &[u8])> = entries
            .iter()
            .map(|(key, payload)| (xxh3_64(key), *payload))
            .collect();
        self.batch_write(hashed_entries)
    }

    /// Batch append multiple entries with precomputed key hashes
    pub fn append_entries_with_key_hashes(&mut self, entries: &[(u64, &[u8])]) -> Result<u64> {
        self.batch_write(entries.to_vec())
    }

    /// Core transaction method (Handles locking, writing, flushing)
    fn batch_write(&mut self, entries: Vec<(u64, &[u8])>) -> Result<u64> {
        {
            let _write_lock = self.lock.write().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire write lock")
            })?;

            let mut buffer = Vec::new(); // Single buffer to hold all writes in this transaction
            let mut last_offset = self.last_offset;

            for (key_hash, payload) in entries {
                if payload.is_empty() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Payload cannot be empty.",
                    ));
                }

                let prev_offset = last_offset;
                let checksum = Self::compute_checksum(payload);

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

                buffer.extend_from_slice(&entry); // Append to transaction buffer

                last_offset += entry.len() as u64;

                // Update key index in-memory
                self.key_index
                    .insert(key_hash, last_offset - METADATA_SIZE as u64);
            }

            // Single write & flush for transaction
            self.file.write_all(&buffer)?;
            self.file.flush()?;

            self.last_offset = last_offset;
        }

        // 🔄 Remap the file **after** dropping the lock
        self.remap_file()?;

        Ok(self.last_offset)
    }

    /// Reads the last entry
    pub fn read_last_entry(&self) -> Option<&[u8]> {
        let _read_lock = self.lock.read().ok()?;

        if self.last_offset < METADATA_SIZE as u64 || self.mmap.len() == 0 {
            return None;
        }

        let metadata_offset = (self.last_offset - METADATA_SIZE as u64) as usize;
        let metadata_bytes = &self.mmap[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        let entry_start = metadata.prev_offset as usize;
        let entry_end = metadata_offset;
        if entry_start >= entry_end || entry_end > self.mmap.len() {
            return None;
        }

        Some(&self.mmap[entry_start..entry_end]) // Return reference instead of copying data
    }

    pub fn get_entry_by_key(&self, key: &[u8]) -> Option<&[u8]> {
        let key_hash = Self::compute_hash(key);

        if let Some(&offset) = self.key_index.get(&key_hash) {
            // Fast lookup
            let metadata_bytes = &self.mmap[offset as usize..offset as usize + METADATA_SIZE];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            let entry_start = metadata.prev_offset as usize;

            let entry = &self.mmap[entry_start..offset as usize];

            // Ensure deleted (null) entries are ignored
            if entry == b"\0" {
                return None;
            }

            return Some(entry);
        }

        None
    }

    /// Compacts the storage by keeping only the latest version of each key.
    pub fn compact(&mut self) -> Result<()> {
        let _write_lock = self.lock.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire write lock")
        })?;

        let compacted_path = self.path.with_extension("bk");
        eprintln!("🛠 Starting compaction. Writing to: {:?}", compacted_path);

        // Create a new AppendStorage instance for the compacted file
        let mut compacted_storage = AppendStorage::open(&compacted_path)?;

        // Iterate over all valid entries
        for entry in self.iter_entries() {
            let entry_start_offset = entry.as_ptr() as usize - self.mmap.as_ptr() as usize;
            let metadata_offset = entry_start_offset + entry.len();

            // Extract metadata separately from mmap
            if metadata_offset + METADATA_SIZE > self.mmap.len() {
                eprintln!(
                    "⚠️ Skipping corrupted entry at offset {}",
                    entry_start_offset
                );
                continue;
            }

            let metadata_bytes = &self.mmap[metadata_offset..metadata_offset + METADATA_SIZE];
            let metadata = EntryMetadata::deserialize(metadata_bytes);

            // TODO: Remove
            eprintln!("metadata: {:?}", metadata);

            // Append the entry with the correct key_hash
            compacted_storage.append_entry_with_key_hash(metadata.key_hash, entry)?;

            eprintln!(
                "Writing key_hash: {} | entry_size: {}",
                metadata.key_hash,
                entry.len()
            );
        }

        compacted_storage.file.flush()?;
        drop(compacted_storage); // Ensure all writes are flushed before swapping

        eprintln!("✅ Compaction completed. Swapping files...");

        std::fs::rename(&compacted_path, &self.path)?;
        // self.remap_file()?; // Remap file to load compacted data

        eprintln!("🎉 Compaction successful.");
        Ok(())
    }

    /// Counts the number of currently active entries.
    pub fn count(&self) -> usize {
        self.iter_entries().count()
    }

    /// Computes a SIMD-accelerated CRC32C-based 3-byte checksum.
    fn compute_checksum(data: &[u8]) -> [u8; 3] {
        let mut hasher = Crc32FastHasher::new();
        hasher.update(data);
        let hash = hasher.finalize(); // Uses SSE4.2 or Neon when available
        [
            (hash & 0xFF) as u8,
            ((hash >> 8) & 0xFF) as u8,
            ((hash >> 16) & 0xFF) as u8,
        ]
    }

    /// Simple key hash function
    fn compute_hash(key: &[u8]) -> u64 {
        xxh3_64(key)
    }
}
