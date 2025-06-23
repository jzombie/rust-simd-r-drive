use crate::storage_engine::constants::*;
use crate::storage_engine::digest::{
    Xxh3BuildHasher, compute_checksum, compute_hash, compute_hash_batch,
};
use crate::storage_engine::simd_copy;
use crate::storage_engine::static_hash_index::flush_static_index;
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

const STATIC_INDEX_FOOTER_SIZE: u64 = 32;

/// Append-Only Storage Engine
pub struct DataStore {
    file: Arc<RwLock<BufWriter<File>>>,
    mmap: Arc<Mutex<Arc<Mmap>>>,
    tail_offset: AtomicU64,
    key_indexer: Arc<RwLock<KeyIndexer<'static>>>,
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
    fn from(path: PathBuf) -> Self {
        DataStore::open(&path).expect("Failed to open storage file")
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

    fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64> {
        let (keys, payloads): (Vec<_>, Vec<_>) = entries.iter().cloned().unzip();
        let hashes = compute_hash_batch(&keys);
        let hashed_entries = hashes
            .into_iter()
            .zip(payloads.into_iter())
            .collect::<Vec<_>>();
        self.batch_write_hashed_payloads(hashed_entries, false)
    }

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
                format!("Key not found: {:?}", String::from_utf8_lossy(key)),
            )
        })?;
        self.copy_entry_handle(&entry_handle, target)
    }

    fn move_entry(&self, key: &[u8], target: &DataStore) -> Result<u64> {
        self.copy_entry(key, target)?;
        self.delete_entry(key)
    }

    fn delete_entry(&self, key: &[u8]) -> Result<u64> {
        let key_hash = compute_hash(key);
        self.batch_write_hashed_payloads(vec![(key_hash, &NULL_BYTE)], true)
    }
}

impl DataStoreReader for DataStore {
    type EntryHandleType = EntryHandle;

    fn read(&self, key: &[u8]) -> Option<EntryHandle> {
        let key_hash = compute_hash(key);
        let key_indexer_guard = self.key_indexer.read().ok()?;
        let mmap_arc = self.get_mmap_arc();

        let offset = key_indexer_guard.get(&key_hash)?;

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

        if entry_end - entry_start == 1 && &mmap_arc[entry_start..entry_end] == NULL_BYTE {
            return None;
        }

        Some(EntryHandle {
            mmap_arc: mmap_arc.clone(),
            range: entry_start..entry_end,
            metadata,
        })
    }

    fn batch_read(&self, keys: &[&[u8]]) -> Result<Vec<Option<EntryHandle>>> {
        let mmap_arc = self.get_mmap_arc();
        let key_indexer_guard = self.key_indexer.read().map_err(|_| {
            Error::new(
                ErrorKind::Other,
                "Key-index lock poisoned during batch_read",
            )
        })?;
        let hashes = compute_hash_batch(keys);

        let results = hashes
            .into_iter()
            .map(|h| {
                key_indexer_guard.get(&h).and_then(|offset| {
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
                    if entry_end - entry_start == 1
                        && &mmap_arc[entry_start..entry_end] == NULL_BYTE
                    {
                        return None;
                    }
                    Some(EntryHandle {
                        mmap_arc: mmap_arc.clone(),
                        range: entry_start..entry_end,
                        metadata,
                    })
                })
            })
            .collect();
        Ok(results)
    }

    fn read_metadata(&self, key: &[u8]) -> Result<Option<EntryMetadata>> {
        Ok(self.read(key).map(|entry| entry.metadata().clone()))
    }

    fn count(&self) -> Result<usize> {
        Ok(self.iter_entries().count())
    }

    fn get_storage_size(&self) -> Result<u64> {
        std::fs::metadata(&self.path).map(|meta| meta.len())
    }
}

impl DataStore {
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
        let mmap_for_indexer: &'static Mmap = unsafe { &*(Arc::as_ptr(&mmap_arc) as *const Mmap) };
        let key_indexer = KeyIndexer::build(mmap_for_indexer, final_len);

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            mmap: Arc::new(Mutex::new(mmap_arc)),
            tail_offset: final_len.into(),
            key_indexer: Arc::new(RwLock::new(key_indexer)),
            path: path.to_path_buf(),
        })
    }

    pub fn open_existing(path: &Path) -> Result<Self> {
        verify_file_existence(&path)?;
        Self::open(path)
    }

    fn open_file_in_append_mode(path: &Path) -> Result<BufWriter<File>> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(BufWriter::new(file))
    }

    fn init_mmap(file: &BufWriter<File>) -> Result<Mmap> {
        unsafe { memmap2::MmapOptions::new().map(file.get_ref()) }
    }

    #[inline]
    fn get_mmap_arc(&self) -> Arc<Mmap> {
        let guard = self.mmap.lock().unwrap();
        let mmap_clone = guard.clone();
        drop(guard);
        mmap_clone
    }

    fn reindex(
        &self,
        write_guard: &std::sync::RwLockWriteGuard<'_, BufWriter<File>>,
        key_hash_offsets: &[(u64, u64)],
        tail_offset: u64,
    ) -> std::io::Result<()> {
        let new_mmap = Self::init_mmap(write_guard)?;
        let mut mmap_guard = self.mmap.lock().unwrap();
        let mut key_indexer_guard = self.key_indexer.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire index lock")
        })?;

        for (key_hash, offset) in key_hash_offsets.iter() {
            key_indexer_guard.insert(*key_hash, *offset);
        }

        *mmap_guard = Arc::new(new_mmap);
        self.tail_offset.store(tail_offset, Ordering::Release);

        Ok(())
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn iter_entries(&self) -> EntryIterator {
        let mmap_clone = self.get_mmap_arc();
        let tail_offset = self.tail_offset.load(Ordering::Acquire);
        EntryIterator::new(mmap_clone, tail_offset)
    }

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

    pub fn write_stream_with_key_hash<R: Read>(
        &self,
        key_hash: u64,
        reader: &mut R,
    ) -> Result<u64> {
        let mut file = self.file.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire file lock")
        })?;
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
        )?;
        Ok(tail_offset)
    }

    pub fn write_with_key_hash(&self, key_hash: u64, payload: &[u8]) -> Result<u64> {
        self.batch_write_hashed_payloads(vec![(key_hash, payload)], false)
    }

    pub fn batch_write_hashed_payloads(
        &self,
        hashed_payloads: Vec<(u64, &[u8])>,
        allow_null_bytes: bool,
    ) -> Result<u64> {
        let mut file = self.file.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "Failed to acquire file lock")
        })?;

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
            simd_copy(&mut entry[..payload.len()], payload);
            entry[payload.len()..].copy_from_slice(&metadata.serialize());
            buffer.extend_from_slice(&entry);

            tail_offset += entry.len() as u64;
            key_hash_offsets.push((key_hash, tail_offset - METADATA_SIZE as u64));
        }

        file.write_all(&buffer)?;
        file.flush()?;

        self.reindex(&file, &key_hash_offsets, tail_offset)?;

        Ok(self.tail_offset.load(Ordering::Acquire))
    }

    pub fn read_last_entry(&self) -> Option<EntryHandle> {
        let mmap_arc = self.get_mmap_arc();
        let tail_offset = self.tail_offset.load(std::sync::atomic::Ordering::Acquire);
        if tail_offset < METADATA_SIZE as u64 || mmap_arc.len() == 0 {
            return None;
        }

        let metadata_offset = (tail_offset - METADATA_SIZE as u64) as usize;
        if metadata_offset + METADATA_SIZE > mmap_arc.len() {
            return None;
        }

        let metadata_bytes = &mmap_arc[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = EntryMetadata::deserialize(metadata_bytes);

        let entry_start = metadata.prev_offset as usize;
        let entry_end = metadata_offset;
        if entry_start >= entry_end || entry_end > mmap_arc.len() {
            return None;
        }

        Some(EntryHandle {
            mmap_arc,
            range: entry_start..entry_end,
            metadata,
        })
    }

    #[inline]
    pub fn read_hashed_with_ctx(
        key_hash: u64,
        mmap_arc: &Arc<Mmap>,
        key_indexer: &KeyIndexer,
    ) -> Option<EntryHandle> {
        let offset = key_indexer.get(&key_hash)?;
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

        if entry_end - entry_start == 1 && &mmap_arc[entry_start..entry_end] == NULL_BYTE {
            return None;
        }

        Some(EntryHandle {
            mmap_arc: mmap_arc.clone(),
            range: entry_start..entry_end,
            metadata,
        })
    }

    fn copy_entry_handle(&self, entry: &EntryHandle, target: &DataStore) -> Result<u64> {
        let mut entry_stream = EntryStream::from(entry.clone_arc());
        target.write_stream_with_key_hash(entry.key_hash(), &mut entry_stream)
    }

    pub fn compact(&mut self) -> std::io::Result<()> {
        let compacted_path = crate::utils::append_extension(&self.path, "bk");
        info!("Starting compaction. Writing to: {:?}", compacted_path);

        let mut compacted_storage = DataStore::open(&compacted_path)?;
        let mut index_pairs: Vec<(u64, u64)> = Vec::new();
        let mut compacted_data_size: u64 = 0;

        for entry in self.iter_entries() {
            let new_tail_offset = self.copy_entry_handle(&entry, &mut compacted_storage)?;
            let stored_metadata_offset = new_tail_offset - METADATA_SIZE as u64;
            index_pairs.push((entry.key_hash(), stored_metadata_offset));
            compacted_data_size += entry.size_with_metadata() as u64;
        }

        let size_before = self.get_storage_size()?;

        // Calculate the overhead of adding a static index
        let need_slots = (index_pairs.len() as f64 / 0.7).ceil() as u64;
        let pow2 = 64 - need_slots.leading_zeros();
        let slots = 1u64 << pow2;
        let index_size = slots * 16;
        let index_overhead = index_size + STATIC_INDEX_FOOTER_SIZE;

        // Only write the static index if it actually saves space
        if size_before > compacted_data_size + index_overhead {
            info!("Compaction will save space. Writing static index.");
            let indexed_up_to = compacted_storage.tail_offset.load(Ordering::Acquire);

            let mut file_guard = compacted_storage.file.write().map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("Lock poisoned: {}", e))
            })?;
            file_guard.flush()?;
            let underlying_file = file_guard.get_mut();
            flush_static_index(underlying_file, &index_pairs, indexed_up_to)?;
        } else {
            info!(
                "Compaction would increase file size (data: {}, index: {}). Skipping static index generation.",
                compacted_data_size, index_overhead
            );
        }

        drop(compacted_storage);

        debug!("Compaction successful. Swapping files...");
        std::fs::rename(&compacted_path, &self.path)?;
        info!("Compaction file swap complete.");
        Ok(())
    }

    pub fn estimate_compaction_savings(&self) -> u64 {
        let total_size = self.get_storage_size().unwrap_or(0);
        let mut unique_entry_size: u64 = 0;
        let mut seen_keys = HashSet::with_hasher(Xxh3BuildHasher);

        for entry in self.iter_entries() {
            if seen_keys.insert(entry.key_hash()) {
                unique_entry_size += entry.size_with_metadata() as u64;
            }
        }
        total_size.saturating_sub(unique_entry_size)
    }

    #[cfg(any(test, debug_assertions))]
    pub fn get_mmap_arc_for_testing(&self) -> Arc<Mmap> {
        self.get_mmap_arc()
    }

    #[cfg(any(test, debug_assertions))]
    pub fn arc_ptr(&self) -> *const u8 {
        self.get_mmap_arc().as_ptr()
    }
}
