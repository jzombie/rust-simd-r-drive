//! Single-process micro-benchmarks for the SIMD-R-Drive append-only
//! engine.  It writes 1 M entries, then exercises sequential, random
//! and *vectorised* (`batch_read`) lookup paths.

use rand::{Rng, rng}; // `rng()` & `random_range` are the new, non-deprecated names
use simd_r_drive::{
    DataStore,
    traits::{DataStoreReader, DataStoreWriter},
};
use std::fs::remove_file;
use std::path::PathBuf;
use std::time::Instant;
use tempfile::NamedTempFile;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

const ENTRY_SIZE: usize = 8; // bytes per value
const WRITE_BATCH_SIZE: usize = 1024; // entries / write
const READ_BATCH_SIZE: usize = 1024; // entries / batch_read

const NUM_ENTRIES: usize = 1_000_000;
const NUM_RANDOM_CHECKS: usize = 1_000_000;
const NUM_BATCH_CHECKS: usize = 1_000_000; // total *entries* verified via batch_read

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let path = temp_file.path().to_path_buf();

    println!("Running storage benchmark…");
    benchmark_append_entries(&path);
    benchmark_sequential_reads(&path);
    benchmark_random_reads(&path);
    benchmark_batch_reads(&path);
    println!("✅ Benchmarks completed.");

    // clean-up (NamedTempFile deletes on drop, but this keeps `cargo bench`
    // output tidy if it ever crashes mid-way)
    remove_file(path).ok();
}

// ---------------------------------------------------------------------------
// 1 ─ Write 1 M entries (batched)
// ---------------------------------------------------------------------------

fn benchmark_append_entries(path: &PathBuf) {
    let storage = DataStore::open(path).expect("Failed to open storage");
    let mut batch = Vec::with_capacity(WRITE_BATCH_SIZE);

    let start_time = Instant::now();

    for i in 0..NUM_ENTRIES {
        let key = format!("bench-key-{i}").into_bytes();

        // Fixed-width little-endian payload
        let mut value = vec![0u8; ENTRY_SIZE];
        let bytes = i.to_le_bytes();
        value[..bytes.len().min(ENTRY_SIZE)].copy_from_slice(&bytes[..bytes.len().min(ENTRY_SIZE)]);

        batch.push((key, value));

        if batch.len() == WRITE_BATCH_SIZE {
            flush_batch(&storage, &mut batch);
        }
    }
    if !batch.is_empty() {
        flush_batch(&storage, &mut batch);
    }

    let dt = start_time.elapsed();
    println!(
        "Wrote {NUM_ENTRIES} entries of {ENTRY_SIZE} bytes in {:#.3}s ({:#.3} writes/s)",
        dt.as_secs_f64(),
        NUM_ENTRIES as f64 / dt.as_secs_f64()
    );
}

fn flush_batch(storage: &DataStore, batch: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    let refs: Vec<(&[u8], &[u8])> = batch
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    storage.batch_write(&refs).expect("Batch write failed");
    batch.clear();
}

// ---------------------------------------------------------------------------
// 2 ─ Sequential iteration (zero-copy)
// ---------------------------------------------------------------------------

fn benchmark_sequential_reads(path: &PathBuf) {
    let storage = DataStore::open(path).expect("Failed to open storage");

    let start_time = Instant::now();
    let mut count = 0;

    for entry in storage.into_iter() {
        let stored = u64::from_le_bytes((&*entry).try_into().unwrap());
        let expected = NUM_ENTRIES as u64 - 1 - count; // iterator returns newest→oldest
        assert_eq!(stored, expected, "Corrupt data at index {count}");
        count += 1;
    }

    let dt = start_time.elapsed();
    println!(
        "Sequentially read {count} entries in {:#.3}s ({:#.3} reads/s)",
        dt.as_secs_f64(),
        count as f64 / dt.as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// 3 ─ Random single-key look-ups
// ---------------------------------------------------------------------------

fn benchmark_random_reads(path: &PathBuf) {
    let storage = DataStore::open(path).expect("Failed to open storage");
    let mut rng = rng();

    let start_time = Instant::now();

    for _ in 0..NUM_RANDOM_CHECKS {
        let i = rng.random_range(0..NUM_ENTRIES);
        let key = format!("bench-key-{i}");
        let handle = storage
            .read(key.as_bytes())
            .expect("Missing entry in random read");

        let stored = u64::from_le_bytes(handle.as_slice().try_into().unwrap());
        assert_eq!(stored, i as u64, "Corrupt data for key {i}");
    }

    let dt = start_time.elapsed();
    println!(
        "Randomly read {NUM_RANDOM_CHECKS} entries in {:#.3}s ({:#.3} reads/s)",
        dt.as_secs_f64(),
        NUM_RANDOM_CHECKS as f64 / dt.as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// 4 ─ Vectorised look-ups (batch_read)
// ---------------------------------------------------------------------------

fn benchmark_batch_reads(path: &PathBuf) {
    let storage = DataStore::open(path).expect("Failed to open storage");
    let mut keys_buf: Vec<Vec<u8>> = Vec::with_capacity(READ_BATCH_SIZE);
    let mut verified = 0usize;

    let start_time = Instant::now();

    for i in 0..NUM_BATCH_CHECKS {
        let key = format!("bench-key-{}", i % NUM_ENTRIES).into_bytes();
        keys_buf.push(key);

        if keys_buf.len() == READ_BATCH_SIZE {
            verified += verify_batch(&storage, &mut keys_buf);
        }
    }
    if !keys_buf.is_empty() {
        verified += verify_batch(&storage, &mut keys_buf);
    }

    let dt = start_time.elapsed();
    println!(
        "Batch-read verified {verified} entries in {:#.3}s ({:#.3} reads/s)",
        dt.as_secs_f64(),
        verified as f64 / dt.as_secs_f64()
    );
}

fn verify_batch(storage: &DataStore, keys_buf: &mut Vec<Vec<u8>>) -> usize {
    let key_refs: Vec<&[u8]> = keys_buf.iter().map(|k| k.as_slice()).collect();
    let handles = storage.batch_read(&key_refs).expect("batch_read failed"); // ← unwrap the Result

    for (k_bytes, opt_handle) in keys_buf.iter().zip(handles.into_iter()) {
        let handle = opt_handle.expect("Missing batch entry");
        let stored = u64::from_le_bytes(handle.as_slice().try_into().unwrap());

        // fast numeric suffix parse without heap allocation
        let idx = {
            let s = std::str::from_utf8(&k_bytes[b"bench-key-".len()..]).unwrap();
            s.parse::<usize>().unwrap()
        };
        assert_eq!(stored, idx as u64, "Corrupt data for key {idx}");
    }

    let n = keys_buf.len();
    keys_buf.clear();
    n
}
