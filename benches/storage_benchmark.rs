//! Single-process micro-benchmarks for the SIMD-R-Drive append-only
//! engine.  It writes 1 M entries, then exercises sequential, random
//! and *vectorized* (`batch_read`) lookup paths.

use bytes::Bytes;
use rand::{Rng, rng}; // `rng()` & `random_range` are the new, non-deprecated names
use simd_r_drive::{
    DataStore,
    traits::{DataStoreReader, DataStoreWriter},
};
use std::fs::remove_file;
use std::path::Path;
use std::time::Instant;
use tempfile::NamedTempFile;
use thousands::Separable;

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
// Write 1 M entries (batched)
// ---------------------------------------------------------------------------

fn benchmark_append_entries(path: &Path) {
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
        "Wrote {} entries of {ENTRY_SIZE} bytes in {}s ({} writes/s)",
        fmt_rate(NUM_ENTRIES as f64),
        dt.as_secs_f64(),
        fmt_rate(NUM_ENTRIES as f64 / dt.as_secs_f64())
    );
}

fn flush_batch(storage: &DataStore, batch: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    let refs: Vec<(Bytes, Bytes)> = batch
        .drain(..)
        .map(|(k, v)| (Bytes::from(k), Bytes::from(v)))
        .collect();

    storage.batch_write(&refs).expect("Batch write failed");
    batch.clear();
}

// ---------------------------------------------------------------------------
// Sequential iteration (zero-copy)
// ---------------------------------------------------------------------------

fn benchmark_sequential_reads(path: &Path) {
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
        "Sequentially read {} entries in {:#.3}s ({} reads/s)",
        fmt_rate(count as f64),
        dt.as_secs_f64(),
        fmt_rate(count as f64 / dt.as_secs_f64())
    );
}

// ---------------------------------------------------------------------------
// Random single-key look-ups
// ---------------------------------------------------------------------------

fn benchmark_random_reads(path: &Path) {
    let storage = DataStore::open(path).expect("Failed to open storage");
    let mut rng = rng();

    let start_time = Instant::now();

    for _ in 0..NUM_RANDOM_CHECKS {
        let i = rng.random_range(0..NUM_ENTRIES);
        let key = format!("bench-key-{i}");
        let handle = storage
            .read(Bytes::from(key))
            .unwrap()
            .expect("Missing entry in random read");

        let stored = u64::from_le_bytes(handle.as_slice().try_into().unwrap());
        assert_eq!(stored, i as u64, "Corrupt data for key {i}");
    }

    let dt = start_time.elapsed();
    println!(
        "Randomly read {} entries in {:#.3}s ({} reads/s)",
        fmt_rate(NUM_RANDOM_CHECKS as f64),
        dt.as_secs_f64(),
        fmt_rate(NUM_RANDOM_CHECKS as f64 / dt.as_secs_f64())
    );
}

// ---------------------------------------------------------------------------
// Vectorized look-ups (batch_read)
// ---------------------------------------------------------------------------

fn benchmark_batch_reads(path: &Path) {
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
        "Batch-read verified {} entries in {:#.3}s ({} reads/s)",
        fmt_rate(verified as f64),
        dt.as_secs_f64(),
        fmt_rate(verified as f64 / dt.as_secs_f64())
    );
}

fn verify_batch(storage: &DataStore, keys_buf: &mut Vec<Vec<u8>>) -> usize {
    let key_refs: Vec<Bytes> = keys_buf.iter().map(|k| Bytes::copy_from_slice(k)).collect();

    let handles = storage.batch_read(&key_refs).expect("batch_read failed");

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

/// Format a positive rate (reads/s or writes/s) with
///   * thousands-separated integral part
///   * exactly three decimals
///
/// 4_741_483.464 → "4,741,483.464"
///        987.0  → "987.000"
/// Format a positive rate (reads/s or writes/s) with
///   * thousands-separated integral part
///   * exactly three decimals
///
/// 4_741_483.464 → "4,741,483.464"
///        987.0  → "987.000"
///
/// Pretty-print a positive rate with comma-separated thousands
/// and **exactly three decimals**, e.g.  
/// `4_741_483.464` → `"4,741,483.464"`
fn fmt_rate(rate: f64) -> String {
    let whole = rate.trunc() as u64;
    let mut frac = (rate.fract() * 1_000.0).round() as u16;

    // Carry if we rounded to 1000.000
    let whole = if frac == 1_000 {
        frac = 0;
        whole + 1
    } else {
        whole
    };

    format!("{}.{:03}", whole.separate_with_commas(), frac)
}
