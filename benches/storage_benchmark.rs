use rand::Rng;
use simd_r_drive::{
    DataStore,
    traits::{DataStoreReader, DataStoreWriter},
};
use std::fs::remove_file;
use std::path::PathBuf;
use std::time::Instant;
use tempfile::NamedTempFile;

const ENTRY_SIZE: usize = 8; // 8-byte values
const WRITE_BATCH_SIZE: usize = 1024;

const NUM_ENTRIES: usize = 1_000_000;
const NUM_RANDOM_CHECKS: usize = 1_000_000;

fn main() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let path = temp_file.path().to_path_buf();

    println!("Running storage benchmark...");
    benchmark_append_entries(&path);
    benchmark_sequential_reads(&path);
    benchmark_random_reads(&path);
    println!("✅ Benchmark completed.");

    remove_file(path).ok();
}

/// Writes 1M entries
fn benchmark_append_entries(path: &PathBuf) {
    let storage = DataStore::open(path).expect("Failed to open storage");
    let mut batch = Vec::with_capacity(WRITE_BATCH_SIZE);

    let start_time = Instant::now();
    for i in 0..NUM_ENTRIES {
        let key = format!("bench-key-{}", i).into_bytes(); // Convert to Vec<u8>

        // Ensure value is exactly ENTRY_SIZE bytes
        let mut value = vec![0u8; ENTRY_SIZE];
        let bytes = i.to_le_bytes();
        value[..bytes.len().min(ENTRY_SIZE)].copy_from_slice(&bytes[..bytes.len().min(ENTRY_SIZE)]);

        batch.push((key, value)); // Store owned values

        if batch.len() >= WRITE_BATCH_SIZE {
            let batch_refs: Vec<(&[u8], &[u8])> = batch
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                .collect();
            storage
                .batch_write(&batch_refs)
                .expect("Batch write failed");
            batch.clear();
        }
    }

    if !batch.is_empty() {
        let batch_refs: Vec<(&[u8], &[u8])> = batch
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        storage
            .batch_write(&batch_refs)
            .expect("Final batch write failed");
    }

    let duration = start_time.elapsed();

    println!(
        "Wrote {} entries of {} bytes each in {:.3} seconds ({:.3} writes/sec)",
        NUM_ENTRIES,
        ENTRY_SIZE,
        duration.as_secs_f64(),
        NUM_ENTRIES as f64 / duration.as_secs_f64()
    );
}

fn benchmark_sequential_reads(path: &PathBuf) {
    let storage = DataStore::open(path).expect("Failed to open storage");

    let start_time = Instant::now();
    let mut count = 0;

    for entry in storage.into_iter() {
        let stored_value = u64::from_le_bytes((&*entry).try_into().expect("Failed to parse"));
        let expected_value = NUM_ENTRIES as u64 - 1 - count; // Reverse expectation
        assert_eq!(
            stored_value, expected_value,
            "Corrupt data at index {}",
            count
        );
        count += 1;
    }

    let duration = start_time.elapsed();
    println!(
        "Sequentially read {} entries in {:.3} seconds ({:.3} reads/sec)",
        count,
        duration.as_secs_f64(),
        count as f64 / duration.as_secs_f64()
    );
}

/// Random read benchmark
fn benchmark_random_reads(path: &PathBuf) {
    let storage = DataStore::open(path).expect("Failed to open storage");
    let mut rng = rand::rng();

    let start_time = Instant::now();
    for _ in 0..NUM_RANDOM_CHECKS {
        let i = rng.random_range(0..NUM_ENTRIES);
        let key = format!("bench-key-{}", i);
        let entry = storage.read(key.as_bytes());

        if let Some(data) = entry {
            let stored_value =
                u64::from_le_bytes(data.as_slice().try_into().expect("Failed to parse"));
            assert_eq!(
                stored_value, i as u64,
                "Corrupt data: expected {}, got {}",
                i, stored_value
            );
        } else {
            panic!("Missing entry for key: {}", key);
        }
    }
    let duration = start_time.elapsed();

    println!(
        "Randomly read {} entries in {:.3} seconds ({:.3} reads/sec)",
        NUM_RANDOM_CHECKS,
        duration.as_secs_f64(),
        NUM_RANDOM_CHECKS as f64 / duration.as_secs_f64()
    );
}
