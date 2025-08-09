// This attribute ensures the entire file is only compiled and run when
// the "parallel" feature is enabled.
#![cfg(feature = "parallel")]

use rayon::prelude::*;
use simd_r_drive::{DataStore, traits::DataStoreWriter};
use std::collections::HashSet;
use tempfile::tempdir;

/// Helper function to create a temporary file for testing.
fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");
    let storage = DataStore::open(&path).expect("Failed to open storage");
    (dir, storage)
}

#[test]
fn test_par_iter_produces_correct_entries() {
    let (_dir, storage) = create_temp_storage();
    let entries = vec![
        (b"key1".as_slice(), b"payload1".as_slice()),
        (b"key2".as_slice(), b"payload2".as_slice()),
        (b"key3".as_slice(), b"payload3".as_slice()),
    ];
    storage.batch_write(&entries).expect("Batch write failed");

    // Use a HashSet to verify that the parallel iterator produces the exact
    // same set of payloads as the sequential one, ignoring order.
    let expected_payloads: HashSet<Vec<u8>> = storage
        .iter_entries()
        .map(|e| e.as_slice().to_vec())
        .collect();

    let parallel_payloads: HashSet<Vec<u8>> = storage
        .par_iter_entries()
        .map(|e| e.as_slice().to_vec())
        .collect();

    assert_eq!(
        expected_payloads, parallel_payloads,
        "Parallel iterator should produce the same set of entries as the sequential one"
    );
    assert_eq!(parallel_payloads.len(), 3);
}

#[test]
fn test_par_iter_skips_deleted_entries() {
    let (_dir, storage) = create_temp_storage();
    let entries = vec![
        (b"key1".as_slice(), b"payload1".as_slice()),
        (b"key_to_delete".as_slice(), b"payload_to_delete".as_slice()),
        (b"key3".as_slice(), b"payload3".as_slice()),
    ];
    storage.batch_write(&entries).expect("Batch write failed");
    storage.delete(b"key_to_delete").expect("Delete failed");

    // Collect all payloads found by the parallel iterator.
    let found_payloads: Vec<Vec<u8>> = storage
        .par_iter_entries()
        .map(|e| e.as_slice().to_vec())
        .collect();

    assert_eq!(
        found_payloads.len(),
        2,
        "Parallel iterator should not include deleted entries"
    );

    // Ensure the deleted payload is not present.
    let deleted_payload = b"payload_to_delete".to_vec();
    assert!(
        !found_payloads.contains(&deleted_payload),
        "Deleted payload should not be found in parallel iteration results"
    );
}

#[test]
fn test_par_iter_on_empty_store() {
    let (_dir, storage) = create_temp_storage();

    let count = storage.par_iter_entries().count();

    assert_eq!(
        count, 0,
        "Parallel iterator should produce zero items for an empty store"
    );
}
