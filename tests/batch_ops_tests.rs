//! Integration-tests for the batch-write / batch-read API.

use simd_r_drive::{
    DataStore, compute_hash, compute_hash_batch,
    traits::{DataStoreReader, DataStoreWriter},
};
use tempfile::tempdir;

/// Helper function to create a temporary file for testing
fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage.bin");

    let storage = DataStore::open(&path).expect("Failed to open storage");
    (dir, storage)
}

/// Happy-path: write a handful of entries in one shot, then read them back
/// individually to make sure they landed where we think they did.
#[test]
fn test_batch_write_and_individual_reads() {
    let (_dir, storage) = create_temp_storage();

    let entries = vec![
        (b"alpha".as_slice(), b"one".as_slice()),
        (b"beta".as_slice(), b"two".as_slice()),
        (b"gamma".as_slice(), b"three".as_slice()),
    ];

    storage.batch_write(&entries).expect("batch_write failed");

    for (k, v) in &entries {
        let got = storage
            .read(k)
            .unwrap()
            .expect("missing key written via batch");
        assert_eq!(got.as_slice(), *v);
    }
}

/// End-to-end test of `batch_read`:
/// * write with `batch_write`
/// * fetch the **same** set with `batch_read`
/// * verify ordering & presence match
#[test]
fn test_batch_write_and_batch_read() {
    let (_dir, storage) = create_temp_storage();

    let entries = vec![
        (b"a".as_slice(), b"AAA".as_slice()),
        (b"b".as_slice(), b"BBB".as_slice()),
        (b"c".as_slice(), b"CCC".as_slice()),
        (b"d".as_slice(), b"DDD".as_slice()),
    ];

    storage.batch_write(&entries).expect("batch_write failed");

    // Pull them back in one call – **note** order must be preserved.
    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| *k).collect();
    let results = storage.batch_read(&keys).unwrap();

    assert_eq!(results.len(), keys.len(), "result length mismatch");

    for ((expected_key, expected_val), got_opt) in entries.iter().zip(results.iter()) {
        let got = got_opt.as_ref().expect("missing key in batch_read");
        assert_eq!(
            got.as_slice(),
            *expected_val,
            "payload mismatch for key {expected_key:?}",
        );
    }
}

/// Safety rails: ensure NULL-byte payloads are rejected by the high-level batch API
#[test]
fn test_batch_write_rejects_null_byte_payload() {
    let (_dir, storage) = create_temp_storage();

    // Note the second entry is a single NULL byte → should blow up.
    let entries = vec![
        (b"good_key".as_slice(), b"OK".as_slice()),
        (b"bad_key".as_slice(), b"\0".as_slice()),
    ];

    let err = storage
        .batch_write(&entries)
        .expect_err("NULL byte payload should error");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

/// Safety rails: empty payloads are also invalid.
#[test]
fn test_batch_write_rejects_empty_payload() {
    let (_dir, storage) = create_temp_storage();

    let entries = vec![(b"empty".as_slice(), b"".as_slice())];

    let err = storage
        .batch_write(&entries)
        .expect_err("empty payload should error");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

/// `batch_write` should be atomic: if any payload is empty EVERY write is rejected
#[test]
fn test_batch_write_rejects_empty_payload_among_many() {
    let (_dir, storage) = create_temp_storage();

    // First call should fail because the middle entry is empty
    let entries = vec![
        (b"k1".as_slice(), b"payload1".as_slice()),
        (b"k_empty".as_slice(), b"".as_slice()), // ← invalid
        (b"k2".as_slice(), b"payload2".as_slice()),
    ];

    let err = storage.batch_write(&entries).expect_err("should fail");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    // Ensure *nothing* was persisted
    assert!(storage.read(b"k1").unwrap().is_none());
    assert!(storage.read(b"k2").unwrap().is_none());
    assert_eq!(
        storage.len().unwrap(),
        0,
        "no entries should have been written"
    );
}

/// `batch_read` should return `None` for keys that are not present while still
/// returning `Some(EntryHandle)` for the keys that are.
#[test]
fn test_batch_read_with_missing_key() {
    let (_dir, storage) = create_temp_storage();

    // Write two entries
    let entries = vec![
        (b"exists_1".as_slice(), b"payload one".as_slice()),
        (b"exists_2".as_slice(), b"payload two".as_slice()),
    ];
    storage.batch_write(&entries).expect("batch_write failed");

    // Prepare a key list that also contains a key we never wrote
    let keys: Vec<&[u8]> = vec![
        b"exists_1".as_slice(),
        b"missing_key".as_slice(), // <- this one is absent
        b"exists_2".as_slice(),
    ];

    let results = storage.batch_read(&keys).unwrap();
    assert_eq!(results.len(), keys.len());

    // Check returned Options in the same order
    assert_eq!(
        results[0].as_ref().unwrap().as_slice(),
        b"payload one",
        "wrong payload for exists_1"
    );
    assert!(
        results[1].is_none(),
        "expected None for missing_key but got Some"
    );
    assert_eq!(
        results[2].as_ref().unwrap().as_slice(),
        b"payload two",
        "wrong payload for exists_2"
    );
}

#[test]
fn test_batch_write_rejects_null_byte_among_many() {
    let (_dir, storage) = create_temp_storage();
    // First call should fail because the middle entry is a null byte
    let entries = vec![
        (b"k1".as_slice(), b"payload1".as_slice()),
        (b"k_null".as_slice(), b"\0".as_slice()), // ← invalid
        (b"k2".as_slice(), b"payload2".as_slice()),
    ];
    let err = storage.batch_write(&entries).expect_err("should fail");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    // Ensure *nothing* was persisted
    assert!(storage.read(b"k1").unwrap().is_none());
    assert!(storage.read(b"k2").unwrap().is_none());
    assert_eq!(
        storage.len().unwrap(),
        0,
        "no entries should have been written"
    );
}

/// End-to-end test of `batch_read_hashed_keys` with full verification.
/// * write with `batch_write`
/// * compute hashes
/// * fetch with `batch_read_hashed_keys` providing both hashes and original keys
/// * verify ordering & presence match
#[test]
fn test_batch_read_hashed_keys_with_verification() {
    let (_dir, storage) = create_temp_storage();
    let entries = vec![
        (b"key1".as_slice(), b"val1".as_slice()),
        (b"key2".as_slice(), b"val2".as_slice()),
    ];
    storage.batch_write(&entries).expect("batch_write failed");

    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| *k).collect();
    let hashes = compute_hash_batch(&keys);

    // Read back using the hashed key method with original keys for verification
    let results = storage
        .batch_read_hashed_keys(&hashes, Some(&keys))
        .unwrap();
    assert_eq!(results.len(), keys.len());

    for ((_expected_key, expected_val), got_opt) in entries.iter().zip(results.iter()) {
        let got = got_opt
            .as_ref()
            .expect("missing key in batch_read_hashed_keys");
        assert_eq!(got.as_slice(), *expected_val);
    }
}

/// End-to-end test of `batch_read_hashed_keys` without verification (hash-only).
#[test]
fn test_batch_read_hashed_keys_without_verification() {
    let (_dir, storage) = create_temp_storage();
    let entries = vec![(b"key1".as_slice(), b"val1".as_slice())];
    storage.batch_write(&entries).expect("batch_write failed");

    let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| *k).collect();
    let hashes = compute_hash_batch(&keys);

    // Read back using only the hash, passing `None` for the original keys
    let results = storage.batch_read_hashed_keys(&hashes, None).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_ref().unwrap().as_slice(), entries[0].1);
}

/// `batch_read_hashed_keys` should return `None` for keys that are not present.
#[test]
fn test_batch_read_hashed_keys_with_missing_keys() {
    let (_dir, storage) = create_temp_storage();
    let entries = vec![(b"exists".as_slice(), b"payload".as_slice())];
    storage.batch_write(&entries).expect("batch_write failed");

    let existing_hash = compute_hash_batch(&[b"exists" as &[u8]])[0];
    let missing_hash = 12345_u64; // A key that was never written

    let hashes = vec![existing_hash, missing_hash];
    let results = storage.batch_read_hashed_keys(&hashes, None).unwrap();

    assert_eq!(results.len(), 2);
    assert!(results[0].is_some(), "expected entry for existing key");
    assert!(results[1].is_none(), "expected None for missing key");
}

/// Verifies that `batch_read_hashed_keys` with key verification enabled
/// will reject a match if the key's tag doesn't align with the hash.
/// This simulates a hash collision and confirms the safety check works.
#[test]
fn test_batch_read_hashed_keys_detects_collision() {
    let (_dir, storage) = create_temp_storage();
    let real_key = b"real_key";
    let fake_key = b"fake_key"; // A different key
    let payload = b"some data";
    storage.write(real_key, payload).unwrap();

    // Get the hash of the key that actually exists in storage.
    let real_hash = compute_hash_batch(&[real_key])[0];

    // Now, try to read using the *real hash* but providing the *fake key*
    // for verification. The tag check inside the read logic should fail.
    let results = storage
        .batch_read_hashed_keys(&[real_hash], Some(&[fake_key]))
        .unwrap();

    assert_eq!(results.len(), 1);
    assert!(
        results[0].is_none(),
        "Read should fail due to tag mismatch, simulating a hash collision"
    );
}

/// Happy-path: write a handful of entries, then delete a subset of them
/// in a single batch operation.
#[test]
fn test_batch_delete() {
    let (_dir, storage) = create_temp_storage();
    let entries = vec![
        (b"alpha".as_slice(), b"one".as_slice()),
        (b"beta".as_slice(), b"two".as_slice()),
        (b"gamma".as_slice(), b"three".as_slice()),
        (b"delta".as_slice(), b"four".as_slice()),
    ];
    storage.batch_write(&entries).expect("batch_write failed");
    assert_eq!(storage.len().unwrap(), 4);

    // Delete two of the entries
    let keys_to_delete = [b"beta".as_slice(), b"delta".as_slice()];
    storage
        .batch_delete(&keys_to_delete)
        .expect("batch_delete failed");

    // Verify store state
    assert_eq!(storage.len().unwrap(), 2, "Length should be reduced by 2");
    assert!(storage.read(b"beta").unwrap().is_none());
    assert!(storage.read(b"delta").unwrap().is_none());

    // Ensure other keys are unaffected
    assert!(storage.read(b"alpha").unwrap().is_some());
    assert!(storage.read(b"gamma").unwrap().is_some());
}

/// Verify that `batch_delete` correctly handles a mix of keys that
/// exist and keys that do not. The operation should succeed, and only
/// existing keys should be deleted.
#[test]
fn test_batch_delete_with_missing_keys() {
    let (_dir, storage) = create_temp_storage();
    let entries = vec![(b"key1".as_slice(), b"val1".as_slice())];
    storage.batch_write(&entries).expect("batch_write failed");
    assert_eq!(storage.len().unwrap(), 1);

    // Attempt to delete one existing and one non-existent key
    let keys_to_delete = [b"key1".as_slice(), b"non_existent_key".as_slice()];
    storage
        .batch_delete(&keys_to_delete)
        .expect("batch_delete should not fail on missing keys");

    // Verify only the existing key was deleted
    assert_eq!(storage.len().unwrap(), 0);
    assert!(storage.is_empty().unwrap());
    assert!(storage.read(b"key1").unwrap().is_none());
}

/// Verify the lowest-level batch delete function works as intended,
/// ignoring hashes for keys that are not present in the store.
#[test]
fn test_batch_delete_key_hashes() {
    let (_dir, storage) = create_temp_storage();
    storage.write(b"real", b"data").unwrap();
    assert_eq!(storage.len().unwrap(), 1);

    let real_hash = compute_hash(b"real");
    let fake_hash = 1234567890_u64; // A hash for a key that doesn't exist

    let hashes_to_delete = [real_hash, fake_hash];
    storage
        .batch_delete_key_hashes(&hashes_to_delete)
        .expect("batch_delete_key_hashes failed");

    // The store should now be empty because the only real key was deleted.
    assert!(storage.is_empty().unwrap());
}
