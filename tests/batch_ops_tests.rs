//! Integration-tests for the batch-write / batch-read API.

use simd_r_drive::{
    DataStore,
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
            "payload mismatch for key {:?}",
            expected_key
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
        storage.count().unwrap(),
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
