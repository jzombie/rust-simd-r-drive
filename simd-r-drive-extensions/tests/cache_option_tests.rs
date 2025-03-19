use serde::{Deserialize, Serialize};
use simd_r_drive::DataStore;
use simd_r_drive_extensions::StorageCacheExt;
use std::io::ErrorKind;
use std::thread::sleep;
use std::time::Duration;
use tempfile::tempdir;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TestData {
    id: u32,
    name: String,
}

/// Helper function to create a temporary file for testing
fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("test_storage_ttl.bin");

    let storage = DataStore::open(&path).expect("Failed to open storage");
    (dir, storage)
}

#[test]
fn test_write_and_read_with_valid_ttl() {
    let (_dir, storage) = create_temp_storage();
    let key = b"valid_ttl_key";
    let data = TestData {
        id: 100,
        name: "Valid".to_string(),
    };

    storage
        .write_with_ttl(key, &data, 5)
        .expect("Failed to write with TTL");

    let retrieved = storage
        .read_with_ttl::<TestData>(key)
        .expect("Failed to read with TTL");
    assert_eq!(
        retrieved,
        Some(data),
        "Data should be retrievable before TTL expiration"
    );
}

#[test]
fn test_ttl_expiration() {
    let (_dir, storage) = create_temp_storage();
    let key = b"expired_ttl_key";
    let data = TestData {
        id: 200,
        name: "Expired".to_string(),
    };

    storage
        .write_with_ttl(key, &data, 2)
        .expect("Failed to write with TTL");
    sleep(Duration::from_secs(3)); // Wait for expiration

    let retrieved = storage
        .read_with_ttl::<TestData>(key)
        .expect("Failed to read with TTL");
    assert_eq!(retrieved, None, "Data should be expired and removed");
}

#[test]
fn test_ttl_does_not_affect_other_keys() {
    let (_dir, storage) = create_temp_storage();
    let key1 = b"persistent_key";
    let key2 = b"expiring_key";

    let data1 = TestData {
        id: 1,
        name: "Persistent".to_string(),
    };
    let data2 = TestData {
        id: 2,
        name: "Temporary".to_string(),
    };

    storage
        .write_with_ttl(key1, &data1, 10)
        .expect("Failed to write persistent");
    storage
        .write_with_ttl(key2, &data2, 2)
        .expect("Failed to write expiring");

    sleep(Duration::from_secs(3));

    let retrieved1 = storage
        .read_with_ttl::<TestData>(key1)
        .expect("Failed to read persistent");
    let retrieved2 = storage
        .read_with_ttl::<TestData>(key2)
        .expect("Failed to read expiring");

    assert_eq!(retrieved1, Some(data1), "Persistent key should still exist");
    assert_eq!(retrieved2, None, "Expiring key should be removed");
}

#[test]
fn test_read_non_existent_key_ttl() {
    let (_dir, storage) = create_temp_storage();
    let key = b"missing_key";

    let retrieved = storage.read_with_ttl::<TestData>(key);
    assert!(
        matches!(retrieved, Err(ref e) if e.kind() == ErrorKind::NotFound),
        "Expected `ErrorKind::NotFound`"
    );
}

#[test]
fn test_multiple_writes_and_expirations() {
    let (_dir, storage) = create_temp_storage();
    let key1 = b"short_ttl";
    let key2 = b"long_ttl";

    let data1 = TestData {
        id: 10,
        name: "Short TTL".to_string(),
    };
    let data2 = TestData {
        id: 20,
        name: "Long TTL".to_string(),
    };

    storage
        .write_with_ttl(key1, &data1, 1)
        .expect("Failed to write short TTL");
    storage
        .write_with_ttl(key2, &data2, 5)
        .expect("Failed to write long TTL");

    sleep(Duration::from_secs(2));

    let retrieved1 = storage
        .read_with_ttl::<TestData>(key1)
        .expect("Failed to read short TTL");
    let retrieved2 = storage
        .read_with_ttl::<TestData>(key2)
        .expect("Failed to read long TTL");

    assert_eq!(retrieved1, None, "Short TTL should have expired");
    assert_eq!(retrieved2, Some(data2), "Long TTL should still be valid");
}
