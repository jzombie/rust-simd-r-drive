#[cfg(test)]
mod tests {

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

    #[test]
    fn test_append_and_read_last_entry() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key".as_slice();
        let payload = b"Hello, world!".as_slice();
        storage.write(key, payload).expect("Failed to append entry");

        let last_entry = storage.read_last_entry().expect("No entry found");
        assert_eq!(
            last_entry.as_slice(),
            payload,
            "Stored payload does not match expected value"
        );
    }

    #[test]
    fn test_multiple_appends_and_reads() {
        let (_dir, storage) = create_temp_storage();

        let entries = vec![
            (b"key1".as_slice(), b"First Entry".as_slice()),
            (b"key2".as_slice(), b"Second Entry".as_slice()),
            (b"key3".as_slice(), b"Third Entry".as_slice()),
        ];

        for (key, payload) in &entries {
            storage.write(key, payload).expect("Failed to append entry");
        }

        let last_entry = storage.read_last_entry().expect("No last entry found");
        assert_eq!(
            last_entry.as_slice(),
            entries.last().unwrap().1,
            "Last entry does not match expected value"
        );
    }

    #[test]
    fn test_varying_payload_sizes() {
        let (_dir, storage) = create_temp_storage();

        let payloads = [
            vec![b'a'; 10],   // Small payload
            vec![b'b'; 1024], // Medium payload
            vec![b'c'; 4096],
        ];

        for (i, payload) in payloads.iter().enumerate() {
            storage
                .write(format!("key{}", i).as_bytes(), payload.as_slice())
                .expect("Failed to append entry");
        }

        let last_entry = storage.read_last_entry().expect("No last entry found");
        assert_eq!(
            last_entry.as_slice(),
            payloads.last().unwrap().as_slice(),
            "Last entry payload does not match expected value"
        );
    }

    #[test]
    fn test_retrieve_entry_by_key() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key".as_slice();
        let payload = b"Hello, world!".as_slice();
        storage.write(key, payload).expect("Failed to append entry");

        let retrieved = storage.read(key);

        assert!(
            retrieved.is_some(),
            "Entry should be found by key, but got None"
        );

        assert_eq!(
            retrieved.unwrap().as_slice(),
            payload,
            "Retrieved payload does not match expected value"
        );
    }

    #[test]
    fn test_update_entries_with_varying_lengths() {
        let (_dir, storage) = create_temp_storage();

        let key1 = b"key1".as_slice();
        let key2 = b"key2".as_slice();
        let key3 = b"key3".as_slice();

        let initial_payload1 = b"Short".as_slice();
        let initial_payload2 = b"Medium length payload".as_slice();
        let initial_payload3 = b"Longer initial payload data".as_slice();

        storage
            .write(key1, initial_payload1)
            .expect("Failed to append entry");
        storage
            .write(key2, initial_payload2)
            .expect("Failed to append entry");
        storage
            .write(key3, initial_payload3)
            .expect("Failed to append entry");

        let updated_payload1 = b"Updated with longer data!".as_slice();
        let updated_payload2 = b"Short".as_slice();

        storage
            .write(key1, updated_payload1)
            .expect("Failed to update entry");
        storage
            .write(key2, updated_payload2)
            .expect("Failed to update entry");

        let retrieved1 = storage.read(key1).expect("Entry for key1 should be found");
        assert_eq!(
            retrieved1.as_slice(),
            updated_payload1,
            "Latest version of key1 was not retrieved"
        );

        let retrieved2 = storage.read(key2).expect("Entry for key2 should be found");
        assert_eq!(
            retrieved2.as_slice(),
            updated_payload2,
            "Latest version of key2 was not retrieved"
        );

        let retrieved3 = storage.read(key3).expect("Entry for key3 should be found");
        assert_eq!(
            retrieved3.as_slice(),
            initial_payload3,
            "Key3 should remain unchanged"
        );
    }

    #[test]
    fn test_open_existing_storage() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_existing.bin");

        // Create the file first
        {
            let _storage = DataStore::open(&path).expect("Failed to create storage file");
        }

        // Now attempt to open it with `open_existing`
        let storage =
            DataStore::open_existing(&path).expect("Failed to open existing storage file");

        // Ensure storage is accessible
        let key = b"test_key".as_slice();
        let payload = b"Existing file test".as_slice();
        storage.write(key, payload).expect("Failed to write entry");

        let retrieved = storage.read(key).expect("Entry should exist in storage");
        assert_eq!(
            retrieved.as_slice(),
            payload,
            "Retrieved payload does not match expected value"
        );
    }

    #[test]
    fn test_open_existing_fails_for_missing_file() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("non_existent_storage.bin");

        let result = DataStore::open_existing(&path);
        assert!(
            result.is_err(),
            "Expected error when opening non-existent file"
        );
    }

    #[test]
    fn test_write_null_byte_fails() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key";

        let result = storage.write(key, b"\x00");

        assert!(
            result.is_err(),
            "Expected error when writing a null-byte payload"
        );
    }
}
