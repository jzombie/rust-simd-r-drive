#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use simd_r_drive::DataStore;
    use simd_r_drive_extensions::{
        utils::prefix_key, StorageOptionExt, TEST_OPTION_PREFIX, TEST_OPTION_TOMBSTONE_MARKER,
    };
    use std::io::ErrorKind;
    use tempfile::tempdir;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TestData {
        id: u32,
        name: String,
    }

    /// Helper function to create a temporary file for testing
    fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage.bin");

        let storage = DataStore::open(&path).expect("Failed to open storage");
        (dir, storage)
    }

    #[test]
    fn test_write_and_read_some() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key";
        let data = TestData {
            id: 42,
            name: "Example".to_string(),
        };

        storage
            .write_option(key, Some(&data))
            .expect("Failed to write option");

        let retrieved = storage
            .read_option::<TestData>(key)
            .expect("Failed to read option");

        assert_eq!(
            retrieved,
            Some(data),
            "Stored and retrieved values do not match"
        );
    }

    #[test]
    fn test_write_and_read_none() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_none";

        storage
            .write_option::<TestData>(key, None)
            .expect("Failed to write tombstone");

        let retrieved = storage
            .read_option::<TestData>(key)
            .expect("Failed to read option");

        assert_eq!(
            retrieved, None,
            "Expected None when reading tombstone marker"
        );
    }

    #[test]
    fn test_write_none_does_not_delete_entry() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key".as_slice();
        let initial_value = TestData {
            id: 42,
            name: "Initial Data".to_string(),
        };

        // Step 1: Write an initial Some(TestData) value
        storage
            .write_option(key, Some(&initial_value))
            .expect("Failed to write initial entry");

        // Verify it was stored correctly
        let retrieved = storage
            .read_option::<TestData>(key)
            .expect("Failed to read initial entry");
        assert_eq!(
            retrieved,
            Some(initial_value),
            "Initial entry should be readable"
        );

        // Step 2: Write None, marking it with the tombstone
        storage
            .write_option::<TestData>(key, None)
            .expect("Failed to write tombstone marker");

        // Step 3: Ensure reading returns None (meaning it's correctly recognized as a tombstone)
        let retrieved_none = storage.read_option::<TestData>(key);
        assert_eq!(
            retrieved_none.unwrap(),
            None,
            "Entry should return None when tombstone is written"
        );

        // Step 4: Ensure the entry still exists in storage (not fully deleted)
        let raw_entry = storage.read(&prefix_key(&TEST_OPTION_PREFIX, key));
        assert!(
            raw_entry.is_some(),
            "Entry should still exist in storage even after writing None"
        );

        // Step 5: Ensure the stored entry matches the expected tombstone marker
        assert_eq!(
            raw_entry.unwrap().as_slice(),
            TEST_OPTION_TOMBSTONE_MARKER,
            "Stored value should be the tombstone marker, not a deleted entry"
        );
    }

    #[test]
    fn test_overwrite_with_none() {
        let (_dir, storage) = create_temp_storage();

        let key = b"overwrite_key";
        let data = TestData {
            id: 99,
            name: "To be deleted".to_string(),
        };

        storage
            .write_option(key, Some(&data))
            .expect("Failed to write initial value");

        let retrieved = storage
            .read_option::<TestData>(key)
            .expect("Failed to read option");
        assert_eq!(
            retrieved,
            Some(data),
            "Initial data does not match expected value"
        );

        // Overwrite with `None`
        storage
            .write_option::<TestData>(key, None)
            .expect("Failed to overwrite with tombstone");

        let retrieved_after_delete = storage
            .read_option::<TestData>(key)
            .expect("Failed to read after deletion");

        assert_eq!(
            retrieved_after_delete, None,
            "Expected None after overwriting with tombstone"
        );
    }

    #[test]
    fn test_read_non_existent_key_error() {
        let (_dir, storage) = create_temp_storage();

        let key = b"non_existent_key";

        let retrieved = storage.read_option::<TestData>(key);

        assert!(
            matches!(retrieved, Err(ref e) if e.kind() == ErrorKind::NotFound),
            "Expected `ErrorKind::NotFound` when reading a non-existent key, got: {:?}",
            retrieved
        );
    }

    #[test]
    fn test_multiple_writes_and_reads() {
        let (_dir, storage) = create_temp_storage();

        let entries = vec![
            (
                b"key1",
                Some(TestData {
                    id: 1,
                    name: "One".to_string(),
                }),
            ),
            (
                b"key2",
                Some(TestData {
                    id: 2,
                    name: "Two".to_string(),
                }),
            ),
            (b"key3", None),
        ];

        for (key, value) in &entries {
            storage
                .write_option(*key, value.as_ref())
                .expect("Failed to write entry");
        }

        for (key, expected) in &entries {
            let retrieved = storage
                .read_option::<TestData>(*key)
                .expect("Failed to read entry");

            assert_eq!(
                &retrieved, expected,
                "Mismatch between written and retrieved data for key {:?}",
                key
            );
        }
    }

    #[test]
    fn test_option_prefix_is_applied_for_some() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key_option";
        let prefixed_key = prefix_key(TEST_OPTION_PREFIX, key);
        let test_value = Some(TestData {
            id: 456,
            name: "Test Option Value".to_string(),
        });

        // Write `Some(value)` with option handling
        storage
            .write_option(key, test_value.as_ref())
            .expect("Failed to write option");

        // Ensure the prefixed key exists in storage
        let raw_data = storage.read(&prefixed_key);
        assert!(
            raw_data.is_some(),
            "Expected data to be stored under the prefixed key"
        );

        // Ensure the unprefixed key does not exist
        let raw_data_unprefixed = storage.read(key);
        assert!(
            raw_data_unprefixed.is_none(),
            "Unprefixed key should not exist in storage"
        );

        // Ensure we can read the value correctly
        let retrieved = storage
            .read_option::<TestData>(key)
            .expect("Failed to read option");

        assert_eq!(
            retrieved, test_value,
            "Stored and retrieved option values do not match"
        );
    }

    #[test]
    fn test_option_prefix_is_applied_for_none() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key_none";
        let prefixed_key = prefix_key(TEST_OPTION_PREFIX, key);

        // Write `None`
        storage
            .write_option::<TestData>(key, None)
            .expect("Failed to write None with option handling");

        // Ensure the prefixed key exists in storage (tombstone marker stored)
        let raw_data = storage.read(&prefixed_key);
        assert!(
            raw_data.is_some(),
            "Expected tombstone marker to be stored under the prefixed key"
        );

        // Ensure the unprefixed key does not exist
        let raw_data_unprefixed = storage.read(key);
        assert!(
            raw_data_unprefixed.is_none(),
            "Unprefixed key should not exist in storage"
        );

        // Ensure we can read the value correctly
        let retrieved = storage
            .read_option::<TestData>(key)
            .expect("Failed to read None with option handling");

        assert_eq!(
            retrieved, None,
            "Expected None when retrieving a stored tombstone marker"
        );
    }

    #[test]
    fn test_option_prefixing_does_not_affect_regular_storage() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key_option_non_prefixed";
        let test_value = TestData {
            id: 789,
            name: "Non-Prefixed Option Value".to_string(),
        };

        // Directly write a non-option value to the base storage
        storage
            .write(key, &bincode::serialize(&test_value).unwrap())
            .expect("Failed to write non-option value");

        // Ensure reading from the option-prefixed key fails (since it was not stored as an option)
        let prefixed_key = prefix_key(TEST_OPTION_PREFIX, key);
        let raw_data_prefixed = storage.read(&prefixed_key);
        assert!(
            raw_data_prefixed.is_none(),
            "No option-prefixed entry should exist for a non-prefixed write"
        );

        // Ensure we can still retrieve the non-prefixed stored value
        let raw_bytes = storage.read(key).expect("Failed to read stored data");
        let retrieved: TestData =
            bincode::deserialize(&raw_bytes).expect("Failed to deserialize TestData");
        assert_eq!(
            retrieved, test_value,
            "Non-prefixed value should be retrievable as a plain `T`"
        );
    }
}
