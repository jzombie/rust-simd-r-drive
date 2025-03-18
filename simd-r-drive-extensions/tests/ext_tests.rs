#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use simd_r_drive::DataStore;
    use simd_r_drive_extensions::StorageOptionExt;
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
    fn test_read_non_existent_key() {
        let (_dir, storage) = create_temp_storage();

        let key = b"non_existent_key";

        let retrieved = storage.read_option::<TestData>(key);

        assert!(
            retrieved.is_none(),
            "Expected None when reading a key that was never written"
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
}
