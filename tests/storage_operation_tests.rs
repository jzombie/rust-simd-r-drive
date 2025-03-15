#[cfg(test)]
mod tests {

    use simd_r_drive::{compute_checksum, compute_hash, DataStore};
    use tempfile::tempdir;

    /// Helper function to create a temporary file for testing
    fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage.bin");

        let storage = DataStore::open(&path).expect("Failed to open storage");
        (dir, storage)
    }

    #[test]
    fn test_copy_entry_between_storages() {
        let (_dir1, source_storage) = create_temp_storage();
        let (_dir2, mut target_storage) = create_temp_storage();

        let key = b"copy_key";
        let payload = b"Data to be copied";

        // Step 1: Append the entry to the source storage
        source_storage
            .write(key, payload)
            .expect("Failed to append entry");

        // Step 2: Copy the entry to the target storage
        source_storage
            .copy_entry(key, &mut target_storage)
            .expect("Failed to copy entry");

        // Step 3: Ensure the original entry still exists in the source
        let original_entry = source_storage.read(key).expect("Source entry should exist");
        assert_eq!(
            original_entry.as_slice(),
            payload,
            "Original data should remain unchanged in source"
        );

        // Step 4: Ensure the copied entry exists in the target
        let copied_entry = target_storage
            .read(key)
            .expect("Copied entry should exist in target");
        assert_eq!(
            copied_entry.as_slice(),
            payload,
            "Copied data should match the original"
        );

        // Step 5: Verify metadata integrity
        assert_eq!(
            original_entry.key_hash(),
            copied_entry.key_hash(),
            "Key hash should remain unchanged after copy"
        );
        assert_eq!(
            original_entry.checksum(),
            copied_entry.checksum(),
            "Checksum should remain unchanged after copy"
        );
        assert!(
            copied_entry.is_valid_checksum(),
            "Copied entry should pass checksum validation"
        );
    }

    #[test]
    fn test_move_entry_between_storages() {
        let (_dir1, source_storage) = create_temp_storage();
        let (_dir2, mut target_storage) = create_temp_storage();

        let key = b"move_key";
        let payload = b"Data to be moved";

        // Step 1: Append the entry to the source storage
        source_storage
            .write(key, payload)
            .expect("Failed to append entry");

        // Step 2: Move the entry to the target storage
        source_storage
            .move_entry(key, &mut target_storage)
            .expect("Failed to move entry");

        // Step 3: Ensure the original entry no longer exists in the source
        assert!(
            source_storage.read(key).is_none(),
            "Moved entry should no longer exist in source"
        );

        // Step 4: Ensure the moved entry exists in the target
        let moved_entry = target_storage
            .read(key)
            .expect("Moved entry should exist in target");
        assert_eq!(
            moved_entry.as_slice(),
            payload,
            "Moved data should match the original"
        );

        // Step 5: Verify metadata integrity
        assert_eq!(
            moved_entry.key_hash(),
            compute_hash(key),
            "Key hash should remain unchanged after move"
        );
        assert_eq!(
            moved_entry.raw_checksum(),
            compute_checksum(payload),
            "Checksum should remain unchanged after move"
        );
        assert!(
            moved_entry.is_valid_checksum(),
            "Moved entry should pass checksum validation"
        );
    }

    #[test]
    fn test_update_and_delete_entry() {
        let (_dir, storage) = create_temp_storage();

        let key1 = b"key1";
        let key2 = b"key2";

        let initial_payload1: &[u8] = b"Initial Data";
        let initial_payload2: &[u8] = b"Initial Other Data";

        let updated_payload1: &[u8] = b"Updated Data";
        let updated_payload2: &[u8] = b"Updated Other Data";

        // Append initial entries
        storage
            .write(key1, initial_payload1)
            .expect("Failed to append entry");
        storage
            .write(key2, initial_payload2)
            .expect("Failed to append entry");

        // Verify initial entries exist
        assert_eq!(storage.read(key1).as_deref(), Some(initial_payload1));
        assert_eq!(storage.read(key2).as_deref(), Some(initial_payload2));

        // Update entries
        storage
            .write(key1, updated_payload1)
            .expect("Failed to update entry");
        storage
            .write(key2, updated_payload2)
            .expect("Failed to update entry");

        // Verify updates were applied correctly
        assert_eq!(storage.read(key1).as_deref(), Some(updated_payload1));
        assert_eq!(storage.read(key2).as_deref(), Some(updated_payload2));

        let count_before_delete = storage.count();

        assert_eq!(count_before_delete, 2);

        // Delete entry for key1
        storage.delete_entry(key1).expect("Failed to delete entry");

        // Verify count is reduced
        let count_after_delete = storage.count();
        assert_eq!(
            count_after_delete,
            count_before_delete - 1,
            "Entry count should decrease after deletion"
        );

        // Verify key1 is no longer retrievable
        assert!(
            storage.read(key1).is_none(),
            "Deleted key1 should not be retrievable"
        );

        // Verify key1 does not appear in iteration
        let keys_in_iteration: Vec<_> = storage.iter_entries().collect();
        for entry in keys_in_iteration {
            assert_ne!(
                entry.as_ref(),
                updated_payload1,
                "Deleted entry should not appear in iteration"
            );
            assert_ne!(
                entry.as_ref(),
                initial_payload1,
                "Older version of deleted entry should not appear in iteration"
            );
        }

        assert_eq!(
            storage.read(b"key2").unwrap().as_slice(),
            updated_payload2,
            "`key2` does not match updated payload"
        );
    }

    #[test]
    fn test_rename_entry() {
        let (_dir, storage) = create_temp_storage();

        let old_key = b"old_key";
        let new_key = b"new_key";
        let payload = b"Data for renaming";

        // Step 1: Write an entry with the old key
        storage
            .write(old_key, payload)
            .expect("Failed to append entry");

        // Step 2: Rename the entry
        storage
            .rename_entry(old_key, new_key)
            .expect("Failed to rename entry");

        // Step 3: Ensure the new key exists and has the same data
        let renamed_entry = storage.read(new_key).expect("Renamed entry should exist");
        assert_eq!(
            renamed_entry.as_slice(),
            payload,
            "Renamed entry data should match the original"
        );

        // Step 4: Ensure the old key no longer exists
        assert!(
            storage.read(old_key).is_none(),
            "Old key should no longer exist after renaming"
        );
    }

    #[test]
    fn test_nested_storage_extraction() {
        let (_dir1, storage1) = create_temp_storage();
        let key1 = b"original_key";
        let payload1 = b"Initial Data";

        // Step 1: Write an entry into the original storage
        storage1
            .write(key1, payload1)
            .expect("Failed to append entry to initial storage");

        // Step 2: Read the full storage as raw bytes
        let storage1_bytes =
            std::fs::read(storage1.get_path()).expect("Failed to read storage file");

        // Step 3: Create a second storage and embed the first storage inside it
        let (_dir2, storage2) = create_temp_storage();
        let nested_key = b"nested_storage";

        storage2
            .write(nested_key, &storage1_bytes)
            .expect("Failed to store the original storage inside the new storage");

        // Step 4: Add additional entries to the second storage
        storage2
            .write(b"extra_key1", b"Extra Entry 1")
            .expect("Failed to append extra entry 1");
        storage2
            .write(b"extra_key2", b"Extra Entry 2")
            .expect("Failed to append extra entry 2");

        // Step 5: Extract the nested storage from storage2
        let extracted_storage_bytes = storage2
            .read(nested_key)
            .expect("Failed to retrieve the nested storage")
            .as_slice()
            .to_vec();

        // Step 6: Write extracted bytes to a new storage file
        let nested_storage_path = _dir2.path().join("extracted_storage.bin");
        std::fs::write(&nested_storage_path, &extracted_storage_bytes)
            .expect("Failed to write extracted storage to file");

        // Step 7: Open the extracted storage as a new storage instance
        let extracted_storage =
            DataStore::open(&nested_storage_path).expect("Failed to open extracted storage");

        // Step 8: Read the original entry from the extracted storage
        let retrieved_entry = extracted_storage
            .read(key1)
            .expect("Failed to retrieve original entry from extracted storage");

        // Step 9: Validate that the extracted storage's data is correct
        assert_eq!(
            retrieved_entry.as_slice(),
            payload1,
            "Extracted storage does not contain the correct original data"
        );
    }
}
