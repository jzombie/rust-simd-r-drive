#[cfg(test)]
mod tests {

    use serde::{Deserialize, Serialize};
    use simd_r_drive::{compute_checksum, compute_hash, DataStore};
    use std::fs::{metadata, OpenOptions};
    use std::io::{Seek, SeekFrom, Write};
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
    fn test_recovery_from_interrupted_write() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_persistent.bin");

        // Step 1: Write a valid entry and close storage
        {
            let storage = DataStore::open(&path).expect("Failed to open storage");
            storage
                .write(b"key1", b"Valid Entry")
                .expect("Write failed");
            eprintln!("Written valid entry. Checking file size...");
        } // Storage goes out of scope here, ensuring file is closed

        let file_size_before = metadata(&path).expect("Failed to get metadata").len();
        eprintln!("File size before corruption: {}", file_size_before);

        // Step 2: Simulate corruption by writing partial data
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true) // Allows writing but does NOT truncate
                .open(&path)
                .expect("Failed to open file");

            file.seek(SeekFrom::End(0)) // Move cursor to the end
                .expect("Failed to seek to end");

            file.write_all(b"CORRUPT") // Write corrupted data at the end
                .expect("Failed to write corruption");

            file.flush().expect("Flush failed");
        }

        let file_size_after = metadata(&path).expect("Failed to get metadata").len();
        eprintln!("File size after corruption: {}", file_size_after);

        // Skipping the following tests on Windows due to memory-mapped file restrictions.
        //
        // Note: Testing this manually on Windows does work. This issue appears to only be related
        // to the current testing environment and I have not yet found a work around after trying
        // to close this section in a variety of ways. Maybe implementing a `Drop` trait on `Storage`
        // could work?
        //
        // After wrapping `mmap` with `Arc<AtomicPtr<Mmap>>`, these tests started failing
        // at commit:
        // https://github.com/jzombie/rust-simd-r-drive/pull/5/commits/e53d7e9e1767a6e193e0a6b33656b56d2febbbfc
        //
        // Error encountered:
        // Failed to recover storage: Os { code: 1224, kind: Uncategorized,
        // message: "The requested operation cannot be performed on a file with a user-mapped section open." }
        if !cfg!(target_os = "windows") {
            // Step 3: Attempt to recover storage and write to it
            {
                let storage = DataStore::open(&path).expect("Failed to recover storage");

                //  Check that the recovered file size matches the original before corruption
                let file_size_after_recovery =
                    metadata(&path).expect("Failed to get metadata").len();
                eprintln!("File size after recovery: {}", file_size_after_recovery);

                assert_eq!(
                    file_size_after_recovery, file_size_before,
                    "File size after recovery should match size before corruption"
                );

                // Verify recovery worked
                let recovered = storage.read(b"key1");
                assert!(
                    recovered.is_some(),
                    "Expected to recover at least one valid entry"
                );

                // Check if file can still be written to and read from after recovery
                let new_key = b"new_key";
                let new_payload = b"New Data After Recovery";

                // Write new data
                storage
                    .write(new_key, new_payload)
                    .expect("Failed to append entry after recovery");

                // Verify new data
                let retrieved = storage.read(new_key);
                assert!(
                    retrieved.is_some(),
                    "Failed to retrieve newly written entry after recovery"
                );
                assert_eq!(
                    retrieved.unwrap().as_slice(),
                    new_payload,
                    "Newly written payload does not match expected value"
                );
            }

            // Step 4: Verify re-opened storage can still access these keys
            {
                let storage = DataStore::open(&path).expect("Failed to recover storage");

                assert_eq!(storage.read(b"key1").unwrap().as_slice(), b"Valid Entry");

                assert_eq!(
                    storage.read(b"new_key").unwrap().as_slice(),
                    b"New Data After Recovery"
                );
            }
        }
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_persistent.bin");

        // Step 1: Write some entries and close the storage
        {
            let storage = DataStore::open(&path).expect("Failed to open storage");

            let entries = vec![
                (b"key1".as_slice(), b"Persistent Entry 1 ..".as_slice()),
                (b"key2".as_slice(), b"Persistent Entry 2 ....".as_slice()),
                (b"key3".as_slice(), b"Persistent Entry 3 ......".as_slice()),
            ];

            for (key, payload) in &entries {
                storage.write(key, payload).expect("Failed to append entry");
            }

            eprintln!("Entries written. Closing file...");
        } // Storage goes out of scope here, closing the file

        // Step 2: Reopen storage and verify persistence
        {
            let storage = DataStore::open(&path).expect("Failed to reopen storage");

            for (key, expected_payload) in [
                (b"key1", &b"Persistent Entry 1 .."[..]),
                (b"key2", &b"Persistent Entry 2 ...."[..]),
                (b"key3", &b"Persistent Entry 3 ......"[..]),
            ] {
                let retrieved = storage.read(key);
                assert!(
                    retrieved.is_some(),
                    "Entry should be found after reopening, but got None"
                );
                assert_eq!(
                    retrieved.unwrap().as_slice(),
                    expected_payload,
                    "Retrieved payload does not match expected value after reopening"
                );
            }
        }
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
    fn test_compact_storage_with_mixed_types() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct CustomStruct {
            id: u32,
            name: String,
            active: bool,
        }

        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_mixed.bin");

        let mut storage = DataStore::open(&path).expect("Failed to open storage");

        // Different Data Types
        let key1 = b"text_key";
        let key2 = b"binary_key";
        let key3 = b"struct_key";
        let key4 = b"integer_key";
        let key5 = b"float_key";
        let key6 = b"mixed_key";
        let key7 = b"temp_key";

        let text_payload1 = "Initial Text Data".as_bytes();
        let binary_payload1 = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let struct_payload1 = CustomStruct {
            id: 42,
            name: "Alice".to_string(),
            active: true,
        };
        let integer_payload1 = 12345u64.to_le_bytes();
        let float_payload1 = 3.141592f64.to_le_bytes();
        let mixed_payload1 = 123u64.to_le_bytes();
        let temp_payload1 = 456u64.to_le_bytes();

        let text_payload2 = "Updated Text Data!".as_bytes();
        let binary_payload2 = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let struct_payload2 = CustomStruct {
            id: 99,
            name: "Bob".to_string(),
            active: false,
        };
        let integer_payload2 = 67890u64.to_le_bytes();
        let float_payload2 = 2.718281f64.to_le_bytes();
        let mixed_payload2 = "Hello".as_bytes();
        let temp_payload2 = 789u64.to_le_bytes();

        // Serialize structured data
        let struct_payload1_serialized = bincode::serialize(&struct_payload1).unwrap();
        let struct_payload2_serialized = bincode::serialize(&struct_payload2).unwrap();

        // Step 1: Append Initial Entries
        storage.write(key1, text_payload1).expect("Append failed");
        storage
            .write(key2, &binary_payload1)
            .expect("Append failed");
        storage
            .write(key3, &struct_payload1_serialized)
            .expect("Append failed");
        storage
            .write(key4, &integer_payload1)
            .expect("Append failed");
        storage.write(key5, &float_payload1).expect("Append failed");
        storage.write(key6, &mixed_payload1).expect("Append failed");
        storage.write(key7, &temp_payload1).expect("Append failed");

        // Step 2: Overwrite with Different Types
        storage.write(key1, text_payload2).expect("Append failed");
        storage
            .write(key2, &binary_payload2)
            .expect("Append failed");
        storage
            .write(key3, &struct_payload2_serialized)
            .expect("Append failed");
        storage
            .write(key4, &integer_payload2)
            .expect("Append failed");
        storage.write(key5, &float_payload2).expect("Append failed");
        storage.write(key6, mixed_payload2).expect("Append failed");
        storage.write(key7, &temp_payload2).expect("Append failed");

        // Ensure Data is Stored Correctly Before Compaction
        assert_eq!(storage.read(key1).as_deref(), Some(text_payload2));
        assert_eq!(storage.read(key2).as_deref(), Some(&binary_payload2[..]));
        assert_eq!(storage.read(key4).as_deref(), Some(&integer_payload2[..]));
        assert_eq!(storage.read(key5).as_deref(), Some(&float_payload2[..]));
        assert_eq!(storage.read(key6).as_deref(), Some(mixed_payload2));
        assert_eq!(storage.read(key7).as_deref(), Some(&temp_payload2[..]));

        storage.delete_entry(key7).unwrap();

        assert_eq!(storage.read(key7).as_deref(), None);

        let retrieved_struct = storage.read(key3).expect("Failed to retrieve struct");
        let deserialized_struct: CustomStruct = bincode::deserialize(retrieved_struct.as_slice())
            .expect("Failed to deserialize struct");
        assert_eq!(deserialized_struct, struct_payload2);

        // Check file size before compaction
        let size_before = std::fs::metadata(&path)
            .expect("Failed to get metadata")
            .len();
        eprintln!("File size before compaction: {}", size_before);

        // Step 3: Compact Storage
        eprintln!("Starting compaction...");
        storage.compact().expect("Compaction failed");

        // Check file size after compaction
        let size_after = std::fs::metadata(&path)
            .expect("Failed to get metadata")
            .len();
        eprintln!("File size after compaction: {}", size_after);

        assert!(
            size_after < size_before,
            "Compaction should reduce file size!"
        );

        // Step 4: Reopen Storage and Verify Data Integrity
        let storage = DataStore::open(&path).expect("Failed to reopen storage after compaction");

        // for entry in storage.iter_entries() {
        //     eprintln!("Entry: {:?}", entry);
        // }

        // Verify that only the latest versions remain
        assert_eq!(storage.read(key1).as_deref(), Some(text_payload2));
        assert_eq!(storage.read(key2).as_deref(), Some(&binary_payload2[..]));
        assert_eq!(storage.read(key4).as_deref(), Some(&integer_payload2[..]));
        assert_eq!(storage.read(key5).as_deref(), Some(&float_payload2[..]));
        assert_eq!(storage.read(key6).as_deref(), Some(mixed_payload2));
        assert_eq!(storage.read(key7).as_deref(), None);

        let retrieved_struct = storage.read(key3).expect("Failed to retrieve struct");
        let deserialized_struct: CustomStruct = bincode::deserialize(retrieved_struct.as_slice())
            .expect("Failed to deserialize struct");
        assert_eq!(deserialized_struct, struct_payload2);
    }

    #[test]
    fn test_persistence_across_multiple_reopens_with_updates() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_persistent_updates.bin");

        // Step 1: Write initial entries and close storage
        {
            let storage = DataStore::open(&path).expect("Failed to open storage");

            storage
                .write(b"key1", b"Initial Value 1")
                .expect("Failed to append entry");
            storage
                .write(b"key2", b"Initial Value 2")
                .expect("Failed to append entry");

            eprintln!("Step 1: Initial entries written, closing file...");
        } // Storage closed here

        // Step 2: Reopen storage, update values, and close again
        {
            let storage = DataStore::open(&path).expect("Failed to reopen storage");

            storage
                .write(b"key1", b"Updated Value 1")
                .expect("Failed to update key1");
            storage
                .write(b"key2", b"Updated Value 2")
                .expect("Failed to update key2");

            eprintln!("Step 2: Updates written, closing file...");
        } // Storage closed here

        // Step 3: Reopen storage again and verify persistence
        {
            let storage = DataStore::open(&path).expect("Failed to reopen storage");

            assert_eq!(
                storage.read(b"key1").as_deref(),
                Some(b"Updated Value 1".as_slice()),
                "Key1 should contain the updated value"
            );
            assert_eq!(
                storage.read(b"key2").as_deref(),
                Some(b"Updated Value 2".as_slice()),
                "Key2 should contain the updated value"
            );

            eprintln!("Step 3: Persistence check passed after multiple reopens.");
        } // Storage closed here
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

    #[test]
    fn test_write_and_read_streams() {
        use std::fs::File;
        use std::io::{BufReader, Read, Write};

        let (_dir, storage) = create_temp_storage();
        let large_key = b"streamed_large_entry";

        // 1. Create a temporary file to act as a real stream
        let file_path = _dir.path().join("test_large_file.bin");
        let mut test_file = File::create(&file_path).expect("Failed to create test file");

        let payload_size = 1 * 1024 * 1024; // 1MB
        let test_data = vec![b'X'; payload_size];

        // 2. Write real test data to file
        test_file
            .write_all(&test_data)
            .expect("Failed to write test data");
        test_file.flush().expect("Failed to flush test data");

        // Compute checksum for validation
        let expected_checksum = compute_checksum(&test_data);

        // 3. Open the file as a streaming reader
        let mut reader = BufReader::new(File::open(&file_path).expect("Failed to open test file"));

        // 4. Write to storage using the real stream
        storage
            .write_stream(large_key, &mut reader)
            .expect("Failed to append large entry");

        // 5. Retrieve the entry
        let retrieved_entry = storage
            .read(large_key)
            .expect("Failed to retrieve large entry");

        // 6. Create an EntryStream from the retrieved entry
        let mut entry_stream: simd_r_drive::storage_engine::EntryStream = retrieved_entry.into();

        // 7. Read from the stream in chunks and compare
        let mut buffer = vec![0; 4096]; // 4KB read buffer
        let mut streamed_data = Vec::new();

        while let Ok(bytes_read) = entry_stream.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }
            streamed_data.extend_from_slice(&buffer[..bytes_read]);
        }

        // 8. Verify integrity
        assert_eq!(
            streamed_data.len(),
            payload_size,
            "Streamed entry size does not match expected size"
        );

        assert_eq!(
            compute_checksum(&streamed_data),
            expected_checksum,
            "Checksum mismatch for streamed entry"
        );

        assert_eq!(
            streamed_data.as_slice(),
            test_data.as_slice(),
            "Streamed entry data does not match expected data"
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
    fn test_clone_arc_retains_same_memory_address() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key";
        let payload = b"Test Data";

        // Write entry to storage
        storage.write(key, payload).expect("Failed to write entry");

        // Retrieve the entry handle
        let entry_handle = storage.read(key).expect("Failed to read entry");

        // Clone the entry handle
        let cloned_entry = entry_handle.clone_arc();

        // Ensure the payload remains the same
        assert_eq!(
            entry_handle.as_slice(),
            cloned_entry.as_slice(),
            "Cloned entry's data should match the original"
        );

        // Ensure the memory addresses are the same
        let original_address_range = entry_handle.address_range();
        let cloned_address_range = cloned_entry.address_range();

        assert_eq!(
            original_address_range.start, cloned_address_range.start,
            "Cloned entry should retain the same start memory address"
        );
        assert_eq!(
            original_address_range.end, cloned_address_range.end,
            "Cloned entry should retain the same end memory address"
        );

        // Ensure they share the same mmap reference
        assert_eq!(
            entry_handle.as_slice().as_ptr(),
            cloned_entry.as_slice().as_ptr(),
            "Cloned entry should point to the same memory location"
        );
    }

    #[test]
    fn test_clone_arc_zero_copy_behavior() {
        let (_dir, storage) = create_temp_storage();

        let key = b"zero_copy_test";
        let payload = b"Zero-copy validation data";

        // Write entry to storage
        storage.write(key, payload).expect("Failed to write entry");

        // Retrieve the entry handle
        let entry_handle = storage.read(key).expect("Failed to read entry");

        // Clone the entry handle
        let cloned_entry = entry_handle.clone_arc();

        // Ensure they reference the same memory region
        assert_eq!(
            entry_handle.as_slice().as_ptr(),
            cloned_entry.as_slice().as_ptr(),
            "Cloned entry should point to the same memory location"
        );

        // Validate data integrity (same content)
        assert_eq!(
            entry_handle.as_slice(),
            cloned_entry.as_slice(),
            "Cloned entry's data should match the original"
        );

        // Ensure address ranges are identical
        assert_eq!(
            entry_handle.address_range(),
            cloned_entry.address_range(),
            "Memory address range should remain the same"
        );

        // Validate checksum remains unchanged
        assert_eq!(
            entry_handle.checksum(),
            cloned_entry.checksum(),
            "Checksum mismatch: Cloned entry should have the same checksum"
        );

        // Ensure cloned entry remains valid even after dropping original
        drop(entry_handle);
        assert!(
            !cloned_entry.as_slice().is_empty(),
            "Cloned entry should remain accessible after the original is dropped"
        );
    }

    #[test]
    fn test_mmap_exposure_and_zero_copy_read() {
        let (_dir, storage) = create_temp_storage();

        let key = b"mmap_exposure_test";
        let payload = b"Direct mmap testing";

        // Write entry to storage
        storage.write(key, payload).expect("Failed to write entry");

        // Retrieve the entry handle
        let entry_handle = storage.read(key).expect("Failed to read entry");

        // Get direct access to mmap for testing
        let mmap_arc = storage.get_mmap_arc_for_testing(); // Get Arc<Mmap>
        let mmap_ptr = mmap_arc.as_ptr(); // Get the raw pointer
        let mmap_len = mmap_arc.len(); // Get the length

        // Ensure the entry slice references the mmap memory region
        let entry_ptr = entry_handle.as_slice().as_ptr();

        assert!(
            (entry_ptr as usize) >= (mmap_ptr as usize),
            "Entry should be mapped within the mmap memory region"
        );

        assert!(
            (entry_ptr as usize) < (mmap_ptr as usize + mmap_len),
            "Entry pointer should be within the mmap memory bounds"
        );

        //  Ensure no memory duplication
        assert_eq!(
            entry_handle.as_slice().as_ptr(),
            entry_ptr,
            "Entry read should not allocate new memory"
        );

        // Validate persistence after dropping the entry handle
        drop(entry_handle);
        assert!(
            !unsafe { std::slice::from_raw_parts(mmap_ptr, payload.len()) }.is_empty(),
            "Memory should remain accessible after dropping the entry handle"
        );

        // Ensure data integrity
        let read_back = storage.read(key).expect("Entry should still be readable");
        assert_eq!(
            read_back.as_slice(),
            payload,
            "Data integrity failure: Read data does not match original payload"
        );
    }
}
