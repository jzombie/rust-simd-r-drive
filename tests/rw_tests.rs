#[cfg(test)]
mod tests {
    use simd_r_drive::AppendStorage;
    use tempfile::tempdir;
    use std::fs::{metadata, OpenOptions};
    use std::io::Write;
    use serde::{Serialize, Deserialize};
    use bincode;

    /// Helper function to create a temporary file for testing
    fn create_temp_storage() -> (tempfile::TempDir, AppendStorage) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage.bin");

        let storage = AppendStorage::open(&path).expect("Failed to open storage");
        (dir, storage)
    }

    #[test]
    fn test_append_and_read_last_entry() {
        let (_dir, mut storage) = create_temp_storage();

        let key = b"test_key".as_slice();
        let payload = b"Hello, world!".as_slice();
        storage.append_entry(key, payload).expect("Failed to append entry");

        let last_entry = storage.read_last_entry().expect("No entry found");
        assert_eq!(last_entry, payload, "Stored payload does not match expected value");
    }

    #[test]
    fn test_multiple_appends_and_reads() {
        let (_dir, mut storage) = create_temp_storage();

        let entries = vec![
            (b"key1".as_slice(), b"First Entry".as_slice()),
            (b"key2".as_slice(), b"Second Entry".as_slice()),
            (b"key3".as_slice(), b"Third Entry".as_slice()),
        ];

        for (key, payload) in &entries {
            storage.append_entry(*key, *payload).expect("Failed to append entry");
        }

        let last_entry = storage.read_last_entry().expect("No last entry found");
        assert_eq!(last_entry, entries.last().unwrap().1, "Last entry does not match expected value");
    }

    #[test]
    fn test_varying_payload_sizes() {
        let (_dir, mut storage) = create_temp_storage();

        let payloads = vec![
            vec![b'a'; 10],   // Small payload
            vec![b'b'; 1024], // Medium payload
            vec![b'c'; 4096], // Large payload
        ];

        for (i, payload) in payloads.iter().enumerate() {
            storage.append_entry(format!("key{}", i).as_bytes(), payload.as_slice())
                .expect("Failed to append entry");
        }

        let last_entry = storage.read_last_entry().expect("No last entry found");
        assert_eq!(last_entry, payloads.last().unwrap().as_slice(), "Last entry payload does not match expected value");
    }

    #[test]
    fn test_retrieve_entry_by_key() {
        let (_dir, mut storage) = create_temp_storage();
    
        let key = b"test_key".as_slice();
        let payload = b"Hello, world!".as_slice();
        storage.append_entry(key, payload).expect("Failed to append entry");
    
        let retrieved = storage.get_entry_by_key(key);
    
        assert!(
            retrieved.is_some(),
            "Entry should be found by key, but got None"
        );
    
        assert_eq!(
            retrieved.unwrap(),
            payload,
            "Retrieved payload does not match expected value"
        );
    }

    #[test]
    fn test_update_entries_with_varying_lengths() {
        let (_dir, mut storage) = create_temp_storage();

        let key1 = b"key1".as_slice();
        let key2 = b"key2".as_slice();
        let key3 = b"key3".as_slice();

        let initial_payload1 = b"Short".as_slice();
        let initial_payload2 = b"Medium length payload".as_slice();
        let initial_payload3 = b"Longer initial payload data".as_slice();

        storage.append_entry(key1, initial_payload1).expect("Failed to append entry");
        storage.append_entry(key2, initial_payload2).expect("Failed to append entry");
        storage.append_entry(key3, initial_payload3).expect("Failed to append entry");

        let updated_payload1 = b"Updated with longer data!".as_slice();
        let updated_payload2 = b"Short".as_slice();

        storage.append_entry(key1, updated_payload1).expect("Failed to update entry");
        storage.append_entry(key2, updated_payload2).expect("Failed to update entry");

        let retrieved1 = storage.get_entry_by_key(key1).expect("Entry for key1 should be found");
        assert_eq!(retrieved1, updated_payload1, "Latest version of key1 was not retrieved");

        let retrieved2 = storage.get_entry_by_key(key2).expect("Entry for key2 should be found");
        assert_eq!(retrieved2, updated_payload2, "Latest version of key2 was not retrieved");

        let retrieved3 = storage.get_entry_by_key(key3).expect("Entry for key3 should be found");
        assert_eq!(retrieved3, initial_payload3, "Key3 should remain unchanged");
    }

    #[test]
    fn test_recovery_from_interrupted_write() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_persistent.bin");

        // Step 1: Write a valid entry and close storage
        {
            let mut storage = AppendStorage::open(&path).expect("Failed to open storage");
            storage.append_entry(b"key1", b"Valid Entry").expect("Write failed");
            eprintln!("Written valid entry. Checking file size...");
        } // **Storage goes out of scope here, ensuring file is closed**

        let file_size_before = metadata(&path).expect("Failed to get metadata").len();
        eprintln!("File size before corruption: {}", file_size_before);
 
        // Step 2: Simulate corruption by writing partial data
        {
            let mut file = OpenOptions::new().write(true).open(&path).expect("Failed to open file");
            file.write_all(b"CORRUPT").expect("Failed to write corruption");
            file.flush().expect("Flush failed");
        } // **File is closed again here**

        let file_size_after = metadata(&path).expect("Failed to get metadata").len();
        eprintln!("File size after corruption: {}", file_size_after);

        // Step 3: Attempt to recover storage **(Now that the file is closed)**
        let storage = AppendStorage::open(&path).expect("Failed to recover storage");

        // Step 4: Check that the recovered file size matches the original before corruption
        let file_size_after_recovery = metadata(&path).expect("Failed to get metadata").len();
        eprintln!("File size after recovery: {}", file_size_after_recovery);

        assert_eq!(
            file_size_after_recovery, file_size_before,
            "File size after recovery should match size before corruption"
        );

        // Step 5: Verify recovery worked
        let recovered = storage.get_entry_by_key(b"key1");
        assert!(
            recovered.is_some(),
            "Expected to recover at least one valid entry"
        );

        eprintln!("Test passed: Recovery successful, file size matches.");
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_persistent.bin");

        // Step 1: Write some entries and close the storage
        {
            let mut storage = AppendStorage::open(&path).expect("Failed to open storage");

            let entries = vec![
                (b"key1".as_slice(), b"Persistent Entry 1 ..".as_slice()),
                (b"key2".as_slice(), b"Persistent Entry 2 ....".as_slice()),
                (b"key3".as_slice(), b"Persistent Entry 3 ......".as_slice()),
            ];

            for (key, payload) in &entries {
                storage.append_entry(*key, *payload).expect("Failed to append entry");
            }

            eprintln!("Entries written. Closing file...");
        } // **Storage goes out of scope here, closing the file**

        // Step 2: Reopen storage and verify persistence
        let storage = AppendStorage::open(&path).expect("Failed to reopen storage");

        for (key, expected_payload) in [
            (b"key1", &b"Persistent Entry 1 .."[..]),
            (b"key2", &b"Persistent Entry 2 ...."[..]),
            (b"key3", &b"Persistent Entry 3 ......"[..]),
        ] {
            let retrieved = storage.get_entry_by_key(key);
            assert!(
                retrieved.is_some(),
                "Entry should be found after reopening, but got None"
            );
            assert_eq!(
                retrieved.unwrap(),
                expected_payload,
                "Retrieved payload does not match expected value after reopening"
            );
        }

        eprintln!("All entries successfully recovered after reopening.");
    }

    #[test]
    fn test_compact_storage() {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_compact.bin");

        // Step 1: Open and Write Initial Entries
        let mut storage = AppendStorage::open(&path).expect("Failed to open storage");

        let key1 = b"key1";
        let key2 = b"key2";
        
        let initial_payload1 = b"First Version of Key1";
        let initial_payload2 = b"First Version of Key2";

        let updated_payload1 = b"Updated Version of Key1";
        let updated_payload2 = b"Updated Version of Key2";
        
        let second_update_payload1 = b"Second Update of Key1"; // Newer than `updated_payload1`

        // Append Initial Entries
        storage.append_entry(key1, initial_payload1).expect("Append failed");
        storage.append_entry(key2, initial_payload2).expect("Append failed");

        // Overwrite entries with newer versions
        storage.append_entry(key1, updated_payload1).expect("Append failed");
        storage.append_entry(key2, updated_payload2).expect("Append failed");

        // One more update for key1
        storage.append_entry(key1, second_update_payload1).expect("Append failed");

        // **Step 2: Check file size before compaction**
        let size_before = std::fs::metadata(&path).expect("Failed to get file metadata").len();
        eprintln!("File size before compaction: {}", size_before);

        // Step 3: Verify that old entries exist **before** compaction
        assert_eq!(storage.get_entry_by_key(key1), Some(&second_update_payload1[..]));
        assert_eq!(storage.get_entry_by_key(key2), Some(&updated_payload2[..]));
        
        // Step 4: Compact the storage
        eprintln!("Starting compaction...");
        storage.compact().expect("Compaction failed");

        // **Step 5: Check file size after compaction**
        let size_after = std::fs::metadata(&path).expect("Failed to get file metadata").len();
        eprintln!("File size after compaction: {}", size_after);

        // **Ensure the file size is reduced**
        assert!(size_after < size_before, "Compaction should reduce file size!");

        // Step 6: **Reopen the storage** to remap it properly
        let storage = AppendStorage::open(&path).expect("Failed to reopen storage after compaction");

        for entry in storage.into_iter() {
            eprintln!("{:?}", entry);
        }

        // Step 7: Verify that **only the latest versions remain**
        let retrieved1 = storage.get_entry_by_key(key1).expect("Failed to retrieve key1 after compaction");
        assert_eq!(
            retrieved1, &second_update_payload1[..],
            "Key1 should contain the latest version after compaction"
        );
        
        let retrieved2 = storage.get_entry_by_key(key2).expect("Failed to retrieve key2 after compaction");
        assert_eq!(
            retrieved2, &updated_payload2[..],
            "Key2 should contain the latest version after compaction"
        );

        eprintln!("✅ Compaction test passed: Only the latest versions of keys remain.");
    }


    #[test]
    fn test_mixed_type_handling() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct CustomStruct {
            id: u32,
            name: String,
            active: bool,
        }

        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage_mixed.bin");

        let mut storage = AppendStorage::open(&path).expect("Failed to open storage");

        // **Different Data Types**
        let key1 = b"text_key";
        let key2 = b"binary_key";
        let key3 = b"struct_key";
        let key4 = b"integer_key";
        let key5 = b"float_key";
        let key6 = b"mixed_key";

        let text_payload1 = "Initial Text Data".as_bytes();
        let binary_payload1 = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let struct_payload1 = CustomStruct { id: 42, name: "Alice".to_string(), active: true };
        let integer_payload1 = 12345u64.to_le_bytes();
        let float_payload1 = 3.141592f64.to_le_bytes();
        let mixed_payload1 = 123u64.to_le_bytes();

        let text_payload2 = "Updated Text Data!".as_bytes();
        let binary_payload2 = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let struct_payload2 = CustomStruct { id: 99, name: "Bob".to_string(), active: false };
        let integer_payload2 = 67890u64.to_le_bytes();
        let float_payload2 = 2.718281f64.to_le_bytes();
        let mixed_payload2 = "Hello".as_bytes();

        // **Serialize structured data**
        let struct_payload1_serialized = bincode::serialize(&struct_payload1).unwrap();
        let struct_payload2_serialized = bincode::serialize(&struct_payload2).unwrap();

        // Step 1: Append Initial Entries
        storage.append_entry(key1, text_payload1).expect("Append failed");
        storage.append_entry(key2, &binary_payload1).expect("Append failed");
        storage.append_entry(key3, &struct_payload1_serialized).expect("Append failed");
        storage.append_entry(key4, &integer_payload1).expect("Append failed");
        storage.append_entry(key5, &float_payload1).expect("Append failed");
        storage.append_entry(key6, &mixed_payload1).expect("Append failed");

        // Step 2: Overwrite with Different Types
        storage.append_entry(key1, text_payload2).expect("Append failed");
        storage.append_entry(key2, &binary_payload2).expect("Append failed");
        storage.append_entry(key3, &struct_payload2_serialized).expect("Append failed");
        storage.append_entry(key4, &integer_payload2).expect("Append failed");
        storage.append_entry(key5, &float_payload2).expect("Append failed");
        storage.append_entry(key6, &mixed_payload2).expect("Append failed");

        // **Ensure Data is Stored Correctly Before Compaction**
        assert_eq!(storage.get_entry_by_key(key1), Some(text_payload2));
        assert_eq!(storage.get_entry_by_key(key2), Some(&binary_payload2[..]));
        assert_eq!(storage.get_entry_by_key(key4), Some(&integer_payload2[..]));
        assert_eq!(storage.get_entry_by_key(key5), Some(&float_payload2[..]));
        assert_eq!(storage.get_entry_by_key(key6), Some(&mixed_payload2[..]));

        let retrieved_struct = storage.get_entry_by_key(key3).expect("Failed to retrieve struct");
        let deserialized_struct: CustomStruct = bincode::deserialize(retrieved_struct).expect("Failed to deserialize struct");
        assert_eq!(deserialized_struct, struct_payload2);

        // **Check file size before compaction**
        let size_before = std::fs::metadata(&path).expect("Failed to get metadata").len();
        eprintln!("File size before compaction: {}", size_before);

        // Step 3: Compact Storage
        eprintln!("Starting compaction...");
        storage.compact().expect("Compaction failed");

        // **Check file size after compaction**
        let size_after = std::fs::metadata(&path).expect("Failed to get metadata").len();
        eprintln!("File size after compaction: {}", size_after);

        assert!(size_after < size_before, "Compaction should reduce file size!");

        // Step 4: Reopen Storage and Verify Data Integrity
        let storage = AppendStorage::open(&path).expect("Failed to reopen storage after compaction");

        for entry in storage.into_iter() {
            eprintln!("Entry: {:?}", entry);
        }

        // **Verify that only the latest versions remain**
        assert_eq!(storage.get_entry_by_key(key1), Some(text_payload2));
        assert_eq!(storage.get_entry_by_key(key2), Some(&binary_payload2[..]));
        assert_eq!(storage.get_entry_by_key(key4), Some(&integer_payload2[..]));
        assert_eq!(storage.get_entry_by_key(key5), Some(&float_payload2[..]));
        assert_eq!(storage.get_entry_by_key(key6), Some(&mixed_payload2[..]));

        let retrieved_struct = storage.get_entry_by_key(key3).expect("Failed to retrieve struct");
        let deserialized_struct: CustomStruct = bincode::deserialize(retrieved_struct).expect("Failed to deserialize struct");
        assert_eq!(deserialized_struct, struct_payload2);

        eprintln!("✅ Mixed type test passed: Strings, numbers, binaries, and structs handled correctly.");
    }


}

