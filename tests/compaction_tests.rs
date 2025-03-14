#[cfg(test)]
mod tests {

    use serde::{Deserialize, Serialize};
    use simd_r_drive::DataStore;
    use tempfile::tempdir;

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
}
