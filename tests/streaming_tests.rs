#[cfg(test)]
mod tests {

    use simd_r_drive::{compute_checksum, DataStore};
    use tempfile::tempdir;

    /// Helper function to create a temporary file for testing
    fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage.bin");

        let storage = DataStore::open(&path).expect("Failed to open storage");
        (dir, storage)
    }

    #[test]
    fn test_write_and_read_streams() {
        use std::fs::File;
        use std::io::{BufReader, Read, Write};

        let (_dir, storage) = create_temp_storage();
        let large_key = b"streamed_large_entry";

        // Create a temporary file to act as a real stream
        let file_path = _dir.path().join("test_large_file.bin");
        let mut test_file = File::create(&file_path).expect("Failed to create test file");

        let payload_size = 1 * 1024 * 1024; // 1MB
        let test_data = vec![b'X'; payload_size];

        // Write real test data to file
        test_file
            .write_all(&test_data)
            .expect("Failed to write test data");
        test_file.flush().expect("Failed to flush test data");

        // Compute checksum for validation
        let expected_checksum = compute_checksum(&test_data);

        // Open the file as a streaming reader
        let mut reader = BufReader::new(File::open(&file_path).expect("Failed to open test file"));

        // Write to storage using the real stream
        storage
            .write_stream(large_key, &mut reader)
            .expect("Failed to append large entry");

        // Retrieve the entry
        let retrieved_entry = storage
            .read(large_key)
            .expect("Failed to retrieve large entry");

        // Create an EntryStream from the retrieved entry
        let mut entry_stream: simd_r_drive::storage_engine::EntryStream = retrieved_entry.into();

        // Read from the stream in chunks and compare
        let mut buffer = vec![0; 4096]; // 4KB read buffer
        let mut streamed_data = Vec::new();

        while let Ok(bytes_read) = entry_stream.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }
            streamed_data.extend_from_slice(&buffer[..bytes_read]);
        }

        // Verify integrity
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
    fn test_write_stream_null_byte_fails() {
        let (_dir, storage) = create_temp_storage();

        let key = b"test_key";
        let mut payload = b"\x00".as_ref(); // Hardcoded null-byte

        let result = storage.write_stream(key, &mut payload);

        assert!(
            result.is_err(),
            "Expected error when writing a null-byte payload via write_stream"
        );
    }
}
