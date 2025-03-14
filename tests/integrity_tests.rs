#[cfg(test)]
mod tests {

    use serde::{Deserialize, Serialize};
    use simd_r_drive::{compute_checksum, compute_hash, DataStore};
    use std::fs::{metadata, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use tempfile::tempdir;

    /// Helper function to create a temporary file for testing
    fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage.bin");

        let storage = DataStore::open(&path).expect("Failed to open storage");
        (dir, storage)
    }

    #[test]
    fn test_entry_checksum_validation() {
        let (_dir, storage) = create_temp_storage();

        let key = b"checksum_test";
        let payload = b"Testing checksum validation";

        // Write entry to storage
        storage.write(key, payload).expect("Failed to write entry");

        // Retrieve the entry handle
        let entry_handle = storage.read(key).expect("Failed to read entry");

        // Ensure checksum validation passes
        assert!(
            entry_handle.is_valid_checksum(),
            "Checksum validation should pass for an unmodified entry"
        );

        // Manually corrupt the file at the entry's location
        let file_path = storage.get_path();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("Failed to open file for corruption");

        file.seek(SeekFrom::Start(entry_handle.start_offset() as u64))
            .expect("Failed to seek to entry position");

        // Flip a bit in the first byte of the entry to corrupt it
        let mut corrupted_byte = [0u8];
        file.read_exact(&mut corrupted_byte)
            .expect("Failed to read byte for corruption");

        corrupted_byte[0] ^= 0xFF; // Flip a bit

        file.seek(SeekFrom::Start(entry_handle.start_offset() as u64))
            .expect("Failed to seek back to entry position");

        file.write_all(&corrupted_byte)
            .expect("Failed to write corrupted byte");

        file.flush().expect("Failed to flush corrupted data");

        // Reopen storage to reload the corrupted mmap
        let storage =
            DataStore::open(&file_path).expect("Failed to reopen storage after corruption");

        // Attempt to read the corrupted entry
        let corrupted_entry = storage.read(key).expect("Failed to read corrupted entry");

        assert!(
            !corrupted_entry.is_valid_checksum(),
            "Checksum validation should fail after corruption"
        );
    }

    #[test]
    fn test_checksum_consistency_across_write_methods() {
        use std::io::Cursor;

        let (_dir, storage) = create_temp_storage();

        let key1 = b"checksum_test_write";
        let key2 = b"checksum_test_stream";

        let identical_payload = b"Consistent Checksum Payload";
        let different_payload = b"Different Payload Data";

        // Write using `write`
        storage
            .write(key1, identical_payload)
            .expect("Failed to write entry");

        // Write using `write_stream`
        let mut stream_reader = Cursor::new(identical_payload);
        storage
            .write_stream(key2, &mut stream_reader)
            .expect("Failed to write stream entry");

        // Read both entries
        let entry1 = storage.read(key1).expect("Failed to read entry from write");
        let entry2 = storage
            .read(key2)
            .expect("Failed to read entry from write_stream");

        // Ensure checksums match
        assert_eq!(
            entry1.checksum(),
            entry2.checksum(),
            "Checksums should be identical when writing the same content via different methods"
        );

        // Write different content using `write`
        let key3 = b"checksum_test_different";
        storage
            .write(key3, different_payload)
            .expect("Failed to write different entry");

        let entry3 = storage.read(key3).expect("Failed to read different entry");

        // Ensure checksum is different for different content
        assert_ne!(
            entry1.checksum(),
            entry3.checksum(),
            "Checksums should differ for different content"
        );

        assert_ne!(
            entry2.checksum(),
            entry3.checksum(),
            "Checksums should differ for different content written via different methods"
        );
    }
}
