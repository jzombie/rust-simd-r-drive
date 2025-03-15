#[cfg(test)]
mod tests {

    use simd_r_drive::DataStore;
    use tempfile::tempdir;

    /// Helper function to create a temporary file for testing
    fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage.bin");

        let storage = DataStore::open(&path).expect("Failed to open storage");
        (dir, storage)
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
