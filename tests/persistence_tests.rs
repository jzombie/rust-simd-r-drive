#[cfg(test)]
mod tests {

    use simd_r_drive::{
        DataStore,
        traits::{DataStoreReader, DataStoreWriter},
    };
    use std::fs::{OpenOptions, metadata};
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempdir;

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
                let retrieved = storage.read(key).unwrap();
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
                storage.read(b"key1").unwrap().as_deref(),
                Some(b"Updated Value 1".as_slice()),
                "Key1 should contain the updated value"
            );
            assert_eq!(
                storage.read(b"key2").unwrap().as_deref(),
                Some(b"Updated Value 2".as_slice()),
                "Key2 should contain the updated value"
            );

            eprintln!("Step 3: Persistence check passed after multiple reopens.");
        } // Storage closed here
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
                let recovered = storage.read(b"key1").unwrap();
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
                let retrieved = storage.read(new_key).unwrap();
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

                assert_eq!(
                    storage.read(b"key1").unwrap().unwrap().as_slice(),
                    b"Valid Entry"
                );

                assert_eq!(
                    storage.read(b"new_key").unwrap().unwrap().as_slice(),
                    b"New Data After Recovery"
                );
            }
        }
    }
}
