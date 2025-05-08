use simd_r_drive::DataStore;
use simd_r_drive_extensions::StorageFileImportExt;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use tempfile::tempdir;

/// Helper to create a test storage backed by a temp file
fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
    let dir = tempdir().expect("Failed to create temp dir");
    let path = dir.path().join("import_test_store.bin");
    let storage = DataStore::open(&path).expect("Failed to open storage");
    (dir, storage)
}

#[test]
fn test_import_directory_and_verify_contents() {
    // Use a subdirectory of the project that is guaranteed to exist and have files
    let source_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("utils");

    assert!(
        source_dir.exists(),
        "Test source directory does not exist: {:?}",
        source_dir
    );

    let (_dir, storage) = create_temp_storage();

    let imported = storage
        .import_dir_recursively(&source_dir, None)
        .expect("Failed to import files");

    assert!(
        !imported.is_empty(),
        "Expected at least one file to be imported"
    );

    for (key, _offset) in imported {
        // Convert key bytes back to a UTF-8 Unix-style path
        let key_str = String::from_utf8(key.clone()).expect("Key is not valid UTF-8");
        let original_path = source_dir.join(key_str.replace('/', std::path::MAIN_SEPARATOR_STR));

        assert!(
            original_path.exists(),
            "Original file missing for key: {:?}",
            original_path
        );

        let mut expected = Vec::new();
        let mut file = fs::File::open(&original_path).expect("Failed to open original file");
        file.read_to_end(&mut expected)
            .expect("Failed to read file");

        let actual = storage.read(&key).expect("Key missing from store");
        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "Mismatch in stored content for key {:?}",
            key_str
        );
    }
}

#[test]
fn test_import_without_namespace() {
    let source_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("utils");

    assert!(
        source_dir.exists(),
        "Expected test source directory to exist: {:?}",
        source_dir
    );

    let (_dir, storage) = create_temp_storage();

    let imported = storage
        .import_dir_recursively(&source_dir, None)
        .expect("Failed to import files");

    for (key, _offset) in &imported {
        let key_str = String::from_utf8(key.clone()).expect("Invalid UTF-8 key");
        let orig_path = source_dir.join(key_str.replace('/', std::path::MAIN_SEPARATOR_STR));

        let mut expected = Vec::new();
        fs::File::open(&orig_path)
            .expect("Original file missing")
            .read_to_end(&mut expected)
            .expect("Failed to read file");

        let stored = storage.read(key).expect("Missing key in store");
        assert_eq!(stored.as_slice(), expected.as_slice());
    }
}
