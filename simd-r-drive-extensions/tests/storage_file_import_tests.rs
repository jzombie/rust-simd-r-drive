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

#[test]
fn test_read_file_entry_returns_correct_contents() {
    let source_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("utils");

    let (_dir, storage) = create_temp_storage();

    storage
        .import_dir_recursively(&source_dir, None)
        .expect("Failed to import files");

    // Pick a known file (assumes at least one exists)
    let known_file = source_dir
        .read_dir()
        .expect("Failed to read source dir")
        .filter_map(Result::ok)
        .find(|entry| entry.path().is_file())
        .expect("No file found in utils dir")
        .file_name();

    let entry = storage
        .read_file_entry(&known_file, None)
        .expect("Expected file entry to be present");

    let mut expected = Vec::new();
    let file_path = source_dir.join(&known_file);
    fs::File::open(&file_path)
        .expect("Failed to open original file")
        .read_to_end(&mut expected)
        .expect("Failed to read original file");

    assert_eq!(entry.as_slice(), expected.as_slice());
}

#[test]
fn test_open_file_stream_reads_all_bytes() {
    let source_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("utils");

    let (_dir, storage) = create_temp_storage();

    storage
        .import_dir_recursively(&source_dir, None)
        .expect("Failed to import files");

    let known_file = source_dir
        .read_dir()
        .expect("Failed to read source dir")
        .filter_map(Result::ok)
        .find(|entry| entry.path().is_file())
        .expect("No file found in utils dir")
        .file_name();

    let mut stream = storage
        .open_file_stream(&known_file, None)
        .expect("Expected file stream to be present");

    let mut streamed = Vec::new();
    stream
        .read_to_end(&mut streamed)
        .expect("Failed to stream file");

    let mut expected = Vec::new();
    fs::File::open(source_dir.join(&known_file))
        .expect("Failed to open original")
        .read_to_end(&mut expected)
        .expect("Failed to read original");

    assert_eq!(streamed, expected);
}
