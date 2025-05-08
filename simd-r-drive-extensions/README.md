# SIMD R Drive Extensions

**Work in progress.**

`simd-r-drive-extensions` provides optional utilities for working with `Option<T>` and TTL-based caching in [SIMD R Drive](https://crates.io/crates/simd-r-drive).

[Documentation](https://docs.rs/simd-r-drive-extensions/latest/simd_r_drive_extensions/)

## Install

```sh
cargo add simd-r-drive-extensions
```

## Usage

### Working with `Option<T>`
```rust
use simd_r_drive::DataStore;
use simd_r_drive_extensions::StorageOptionExt;
use std::path::PathBuf;

let storage = DataStore::open(&PathBuf::from("test_store.bin")).unwrap();

// Write Some value
storage.write_option(b"key_with_some_value", Some(&42)).unwrap();
assert_eq!(
    storage.read_option::<i32>(b"key_with_some_value").expect("Failed to read key1"),
    Some(42)
);

// Write None
storage.write_option::<i32>(b"key_with_none_value", None).unwrap();
assert_eq!(
    storage.read_option::<i32>(b"key_with_none_value").expect("Failed to read key2"),
    None
);

// Errors on non-existent keys
assert!(storage.read_option::<i32>(b"non_existent_key").is_err());
```

#### Notes

- Uses a predefined tombstone marker (`[0xFF, 0xFE]`) to represent `None`.
- Values are serialized using [bincode](https://crates.io/crates/bincode).
- ⚠️ Unlike [SIMD R Drive](https://crates.io/crates/simd-r-drive), values are non-zero-copy, as they require deserialization.

### Working with TTL-based Caching
```rust
use simd_r_drive::DataStore;
use simd_r_drive_extensions::StorageCacheExt;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

let storage = DataStore::open(&PathBuf::from("test_store.bin")).unwrap();

// Write value with a TTL of 5 seconds
storage.write_with_ttl(b"key_with_ttl", &42, 5).unwrap();
assert_eq!(
    storage.read_with_ttl::<i32>(b"key_with_ttl").expect("Failed to read key"),
    Some(42)
);

// Wait for TTL to expire
sleep(Duration::from_secs(6));
assert_eq!(
    storage.read_with_ttl::<i32>(b"key_with_ttl").expect("Failed to read key"),
    None // Key should be expired and removed
);
```

#### Notes

- TTL values are stored as a **binary prefix** before the actual value.
- Values are serialized using [bincode](https://crates.io/crates/bincode).
- ⚠️ Unlike [SIMD R Drive](https://crates.io/crates/simd-r-drive), values are non-zero-copy, as they require deserialization.
- TTL-based storage will **automatically evict expired values upon read** to prevent stale data.

### Importing Files from a Directory (Recursive + Streaming)

```rust
use simd_r_drive::DataStore;
use simd_r_drive_extensions::StorageFileImportExt;
use std::path::PathBuf;

let storage = DataStore::open(&PathBuf::from("test_store.bin")).unwrap();

// Recursively stream and import all files under `./assets`
// Keys will use Unix-style paths like "subdir/file.txt"
let imported = storage
    .import_dir_recursively("../assets", None)
    .expect("Failed to import directory");

for (key, offset) in &imported {
    println!(
        "Imported file at key: {} (offset {})",
        String::from_utf8_lossy(key),
        offset
    );
}

// Optional: use a namespace to avoid key collisions
let namespace: Option<&[u8]> = Some(b"assets");
let namespace_imported = storage
    .import_dir_recursively("../assets", namespace)
    .expect("Failed to import with namespace");

for (key, offset) in &namespace_imported {
    println!(
        "Imported (namespaced) file at key: {:02X?} (offset {})",
        key,
        offset
    );
}
```

#### Note

- File import uses **streaming I/O**, avoiding full file loads into memory.

### Reading Files from Storage (by Relative Path)

```rust
use simd_r_drive::DataStore;
use simd_r_drive_extensions::StorageFileImportExt;
use std::fs;
use std::io::{Read, BufReader};
use std::path::PathBuf;

let storage = DataStore::open(&PathBuf::from("test_store.bin")).unwrap();

let import_dir = "../.github";
let relative_file = "workflows/rust-release.yml";

// Import the directory
storage
    .import_dir_recursively(import_dir, Some(b"some-namespace"))
    .expect("Failed to import directory");

// Read file from the store
let mut stored = storage
    .open_file_stream(relative_file, Some(b"some-namespace"))
    .expect("File not found in storage");

let mut stored_contents = String::new();
stored
    .read_to_string(&mut stored_contents)
    .expect("Failed to read from store");

// Read original file directly
let full_path = PathBuf::from(import_dir).join(relative_file);
let mut file = BufReader::new(fs::File::open(&full_path).expect("Missing original file"));
let mut original_contents = String::new();
file.read_to_string(&mut original_contents)
    .expect("Failed to read original file");

// Compare contents
assert_eq!(
    stored_contents, original_contents,
    "Mismatch between stored and original file contents"
);
```

### Notes
- These methods do not directly access the filesystem — they operate entirely within the DataStore.
- Relative paths must match those used during import, and must use the same namespace if provided.
- Internally uses zero-copy range handles (EntryStream) backed by memory-mapped file reads.

## License

Licensed under the [Apache-2.0 License](LICENSE).
