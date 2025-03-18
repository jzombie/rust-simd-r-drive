# SIMD R Drive Extensions

**Work in progress.**

`simd-r-drive-extensions` provides optional utilities for working with `Option<T>` in [SIMD R Drive](https://crates.io/crates/simd-r-drive).

[Documentation](https://docs.rs/simd-r-drive-extensions/latest/simd_r_drive_extensions/)

## Install

```sh
cargo add simd-r-drive-extensions
```

## Usage

```rust
use simd_r_drive::DataStore;
use simd_r_drive_extensions::StorageOptionExt;
use tempfile::tempdir;

// Create temporary storage
let (_dir, storage) = {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test_storage.bin");
    let store = DataStore::open(&path).unwrap();
    (dir, store)
};

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

// Check if the key exists in storage, regardless of whether it's `Some` or `None`
if let Ok(none_option) = storage.read_option::<i32>(b"key_with_none_value") {
    assert!(none_option.is_none());
} else {
    // Just to check the example
    panic!("Failed to read key: `key_with_none_value` does not exist or read error occurred.");
}

// Alternative, concise check
let none_option = storage.read_option::<i32>(b"key_with_none_value").unwrap();
assert!(none_option.is_none()); // Ensures `Option<T>` exists

// Errors on non-existent keys
assert!(storage.read_option::<i32>(b"non_existent_key").is_err());

```

## Implementation Details

- Uses a predefined tombstone marker (`[0xFF, 0xFE]`) to represent `None`.
- Values are serialized using bincode.

## License

Licensed under the [Apache-2.0 License](LICENSE).
