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
storage.write_option(b"key1", Some(&42)).unwrap();
assert_eq!(
    storage.read_option::<i32>(b"key1").expect("Failed to read key1"),
    Some(42)
);

// Write None
storage.write_option::<i32>(b"key2", None).unwrap();
assert_eq!(
    storage.read_option::<i32>(b"key2").expect("Failed to read key2"),
    None
);

```

## Implementation Details

- Uses a predefined tombstone marker (`[0xFF, 0xFE]`) to represent `None`.
- Values are serialized using bincode.

## License

Licensed under the [Apache-2.0 License](LICENSE).
