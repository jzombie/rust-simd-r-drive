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

## Implementation Details

- Uses a predefined tombstone marker (`[0xFF, 0xFE]`) to represent `None`.
- TTL values are stored as a **binary prefix** before the actual value.
- Values are serialized using [bincode](https://crates.io/crates/bincode).
- ⚠️ Unlike [SIMD R Drive](https://crates.io/crates/simd-r-drive), values are non-zero-copy, as they require deserialization.
- TTL-based storage will **automatically evict expired values upon read** to prevent stale data.

## License

Licensed under the [Apache-2.0 License](LICENSE).
