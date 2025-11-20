# SIMD R Drive - Portable Entry Types

Standalone, storage agnostic*, `mmap`-friendly definitions of [EntryHandle](./src/entry_handle.rs) and [EntryMetadata](./src/entry_metadata.rs) compatible with [SIMD R Drive](https://crates.io/crates/simd-r-drive).

Use these types from other storage backends (e.g., in-memory stores, object storage, custom files) that need to read/write the same binary layout—without depending on the full `SIMD R Drive` crate or even a local filesystem. They support zero-copy via `mmap` when available, but don't require it.

* Note: This crate has not been tested in WASM and is likely not yet compatible.

## Features

### `arrow`
Enables zero-copy conversion to Apache Arrow `Buffer` types via `as_arrow_buffer()` and `into_arrow_buffer()`.

### `bytes`
Enables zero-copy conversion to `bytes::Bytes` via `as_bytes()` and `into_bytes()`. Perfect for network protocols and async I/O.

### Usage Example

```toml
[dependencies]
simd-r-drive-entry-handle = { version = "0.15", features = ["bytes"] }
```

```rust
use simd_r_drive_entry_handle::EntryHandle;

// Create an in-memory entry
let data = b"Hello, zero-copy world!";
let handle = EntryHandle::from_owned_bytes_anon(data, 12345)?;

// Convert to bytes::Bytes without copying
let bytes = handle.as_bytes();
assert_eq!(&bytes[..], data);
```

## License

Licensed under the [Apache-2.0 License](./LICENSE).
