# SIMD R Drive - Portable Entry Types

Standalone, storage agnostic*, `mmap`-friendly definitions of [EntryHandle](./src/entry_handle.rs) and [EntryMetadata](./src/entry_metadata.rs) compatible with [SIMD R Drive](https://crates.io/crates/simd-r-drive).

Use these types from other storage backends (e.g., in-memory stores, object storage, custom files) that need to read/write the same binary layout—without depending on the full `SIMD R Drive` crate or even a local filesystem. They support zero-copy via `mmap` when available, but don’t require it.

* Note: This crate has not been tested in WASM and is likely not yet compatible.

## License

Licensed under the [Apache-2.0 License](./LICENSE).
