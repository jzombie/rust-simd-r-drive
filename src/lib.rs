//! # SIMD R Drive Storage Engine
//!
//! This crate provides an append-only, pinned storage engine optimized for
//! high-performance applications. It is designed to offer:
//! - **Zero-copy reads** using memory-mapped files.
//! - **Append-only writes** ensuring immutability and data integrity.
//! - **Efficient key lookups** via a custom hash index.
//! - **Concurrent access** with atomic offsets and thread-safe locking.
//!
//! ## Features
//! - **Append-Only Model**: Data can only be appended, never modified or deleted in place.
//! - **Memory-Mapped Storage**: Efficient read performance with `memmap2`.
//! - **SIMD Optimization**: Uses vectorized operations for fast memory copying.
//! - **Fast Lookups**: Hash-based key index for quick retrieval.
//! - **Crash Recovery**: Ensures that only valid data is loaded on restart.
//!
//! ## Example Usage
//! ```rust
//! use simd_r_drive::AppendStorage;
//! use std::path::PathBuf;
//!
//! // Open or create a new storage file
//! let mut storage = AppendStorage::open(&PathBuf::from("test_storage.bin")).unwrap();
//!
//! // TODO: Add streaming examples
//!
//! // Append some key-value entries
//! storage.append_entry(b"key1", b"value1").unwrap();
//! storage.append_entry(b"key2", b"value2").unwrap();
//! storage.append_entry(b"key3", b"value3").unwrap();
//! storage.append_entry(b"key4", b"value4").unwrap();
//! storage.append_entry(b"key5", b"value5").unwrap();
//!
//! // Retrieve some entries
//! let entry = storage.get_entry_by_key(b"key1").unwrap();
//! assert_eq!(entry.as_slice(), b"value1");
//!
//! let entry = storage.get_entry_by_key(b"key2").unwrap();
//! assert_eq!(entry.as_slice(), b"value2");
//!
//! let entry = storage.get_entry_by_key(b"key3").unwrap();
//! assert_eq!(entry.as_slice(), b"value3");
//!
//! let entry = storage.get_entry_by_key(b"key4").unwrap();
//! assert_eq!(entry.as_slice(), b"value4");
//!
//! let entry = storage.get_entry_by_key(b"key5").unwrap();
//! assert_eq!(entry.as_slice(), b"value5");
//!
//! // Overwrite an entry
//! storage.append_entry(b"key3", b"A new value").unwrap();
//! let entry = storage.get_entry_by_key(b"key3").unwrap();
//! assert_eq!(entry.as_slice(), b"A new value");
//!
//! // Delete an entry
//! storage.delete_entry(b"key3").unwrap();
//! let entry = storage.get_entry_by_key(b"key3");
//! assert!(entry.is_none());
//!
//! ```
//!
//! ## Modules
//! - `append_storage` - Core storage implementation.
//! - `digest` - Hashing utilities for fast key lookups.
//! - `constants` - Constants and configuration settings.
//! - `entry` - Structures representing storage entries.
//!
//! ## Performance Considerations
//! - **Use memory-mapped reads** for best performance.
//! - **Batch writes** to reduce file I/O overhead.
//! - **Avoid unnecessary locks** to maximize concurrency.
//!
//! ## Safety Notes
//! - Memory-mapped files should not be resized while in use.
//! - Ensure proper file synchronization after writes.
//!
//! ## License
//! This project is licensed under the Apache-2.0 License.

pub mod storage_engine;

pub use storage_engine::digest::*;
pub use storage_engine::*;
