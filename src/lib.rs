// TODO: Integrate examples in main README

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
//! use simd_r_drive::{DataStore, traits::{DataStoreReader, DataStoreWriter}};
//! use std::path::PathBuf;
//! use tempfile::tempdir;
//!
//! let temp_dir = tempdir().expect("Failed to create temp dir");
//! let temp_path = temp_dir.path().join("test_storage.bin");
//!
//! // Open or create a new storage file
//! let mut storage = DataStore::open(&PathBuf::from(temp_path)).unwrap();
//!
//! // Append some key-value entries
//! storage.write(b"key1", b"value1").unwrap();
//! storage.write(b"key2", b"value2").unwrap();
//! storage.write(b"key3", b"value3").unwrap();
//! storage.write(b"key4", b"value4").unwrap();
//! storage.write(b"key5", b"value5").unwrap();
//!
//! // Retrieve some entries
//! let entry = storage.read(b"key1").unwrap();
//! assert_eq!(entry.as_slice(), b"value1");
//!
//! let entry = storage.read(b"key2").unwrap();
//! assert_eq!(entry.as_slice(), b"value2");
//!
//! let entry = storage.read(b"key3").unwrap();
//! assert_eq!(entry.as_slice(), b"value3");
//!
//! let entry = storage.read(b"key4").unwrap();
//! assert_eq!(entry.as_slice(), b"value4");
//!
//! let entry = storage.read(b"key5").unwrap();
//! assert_eq!(entry.as_slice(), b"value5");
//!
//! // Overwrite an entry
//! storage.write(b"key3", b"A new value").unwrap();
//! let entry = storage.read(b"key3").unwrap();
//! assert_eq!(entry.as_slice(), b"A new value");
//!
//! // Delete an entry
//! storage.delete_entry(b"key3").unwrap();
//! let entry = storage.read(b"key3");
//! assert!(entry.is_none());
//! ```
//!
//! ## Streaming Example
//! ```rust
//! use simd_r_drive::{DataStore, EntryStream, traits::{DataStoreReader, DataStoreWriter}};
//! use std::fs::File;
//! use std::io::{Cursor, Read, Write};
//! use std::path::PathBuf;
//! use tempfile::tempdir;
//!
//! let temp_dir = tempdir().expect("Failed to create temp dir");
//! let temp_path = temp_dir.path().join("test_storage_stream.bin");
//!
//! // Open or create a new storage file
//! let mut storage = DataStore::open(&PathBuf::from(temp_path)).unwrap();
//!
//! // Example streaming data
//! let stream_data = b"Streaming payload with large data";
//! let mut cursor = Cursor::new(stream_data);
//!
//! // Write streaming data
//! storage.write_stream(b"stream_key", &mut cursor).unwrap();
//!
//! // Read and validate streaming data using `EntryStream`
//! let entry_handle = storage.read(b"stream_key").unwrap(); // Get EntryHandle
//! let mut retrieved_stream = EntryStream::from(entry_handle); // Convert to EntryStream
//! let mut buffer = Vec::new();
//!
//! retrieved_stream.read_to_end(&mut buffer).unwrap(); // Read stream in chunks
//! assert_eq!(buffer, stream_data);
//!
//! // Create a temporary file for testing
//! let temp_path = "test_large_file.bin";
//! let mut temp_file = File::create(temp_path).expect("Failed to create temp file");
//! temp_file.write_all(b"Temporary file content").unwrap();
//! temp_file.sync_all().unwrap(); // Ensure file is written
//!
//! // Open the file for streaming
//! let mut file = File::open(temp_path).expect("File not found");
//! storage.write_stream(b"file_stream_key", &mut file).unwrap();
//!
//! // Read back the streamed file using `EntryStream`
//! let file_entry = storage.read(b"file_stream_key").unwrap(); // Get EntryHandle
//! let mut file_stream = EntryStream::from(file_entry); // Convert to EntryStream
//! let mut file_buffer = Vec::new();
//!
//! file_stream.read_to_end(&mut file_buffer).unwrap();
//!
//! assert_eq!(file_buffer, b"Temporary file content");
//!
//! // Cleanup test file
//! std::fs::remove_file(temp_path).unwrap();
//! ```
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

pub mod utils;
