# SIMD R Drive

**Work in progress.**

`SIMD R Drive` is a high-performance single-file storage engine optimized for zero-copy binary access.

## Zero-Copy Memory-Mapped Access

`SIMD R Drive` is a schema-less, append-only binary storage engine designed for high-performance runtime read/write access. It provides zero-copy reads by memory-mapping the storage file (`mmap`), allowing direct data access without additional deserialization. Unlike `FlatBuffers`, which also supports zero-copy reads but requires predefined schemas, `SIMD R Drive` operates without IDLs or schemas, enabling flexible, raw binary storage optimized for real-time applications.

## Single-File Storage Engine with Raw Binary Storage

This storage engine is intentionally designed as a low-level library, meaning it does not interpret or modify stored data. The payload is treated as raw bytes (`&[u8]`), ensuring that data is stored and retrieved exactly as written. This approach provides maximum flexibility, allowing users to store arbitrary binary formats without constraints.

`SIMD R Drive` does not enforce endianness or serialization formats, leaving these concerns to the application. If an application requires a specific encoding (e.g., little-endian numbers), it must explicitly convert the data before storing it and decode it after retrieval. This design ensures optimal performance while avoiding unnecessary overhead from automatic transformations.

By focusing solely on efficient data storage and retrieval, `SIMD R Drive` provides a lightweight and flexible foundation for applications that require high-speed access to structured or unstructured binary data without the complexity of schema management.

<div align="center">
  <img src="assets/storage-layout.png" title="Storage Layout" />
</div>

## Thread Safety and Concurrency Handling

`SIMD R Drive` supports concurrent access using a combination of **read/write locks (`RwLock`)**, **atomic operations (`AtomicU64`)**, and **reference counting (`Arc`)** to ensure safe access across multiple threads. 

- **Reads are zero-copy and lock-free**: Since entries are read directly from a memory-mapped file (`mmap`), multiple threads can safely perform reads in parallel without requiring synchronization. The storage structure does not modify entries once written, ensuring safe concurrent reads.
  
- **Writes are synchronized with `RwLock`**: All write operations acquire a **write lock** (`RwLock<File>`), ensuring only one thread can modify the storage file at a time. This prevents race conditions when appending new entries.
  
- **Index updates use `RwLock<HashMap>`**: The in-memory key index is wrapped in an `RwLock<HashMap>` to allow concurrent lookups while ensuring exclusive access during modifications.

- **Memory mapping (`mmap`) is protected by `Mutex<Arc<Mmap>>`**: The memory-mapped file reference is wrapped in a `Mutex<Arc<Mmap>>` to prevent unsafe remapping while reads are in progress. This ensures that readers always have a valid view of the storage file.

- **Atomic offsets ensure correct ordering**: The last written offset (`last_offset`) is managed using `AtomicU64`, avoiding unnecessary locking while ensuring correct sequential writes.

These mechanisms ensure that `SIMD R Drive` can handle concurrent reads and writes safely in a **single-process, multi-threaded** environment. However, **multiple instances of the application accessing the same file are not synchronized**, meaning external file locking should be used if multiple processes need to coordinate access to the same storage file.
