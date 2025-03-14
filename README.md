# SIMD R Drive

**Work in progress.**

`SIMD R Drive` is a high-performance single-file storage engine optimized for zero-copy binary access.

[Documentation](https://docs.rs/simd-r-drive/latest/simd_r_drive/)

## Zero-Copy Memory-Mapped Access

`SIMD R Drive` is a schema-less, append-only binary storage engine designed for high-performance runtime read/write access. It provides zero-copy reads by memory-mapping the storage file (`mmap`), allowing direct data access without additional deserialization. Unlike `FlatBuffers`, which also supports zero-copy reads but requires predefined schemas, `SIMD R Drive` operates without IDLs or schemas, enabling flexible, raw binary storage optimized for real-time applications.

Additionally, `SIMD R Drive` is designed to handle datasets larger than available RAM by leveraging memory mapping. The system transparently accesses only the necessary portions of the file, reducing memory pressure and enabling efficient storage operations on large-scale datasets.

## Streaming Support

In addition to zero-copy reads, `SIMD R Drive` supports streaming individual entries without requiring the entire entry to be loaded into memory at once. When an entry is accessed, a handle to the entry can be obtained, and a streaming output can be attached directly to it. This allows efficient data transfer while minimizing memory usage.

Unlike zero-copy reads, streamed entries are not zero-copy because the data is transferred through a buffer during streaming. However, this approach ensures that even large entries can be processed efficiently without needing to fit entirely within RAM.

> `SIMD R Drive` also supports streaming writes, individual writes, and batch writes.

## SIMD Acceleration

`SIMD R Drive` leverages SIMD (Single Instruction, Multiple Data) acceleration to optimize performance in key operations, specifically focusing on **write** operations and indexing efficiency.

- **SIMD-Optimized File Writing (`simd_copy`)**: During write operations, `SIMD R Drive` employs a specialized SIMD-accelerated memory copy function (`simd_copy`) to efficiently transfer data into buffers before writing to disk. This reduces CPU overhead and speeds up bulk writes by leveraging vectorized memory operations instead of relying on standard byte-wise copying. The use of `simd_copy` ensures that data is efficiently staged in memory before being flushed to disk, optimizing write throughput.

- **SIMD-Accelerated Hashing (`xxh3_64`)**: The hashing mechanism used for indexing (`xxh3_64`) is optimized with SIMD extensions. This improves key lookups and indexing efficiency, particularly for large datasets with high query throughput.

By using SIMD for these performance-critical tasks, `SIMD R Drive` minimizes CPU cycles spent on memory movement and hashing, leading to optimized storage performance in high-throughput, write-heavy workloads. Note that **SIMD is not used for reading or zero-copy memory-mapped access**, as those operations benefit from direct memory access without additional transformations.

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

### Thread Safety Matrix


| **Environment**                     | **Reads** | **Writes** | **Index Updates** | **Storage Safety** |
|--------------------------------------|----------|------------|-------------------|---------------------|
| **Single Process, Single Thread**   | ✅ Safe  | ✅ Safe     | ✅ Safe           | ✅ Safe             |
| **Single Process, Multi-Threaded**  | ✅ Safe (lock-free, zero-copy) | ✅ Safe (`RwLock<File>`) | ✅ Safe (`RwLock<HashMap>`) | ✅ Safe (`Mutex<Arc<Mmap>>`) |
| **Multiple Processes, Shared File** | ⚠️ Unsafe (no cross-process coordination) | ❌ Unsafe (no external locking) | ❌ Unsafe (separate memory spaces) | ❌ Unsafe (risk of race conditions) |

**Legend**

- ✅ **Safe for single-process, multi-threaded workloads** thanks to `RwLock`, `Mutex`, and `AtomicU64`.
- ⚠️ **Not safe for multiple processes sharing the same file** unless an external file locking mechanism is used.
- **If multiple instances need to access the same file, external locking (e.g., `flock`, advisory locking) is required** to prevent corruption.
