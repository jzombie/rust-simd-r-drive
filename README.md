# SIMD R Drive

**Work in progress. Not thread safe.**

`SIMD R Drive` is a high-performance single-file storage engine optimized for zero-copy binary access.

## Zero-Copy Memory-Mapped Access

`SIMD R Drive` is a schema-less, append-only binary storage engine designed for high-performance runtime read/write access. It provides zero-copy reads by memory-mapping the storage file (`mmap`), allowing direct data access without additional deserialization. Unlike `FlatBuffers`, which also supports zero-copy reads but requires predefined schemas, `SIMD R Drive` operates without IDLs or schemas, enabling flexible, raw binary storage optimized for real-time applications.

## Single-File Storage Engine with Raw Binary Storage

This storage engine is intentionally designed as a low-level library, meaning it does not interpret or modify stored data. The payload is treated as raw bytes (`&[u8]`), ensuring that data is stored and retrieved exactly as written. This approach provides maximum flexibility, allowing users to store arbitrary binary formats without constraints.

`SIMD R Drive` does not enforce endianness or serialization formats, leaving these concerns to the application. If an application requires a specific encoding (e.g., little-endian numbers), it must explicitly convert the data before storing it and decode it after retrieval. This design ensures optimal performance while avoiding unnecessary overhead from automatic transformations.

By focusing solely on efficient data storage and retrieval, `SIMD R Drive` provides a lightweight and flexible foundation for applications that require high-speed access to structured or unstructured binary data without the complexity of schema management.

<div style="text-align: center">
  <img src="assets/storage-layout.png" title="Storage Layout" />
</div>

## Currently Not Implemented: Thread Safety and Multiple Instance Locking

As of now, the system does not support full thread safety, and multiple instances of the application accessing the same file may result in unpredictable behavior. Specifically, the following aspects are not yet implemented:

- **Thread safety**: There is no mechanism in place to guarantee that multiple threads can safely access and modify the data concurrently. This could lead to race conditions, corrupted data, or other undefined behaviors when read and write operations are happening simultaneously in different threads.
  
- **Multiple instance synchronization**: If multiple instances of the application attempt to access the same storage file concurrently, there is no locking or coordination to ensure data integrity. Without proper synchronization, actions from one instance could conflict with another, leading to data corruption or loss.
