# SIMD R Drive

Work in progress.

`SIMD R Drive` is a schema-less, append-only binary storage engine designed for high-performance runtime read/write access. It provides zero-copy reads by memory-mapping the storage file (mmap), allowing direct data access without additional deserialization. Unlike FlatBuffers, which also supports zero-copy reads but requires predefined schemas, `SIMD R Drive` operates without IDLs or schemas, enabling flexible, raw binary storage optimized for real-time applications.

## Thread Safety and Multiple Instance Locking

### Currently Not Implemented

As of now, the system does not support full thread safety, and multiple instances of the application accessing the same file may result in unpredictable behavior. Specifically, the following aspects are not yet implemented:

- **Thread safety**: There is no mechanism in place to guarantee that multiple threads can safely access and modify the data concurrently. This could lead to race conditions, corrupted data, or other undefined behaviors when read and write operations are happening simultaneously in different threads.
  
- **Multiple instance synchronization**: If multiple instances of the application attempt to access the same storage file concurrently, there is no locking or coordination to ensure data integrity. Without proper synchronization, actions from one instance could conflict with another, leading to data corruption or loss.

### Unpredictable Results

If your application requires the following, the results may be unpredictable:
  
- **Concurrent reads and writes** from multiple threads.
- **Multiple instances** of the application running and accessing the same storage file.

### To Be Done (TBD)

We plan to implement proper locking mechanisms and synchronization techniques to address these issues, including:

- **Thread synchronization**: Implementing read-write locks or similar mechanisms to ensure that only one thread can modify the data at a time while still allowing multiple threads to read concurrently.
  
- **Instance locking**: Using file-level or system-level locks to prevent multiple instances from accessing the same storage file simultaneously, ensuring that no conflicts or data corruption occur when multiple processes are involved.
