# Test Suite Structure

1. **Basic Operations**  
   Focuses on fundamental read/write operations, ensuring that appending, retrieving, and updating entries work correctly with different payload sizes.

2. **Persistence**  
   Tests the ability of the storage system to maintain data across multiple reopenings and handle interrupted writes without corruption.

3. **Integrity**  
   Ensures data validity through checksum validation, verifying that corrupted data is detected and that identical writes produce the same checksum.

4. **Compaction**  
   Validates the effectiveness of compaction, ensuring that only the latest versions of data remain while reducing file size.

5. **Streaming**  
   Tests writing and reading large payloads via stream-based operations, ensuring efficient handling of large data inputs without unnecessary memory allocation.

6. **Storage Operations**  
   Covers higher-level operations such as copying, moving, renaming, and deleting entries between different storage instances.

7. **Memory-Mapped (MMAP) and Zero-Copy**  
   Ensures that memory-mapped reads are correctly implemented and that cloned references do not create unnecessary copies while maintaining data integrity.

8. **Concurrency**  
   Verifies safe concurrent access by testing simultaneous reads and writes across multiple threads while preventing race conditions or data corruption.

