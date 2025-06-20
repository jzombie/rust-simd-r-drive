from typing import Optional, IO, Tuple, Iterator, final

__all__ = ["DataStore", "EntryHandle", "EntryStream", "NamespaceHasher"]

@final
class EntryHandle:
    """
    A memory-mapped handle to a binary entry in the datastore.

    This class represents a handle to an entry in the storage that has been memory-mapped.
    It provides direct access to the data entry, including its raw bytes, metadata (such as key hash and checksum),
    and additional helper methods to facilitate data integrity verification, memory access, and manipulation.

    This handle guarantees zero-copy access to the entry data, ensuring efficient reading without unnecessary allocations.
    """

    def as_memoryview(self) -> memoryview:
        """
        Returns the entry as a memoryview.

        This allows the entry to be accessed and manipulated as a memory view, providing
        an efficient way to access the data in a low-level, binary format.

        Returns:
            memoryview: A memoryview of the entry's data, allowing zero-copy access to the entry's bytes.
        """
        ...

    def as_slice(self) -> bytes:
        """
        Returns the entry as a byte slice.

        This method provides access to the entry's data as a byte slice (`&[u8]`), directly referencing
        the underlying memory-mapped file. The data is not copied, but the byte slice is wrapped in a 
        Python object (`PyBytes`), which introduces a small overhead. While no physical data copy occurs, 
        creating the `PyBytes` object does involve allocating memory to store the reference.

        Returns:
            bytes: A byte slice of the entry's data, backed by the underlying memory-mapped file.
        """
        ...

    def raw_checksum(self) -> bytes:
        """
        Returns the raw checksum of the entry.

        This method retrieves the checksum associated with the entry’s data in its raw form (as bytes),
        which can be used for verification or further processing.

        Returns:
            bytes: A 4-byte checksum of the entry's data.
        """
        ...

    def is_valid_checksum(self) -> bool:
        """
        Validates the integrity of the entry using its checksum.

        This method computes the checksum of the entry’s data and compares it against the stored checksum to verify
        data integrity. If the computed checksum matches the stored value, it confirms that the data is intact.

        Returns:
            bool: True if the checksum is valid, False otherwise.
        """
        ...

    def offset_range(self) -> Tuple[int, int]:
        """
        Returns the byte offset range within the memory-mapped file corresponding to the entry's data.

        This provides the start and end byte offsets for the entry in the memory-mapped file, which can be used
        for low-level memory operations or diagnostics.

        Returns:
            Tuple[int, int]: A tuple representing the start and end byte offsets of the entry within the file.
        """
        ...

    def address_range(self) -> Tuple[int, int]:
        """
        Returns the virtual address range of the entry in memory.

        This method provides the memory addresses corresponding to the start and end of the entry's data
        in the current process’s address space. The address range can be useful for debugging or low-level analysis.

        Returns:
            Tuple[int, int]: A tuple representing the start and end virtual memory addresses of the entry.
        """
        ...

    def clone_arc(self) -> "EntryHandle":
        """
        Clones the entry handle, sharing the same underlying memory.

        This method clones the `EntryHandle` and returns a new instance, but both handles will reference
        the same underlying memory-mapped data, avoiding any unnecessary memory duplication. The reference count
        on the `Arc<Mmap>` is incremented to ensure that the memory remains valid as long as any handle exists.

        Returns:
            EntryHandle: A new `EntryHandle` that shares the same memory-mapped data.
        """
        ...

    def __len__(self) -> int:
        """
        Returns the size of the entry (payload size).

        This method returns the size of the data portion of the entry, excluding metadata. This corresponds
        to the number of bytes in the payload.

        Returns:
            int: The size of the entry's payload in bytes.
        """
        ...

    @property
    def size(self) -> int:
        """
        Property: Returns the size of the entry's payload.

        This is a convenience method for getting the size of the entry's data. It is equivalent to calling 
        `__len__` on the entry, so you can also use `len(entry)` as an alternative.

        Both `size` and `len()` access the value directly from the memory-mapped file, so they do not require
        the entry's data to be loaded into RAM.

        Returns:
            int: The size of the entry's payload in bytes.
        """
        ...

    @property
    def size_with_metadata(self) -> int:
        """
        Property: Returns the total size of the entry, including metadata.

        This method includes the metadata overhead (e.g., checksum, key hash) in the total size, providing
        the complete size of the entry including both data and associated metadata.

        Both `size_with_metadata` and `size` access the value directly from the memory-mapped file, so they do not
        require the data to be loaded into RAM.

        Returns:
            int: The total size of the entry, including metadata.
        """
        ...

    @property
    def key_hash(self) -> int:
        """
        Property: Returns the computed hash of the entry's key.

        This is the key hash used to quickly look up the entry in the datastore. It is derived from the key itself
        using a hashing algorithm for efficient lookups.

        Returns:
            int: The hash of the entry's key.
        """
        ...

    @property
    def checksum(self) -> int:
        """
        Property: Returns the checksum of the entry's payload.

        The checksum is a 32-bit value used to verify the integrity of the data.

        Returns:
            int: The checksum of the entry's payload.
        """
        ...

    @property
    def start_offset(self) -> int:
        """
        Property: Returns the start byte offset within the memory-mapped file.

        This is the offset at which the entry’s data begins within the storage file.

        Returns:
            int: The start byte offset of the entry in the file.
        """
        ...

    @property
    def end_offset(self) -> int:
        """
        Property: Returns the end byte offset within the memory-mapped file.

        This is the offset at which the entry’s data ends within the storage file.

        Returns:
            int: The end byte offset of the entry in the file.
        """
        ...

@final
class EntryStream:
    """
     A streaming reader for large binary entries.

    `EntryStream` provides a **streaming interface** over an `EntryHandle`, allowing for reading 
    large entries in chunks instead of loading the entire entry into memory. This is especially 
    useful for working with entries that may be larger than available RAM.

    # ⚠️ **Non Zero-Copy Warning**
    Unlike `EntryHandle`, `EntryStream` **performs memory copies**. Each call to `read()` 
    copies a portion of the entry into a user-provided buffer. If you need **zero-copy access**, 
    use `EntryHandle::as_slice()` instead.
    """

    def read(self, size: int) -> bytes:
        """
        Reads up to `size` bytes from the entry stream.

        This method allows you to read the entry's data in chunks, making it ideal 
        for processing large entries that may not fit entirely in memory. Each 
        call to `read()` advances the position in the entry by the number of bytes 
        that have been read.

        Args:
            size (int): The number of bytes to read.

        Returns:
            bytes: A chunk of the entry’s data. The chunk size will be at most `size`, 
            or smaller if the remaining entry data is less than `size`.

        # ⚠️ **Non Zero-Copy Warning**
        - Data is **copied** from the memory-mapped file into the buffer.
        - For zero-copy access, use `EntryHandle::as_slice()` instead.
        """
        ...

    def __iter__(self) -> Iterator[bytes]:
        """
        Returns the `EntryStream` itself as an iterator.

        This allows you to iterate over the entry in chunks. Each iteration reads a 
        new portion of the entry, enabling efficient processing of large entries.

        Returns:
            Iterator[bytes]: An iterator over the entry, yielding chunks of data 
            from the entry.

        # ⚠️ **Non Zero-Copy Warning**
        - Data is **copied** into each chunk returned by the iterator.
        - Use `EntryHandle::as_slice()` for zero-copy access.
        """
        ...

    def __next__(self) -> bytes:
        """
        Reads the next chunk of the entry.

        This method is used when the `EntryStream` is treated as an iterator. It reads 
        the next portion of the entry’s data.

        Returns:
            bytes: The next chunk of the entry’s data.

        # ⚠️ **Non Zero-Copy Warning**
        - This method **copies** data from the memory-mapped file into the buffer.
        - Use `EntryHandle::as_slice()` for zero-copy access.
        """
        ...


@final
class DataStore:
    """
    A high-performance, append-only binary key/value store.

    This class allows the creation, modification, and querying of a datastore.
    The datastore is append-only and optimized for large binary data, supporting
    key/value pairs, streaming writes, and zero-copy reads.
    """

    def __init__(self, path: str) -> None:
        """
        Opens or creates an append-only binary storage file at the given path.

        This function maps the storage file into memory for fast access and
        initializes the necessary internal structures (like key indexer).

        Args:
            path (str): The path to the storage file.
        """
        ...

    def write(self, key: bytes, data: bytes) -> None:
        """
        Appends a key/value pair to the store.

        This method appends a key-value pair to the storage. If the key already
        exists, it overwrites the previous value.

        Args:
            key (bytes): The key to store.
            data (bytes): The data associated with the key.
        """
        ...

    def batch_write(self, items: list[tuple[bytes, bytes]]) -> None:
        """
        Writes multiple key/value pairs in a single operation.

        This method allows for more efficient storage operations by writing
        multiple key-value pairs in one batch.

        Args:
            items (list): A list of (key, value) tuples, where both `key` and `value`
                are byte arrays.
        """
        ...

    def write_stream(self, key: bytes, reader: IO[bytes]) -> None:
        """
        Streams large values from a file-like object.

        This method allows writing large data entries by streaming them from
        a file-like object, rather than loading them all into memory at once.

        Args:
            key (bytes): The key for the data entry.
            reader (IO[bytes]): A readable stream that provides the data.
        """
        ...

    def read(self, key: bytes) -> Optional[bytes]:
        """
        Reads the value for a given key.

        This method retrieves the value for the given key from the datastore. 
        Note that this operation **performs a memory copy** of the data into 
        a new `bytes` object. If **zero-copy access** is required, use 
        `read_entry` instead.

        Args:
            key (bytes): The key whose value is to be retrieved.

        Returns:
            Optional[bytes]: The data associated with the key, or `None` if the key 
            does not exist.

        # ⚠️ **Non Zero-Copy Warning**
        - Unlike the Rust library this method **copies** data from the memory-mapped file into a new `bytes` object.
        - If zero-copy access is needed, use `read_entry` instead, which provides a memory-mapped handle to the data.
        """
        ...

    def read_entry(self, key: bytes) -> Optional[EntryHandle]:
        """
        Returns a memory-mapped handle to the value for a given key.

        This method retrieves the value for the key as an `EntryHandle`, which 
        provides **zero-copy access** to the entry data. It does not perform 
        any memory copying, instead returning a handle to the original memory-mapped 
        entry in the datastore. This is the preferred method for accessing data 
        when zero-copy access is necessary.

        Args:
            key (bytes): The key whose value is to be retrieved.

        Returns:
            Optional[EntryHandle]: A handle to the entry, or `None` if the key 
            does not exist.

        # Zero-Copy Access
        - This method does **not copy** data, and instead provides a memory-mapped handle.
        - Use this method for accessing data directly from memory without copying.
        """
        ...

    def read_stream(self, key: bytes) -> Optional[EntryStream]:
        """
        Returns a stream reader for the value associated with a given key.

        This method returns an `EntryStream`, which can be used to stream large
        values associated with a key.

        Args:
            key (bytes): The key whose associated value is to be streamed.

        Returns:
            Optional[EntryStream]: A stream reader for the entry, or None if the key does not exist.
        """
        ...

    def delete(self, key: bytes) -> None:
        """
        Marks the key as deleted (logically removes it).

        This operation does not physically remove the data but appends a tombstone
        entry to mark the key as deleted.

        Args:
            key (bytes): The key to mark as deleted.
        """
        ...

    def exists(self, key: bytes) -> bool:
        """
        Returns True if the key is present in the store.

        This method checks whether the key exists and has not been deleted.

        Args:
            key (bytes): The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        ...

    def __contains__(self, key: bytes) -> bool:
        """
        Allows usage of the `in` operator to check key existence.

        This method provides an interface to use `key in store` to check if the key exists in the datastore.

        Args:
            key (bytes): The key to check.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return self.exists(key)

@final
class NamespaceHasher:
    """
    A utility for generating namespaced keys using XXH3 hashing.

    `NamespaceHasher` ensures that keys are uniquely scoped to a given namespace
    by combining separate hashes of the namespace and the key. This avoids
    accidental collisions across logical domains (e.g., "opt:foo" vs "sys:foo").

    The final namespaced key is a fixed-length 16-byte identifier:
    8 bytes for the namespace hash + 8 bytes for the key hash.

     Example:
        >>> hasher = NamespaceHasher(b"users")
        >>> key = hasher.namespace(b"user123")
        >>> assert len(key) == 16
    """

    def __init__(self, prefix: bytes) -> None:
        """
        Initializes the `NamespaceHasher` with a namespace prefix.

        The prefix is hashed once using XXH3 to serve as a unique identifier for
        the namespace. All keys passed to `namespace()` will be scoped to this
        prefix.

        Args:
            prefix (bytes): A byte string that represents the namespace prefix.
        """
        ...

    def namespace(self, key: bytes) -> bytes:
        """
        Returns a 16-byte namespaced key based on the given input key.

        The output is constructed by concatenating the namespace hash and the
        hash of the key:
            - First 8 bytes: XXH3 hash of the namespace prefix.
            - Next 8 bytes: XXH3 hash of the key.

        This design ensures deterministic and collision-isolated key derivation.

        Args:
            key (bytes): The key to hash within the current namespace.

        Returns:
            bytes: A 16-byte namespaced key (`prefix_hash || key_hash`).
        """
        ...
