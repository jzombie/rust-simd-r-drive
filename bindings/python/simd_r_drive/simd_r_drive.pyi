from typing import Optional, IO, Tuple, Iterator, final

__all__ = ["DataStore", "EntryHandle", "EntryStream"]

@final
class EntryHandle:
    """
    A memory-mapped handle to a binary entry in the datastore.
    """

    def as_memoryview(self) -> memoryview: ...
    def as_slice(self) -> bytes: ...
    def raw_checksum(self) -> bytes: ...
    def is_valid_checksum(self) -> bool: ...
    def offset_range(self) -> Tuple[int, int]: ...
    def address_range(self) -> Tuple[int, int]: ...
    def clone_arc(self) -> "EntryHandle": ...
    def __len__(self) -> int: ...

    @property
    def size(self) -> int: ...
    @property
    def size_with_metadata(self) -> int: ...
    @property
    def key_hash(self) -> int: ...
    @property
    def checksum(self) -> int: ...
    @property
    def start_offset(self) -> int: ...
    @property
    def end_offset(self) -> int: ...


@final
class EntryStream:
    """
    A streaming reader for large binary entries.
    """

    def read(self, size: int) -> bytes:
        """
        Reads up to `size` bytes from the entry stream.
        """
        ...

    def __iter__(self) -> Iterator[bytes]:
        """
        Returns self as an iterator.
        """
        ...

    def __next__(self) -> bytes:
        """
        Reads the next chunk from the stream.
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

        This method retrieves the value for a given key from the datastore.

        Args:
            key (bytes): The key whose value is to be retrieved.

        Returns:
            Optional[bytes]: The data associated with the key, or None if the key
            does not exist.
        """
        ...

    def read_entry(self, key: bytes) -> Optional[EntryHandle]:
        """
        Returns a memory-mapped handle to the value for a given key.

        This method retrieves the value for the key as an `EntryHandle`, which
        allows zero-copy access to the entry data.

        Args:
            key (bytes): The key whose value is to be retrieved.

        Returns:
            Optional[EntryHandle]: A handle to the entry, or None if the key does not exist.
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
