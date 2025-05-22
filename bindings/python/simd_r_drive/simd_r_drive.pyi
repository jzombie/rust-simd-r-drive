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
    """

    def __init__(self, path: str) -> None:
        """
        Opens (or creates) a datastore at the given file path.
        """
        ...

    def write(self, key: bytes, data: bytes) -> None:
        """
        Appends a key/value pair to the store.
        """
        ...

    def batch_write(self, items: list[tuple[bytes, bytes]]) -> None:
        """
        Writes multiple key/value pairs in a single operation.
        """
        ...

    def write_stream(self, key: bytes, reader: IO[bytes]) -> None:
        """
        Writes from a stream (e.g., BytesIO or file) as the value for `key`.
        """
        ...

    def read(self, key: bytes) -> Optional[bytes]:
        """
        Reads the value for a given key, or returns None if missing.
        """
        ...

    def read_entry(self, key: bytes) -> Optional[EntryHandle]:
        """
        Returns a memory-mapped handle to the value for `key`, if it exists.
        """
        ...

    def read_stream(self, key: bytes) -> Optional[EntryStream]:
        """
        Returns a stream reader for the value associated with `key`.
        """
        ...

    def delete(self, key: bytes) -> None:
        """
        Marks the key as deleted (logically removes it).
        """
        ...

    def exists(self, key: bytes) -> bool:
        """
        Returns True if the key is present in the store.
        """
        ...
