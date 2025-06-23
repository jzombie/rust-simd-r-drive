from typing import Optional, final

__all__ = ["DataStoreWsClient", "NamespaceHasher"]

@final
class DataStoreWsClient:
    """
    A high-performance, append-only binary key/value store.

    This class allows the creation, modification, and querying of a datastore.
    The datastore is append-only and optimized for large binary data, supporting
    key/value pairs, streaming writes, and zero-copy reads.
    """

    def __init__(self, ws_addr: str) -> None:
        """
        Connects to a SIMD R Drive server.

        Args:
            ws_addr (str): The WebSocket address of the SIMD R Drive server.
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
        """

    def batch_read(self, keys: list[bytes]) -> list[Optional[bytes]]:
        """
        Reads many keys in one shot.

        Args:
            keys (list[bytes]): The keys whose values are to be retrieved.

        Returns:
            list[Optional[bytes]]: A list of optional values, using None type if the
            key is not present.
        """
        ...
    #
    #
    # TODO: Integrate
    # def delete(self, key: bytes) -> None:
    #     """
    #     Marks the key as deleted (logically removes it).

    #     This operation does not physically remove the data but appends a tombstone
    #     entry to mark the key as deleted.

    #     Args:
    #         key (bytes): The key to mark as deleted.
    #     """
    #     ...

    # def exists(self, key: bytes) -> bool:
    #     """
    #     Returns True if the key is present in the store.

    #     This method checks whether the key exists and has not been deleted.

    #     Args:
    #         key (bytes): The key to check.

    #     Returns:
    #         bool: True if the key exists, False otherwise.
    #     """
    #     ...

    # def __contains__(self, key: bytes) -> bool:
    #     """
    #     Allows usage of the `in` operator to check key existence.

    #     This method provides an interface to use `key in store` to check if the key exists in the datastore.

    #     Args:
    #         key (bytes): The key to check.

    #     Returns:
    #         bool: True if the key exists, False otherwise.
    #     """
    #     return self.exists(key)

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
