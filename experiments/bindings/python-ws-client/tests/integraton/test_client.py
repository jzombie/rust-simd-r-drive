import pytest
from simd_r_drive_ws_client import DataStoreWsClient
import time
import os
import secrets

# Server address, configurable via environment variable
SERVER_HOST = os.environ.get("TEST_SERVER_HOST", "127.0.0.1")
SERVER_PORT = int(os.environ.get("TEST_SERVER_PORT", 34129))


@pytest.fixture(scope="module")
def client():
    """
    Fixture to create and connect the WsClient.
    The scope is 'module' so it connects only once for all tests.
    """
    # Allow some time for the server to start up.
    time.sleep(2)
    try:
        ws_client = DataStoreWsClient(SERVER_HOST, SERVER_PORT)
        yield ws_client
    except Exception as e:
        pytest.fail(
            f"Failed to connect to the WebSocket server at {SERVER_HOST}. Is it running? Error: {e}"
        )


def test_simple_read_write(client):
    """Tests a simple write operation followed by a read."""
    key = b"simple-key"
    value = b"hello-from-pytest"

    try:
        print(f"Attempting to write: key='{key.decode()}', value='{value.decode()}'")
        client.write(key, value)
        print("Write operation completed.")

        print(f"Attempting to read key: '{key.decode()}'")
        read_value = client.read(key)
        print(f"Read operation completed. Got: {read_value}")

        assert (
            read_value is not None
        ), f"FAIL: Expected a value for key '{key.decode()}', but received None."
        assert (
            read_value == value
        ), f"FAIL: Read value '{read_value.decode()}' does not match written value '{value.decode()}'."

        print("SUCCESS: Simple read/write test passed.")

    except Exception as e:
        pytest.fail(f"An exception occurred during the read/write test: {e}")


def test_batch_write_and_read(client):
    """Tests a batch write operation followed by individual reads."""
    entries = [
        (b"batch-key-1", b"value-alpha"),
        (b"batch-key-2", b"value-beta"),
        (b"batch-key-3", b"value-gamma"),
    ]

    try:
        print("Attempting to perform a batch write...")
        client.batch_write(entries)
        print("Batch write operation completed.")

        print("Verifying batch write by reading each key...")
        for key, value in entries:
            read_value = client.read(key)
            assert (
                read_value is not None
            ), f"FAIL: Key '{key.decode()}' not found after batch write."
            assert (
                read_value == value
            ), f"FAIL: Value mismatch for key '{key.decode()}'."

        print("SUCCESS: Batch write test passed.")

    except Exception as e:
        pytest.fail(f"An exception occurred during the batch write test: {e}")


def test_large_batch_write(client):
    """Tests a batch write of four 256KB payloads."""
    payload_size = 256 * 1024  # 256KB
    num_payloads = 4

    print(
        f"\n--- Starting large batch write test with {num_payloads} payloads of size {payload_size} bytes ---"
    )

    entries = []
    for i in range(num_payloads):
        key = f"large-batch-key-{i}".encode("utf-8")
        value = secrets.token_bytes(payload_size)
        entries.append((key, value))

    try:
        print("Attempting to batch write large payloads...")
        client.batch_write(entries)
        print("Large batch write operation completed.")

        print("Verifying large batch write by reading each key...")
        for key, expected_value in entries:
            read_value = client.read(key)
            assert (
                read_value is not None
            ), f"FAIL: Read returned None for key {key.decode()}"
            assert (
                len(read_value) == payload_size
            ), f"FAIL: Incorrect payload size for key {key.decode()}"
            assert (
                read_value == expected_value
            ), f"FAIL: Data mismatch for key {key.decode()}"

        print("SUCCESS: Large batch write test passed.")

    except Exception as e:
        pytest.fail(f"An exception occurred during the large batch write test: {e}")


def test_batch_read_with_missing_key(client):
    """
    Verifies that batch_read:
    - returns payloads for existing keys,
    - returns None for keys that are absent,
    - preserves order (results[i] matches keys[i]).
    """
    # --- Arrange ----------------------------------------------------------
    entries = [
        (b"br-key-1", b"br-val-alpha"),
        (b"br-key-2", b"br-val-beta"),
        (b"br-key-3", b"br-val-gamma"),
    ]
    client.batch_write(entries)

    # Keys to fetch (include one that does not exist)
    keys_to_fetch = [k for k, _ in entries] + [b"br-key-missing"]

    # --- Act --------------------------------------------------------------
    results = client.batch_read(keys_to_fetch)

    # --- Assert -----------------------------------------------------------
    assert len(results) == len(
        keys_to_fetch
    ), "Result vector length mismatch with query keys"

    for idx, (key, expected_payload) in enumerate(
        entries + [(b"br-key-missing", None)]
    ):
        result = results[idx]
        if expected_payload is None:
            assert (
                result is None
            ), f"Expected None for absent key {key.decode()}, got {result!r}"
        else:
            assert (
                result == expected_payload
            ), f"Payload mismatch for key {key.decode()}"


def test_batch_read_structured_single_dict(client):
    """
    Tests batch_read_structured with a single dictionary, including a missing key.
    """
    # Arrange: Write some initial data to the store
    client.batch_write(
        [
            (b"struct-key-name", b"jeremy"),
            (b"struct-key-data", b"some-data-payload"),
        ]
    )

    # Act: Call the method with a dictionary containing store keys as values
    request_dict = {
        "user_name": b"struct-key-name",
        "user_data": b"struct-key-data",
        "missing_field": b"this-key-does-not-exist",
    }
    result = client.batch_read_structured(request_dict)

    # Assert: The returned dictionary should have the same keys but hydrated values
    assert isinstance(result, dict), "Result should be a dictionary"

    expected_result = {
        "user_name": b"jeremy",
        "user_data": b"some-data-payload",
        "missing_field": None,
    }
    assert (
        result == expected_result
    ), "The hydrated dictionary does not match the expected result"
    print("\nSUCCESS: batch_read_structured with single dictionary passed.")


def test_batch_read_structured_list_of_dicts(client):
    """
    Tests batch_read_structured with a list of dictionaries.
    """
    # Arrange: Write some initial data to the store
    client.batch_write(
        [
            (b"list-key-1", b"value-one"),
            (b"list-key-2", b"value-two"),
        ]
    )

    # Act: Call the method with a list of dictionaries
    request_list = [
        {"field_a": b"list-key-1", "field_b": b"non-existent-key"},
        {"field_c": b"list-key-2", "field_d": b"list-key-1"},
    ]
    result = client.batch_read_structured(request_list)

    # Assert: The returned list should have the same structure but with hydrated values
    assert isinstance(result, list), "Result should be a list"
    assert len(result) == 2, "Result list should have the same length as the input"

    expected_result = [
        {"field_a": b"value-one", "field_b": None},
        {"field_c": b"value-two", "field_d": b"value-one"},
    ]
    assert (
        result == expected_result
    ), "The hydrated list of dictionaries does not match the expected result"
    print("SUCCESS: batch_read_structured with a list of dictionaries passed.")


def test_delete_key(client):
    """Tests that deleting a key makes it non-existent for reads."""
    key = b"key-to-be-deleted"
    value = b"some-data-to-remove"
    print("\n--- Starting delete handling test ---")

    # Arrange: Write a key, and verify it exists.
    print(f"Writing key '{key.decode()}' for deletion test.")
    client.write(key, value)
    initial_read = client.read(key)
    assert initial_read == value, "Pre-condition failed: Key was not written correctly before delete."
    print("Key confirmed to exist before deletion.")

    # Act: Delete the key.
    print(f"Deleting key '{key.decode()}'.")
    client.delete(key)

    # Assert: The key should no longer exist.
    final_read = client.read(key)
    print(f"Read after delete returned: {final_read}")
    assert final_read is None, "FAIL: Reading a deleted key should return None."
    print("SUCCESS: Delete handling test passed.")


def test_delete_with_batch_read(client):
    """
    Tests that a deleted key is correctly handled as `None` in a batch_read.
    """
    print("\n--- Starting delete with batch_read test ---")
    # Arrange: Write a set of keys.
    entries = [
        (b"dbr-1", b"value-one"),
        (b"dbr-to-delete", b"this-should-vanish"),
        (b"dbr-3", b"value-three"),
    ]
    key_to_delete = b"dbr-to-delete"
    
    print("Writing initial batch for delete test...")
    client.batch_write(entries)

    # Act: Delete one of the keys from the batch.
    print(f"Deleting key '{key_to_delete.decode()}'.")
    client.delete(key_to_delete)

    # Assert: Perform a batch_read and check for None in the deleted key's slot.
    keys_to_fetch = [key for key, _ in entries]
    print(f"Performing batch_read on keys: {[k.decode() for k in keys_to_fetch]}")
    results = client.batch_read(keys_to_fetch)
    
    expected_results = [b"value-one", None, b"value-three"]
    
    assert results == expected_results, \
        f"FAIL: batch_read did not correctly handle the deleted key. Expected {expected_results}, but got {results}."
    print("SUCCESS: Delete with batch_read test passed.")