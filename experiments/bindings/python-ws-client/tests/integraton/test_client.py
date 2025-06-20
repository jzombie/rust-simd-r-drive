import pytest
from simd_r_drive_ws_client import DataStoreWsClient
import time
import os
import subprocess

# Server address, configurable via environment variable
SERVER_ADDR = os.environ.get("TEST_SERVER_ADDR", "127.0.0.1:34129")


@pytest.fixture(scope="module")
def client():
    """
    Fixture to create and connect the WsClient.
    The scope is 'module' so it connects only once for all tests.
    """
    # Allow some time for the server to start up.
    time.sleep(2)
    try:
        ws_client = DataStoreWsClient(SERVER_ADDR)
        yield ws_client
    except Exception as e:
        pytest.fail(
            f"Failed to connect to the WebSocket server at {SERVER_ADDR}. Is it running? Error: {e}"
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
