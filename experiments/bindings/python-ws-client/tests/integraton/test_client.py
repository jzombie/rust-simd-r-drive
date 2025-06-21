import pytest
from simd_r_drive_ws_client import DataStoreWsClient
import time
import os
import threading
import random
import secrets

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


def test_concurrent_read_write_stress(client):
    """
    Stress test with multiple threads performing concurrent reads and writes.
    Each thread creates its own client connection.
    """
    NUM_THREADS = 8
    OPERATIONS_PER_THREAD = 50
    KEY_PREFIX = "stress-test"

    # Shared dictionary to store key-value pairs written by all threads
    # This needs a lock to be thread-safe
    written_data = {}
    lock = threading.Lock()
    threads = []
    errors = []

    def worker(thread_id):
        """The function each thread will execute."""
        # Each worker creates its own client connection to simulate concurrent users.
        local_client = None
        try:
            local_client = DataStoreWsClient(SERVER_ADDR)
            for i in range(OPERATIONS_PER_THREAD):
                # Randomly choose between writing and reading
                if (
                    random.random() < 0.6 or len(written_data) == 0
                ):  # Bias towards writing initially
                    # --- WRITE OPERATION ---
                    key = f"{KEY_PREFIX}-thread{thread_id}-op{i}".encode("utf-8")
                    value = secrets.token_bytes(32)  # Generate 32 random bytes

                    # Write to the server using the thread's local client
                    local_client.write(key, value)

                    # Store the written key-value pair in the shared dictionary
                    with lock:
                        written_data[key] = value
                else:
                    # --- READ OPERATION ---
                    with lock:
                        # Pick a random key that has been written by any thread
                        random_key, expected_value = random.choice(
                            list(written_data.items())
                        )

                    # Read from the server and verify using the local client
                    read_value = local_client.read(random_key)

                    assert (
                        read_value is not None
                    ), f"[Thread {thread_id}] FAIL: Read returned None for key {random_key.decode()}"
                    assert (
                        read_value == expected_value
                    ), f"[Thread {thread_id}] FAIL: Data mismatch for key {random_key.decode()}"
        except Exception as e:
            # Store any exceptions to be re-raised in the main thread
            errors.append(e)

    print(f"\n--- Starting concurrent stress test with {NUM_THREADS} threads ---")

    # Create and start all threads
    for i in range(NUM_THREADS):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    print("--- All worker threads finished. Performing final verification... ---")

    # Check for any exceptions that occurred in threads
    if errors:
        pytest.fail(f"Test failed due to exceptions in worker threads: {errors}")

    # Final verification: read all keys using the main test client and check their values
    with lock:
        items_to_verify = list(written_data.items())

    for key, expected_value in items_to_verify:
        read_value = client.read(key)
        assert (
            read_value is not None
        ), f"[Final Verification] FAIL: Read returned None for key {key.decode()}"
        assert (
            read_value == expected_value
        ), f"[Final Verification] FAIL: Data mismatch for key {key.decode()}"

    print(
        f"--- SUCCESS: Concurrent stress test passed. Verified {len(items_to_verify)} entries. ---"
    )


def test_batch_read_with_missing_key(client):
    """
    Verifies that batch_read:
    – returns payloads for existing keys,
    – returns None for keys that are absent,
    – preserves order (results[i] matches keys[i]).
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
