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
