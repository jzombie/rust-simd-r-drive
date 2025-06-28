import pytest
from simd_r_drive_ws_client import DataStoreWsClient, NamespaceHasher
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


def test_namespace_hashing_differentiates_namespaces(client):
    key = b"user:1"
    payload = b"example_payload"

    # Create two different namespace hashers
    hasher1 = NamespaceHasher(b"namespaceA")
    hasher2 = NamespaceHasher(b"namespaceB")

    key1 = hasher1.namespace(key)
    key2 = hasher2.namespace(key)

    assert key1 != key2, "Keys with different namespaces should not match"

    # Write both entries
    client.write(key1, payload)
    client.write(key2, payload[::-1])  # reverse for distinction

    result1 = client.read(key1)
    result2 = client.read(key2)

    assert result1 == payload
    assert result2 == payload[::-1]


def test_namespace_hashing_consistency():
    hasher = NamespaceHasher(b"shared")

    key = b"important_key"
    namespaced1 = hasher.namespace(key)
    namespaced2 = hasher.namespace(key)

    assert (
        namespaced1 == namespaced2
    ), "Same key and namespace must produce consistent result"


def test_namespace_hashing_allows_collision_avoidance(client):
    keys = [b"user:alpha", b"user:beta", b"user:gamma"]
    namespaces = [b"ns1", b"ns2", b"ns3"]

    seen = set()

    for ns in namespaces:
        hasher = NamespaceHasher(ns)
        for key in keys:
            namespaced = bytes(hasher.namespace(key))
            assert namespaced not in seen, "Namespaced key should be unique"
            seen.add(namespaced)
            client.write(namespaced, b"data")

    assert len(seen) == len(keys) * len(namespaces)
