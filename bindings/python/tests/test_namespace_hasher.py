import tempfile
import os
from simd_r_drive import DataStore, NamespaceHasher


def test_namespace_hashing_differentiates_namespaces():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        key = b"user:1"
        payload = b"example_payload"

        # Create two different namespace hashers
        hasher1 = NamespaceHasher(b"namespaceA")
        hasher2 = NamespaceHasher(b"namespaceB")

        key1 = hasher1.namespace(key)
        key2 = hasher2.namespace(key)

        assert key1 != key2, "Keys with different namespaces should not match"

        # Write both entries
        engine.write(key1, payload)
        engine.write(key2, payload[::-1])  # reverse for distinction

        result1 = engine.read(key1)
        result2 = engine.read(key2)

        assert result1 == payload
        assert result2 == payload[::-1]

        del engine


def test_namespace_hashing_consistency():
    hasher = NamespaceHasher(b"shared")

    key = b"important_key"
    namespaced1 = hasher.namespace(key)
    namespaced2 = hasher.namespace(key)

    assert namespaced1 == namespaced2, "Same key and namespace must produce consistent result"


def test_namespace_hashing_allows_collision_avoidance():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "store.bin")
        engine = DataStore(filepath)

        keys = [b"user:alpha", b"user:beta", b"user:gamma"]
        namespaces = [b"ns1", b"ns2", b"ns3"]

        seen = set()

        for ns in namespaces:
            hasher = NamespaceHasher(ns)
            for key in keys:
                namespaced = bytes(hasher.namespace(key))
                assert namespaced not in seen, "Namespaced key should be unique"
                seen.add(namespaced)
                engine.write(namespaced, b"data")

        assert len(seen) == len(keys) * len(namespaces)

        del engine
