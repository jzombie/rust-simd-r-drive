#[cfg(test)]
mod tests {
    use simd_r_drive::{
        DataStore,
        traits::{DataStoreReader, DataStoreWriter},
        utils::NamespaceHasher,
    };
    use tempfile::tempdir;

    /// Helper function to create a temporary file for testing
    fn create_temp_storage() -> (tempfile::TempDir, DataStore) {
        let dir = tempdir().expect("Failed to create temp dir");
        let path = dir.path().join("test_storage.bin");

        let storage = DataStore::open(&path).expect("Failed to open storage");
        (dir, storage)
    }

    #[test]
    fn test_namespace_hasher_basic() {
        let hasher = NamespaceHasher::new(b"namespace1");

        // Hashing a key under "namespace1"
        let namespaced_key = hasher.namespace(b"key1");

        assert_eq!(
            namespaced_key.len(),
            16,
            "Namespaced key should be 16 bytes"
        );
    }

    #[test]
    fn test_namespace_collision_prevention() {
        let hasher1 = NamespaceHasher::new(b"namespace1");
        let hasher2 = NamespaceHasher::new(b"namespace2");

        // Hashing the same key under different namespaces
        let key = b"key1";

        let namespaced_key1 = hasher1.namespace(key);
        let namespaced_key2 = hasher2.namespace(key);

        // Ensure that namespaced keys for the same key under different namespaces do not collide
        assert_ne!(
            namespaced_key1, namespaced_key2,
            "Keys from different namespaces should not collide"
        );
    }

    #[test]
    fn test_namespaced_key_length() {
        let hasher = NamespaceHasher::new(b"namespace");
        let key = b"key1";

        let namespaced_key = hasher.namespace(key);

        // Ensure the namespaced key is 16 bytes long
        assert_eq!(
            namespaced_key.len(),
            16,
            "Namespaced key should be exactly 16 bytes"
        );
    }

    #[test]
    fn test_multiple_keys_under_same_namespace() {
        let hasher = NamespaceHasher::new(b"namespace");

        // Hash multiple keys under the same namespace
        let key1 = b"key1";
        let key2 = b"key2";

        let namespaced_key1 = hasher.namespace(key1);
        let namespaced_key2 = hasher.namespace(key2);

        // Ensure that different keys under the same namespace produce different namespaced keys
        assert_ne!(
            namespaced_key1, namespaced_key2,
            "Different keys under the same namespace should have different namespaced keys"
        );
    }

    #[test]
    fn test_namespace_reusability() {
        let hasher = NamespaceHasher::new(b"namespace1");

        // Hash the same key twice under the same namespace
        let key = b"key1";

        let namespaced_key1 = hasher.namespace(key);
        let namespaced_key2 = hasher.namespace(key);

        // Ensure that the same key under the same namespace always generates the same namespaced key
        assert_eq!(
            namespaced_key1, namespaced_key2,
            "The same key in the same namespace should produce the same namespaced key"
        );
    }

    #[test]
    fn test_namespace_isolation() {
        let (_dir, storage) = create_temp_storage();

        let key = b"shared_key";
        let payload_ns1 = b"Data from namespace1".as_slice();
        let payload_ns2 = b"Data from namespace2".as_slice();
        let payload_ns3 = b"Data from namespace3".as_slice();

        let ns1 = NamespaceHasher::new(b"ns1");
        let ns2 = NamespaceHasher::new(b"ns2");
        let ns3 = NamespaceHasher::new(b"ns3");

        let key_ns1 = ns1.namespace(key);
        let key_ns2 = ns2.namespace(key);
        let key_ns3 = ns3.namespace(key);

        // Ensure the keys are different
        assert_ne!(
            key_ns1, key_ns2,
            "Namespaces ns1 and ns2 should not collide"
        );
        assert_ne!(
            key_ns1, key_ns3,
            "Namespaces ns1 and ns3 should not collide"
        );
        assert_ne!(
            key_ns2, key_ns3,
            "Namespaces ns2 and ns3 should not collide"
        );

        // Write each payload under the corresponding namespaced key
        storage
            .write(&key_ns1, payload_ns1)
            .expect("Failed to write ns1 data");
        storage
            .write(&key_ns2, payload_ns2)
            .expect("Failed to write ns2 data");
        storage
            .write(&key_ns3, payload_ns3)
            .expect("Failed to write ns3 data");

        // Read back and verify
        let read_ns1 = storage.read(&key_ns1).expect("Missing ns1 entry");
        let read_ns2 = storage.read(&key_ns2).expect("Missing ns2 entry");
        let read_ns3 = storage.read(&key_ns3).expect("Missing ns3 entry");

        assert_eq!(read_ns1.as_slice(), payload_ns1);
        assert_eq!(read_ns2.as_slice(), payload_ns2);
        assert_eq!(read_ns3.as_slice(), payload_ns3);
    }
}
