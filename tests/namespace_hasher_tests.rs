#[cfg(test)]
mod tests {
    use simd_r_drive::{utils::NamespaceHasher, DataStore};
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
    fn test_hashing_keys_and_pca() {
        let (_dir, storage) = create_temp_storage();

        // Example ticker data for testing namespace hashing
        let data = vec![
            (
                b"key1",
                b"First Entry".as_slice(),
                b"First Entry PCA".as_slice(),
            ),
            (
                b"key2",
                b"Second Entry".as_slice(),
                b"Second Entry PCA".as_slice(),
            ),
            (
                b"key3",
                b"Third Entry".as_slice(),
                b"Third Entry PCA".as_slice(),
            ),
        ];

        // Hash each key using the NamespaceHasher and write to the storage
        let hasher = NamespaceHasher::new(b"namespace1");

        for (key, payload, pca_payload) in data {
            let namespaced_key = hasher.namespace(key);

            // Save namespaced keys with vector and PCA data
            storage
                .write(&namespaced_key, payload)
                .expect("Failed to write entry");

            // Use extend_from_slice to append the ":pca" part to the namespaced key
            let mut namespaced_key_pca = namespaced_key.clone();
            namespaced_key_pca.extend_from_slice(b":pca");

            storage
                .write(&namespaced_key_pca, pca_payload)
                .expect("Failed to write PCA entry");
        }

        // Verify retrieval
        let namespaced_key1 = hasher.namespace(b"key1");
        let retrieved = storage
            .read(&namespaced_key1)
            .expect("Entry should be found");

        assert_eq!(
            retrieved.as_slice(),
            b"First Entry",
            "Retrieved payload does not match expected value for key1"
        );

        // Check PCA entry
        let mut namespaced_key_pca = namespaced_key1.clone();
        namespaced_key_pca.extend_from_slice(b":pca");
        let retrieved_pca = storage
            .read(&namespaced_key_pca)
            .expect("PCA entry should be found");

        assert_eq!(
            retrieved_pca.as_slice(),
            b"First Entry PCA",
            "Retrieved PCA payload does not match expected value for key1"
        );
    }
}
