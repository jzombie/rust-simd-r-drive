use crate::storage_engine::EntryMetadata;
use std::io::Result;

pub trait DataStoreReader {
    type EntryHandleType;

    fn read(&self, key: &[u8]) -> Option<Self::EntryHandleType>;

    // TODO: Implement
    // fn batch_read(&self, keys: &[&[u8]]) -> Vec<Option<Self::EntryHandleType>>;

    fn read_metadata(&self, key: &[u8]) -> Option<EntryMetadata>;

    fn count(&self) -> usize;

    fn get_storage_size(&self) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDataStoreReader {
    type EntryHandleType;

    async fn read(&self, key: &[u8]) -> Option<Self::EntryHandleType>;

    // TODO: Implement
    // async fn batch_read(&self, keys: &[&[u8]]) -> Vec<Option<Self::EntryHandleType>>;

    async fn read_metadata(&self, key: &[u8]) -> Option<EntryMetadata>;

    async fn count(&self) -> usize;

    async fn get_storage_size(&self) -> Result<u64>;
}
