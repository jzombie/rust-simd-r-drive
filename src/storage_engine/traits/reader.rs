use crate::storage_engine::EntryMetadata;
use std::io::Result;

// TODO: Add `read_last_entry` here

pub trait DataStoreReader {
    type EntryHandleType;

    fn read(&self, key: &[u8]) -> Result<Option<Self::EntryHandleType>>;

    fn read_last_entry(&self) -> Result<Option<Self::EntryHandleType>>;

    fn batch_read(&self, keys: &[&[u8]]) -> Result<Vec<Option<Self::EntryHandleType>>>;

    fn read_metadata(&self, key: &[u8]) -> Result<Option<EntryMetadata>>;

    fn count(&self) -> Result<usize>;

    fn get_storage_size(&self) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDataStoreReader {
    type EntryHandleType;

    async fn read(&self, key: &[u8]) -> Result<Option<Self::EntryHandleType>>;

    async fn read_last_entry(&self) -> Result<Option<Self::EntryHandleType>>;

    async fn batch_read(&self, keys: &[&[u8]]) -> Result<Vec<Option<Self::EntryHandleType>>>;

    async fn read_metadata(&self, key: &[u8]) -> Result<Option<EntryMetadata>>;

    async fn count(&self) -> Result<usize>;

    async fn get_storage_size(&self) -> Result<u64>;
}
