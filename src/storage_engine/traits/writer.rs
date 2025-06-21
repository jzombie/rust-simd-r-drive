use crate::storage_engine::DataStore;
use std::io::{Read, Result};

pub trait DataStoreWriter {
    fn write_stream<R: Read>(&self, key: &[u8], reader: &mut R) -> Result<u64>;

    fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64>;

    fn buf_write(&self, key: &[u8], payload: &[u8]) -> Result<bool>;

    fn buf_write_flush(&self) -> Result<u64>;

    fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64>;

    fn rename_entry(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64>;

    fn copy_entry(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    fn move_entry(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    fn delete_entry(&self, key: &[u8]) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDataStoreWriter {
    async fn write_stream<R: Read>(&self, key: &[u8], reader: &mut R) -> Result<u64>;

    async fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64>;

    async fn buf_write(&self, key: &[u8], payload: &[u8]) -> Result<bool>;

    async fn buf_write_flush(&self) -> Result<u64>;

    async fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64>;

    async fn rename_entry(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64>;

    async fn copy_entry(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    async fn move_entry(&self, key: &[u8], target: &DataStore) -> Result<u64>;

    async fn delete_entry(&self, key: &[u8]) -> Result<u64>;
}
