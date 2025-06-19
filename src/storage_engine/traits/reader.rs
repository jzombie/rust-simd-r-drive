use crate::storage_engine::EntryMetadata;
use std::io::Result;

pub trait DataStoreReader {
    type EntryHandleType;

    fn read(&self, key: &[u8]) -> Option<Self::EntryHandleType>;

    fn read_metadata(&self, key: &[u8]) -> Option<EntryMetadata>;

    fn count(&self) -> usize;

    fn get_storage_size(&self) -> Result<u64>;
}
