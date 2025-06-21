use std::io::Result;

pub trait DataStoreStageWriter {
    fn stage_write(&self, key: &[u8], payload: &[u8]) -> Result<bool>;

    fn stage_write_flush(&self) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDataStoreStageWriter {
    async fn stage_write(&self, key: &[u8], payload: &[u8]) -> Result<bool>;

    async fn stage_write_flush(&self) -> Result<u64>;
}
