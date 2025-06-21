use std::io::Result;

pub trait DataStoreBufWriter {
    fn buf_write(&self, key: &[u8], payload: &[u8]) -> Result<bool>;

    fn buf_write_flush(&self) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDataStoreBufWriter {
    async fn buf_write(&self, key: &[u8], payload: &[u8]) -> Result<bool>;

    async fn buf_write_flush(&self) -> Result<u64>;
}
