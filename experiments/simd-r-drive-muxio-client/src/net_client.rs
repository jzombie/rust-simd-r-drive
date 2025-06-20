use muxio_rpc_service_caller::prebuffered::RpcCallPrebuffered;
use muxio_tokio_rpc_client::RpcClient;
use simd_r_drive::{
    DataStore, EntryMetadata,
    traits::{AsyncDataStoreReader, AsyncDataStoreWriter},
};
use simd_r_drive_muxio_service_definition::prebuffered::{
    BatchWrite, BatchWriteRequestParams, Read, ReadRequestParams, Write, WriteRequestParams,
};
use std::io::{Error, ErrorKind, Result};

pub struct NetClient {
    rpc_client: RpcClient,
}

impl NetClient {
    pub async fn new(websocket_address: &str) -> Self {
        let rpc_client = RpcClient::new(&format!("ws://{}/ws", websocket_address)).await;

        Self { rpc_client }
    }
}

#[async_trait::async_trait]
impl AsyncDataStoreWriter for NetClient {
    async fn write_stream<R: std::io::Read>(&self, _key: &[u8], _reader: &mut R) -> Result<u64> {
        unimplemented!("`write_stream` is not currently implemented");
    }

    async fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64> {
        let resp = Write::call(
            &self.rpc_client,
            WriteRequestParams {
                key: key.to_vec(),
                payload: payload.to_vec(),
            },
        )
        .await?;

        resp.result
            .ok_or_else(|| Error::new(ErrorKind::Other, "no offset returned"))
    }

    async fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64> {
        let resp = BatchWrite::call(
            &self.rpc_client,
            BatchWriteRequestParams {
                entries: entries
                    .iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect(),
            },
        )
        .await?;

        resp.result
            .ok_or_else(|| Error::new(ErrorKind::Other, "no offset returned"))
    }

    async fn rename_entry(&self, _old_key: &[u8], _new_key: &[u8]) -> Result<u64> {
        unimplemented!("`rename_entry` is not currently implemented");
    }

    async fn copy_entry(&self, _key: &[u8], _target: &DataStore) -> Result<u64> {
        unimplemented!("`copy_entry` is not currently implemented");
    }

    async fn move_entry(&self, _key: &[u8], _target: &DataStore) -> Result<u64> {
        unimplemented!("`move_entry` is not currently implemented");
    }

    async fn delete_entry(&self, _key: &[u8]) -> Result<u64> {
        unimplemented!("`delete_entry` is not currently implemented");
    }
}

#[async_trait::async_trait]
impl AsyncDataStoreReader for NetClient {
    // TODO: This is a workaround until properly implementing a streamable handle
    type EntryHandleType = Vec<u8>;

    async fn read(&self, key: &[u8]) -> Option<Self::EntryHandleType> {
        let resp = Read::call(&self.rpc_client, ReadRequestParams { key: key.to_vec() })
            .await
            .ok()?;

        resp.result
    }

    async fn read_metadata(&self, _key: &[u8]) -> Option<EntryMetadata> {
        unimplemented!("`read_metadata` is not currently implemented");
    }

    async fn count(&self) -> usize {
        unimplemented!("`count` is not currently implemented");
    }

    async fn get_storage_size(&self) -> Result<u64> {
        unimplemented!("`get_storage_size` is not currently implemented");
    }
}
