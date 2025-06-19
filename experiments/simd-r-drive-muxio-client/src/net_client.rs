use muxio_rpc_service_caller::prebuffered::RpcCallPrebuffered;
use muxio_tokio_rpc_client::RpcClient;
use simd_r_drive::{
    DataStore,
    traits::{AsyncDataStoreReader, AsyncDataStoreWriter},
};
use simd_r_drive_muxio_service_definition::prebuffered::{
    Read, ReadRequestParams, ReadResponseParams, Write, WriteRequestParams, WriteResponseParams,
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

impl AsyncDataStoreWriter for NetClient {
    async fn write_stream<R: std::io::Read>(&self, key: &[u8], reader: &mut R) -> Result<u64> {
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
        unimplemented!("`batch_write` is not currently implemented");
    }

    async fn rename_entry(&self, old_key: &[u8], new_key: &[u8]) -> Result<u64> {
        unimplemented!("`rename_entry` is not currently implemented");
    }

    async fn copy_entry(&self, key: &[u8], target: &DataStore) -> Result<u64> {
        unimplemented!("`copy_entry` is not currently implemented");
    }

    async fn move_entry(&self, key: &[u8], target: &DataStore) -> Result<u64> {
        unimplemented!("`move_entry` is not currently implemented");
    }

    async fn delete_entry(&self, key: &[u8]) -> Result<u64> {
        unimplemented!("`delete_entry` is not currently implemented");
    }
}
