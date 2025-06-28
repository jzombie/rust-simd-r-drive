use muxio_rpc_service_caller::{RpcServiceCallerInterface, prebuffered::RpcCallPrebuffered};
use muxio_tokio_rpc_client::{RpcClient, RpcTransportState};
use simd_r_drive::{
    DataStore, EntryMetadata,
    traits::{AsyncDataStoreReader, AsyncDataStoreWriter},
};
use simd_r_drive_muxio_service_definition::prebuffered::{
    BatchRead, BatchReadRequestParams, BatchWrite, BatchWriteRequestParams, Read,
    ReadRequestParams, Write, WriteRequestParams,
};
use std::io::Result;

pub struct WsClient {
    rpc_client: RpcClient,
}

impl WsClient {
    pub async fn new(host: &str, port: u16) -> Result<Self> {
        let rpc_client = RpcClient::new(host, port).await?;

        Ok(Self { rpc_client })
    }

    /// Sets a callback that will be invoked with the current `RpcTransportState`
    /// whenever the WebSocket connection status changes.
    pub fn set_state_change_handler(
        &self,
        handler: impl Fn(RpcTransportState) + Send + Sync + 'static,
    ) {
        self.rpc_client.set_state_change_handler(handler);
    }
}

#[async_trait::async_trait]
impl AsyncDataStoreWriter for WsClient {
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

        Ok(resp.tail_offset)
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

        Ok(resp.tail_offset)
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
impl AsyncDataStoreReader for WsClient {
    // TODO: This is a workaround until properly implementing a stream-able handle
    type EntryHandleType = Vec<u8>;

    async fn read(&self, key: &[u8]) -> Result<Option<Self::EntryHandleType>> {
        let resp = Read::call(&self.rpc_client, ReadRequestParams { key: key.to_vec() }).await?;

        Ok(resp.entry_payload)
    }

    async fn read_last_entry(&self) -> Result<Option<Self::EntryHandleType>> {
        unimplemented!("`read_last_entry` is not currently implemented");
    }

    async fn batch_read(&self, keys: &[&[u8]]) -> Result<Vec<Option<Self::EntryHandleType>>> {
        let batch_read_result = BatchRead::call(
            &self.rpc_client,
            BatchReadRequestParams {
                keys: keys.iter().map(|key| key.to_vec()).collect(),
            },
        )
        .await?;

        Ok(batch_read_result.entries_payloads)
    }

    async fn read_metadata(&self, _key: &[u8]) -> Result<Option<EntryMetadata>> {
        unimplemented!("`read_metadata` is not currently implemented");
    }

    async fn count(&self) -> Result<usize> {
        unimplemented!("`count` is not currently implemented");
    }

    async fn get_storage_size(&self) -> Result<u64> {
        unimplemented!("`get_storage_size` is not currently implemented");
    }
}
