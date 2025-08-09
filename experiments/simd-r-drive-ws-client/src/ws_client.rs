use muxio_rpc_service_caller::{RpcServiceCallerInterface, prebuffered::RpcCallPrebuffered};
use muxio_tokio_rpc_client::{RpcClient, RpcTransportState};
use simd_r_drive::{
    DataStore, EntryMetadata,
    traits::{AsyncDataStoreReader, AsyncDataStoreWriter},
};
use simd_r_drive_muxio_service_definition::prebuffered::{
    BatchRead, BatchReadRequestParams, BatchWrite, BatchWriteRequestParams, Delete,
    DeleteRequestParams, Exists, ExistsRequestParams, FileSize, FileSizeRequestParams, IsEmpty,
    IsEmptyRequestParams, Len, LenRequestParams, Read, ReadRequestParams, Write,
    WriteRequestParams,
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

    async fn write_stream_with_key_hash<R: std::io::Read>(
        &self,
        _key_hash: u64,
        _reader: &mut R,
    ) -> Result<u64> {
        unimplemented!("`write_stream_with_key_hash` is not currently implemented");
    }

    async fn write(&self, key: &[u8], payload: &[u8]) -> Result<u64> {
        let response_params = Write::call(
            &self.rpc_client,
            WriteRequestParams {
                key: key.to_vec(),
                payload: payload.to_vec(),
            },
        )
        .await?;

        Ok(response_params.tail_offset)
    }

    async fn write_with_key_hash(&self, _key_hash: u64, _payload: &[u8]) -> Result<u64> {
        unimplemented!("`write_with_key_hash` is not currently implemented");
    }

    async fn batch_write(&self, entries: &[(&[u8], &[u8])]) -> Result<u64> {
        let response_params = BatchWrite::call(
            &self.rpc_client,
            BatchWriteRequestParams {
                entries: entries
                    .iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect(),
            },
        )
        .await?;

        Ok(response_params.tail_offset)
    }

    async fn batch_write_with_key_hashes(
        &self,
        _prehashed_keys: Vec<(u64, &[u8])>,
        _allow_null_bytes: bool,
    ) -> Result<u64> {
        unimplemented!("`batch_write_with_key_hashes` is not currently implemented");
    }

    async fn rename(&self, _old_key: &[u8], _new_key: &[u8]) -> Result<u64> {
        unimplemented!("`rename` is not currently implemented");
    }

    async fn copy(&self, _key: &[u8], _target: &DataStore) -> Result<u64> {
        unimplemented!("`copy` is not currently implemented");
    }

    async fn transfer(&self, _key: &[u8], _target: &DataStore) -> Result<u64> {
        unimplemented!("`transfer` is not currently implemented");
    }

    async fn delete(&self, key: &[u8]) -> Result<u64> {
        let resp =
            Delete::call(&self.rpc_client, DeleteRequestParams { key: key.to_vec() }).await?;

        Ok(resp.tail_offset)
    }

    async fn batch_delete(&self, _keys: &[&[u8]]) -> Result<u64> {
        unimplemented!("`batch_delete` is not currently implemented");
    }

    async fn batch_delete_key_hashes(&self, _prehashed_keys: &[u64]) -> Result<u64> {
        unimplemented!("`batch_delete_key_hashes` is not currently implemented");
    }
}

#[async_trait::async_trait]
impl AsyncDataStoreReader for WsClient {
    // FIXME: This is a workaround until properly implementing a stream-able handle
    type EntryHandleType = Vec<u8>;

    async fn exists(&self, key: &[u8]) -> Result<bool> {
        let response_params =
            Exists::call(&self.rpc_client, ExistsRequestParams { key: key.to_vec() }).await?;

        Ok(response_params.exists)
    }

    async fn exists_with_key_hash(&self, _prehashed_key: u64) -> Result<bool> {
        unimplemented!("`exists_with_key_hash` is not currently implemented");
    }

    async fn read(&self, key: &[u8]) -> Result<Option<Self::EntryHandleType>> {
        let response_params =
            Read::call(&self.rpc_client, ReadRequestParams { key: key.to_vec() }).await?;

        Ok(response_params.entry_payload)
    }

    async fn read_with_key_hash(
        &self,
        _prehashed_key: u64,
    ) -> Result<Option<Self::EntryHandleType>> {
        unimplemented!("`read_with_key_hash` is not currently implemented");
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

    async fn batch_read_hashed_keys(
        &self,
        _prehashed_keys: &[u64],
        _non_hashed_keys: Option<&[&[u8]]>,
    ) -> Result<Vec<Option<Self::EntryHandleType>>> {
        unimplemented!("`batch_read_hashed_keys` is not currently implemented");
    }

    async fn read_metadata(&self, _key: &[u8]) -> Result<Option<EntryMetadata>> {
        unimplemented!("`read_metadata` is not currently implemented");
    }

    async fn len(&self) -> Result<usize> {
        let response_params = Len::call(&self.rpc_client, LenRequestParams {}).await?;

        Ok(response_params.total_entries)
    }

    async fn is_empty(&self) -> Result<bool> {
        let response_params = IsEmpty::call(&self.rpc_client, IsEmptyRequestParams {}).await?;

        Ok(response_params.is_empty)
    }

    async fn file_size(&self) -> Result<u64> {
        let response_params = FileSize::call(&self.rpc_client, FileSizeRequestParams {}).await?;

        Ok(response_params.file_size)
    }
}
