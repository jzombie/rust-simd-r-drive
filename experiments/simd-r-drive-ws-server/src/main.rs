use std::sync::Arc;
use tokio::join;
use tokio::sync::RwLock;
use tokio::task;
use tracing::info;

use muxio_rpc_service::prebuffered::RpcMethodPrebuffered;
use muxio_tokio_rpc_server::{RpcServer, RpcServiceEndpointInterface};
use simd_r_drive::{
    DataStore,
    traits::{DataStoreReader, DataStoreWriter},
};

use simd_r_drive_muxio_service_definition::prebuffered::{
    BatchRead, BatchReadResponseParams, BatchWrite, BatchWriteResponseParams, Count,
    CountResponseParams, Delete, DeleteResponseParams, Read, ReadResponseParams, Write,
    WriteResponseParams,
};
mod cli;
use crate::cli::Cli;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Cli::parse_args();

    tracing_subscriber::fmt().with_env_filter("info").init();

    let store_path = args.storage;

    // Wrap the DataStore in a tokio::RwLock to support:
    // - multiple concurrent readers
    // - exclusive write access when needed
    //
    // This improves read throughput by allowing parallel read-only RPCs.
    let store =
        Arc::new(RwLock::new(DataStore::open(&store_path).map_err(|e| {
            std::io::Error::other(format!("store open failed: {e}"))
        })?));
    info!("MAIN: DataStore opened and wrapped in Arc<RwLock>.");

    let rpc_server = RpcServer::new();
    let endpoint = rpc_server.endpoint();

    // TODO: Rename with consistency; `delete_store` sounds like it deletes the entire store
    let write_store = Arc::clone(&store);
    let batch_write_store = Arc::clone(&store);
    let read_store = Arc::clone(&store);
    let batch_read_store = Arc::clone(&store);
    let delete_store = Arc::clone(&store);
    let count_store = Arc::clone(&store);

    let _ = join!(
        endpoint.register_prebuffered(Write::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&write_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let params = Write::decode_request(&bytes)?;
                        let store = store_mutex.blocking_write();
                        let tail_offset = store.write(&params.key, &params.payload)?;
                        let response_bytes =
                            Write::encode_response(WriteResponseParams { tail_offset })?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response_bytes)
                    })
                    .await
                    .map_err(|e| std::io::Error::other(format!("write task: {e}")))??;
                    Ok(resp)
                }
            }
        }),
        endpoint.register_prebuffered(BatchWrite::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&batch_write_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let params = BatchWrite::decode_request(&bytes)?;
                        let store = store_mutex.blocking_write();
                        let borrowed_entries: Vec<(&[u8], &[u8])> = params
                            .entries
                            .iter()
                            .map(|(k, v)| (k.as_slice(), v.as_slice()))
                            .collect();
                        let tail_offset = store.batch_write(&borrowed_entries)?;
                        let response_bytes =
                            BatchWrite::encode_response(BatchWriteResponseParams { tail_offset })?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response_bytes)
                    })
                    .await
                    .map_err(|e| std::io::Error::other(format!("batch task: {e}")))??;
                    Ok(resp)
                }
            }
        }),
        endpoint.register_prebuffered(Read::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&read_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let params = Read::decode_request(&bytes)?;
                        let store = store_mutex.blocking_read();
                        let entry_payload = store
                            .read(&params.key)?
                            .map(|handle| handle.as_slice().to_vec());
                        let response_bytes =
                            Read::encode_response(ReadResponseParams { entry_payload })?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response_bytes)
                    })
                    .await
                    .map_err(|e| std::io::Error::other(format!("read task: {e}")))??;
                    Ok(resp)
                }
            }
        }),
        endpoint.register_prebuffered(BatchRead::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&batch_read_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let params = BatchRead::decode_request(&bytes)?;
                        let store_guard = store_mutex.blocking_read();
                        let key_refs: Vec<&[u8]> =
                            params.keys.iter().map(|k| k.as_slice()).collect();
                        let handles = store_guard.batch_read(&key_refs)?;

                        drop(store_guard); // free the lock ASAP

                        let entries_payloads: Vec<Option<Vec<u8>>> = handles
                            .into_iter()
                            .map(|opt| opt.map(|h| h.as_slice().to_vec()))
                            .collect();
                        let response_bytes = BatchRead::encode_response(BatchReadResponseParams {
                            entries_payloads,
                        })?;

                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response_bytes)
                    })
                    .await
                    .map_err(|e| std::io::Error::other(format!("batch read task: {e}")))??;

                    Ok(resp)
                }
            }
        }),
        endpoint.register_prebuffered(Delete::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&delete_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let params = Delete::decode_request(&bytes)?;
                        let store = store_mutex.blocking_write();
                        let tail_offset = store.delete(&params.key)?;
                        let response_bytes =
                            Delete::encode_response(DeleteResponseParams { tail_offset })?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response_bytes)
                    })
                    .await
                    .map_err(|e| std::io::Error::other(format!("write task: {e}")))??;
                    Ok(resp)
                }
            }
        }),
        endpoint.register_prebuffered(Count::METHOD_ID, {
            move |_, _bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&count_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let store = store_mutex.blocking_read();
                        let total_entries = store.count()?;
                        let response_bytes =
                            Count::encode_response(CountResponseParams { total_entries })?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response_bytes)
                    })
                    .await
                    .map_err(|e| std::io::Error::other(format!("write task: {e}")))??;
                    Ok(resp)
                }
            }
        }),
    );

    rpc_server
        .serve_on(&args.host, args.port)
        .await
        .map_err(std::io::Error::other)?;

    Ok(())
}
