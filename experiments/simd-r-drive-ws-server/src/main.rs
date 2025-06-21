use std::path::PathBuf;
use std::sync::Arc;
use tokio::join;
use tokio::net::TcpListener;
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
    BatchWrite, BatchWriteResponseParams, Read, ReadResponseParams, Write, WriteResponseParams,
};
mod cli;
use crate::cli::Cli;

// TODO: Implement batch_read
// TODO: Implement API-controlled write buffering
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Cli::parse_args();

    tracing_subscriber::fmt().with_env_filter("info").init();

    let store_path = PathBuf::from(args.storage);
    let listener = TcpListener::bind(args.listen).await?;
    let addr = listener.local_addr()?;

    // Wrap the DataStore in a tokio::RwLock to support:
    // - multiple concurrent readers
    // - exclusive write access when needed
    //
    // This improves read throughput by allowing parallel read-only RPCs.
    let store = Arc::new(RwLock::new(DataStore::open(&store_path).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("store open failed: {e}"))
    })?));
    info!("MAIN: DataStore opened and wrapped in Arc<RwLock>.");

    let rpc_server = RpcServer::new();
    let endpoint = rpc_server.endpoint();

    let write_store = Arc::clone(&store);
    let batch_write_store = Arc::clone(&store);
    let read_store = Arc::clone(&store);

    let _ = join!(
        endpoint.register_prebuffered(Write::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&write_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let req = Write::decode_request(&bytes)?;

                        // Acquire exclusive write lock.
                        //
                        // This blocks all concurrent readers and writers
                        // until the mutation is complete.
                        //
                        // Tokio's blocking_write ensures the thread isn't stalled.
                        let store = store_mutex.blocking_write();
                        let result = store.write(&req.key, &req.payload);
                        let resp = Write::encode_response(WriteResponseParams {
                            result: result.ok(),
                        })?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(resp)
                    })
                    .await
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, format!("write task: {e}"))
                    })??;
                    Ok(resp)
                }
            }
        }),
        endpoint.register_prebuffered(BatchWrite::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&batch_write_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let req = BatchWrite::decode_request(&bytes)?;

                        // Acquire exclusive lock for batch write.
                        //
                        // Like Write, this prevents all concurrent access
                        // while the batch mutation occurs.
                        let store = store_mutex.blocking_write();
                        let borrowed_entries: Vec<(&[u8], &[u8])> = req
                            .entries
                            .iter()
                            .map(|(k, v)| (k.as_slice(), v.as_slice()))
                            .collect();
                        let result = store.batch_write(&borrowed_entries);
                        let resp = BatchWrite::encode_response(BatchWriteResponseParams {
                            result: result.ok(),
                        })?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(resp)
                    })
                    .await
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, format!("batch task: {e}"))
                    })??;
                    Ok(resp)
                }
            }
        }),
        endpoint.register_prebuffered(Read::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                let store_mutex = Arc::clone(&read_store);
                async move {
                    let resp = task::spawn_blocking(move || {
                        let req = Read::decode_request(&bytes)?;

                        // Acquire shared read lock.
                        //
                        // This allows multiple concurrent readers to access
                        // the store at the same time *as long as no writer holds the lock*.
                        //
                        // We extract the data into memory immediately,
                        // and then drop the read lock to maximize concurrency.
                        let store = store_mutex.blocking_read();
                        let result_data = store
                            .read(&req.key)
                            .map(|handle| handle.as_slice().to_vec());
                        let resp = Read::encode_response(ReadResponseParams {
                            result: result_data,
                        })?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(resp)
                    })
                    .await
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::Other, format!("read task: {e}"))
                    })??;
                    Ok(resp)
                }
            }
        }),
    );

    info!(address = %addr, "MAIN: RPC Server listening.");

    Arc::new(rpc_server)
        .serve_with_listener(listener)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(())
}
