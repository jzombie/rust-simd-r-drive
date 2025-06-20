use std::path::PathBuf;
use std::sync::Arc;
// Use a standard, blocking Mutex, as it will be used in blocking tasks.
use std::sync::Mutex;
use tokio::join;
use tokio::net::TcpListener;
use tokio::task;
use tracing::info;

// Local imports
use muxio_rpc_service::prebuffered::RpcMethodPrebuffered;
use muxio_tokio_rpc_server::{RpcServer, RpcServiceEndpointInterface};
use simd_r_drive::{
    DataStore,
    traits::{DataStoreReader, DataStoreWriter},
};
use simd_r_drive_muxio_service_definition::prebuffered::{
    BatchWrite, BatchWriteResponseParams, Read, ReadResponseParams, Write, WriteResponseParams,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // TODO: Do not hardcode
    // 1. Initialize the DataStore ONLY ONCE at startup.
    let store_path = PathBuf::from(
        "/Users/jeremy/Projects/rust-sec-fetcher/python/narrative_stack/data/proto3.bin",
    );
    let store = Arc::new(Mutex::new(DataStore::open(&store_path).unwrap()));
    info!("MAIN: DataStore opened and wrapped in Arc<Mutex>.");

    let rpc_server = RpcServer::new();
    let endpoint = rpc_server.endpoint();

    // 2. THE FIX: Clone the Arc for each handler BEFORE the `join!` macro.
    // This gives each handler a shared pointer to the same store without moving ownership.
    let write_store = Arc::clone(&store);
    let batch_write_store = Arc::clone(&store);
    let read_store = Arc::clone(&store);

    let _ = join!(
        endpoint.register_prebuffered(Write::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                // The moved `write_store` is cloned again for the async block.
                let store_mutex = Arc::clone(&write_store);
                async move {
                    task::spawn_blocking(
                        move || -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
                            let req = Write::decode_request(&bytes)?;
                            let store = store_mutex.lock().unwrap();
                            let result = store.write(&req.key, &req.payload);
                            let resp = Write::encode_response(WriteResponseParams {
                                result: result.ok(),
                            })?;
                            Ok(resp)
                        },
                    )
                    .await
                    .unwrap()
                }
            }
        }),
        endpoint.register_prebuffered(BatchWrite::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                // The moved `batch_write_store` is cloned for the async block.
                let store_mutex = Arc::clone(&batch_write_store);
                async move {
                    task::spawn_blocking(
                        move || -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
                            let req = BatchWrite::decode_request(&bytes)?;
                            let store = store_mutex.lock().unwrap();
                            let borrowed_entries: Vec<(&[u8], &[u8])> = req
                                .entries
                                .iter()
                                .map(|(k, v)| (k.as_slice(), v.as_slice()))
                                .collect();
                            let result = store.batch_write(&borrowed_entries);
                            let resp = BatchWrite::encode_response(BatchWriteResponseParams {
                                result: result.ok(),
                            })?;
                            Ok(resp)
                        },
                    )
                    .await
                    .unwrap()
                }
            }
        }),
        endpoint.register_prebuffered(Read::METHOD_ID, {
            move |_, bytes: Vec<u8>| {
                // The moved `read_store` is cloned for the async block.
                let store_mutex = Arc::clone(&read_store);
                async move {
                    task::spawn_blocking(
                        move || -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
                            let req = Read::decode_request(&bytes)?;
                            let store = store_mutex.lock().unwrap();
                            let result_data = store
                                .read(&req.key)
                                .map(|handle| handle.as_slice().to_vec());
                            let resp = Read::encode_response(ReadResponseParams {
                                result: result_data,
                            })?;
                            Ok(resp)
                        },
                    )
                    .await
                    .unwrap()
                }
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    info!(address = %addr, "MAIN: RPC Server listening.");

    Arc::new(rpc_server)
        .serve_with_listener(listener)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(())
}
