use muxio_rpc_service::prebuffered::RpcMethodPrebuffered;
use muxio_tokio_rpc_server::{RpcServer, RpcServiceEndpointInterface};
use simd_r_drive::{
    DataStore,
    traits::{DataStoreReader, DataStoreWriter},
};
use simd_r_drive_muxio_service_definition::prebuffered::{
    BatchWrite, BatchWriteRequestParams, BatchWriteResponseParams, Read, ReadRequestParams,
    ReadResponseParams, Write, WriteRequestParams, WriteResponseParams,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::join;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

// TODO: Enable total number of threads to be configured
#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // TODO: Don't hardcode store path
    let store = DataStore::open(&PathBuf::from("STATIC.bin")).unwrap();
    let store_mutex = Arc::new(Mutex::new(Arc::new(store)));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    // let addr = listener.local_addr().unwrap();

    let rpc_server = RpcServer::new();

    let _ = join!(
        rpc_server.register_prebuffered(Write::METHOD_ID, {
            let store_mutex = Arc::clone(&store_mutex);
            move |_, bytes| {
                let store_mutex = Arc::clone(&store_mutex);
                async move {
                    let req = Write::decode_request(&bytes)?;

                    let store = {
                        let store_guard = store_mutex.lock().await;
                        Arc::clone(&*store_guard)
                    };

                    let result = store.write(&req.key, &req.payload);

                    let resp = Write::encode_response(WriteResponseParams {
                        result: result.ok(),
                    })?;

                    Ok(resp)
                }
            }
        }),
        rpc_server.register_prebuffered(BatchWrite::METHOD_ID, {
            let store_mutex = Arc::clone(&store_mutex);
            move |_, bytes| {
                let store_mutex = Arc::clone(&store_mutex);
                async move {
                    let req = BatchWrite::decode_request(&bytes)?;

                    let store = {
                        let store_guard = store_mutex.lock().await;
                        Arc::clone(&*store_guard)
                    };

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
                }
            }
        }),
        rpc_server.register_prebuffered(Read::METHOD_ID, {
            let store_mutex = Arc::clone(&store_mutex);
            move |_, bytes| {
                let store_mutex = Arc::clone(&store_mutex);
                async move {
                    let req = Read::decode_request(&bytes)?;

                    let store = {
                        let store_guard = store_mutex.lock().await;
                        Arc::clone(&*store_guard)
                    };

                    let result = store.read(&req.key);

                    let resp = Read::encode_response(ReadResponseParams {
                        result: match result {
                            Some(entry_handle) => Some(entry_handle.as_slice().into()),
                            None => None,
                        },
                    })?;

                    Ok(resp)
                }
            }
        }),
    );

    Arc::new(rpc_server)
        .serve_with_listener(listener)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(())
}
