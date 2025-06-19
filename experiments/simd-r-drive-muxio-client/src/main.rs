use muxio_rpc_service::prebuffered::RpcMethodPrebuffered;
use muxio_rpc_service_caller::prebuffered::RpcCallPrebuffered;
use muxio_tokio_rpc_client::RpcClient;
use simd_r_drive_muxio_service_definition::prebuffered::{
    Read, ReadRequestParams, ReadResponseParams, Write, WriteRequestParams, WriteResponseParams,
};
use std::sync::Arc;
use tokio::join;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:34129";

    // Use the actual bound address for the client
    let rpc_client = RpcClient::new(&format!("ws://{}/ws", addr)).await;

    Write::call(
        &rpc_client,
        WriteRequestParams {
            key: b"testing".into(),
            payload: b"testing12345".into(),
        },
    )
    .await;

    let resp = Read::call(
        &rpc_client,
        ReadRequestParams {
            key: b"testing".into(),
        },
    )
    .await;

    println!("response: {:?}", resp);
}
