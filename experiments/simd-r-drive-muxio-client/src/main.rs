// use muxio_rpc_service::prebuffered::RpcMethodPrebuffered;
// use muxio_rpc_service_caller::prebuffered::RpcCallPrebuffered;
// use muxio_tokio_rpc_client::RpcClient;
// use simd_r_drive_muxio_service_definition::prebuffered::{
//     Read, ReadRequestParams, ReadResponseParams, Write, WriteRequestParams, WriteResponseParams,
// };
// use std::sync::Arc;
// use tokio::join;
// use tokio::net::TcpListener;
use simd_r_drive_muxio_client::{AsyncDataStoreReader, AsyncDataStoreWriter, NetClient};

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:34129";

    // Use the actual bound address for the client
    let net_client = NetClient::new(addr).await;

    net_client
        .write(b"hello".as_slice(), b"123454321".as_slice())
        .await;

    println!("Response: {:?}", net_client.read(b"hello".as_slice()).await);
}
