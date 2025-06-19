use muxio_rpc_service::prebuffered::RpcMethodPrebuffered;
use muxio_rpc_service_caller::prebuffered::RpcCallPrebuffered;
use muxio_tokio_rpc_client::RpcClient;
use simd_r_drive_muxio_service_definition::prebuffered::{
    Write, WriteRequestParams, WriteResponseParams,
};
use std::sync::Arc;
use tokio::join;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:40159";

    // Use the actual bound address for the client
    let rpc_client = RpcClient::new(&format!("ws://{}/ws", addr)).await;

    Write::call(
        &rpc_client,
        WriteRequestParams {
            key: b"testing".into(),
            payload: b"testing123".into(),
        },
    )
    .await;

    // // `join!` will await all responses before proceeding
    // let (res1, res2, res3, res4, res5, res6) = join!(
    //     Add::call(&rpc_client, vec![1.0, 2.0, 3.0]),
    //     Add::call(&rpc_client, vec![8.0, 3.0, 7.0]),
    //     Mult::call(&rpc_client, vec![8.0, 3.0, 7.0]),
    //     Mult::call(&rpc_client, vec![1.5, 2.5, 8.5]),
    //     Echo::call(&rpc_client, b"testing 1 2 3".into()),
    //     Echo::call(&rpc_client, b"testing 4 5 6".into()),
    // );

    // tracing::info!("Result from first add(): {:?}", res1);
    // tracing::info!("Result from second add(): {:?}", res2);
    // tracing::info!("Result from first mult(): {:?}", res3);
    // tracing::info!("Result from second mult(): {:?}", res4);
    // tracing::info!(
    //     "Result from first echo(): {:?}",
    //     String::from_utf8(res5.unwrap())
    // );
    // tracing::info!(
    //     "Result from second echo(): {:?}",
    //     String::from_utf8(res6.unwrap())
    // );
}
