// TODO: Move this to an example directory and add unit tests

// use simd_r_drive_ws_client::{AsyncDataStoreReader, AsyncDataStoreWriter, WsClient};
// use std::io::{Error, ErrorKind};

// #[tokio::main]
// async fn main() -> std::io::Result<()> {
//     let addr = "127.0.0.1:34129";

//     // Use the actual bound address for the client
//     let ws_client = WsClient::new(addr).await;

//     ws_client
//         .write(b"hello", b"Hello world!")
//         .await
//         .map_err(|e| Error::new(ErrorKind::Other, e))?;

//     let read_result = ws_client.read(b"hello").await;

//     if let Some(bytes) = read_result {
//         println!("Response: {:?}", std::str::from_utf8(&bytes));
//     }

//     Ok(())
// }
