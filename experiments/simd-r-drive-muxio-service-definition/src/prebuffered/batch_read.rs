// TODO: Switch back to `bitcode` implementation?

use crate::utils::batch_codec::BatchCodec;
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

/// ------------------------------------------------------------------------
///   Wire-format structs (unchanged)
/// ------------------------------------------------------------------------
#[derive(Debug, PartialEq)]
pub struct BatchReadRequestParams {
    pub keys: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub struct BatchReadResponseParams {
    pub results: Vec<Option<Vec<u8>>>,
}

/// ------------------------------------------------------------------------
///   RPC Method implementation – now based on `BatchCodec`
/// ------------------------------------------------------------------------
pub struct BatchRead;

impl RpcMethodPrebuffered for BatchRead {
    const METHOD_ID: u64 = rpc_method_id!("batch_read");

    type Input = BatchReadRequestParams;
    type Output = BatchReadResponseParams;

    /* --------------- request (keys) ----------------------------------- */
    fn encode_request(req: Self::Input) -> io::Result<Vec<u8>> {
        Ok(BatchCodec::encode_keys(&req.keys))
    }

    fn decode_request(buf: &[u8]) -> io::Result<Self::Input> {
        let keys = BatchCodec::decode_keys(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(BatchReadRequestParams { keys })
    }

    /* --------------- response (optional payloads) --------------------- */
    fn encode_response(resp: Self::Output) -> io::Result<Vec<u8>> {
        Ok(BatchCodec::encode_optional_payloads(&resp.results))
    }

    fn decode_response(buf: &[u8]) -> io::Result<Self::Output> {
        let results = BatchCodec::decode_optional_payloads(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(BatchReadResponseParams { results })
    }
}
