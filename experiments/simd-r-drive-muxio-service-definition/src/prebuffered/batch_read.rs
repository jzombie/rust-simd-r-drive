use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, Debug, PartialEq)]
pub struct BatchReadRequestParams {
    pub keys: Vec<Vec<u8>>,
}

#[derive(Encode, Decode, Debug, PartialEq)]
pub struct BatchReadResponseParams {
    pub entries: Vec<Option<Vec<u8>>>,
}

pub struct BatchRead;

impl RpcMethodPrebuffered for BatchRead {
    const METHOD_ID: u64 = rpc_method_id!("batch_read");

    type Input = BatchReadRequestParams;
    type Output = BatchReadResponseParams;

    fn encode_request(read_request_params: BatchReadRequestParams) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&read_request_params))
    }

    fn decode_request(bytes: &[u8]) -> Result<Self::Input, io::Error> {
        let req_params = bitcode::decode::<BatchReadRequestParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(req_params)
    }

    fn encode_response(results: Self::Output) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&results))
    }

    fn decode_response(bytes: &[u8]) -> Result<Self::Output, io::Error> {
        let resp_params = bitcode::decode::<BatchReadResponseParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(resp_params)
    }
}
