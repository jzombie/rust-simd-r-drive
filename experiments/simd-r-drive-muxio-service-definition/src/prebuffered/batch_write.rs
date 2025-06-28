use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, Debug, PartialEq)]
pub struct BatchWriteRequestParams {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>, // key → payload
}

#[derive(Encode, Decode, Debug, PartialEq)]
pub struct BatchWriteResponseParams {
    pub tail_offset: u64,
}

pub struct BatchWrite;

impl RpcMethodPrebuffered for BatchWrite {
    const METHOD_ID: u64 = rpc_method_id!("batch_write");

    type Input = BatchWriteRequestParams;
    type Output = BatchWriteResponseParams;

    fn encode_request(write_request_params: Self::Input) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&write_request_params))
    }

    fn decode_request(bytes: &[u8]) -> Result<Self::Input, io::Error> {
        let req_params = bitcode::decode::<Self::Input>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(req_params)
    }

    fn encode_response(result: Self::Output) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&result))
    }

    fn decode_response(bytes: &[u8]) -> Result<Self::Output, io::Error> {
        let resp_params = bitcode::decode::<Self::Output>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(resp_params)
    }
}
