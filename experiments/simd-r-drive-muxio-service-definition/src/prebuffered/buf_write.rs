use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct BufWriteRequestParams {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct BufWriteResponseParams {
    pub result: Option<u64>,
}

pub struct BufWrite;

impl RpcMethodPrebuffered for BufWrite {
    const METHOD_ID: u64 = rpc_method_id!("buf_write");

    type Input = BufWriteRequestParams;
    type Output = BufWriteResponseParams;

    fn encode_request(write_request_params: BufWriteRequestParams) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&write_request_params))
    }

    fn decode_request(bytes: &[u8]) -> Result<Self::Input, io::Error> {
        let req_params = bitcode::decode::<BufWriteRequestParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(req_params)
    }

    fn encode_response(result: Self::Output) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&result))
    }

    fn decode_response(bytes: &[u8]) -> Result<Self::Output, io::Error> {
        let resp_params = bitcode::decode::<BufWriteResponseParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(resp_params)
    }
}
