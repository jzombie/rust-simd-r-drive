use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct ReadRequestParams {
    pub key: Vec<u8>,
}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct ReadResponseParams {
    pub result: Option<Vec<u8>>,
}

pub struct Read;

impl RpcMethodPrebuffered for Read {
    const METHOD_ID: u64 = rpc_method_id!("read");

    type Input = ReadRequestParams;
    type Output = ReadResponseParams;

    fn encode_request(read_request_params: ReadRequestParams) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&read_request_params))
    }

    fn decode_request(bytes: &[u8]) -> Result<Self::Input, io::Error> {
        let req_params = bitcode::decode::<ReadRequestParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(req_params)
    }

    fn encode_response(result: Self::Output) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&result))
    }

    fn decode_response(bytes: &[u8]) -> Result<Self::Output, io::Error> {
        let resp_params = bitcode::decode::<ReadResponseParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(resp_params)
    }
}
