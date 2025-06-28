use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct ReadRequestParams {
    pub key: Vec<u8>,
}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct ReadResponseParams {
    pub entry_payload: Option<Vec<u8>>,
}

pub struct Read;

impl RpcMethodPrebuffered for Read {
    const METHOD_ID: u64 = rpc_method_id!("read");

    type Input = ReadRequestParams;
    type Output = ReadResponseParams;

    fn encode_request(request_params: Self::Input) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&request_params))
    }

    fn decode_request(bytes: &[u8]) -> Result<Self::Input, io::Error> {
        let request_params = bitcode::decode::<Self::Input>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(request_params)
    }

    fn encode_response(response_params: Self::Output) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&response_params))
    }

    fn decode_response(response_bytes: &[u8]) -> Result<Self::Output, io::Error> {
        let response_params = bitcode::decode::<Self::Output>(response_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(response_params)
    }
}
