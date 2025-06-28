use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct LenRequestParams {}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct LenResponseParams {
    pub total_entries: usize,
}

pub struct Len;

impl RpcMethodPrebuffered for Len {
    const METHOD_ID: u64 = rpc_method_id!("len");

    type Input = LenRequestParams;
    type Output = LenResponseParams;

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
