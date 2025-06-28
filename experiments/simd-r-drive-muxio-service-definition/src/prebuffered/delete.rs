use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct DeleteRequestParams {
    pub key: Vec<u8>,
}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct DeleteResponseParams {
    pub tail_offset: u64,
}

pub struct Delete;

impl RpcMethodPrebuffered for Delete {
    const METHOD_ID: u64 = rpc_method_id!("delete");

    type Input = DeleteRequestParams;
    type Output = DeleteResponseParams;

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
