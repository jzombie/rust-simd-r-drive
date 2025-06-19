use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct WriteRequestParams {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct WriteResponseParams {
    pub result: Option<u64>,
}

pub struct Write;

impl RpcMethodPrebuffered for Write {
    const METHOD_ID: u64 = rpc_method_id!("write");

    type Input = WriteRequestParams;
    type Output = WriteResponseParams;

    fn encode_request(write_request_paarms: WriteRequestParams) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&write_request_paarms))
    }

    fn decode_request(bytes: &[u8]) -> Result<Self::Input, io::Error> {
        let req_params = bitcode::decode::<WriteRequestParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(req_params)
    }

    fn encode_response(result: Self::Output) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&result))
    }

    fn decode_response(bytes: &[u8]) -> Result<Self::Output, io::Error> {
        let resp_params = bitcode::decode::<WriteResponseParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(resp_params)
    }
}
