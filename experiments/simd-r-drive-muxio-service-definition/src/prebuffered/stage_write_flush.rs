use bitcode::{Decode, Encode};
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct StageWriteFlushRequestParams {}

#[derive(Encode, Decode, PartialEq, Debug)]
pub struct StageWriteFlushResponseParams {
    pub result: u64, // TODO: Rename `result`
}

pub struct StageWriteFlush;

impl RpcMethodPrebuffered for StageWriteFlush {
    const METHOD_ID: u64 = rpc_method_id!("stage_write_flush");

    type Input = StageWriteFlushRequestParams;
    type Output = StageWriteFlushResponseParams;

    fn encode_request(
        write_request_params: StageWriteFlushRequestParams,
    ) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&write_request_params))
    }

    fn decode_request(bytes: &[u8]) -> Result<Self::Input, io::Error> {
        let req_params = bitcode::decode::<StageWriteFlushRequestParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(req_params)
    }

    fn encode_response(result: Self::Output) -> Result<Vec<u8>, io::Error> {
        Ok(bitcode::encode(&result))
    }

    fn decode_response(bytes: &[u8]) -> Result<Self::Output, io::Error> {
        let resp_params = bitcode::decode::<StageWriteFlushResponseParams>(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(resp_params)
    }
}
