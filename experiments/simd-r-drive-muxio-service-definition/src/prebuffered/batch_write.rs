//! src/rpc/batch_write.rs
//! ----------------------

use crate::utils::BatchCodec;
use muxio_rpc_service::{prebuffered::RpcMethodPrebuffered, rpc_method_id};
use std::io;

/// --- Request / Response DTOs ---
#[derive(Debug, PartialEq)]
pub struct BatchWriteRequestParams {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>, // key → payload
}

#[derive(Debug, PartialEq)]
pub struct BatchWriteResponseParams {
    pub result: u64, // total payload bytes
}

/// RPC method: `batch_write`
pub struct BatchWrite;

impl RpcMethodPrebuffered for BatchWrite {
    const METHOD_ID: u64 = rpc_method_id!("batch_write");

    type Input = BatchWriteRequestParams;
    type Output = BatchWriteResponseParams;

    /* -------------------------------- encode --------------------------- */

    fn encode_request(req: Self::Input) -> Result<Vec<u8>, io::Error> {
        // (1) split keys / payloads -------------------------------------------------
        let (keys, payloads): (Vec<_>, Vec<_>) = req.entries.into_iter().unzip();

        // (2) encode each side with BatchCodec -------------------------------------
        let mut buf = BatchCodec::encode_keys(&keys);
        let mut buf2 = BatchCodec::encode_payloads(&payloads); // <-- new helper
        buf.append(&mut buf2);

        Ok(buf)
    }

    /* -------------------------------- decode --------------------------- */

    fn decode_request(bytes: &[u8]) -> Result<Self::Input, io::Error> {
        // (1) first vec – keys ------------------------------------------------------
        let keys = BatchCodec::decode_keys(bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // (2) second vec – payloads -------------------------------------------------
        // offset = 4 + Σ(4 + key.len)  — reuse helper to find the split point
        let off = BatchCodec::encoded_keys_len(&keys);
        let payloads = BatchCodec::decode_payloads(&bytes[off..])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        if keys.len() != payloads.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "key/payload count mismatch",
            ));
        }

        Ok(BatchWriteRequestParams {
            entries: keys.into_iter().zip(payloads).collect(),
        })
    }

    /* ---------------------------- encode response ---------------------- */

    fn encode_response(resp: Self::Output) -> Result<Vec<u8>, io::Error> {
        // tag (1) + u64 (8)
        Ok({
            let mut v = Vec::with_capacity(9);
            v.push(1); // “Some” always – zero is unused
            v.extend_from_slice(&resp.result.to_le_bytes());
            v
        })
    }

    /* ---------------------------- decode response ---------------------- */

    fn decode_response(bytes: &[u8]) -> Result<Self::Output, io::Error> {
        if bytes.len() != 9 || bytes[0] != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid response buffer",
            ));
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[1..9]);
        Ok(BatchWriteResponseParams {
            result: u64::from_le_bytes(arr),
        })
    }
}
