//! batch_codec.rs
//! --------------
//! A tiny helper to (de)serialise the byte‐heavy payloads we pass over
//! the wire.  No macros, no external crates, zero-copy decoding where
//! possible.
//
//  Format (little-endian):
//
//  ┌────────────┬────────────────────────────────────────────┐
//  │ u32 count  │ repeated element …                         │
//  ├────────────┼────────────────────────────────────────────┤
//  │ element    │ • for `Vec<u8>`:   u32 len  |  bytes …     │
//  │            │ • for `Option<Vec<u8>>`:                  │
//  │            │       u8 tag (0 = None, 1 = Some)          │
//  │            │       [if tag == 1]  u32 len | bytes …     │
//  └────────────┴────────────────────────────────────────────┘
//
//  Length fields are 32-bit, so each entry is limited to < 4 GiB.

use std::{error::Error, fmt};

/// --------------------------------------------------------------------
/// Simple error type – no `thiserror` needed
/// --------------------------------------------------------------------
#[derive(Debug)]
pub struct CodecError(&'static str);

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for CodecError {}

/// --------------------------------------------------------------------
/// Public API
/// --------------------------------------------------------------------
pub struct BatchCodec;

impl BatchCodec {
    /* =================== Vec<Vec<u8>> ====================== */

    pub fn encode_keys(keys: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + keys.iter().map(|k| 4 + k.len()).sum::<usize>());
        out.extend_from_slice(&(keys.len() as u32).to_le_bytes());
        for k in keys {
            out.extend_from_slice(&(k.len() as u32).to_le_bytes());
            out.extend_from_slice(k);
        }
        out
    }

    pub fn decode_keys(buf: &[u8]) -> Result<Vec<Vec<u8>>, CodecError> {
        let (count, mut pos) = read_u32(buf, 0)?;
        let mut keys = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (len, p) = read_u32(buf, pos)?;
            pos = p;
            let end = pos + len as usize;
            if end > buf.len() {
                return Err(CodecError("truncated key payload"));
            }
            keys.push(buf[pos..end].to_vec());
            pos = end;
        }
        Ok(keys)
    }

    /* ===== Vec<Option<Vec<u8>>> (Some/None tagged) ========= */

    pub fn encode_optional_payloads(vals: &[Option<Vec<u8>>]) -> Vec<u8> {
        // Worst case: every element is Some -> 1(tag)+4(len)+data
        let cap = 4 + vals
            .iter()
            .map(|v| 1 + 4 + v.as_ref().map_or(0, |d| d.len()))
            .sum::<usize>();

        let mut out = Vec::with_capacity(cap);
        out.extend_from_slice(&(vals.len() as u32).to_le_bytes());

        for v in vals {
            match v {
                None => out.push(0),
                Some(bytes) => {
                    out.push(1);
                    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    out.extend_from_slice(bytes);
                }
            }
        }
        out
    }

    pub fn decode_optional_payloads(buf: &[u8]) -> Result<Vec<Option<Vec<u8>>>, CodecError> {
        let (count, mut pos) = read_u32(buf, 0)?;
        let mut out = Vec::with_capacity(count as usize);

        for _ in 0..count {
            // tag ---------------------------------------------------------
            if pos >= buf.len() {
                return Err(CodecError("truncated tag"));
            }
            let tag = buf[pos];
            pos += 1;

            match tag {
                0 => out.push(None),
                1 => {
                    let (len, p) = read_u32(buf, pos)?;
                    pos = p;
                    let end = pos + len as usize;
                    if end > buf.len() {
                        return Err(CodecError("truncated Some(payload)"));
                    }
                    out.push(Some(buf[pos..end].to_vec()));
                    pos = end;
                }
                _ => return Err(CodecError("invalid tag (must be 0 or 1)")),
            }
        }
        Ok(out)
    }

    /* =============================================================== */
    /*  Vec<Vec<u8>> again – but *all present*, no Option tag needed   */
    /* =============================================================== */

    pub fn encode_payloads(vals: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + vals.iter().map(|v| 4 + v.len()).sum::<usize>());
        out.extend_from_slice(&(vals.len() as u32).to_le_bytes());
        for v in vals {
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v);
        }
        out
    }

    pub fn decode_payloads(buf: &[u8]) -> Result<Vec<Vec<u8>>, CodecError> {
        Self::decode_keys(buf) // identical layout
    }

    /// Helper: byte length of the encoded keys vector – lets us jump
    /// to the start of the payload section when decoding a request.
    pub fn encoded_keys_len(keys: &[Vec<u8>]) -> usize {
        4 + keys.iter().map(|k| 4 + k.len()).sum::<usize>()
    }
}

/// --------------------------------------------------------------------
/// Small helper – read a LE u32 & return (value, new_pos)
/// --------------------------------------------------------------------
#[inline(always)]
fn read_u32(buf: &[u8], pos: usize) -> Result<(u32, usize), CodecError> {
    if pos + 4 > buf.len() {
        return Err(CodecError("truncated u32"));
    }
    let val = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
    Ok((val, pos + 4))
}

/* ------------------------------------------------------------------ */
/*                              tests                                 */
/* ------------------------------------------------------------------ */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_keys() {
        let keys = vec![
            b"alpha".to_vec(),
            b"beta".to_vec(),
            Vec::new(),
            b"gamma".to_vec(),
        ];
        let enc = BatchCodec::encode_keys(&keys);
        let dec = BatchCodec::decode_keys(&enc).unwrap();
        assert_eq!(dec, keys);
    }

    #[test]
    fn roundtrip_optional_payloads() {
        let values = vec![Some(b"foo".to_vec()), None, Some(b"barbaz".to_vec()), None];
        let enc = BatchCodec::encode_optional_payloads(&values);
        let dec = BatchCodec::decode_optional_payloads(&enc).unwrap();
        assert_eq!(dec, values);
    }
}
