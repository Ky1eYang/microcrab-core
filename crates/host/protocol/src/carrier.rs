//! Native WebSocket bulk carrier. Control envelopes remain JSON; one message is
//! one bounded chunk. Transfer identity, FIFO sequence, EOF and final digest keep
//! their existing owners, independently of this compact representation.
use crate::{ErrorCode, MAX_CHUNK_BYTES, WireError, WireFrame};
const HEADER: usize = 11;
const MAGIC: &[u8; 4] = b"MCB1";

pub fn encode_transfer_chunk(
    transfer_id: &str,
    sequence: u32,
    data: &[u8],
    eof: bool,
) -> Result<Vec<u8>, WireError> {
    if transfer_id.is_empty() || transfer_id.len() > 256 || data.len() > MAX_CHUNK_BYTES {
        return Err(WireError::new(
            ErrorCode::Limit,
            "binary transfer identity or payload exceeds bounds",
        ));
    }
    let mut bytes = Vec::with_capacity(HEADER + transfer_id.len() + data.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(transfer_id.len() as u16).to_be_bytes());
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.push(u8::from(eof));
    bytes.extend_from_slice(transfer_id.as_bytes());
    bytes.extend_from_slice(data);
    Ok(bytes)
}

pub fn decode_transfer_chunk(bytes: &[u8]) -> Result<WireFrame, WireError> {
    let invalid = |message| WireError::new(ErrorCode::InvalidRequest, message);
    if bytes.len() < HEADER || &bytes[..4] != MAGIC {
        return Err(invalid("binary transfer header or version differs"));
    }
    let length = u16::from_be_bytes([bytes[4], bytes[5]]) as usize;
    if length == 0
        || length > 256
        || bytes.len() < HEADER + length
        || bytes.len() - HEADER - length > MAX_CHUNK_BYTES
        || bytes[10] > 1
    {
        return Err(invalid(
            "binary transfer identity, flags or payload exceeds bounds",
        ));
    }
    let transfer_id = std::str::from_utf8(&bytes[HEADER..HEADER + length])
        .map_err(|_| invalid("binary transfer identity is not UTF-8"))?
        .to_owned();
    Ok(WireFrame::TransferChunk {
        transfer_id,
        sequence: u32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]),
        eof: bytes[10] == 1,
        data: bytes[HEADER + length..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_binary_chunk_roundtrip_rejects_truncation_and_unknown_flags() {
        let bytes = encode_transfer_chunk("transfer", 17, &[0, 255, 42], true).unwrap();
        assert!(
            matches!(decode_transfer_chunk(&bytes).unwrap(), WireFrame::TransferChunk { transfer_id, sequence: 17, data, eof: true } if transfer_id == "transfer" && data == [0,255,42])
        );
        assert!(decode_transfer_chunk(&bytes[..HEADER]).is_err());
        let mut invalid = bytes;
        invalid[10] = 2;
        assert!(decode_transfer_chunk(&invalid).is_err());
        assert!(
            encode_transfer_chunk("transfer", 0, &vec![0; MAX_CHUNK_BYTES + 1], false).is_err()
        );
    }
}
