use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use thiserror::Error;

mod http;
mod udp;

pub use http::{
    AnnounceEvent, HttpTrackerClient, TrackerAnnounce, TrackerRequest, TrackerServiceError,
};
pub use udp::{UdpTrackerClient, UdpTrackerError};

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TrackerCodecError {
    #[error("compact IPv4 peer list length {0} is not divisible by 6")]
    InvalidIpv4Peers(usize),
    #[error("compact IPv6 peer list length {0} is not divisible by 18")]
    InvalidIpv6Peers(usize),
    #[error("UDP tracker response is truncated: expected at least {expected}, got {actual}")]
    Truncated { expected: usize, actual: usize },
    #[error("UDP tracker transaction mismatch: expected {expected}, got {actual}")]
    TransactionMismatch { expected: u32, actual: u32 },
    #[error("unexpected UDP tracker action {actual}; expected {expected}")]
    UnexpectedAction { expected: u32, actual: u32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectResponse {
    pub connection_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnnounceResponse {
    pub interval_seconds: u32,
    pub leechers: u32,
    pub seeders: u32,
    pub peers: Vec<SocketAddr>,
}

pub fn parse_compact_ipv4(input: &[u8]) -> Result<Vec<SocketAddr>, TrackerCodecError> {
    let chunks = input.chunks_exact(6);
    if !chunks.remainder().is_empty() {
        return Err(TrackerCodecError::InvalidIpv4Peers(input.len()));
    }
    Ok(chunks
        .map(|chunk| {
            SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]),
                u16::from_be_bytes([chunk[4], chunk[5]]),
            ))
        })
        .collect())
}

pub fn parse_compact_ipv6(input: &[u8]) -> Result<Vec<SocketAddr>, TrackerCodecError> {
    let chunks = input.chunks_exact(18);
    if !chunks.remainder().is_empty() {
        return Err(TrackerCodecError::InvalidIpv6Peers(input.len()));
    }
    chunks
        .map(|chunk| {
            let address: [u8; 16] = chunk[..16]
                .try_into()
                .map_err(|_| TrackerCodecError::InvalidIpv6Peers(input.len()))?;
            Ok(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(address),
                u16::from_be_bytes([chunk[16], chunk[17]]),
                0,
                0,
            )))
        })
        .collect()
}

pub fn decode_connect_response(
    input: &[u8],
    expected_transaction: u32,
) -> Result<ConnectResponse, TrackerCodecError> {
    require_bytes(input, 16)?;
    validate_header(input, 0, expected_transaction)?;
    Ok(ConnectResponse {
        connection_id: read_u64(input, 8)?,
    })
}

pub fn decode_announce_response(
    input: &[u8],
    expected_transaction: u32,
) -> Result<AnnounceResponse, TrackerCodecError> {
    require_bytes(input, 20)?;
    validate_header(input, 1, expected_transaction)?;
    Ok(AnnounceResponse {
        interval_seconds: read_u32(input, 8)?,
        leechers: read_u32(input, 12)?,
        seeders: read_u32(input, 16)?,
        peers: parse_compact_ipv4(&input[20..])?,
    })
}

pub fn decode_announce_response_ipv6(
    input: &[u8],
    expected_transaction: u32,
) -> Result<AnnounceResponse, TrackerCodecError> {
    require_bytes(input, 20)?;
    validate_header(input, 1, expected_transaction)?;
    Ok(AnnounceResponse {
        interval_seconds: read_u32(input, 8)?,
        leechers: read_u32(input, 12)?,
        seeders: read_u32(input, 16)?,
        peers: parse_compact_ipv6(&input[20..])?,
    })
}

fn validate_header(
    input: &[u8],
    expected_action: u32,
    expected_transaction: u32,
) -> Result<(), TrackerCodecError> {
    let action = read_u32(input, 0)?;
    if action != expected_action {
        return Err(TrackerCodecError::UnexpectedAction {
            expected: expected_action,
            actual: action,
        });
    }
    let transaction = read_u32(input, 4)?;
    if transaction != expected_transaction {
        return Err(TrackerCodecError::TransactionMismatch {
            expected: expected_transaction,
            actual: transaction,
        });
    }
    Ok(())
}

fn require_bytes(input: &[u8], expected: usize) -> Result<(), TrackerCodecError> {
    if input.len() < expected {
        Err(TrackerCodecError::Truncated {
            expected,
            actual: input.len(),
        })
    } else {
        Ok(())
    }
}

fn read_u32(input: &[u8], offset: usize) -> Result<u32, TrackerCodecError> {
    let bytes = input
        .get(offset..offset + 4)
        .ok_or(TrackerCodecError::Truncated {
            expected: offset + 4,
            actual: input.len(),
        })?;
    let array = bytes.try_into().map_err(|_| TrackerCodecError::Truncated {
        expected: offset + 4,
        actual: input.len(),
    })?;
    Ok(u32::from_be_bytes(array))
}

fn read_u64(input: &[u8], offset: usize) -> Result<u64, TrackerCodecError> {
    let bytes = input
        .get(offset..offset + 8)
        .ok_or(TrackerCodecError::Truncated {
            expected: offset + 8,
            actual: input.len(),
        })?;
    let array = bytes.try_into().map_err(|_| TrackerCodecError::Truncated {
        expected: offset + 8,
        actual: input.len(),
    })?;
    Ok(u64::from_be_bytes(array))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_peers_require_complete_records() {
        assert!(matches!(
            parse_compact_ipv4(&[127, 0, 0, 1, 0]),
            Err(TrackerCodecError::InvalidIpv4Peers(5))
        ));
        assert!(matches!(
            parse_compact_ipv6(&[0; 17]),
            Err(TrackerCodecError::InvalidIpv6Peers(17))
        ));
    }

    #[test]
    fn announce_response_validates_transaction_and_peers() {
        let mut response = Vec::new();
        response.extend_from_slice(&1_u32.to_be_bytes());
        response.extend_from_slice(&7_u32.to_be_bytes());
        response.extend_from_slice(&60_u32.to_be_bytes());
        response.extend_from_slice(&2_u32.to_be_bytes());
        response.extend_from_slice(&3_u32.to_be_bytes());
        response.extend_from_slice(&[127, 0, 0, 1]);
        response.extend_from_slice(&6881_u16.to_be_bytes());
        let decoded = decode_announce_response(&response, 7);
        assert_eq!(
            decoded.as_ref().ok().map(|value| value.peers.as_slice()),
            Some(&[SocketAddr::from(([127, 0, 0, 1], 6881))][..])
        );
        assert!(matches!(
            decode_announce_response(&response, 8),
            Err(TrackerCodecError::TransactionMismatch { .. })
        ));
    }
}
