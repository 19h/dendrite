use bytes::{BufMut as _, Bytes, BytesMut};
use dendrite_core::{Sha1Hash, Sha256Hash};
use thiserror::Error;

mod session;

pub use session::{PeerConnection, PeerEvent, PeerSender, PeerSessionError};

const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
pub const HANDSHAKE_BYTES: usize = 68;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EncryptionPolicy {
    #[default]
    Disabled,
    Preferred,
    Required,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerCodecLimits {
    pub frame_bytes: usize,
    pub block_bytes: usize,
    pub bitfield_bytes: usize,
    pub extension_bytes: usize,
    pub hash_bytes: usize,
}

impl Default for PeerCodecLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 2 * 1024 * 1024,
            block_bytes: 16 * 1024,
            bitfield_bytes: 1024 * 1024,
            extension_bytes: 1024 * 1024,
            hash_bytes: (512 + 32) * 32,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PeerId([u8; 20]);

impl PeerId {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: Sha1Hash,
    pub peer_id: PeerId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRequest {
    pub piece: u32,
    pub begin: u32,
    pub length: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HashRequest {
    pub pieces_root: Sha256Hash,
    pub base_layer: u32,
    pub index: u32,
    pub length: u32,
    pub proof_layers: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Bytes),
    Request(BlockRequest),
    Piece {
        piece: u32,
        begin: u32,
        block: Bytes,
    },
    Cancel(BlockRequest),
    Reject(BlockRequest),
    HashRequest(HashRequest),
    Hashes {
        request: HashRequest,
        hashes: Bytes,
    },
    HashReject(HashRequest),
    Port(u16),
    Extended {
        extension_id: u8,
        payload: Bytes,
    },
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PeerCodecError {
    #[error("invalid BitTorrent handshake")]
    InvalidHandshake,
    #[error("peer frame length {actual} exceeds limit {maximum}")]
    FrameLimit { actual: usize, maximum: usize },
    #[error("peer message {id} has length {actual}; expected {expected}")]
    ExactLength {
        id: u8,
        actual: usize,
        expected: usize,
    },
    #[error("peer message {id} has invalid length {actual}")]
    InvalidLength { id: u8, actual: usize },
    #[error("piece block length {actual} exceeds limit {maximum}")]
    BlockLimit { actual: usize, maximum: usize },
    #[error("bitfield length {actual} exceeds limit {maximum}")]
    BitfieldLimit { actual: usize, maximum: usize },
    #[error("extension payload length {actual} exceeds limit {maximum}")]
    ExtensionLimit { actual: usize, maximum: usize },
    #[error("hash payload length {actual} exceeds limit {maximum}")]
    HashLimit { actual: usize, maximum: usize },
    #[error("unknown peer message id {0}")]
    UnknownMessage(u8),
    #[error("encoded peer message exceeds the u32 wire length")]
    EncodedLength,
}

impl Handshake {
    pub fn decode(input: &[u8]) -> Result<Self, PeerCodecError> {
        if input.len() != HANDSHAKE_BYTES || input[0] != 19 || &input[1..20] != PROTOCOL {
            return Err(PeerCodecError::InvalidHandshake);
        }
        let reserved: [u8; 8] = input[20..28]
            .try_into()
            .map_err(|_| PeerCodecError::InvalidHandshake)?;
        let hash: [u8; 20] = input[28..48]
            .try_into()
            .map_err(|_| PeerCodecError::InvalidHandshake)?;
        let peer: [u8; 20] = input[48..68]
            .try_into()
            .map_err(|_| PeerCodecError::InvalidHandshake)?;
        Ok(Self {
            reserved,
            info_hash: Sha1Hash::from_bytes(hash),
            peer_id: PeerId::from_bytes(peer),
        })
    }

    #[must_use]
    pub fn encode(self) -> [u8; HANDSHAKE_BYTES] {
        let mut output = [0_u8; HANDSHAKE_BYTES];
        output[0] = 19;
        output[1..20].copy_from_slice(PROTOCOL);
        output[20..28].copy_from_slice(&self.reserved);
        output[28..48].copy_from_slice(self.info_hash.as_bytes());
        output[48..68].copy_from_slice(self.peer_id.as_bytes());
        output
    }
}

pub fn decode_message(
    input: &mut BytesMut,
    limits: PeerCodecLimits,
) -> Result<Option<PeerMessage>, PeerCodecError> {
    if input.len() < 4 {
        return Ok(None);
    }
    let wire_length = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    let length = usize::try_from(wire_length).map_err(|_| PeerCodecError::FrameLimit {
        actual: usize::MAX,
        maximum: limits.frame_bytes,
    })?;
    if length > limits.frame_bytes {
        return Err(PeerCodecError::FrameLimit {
            actual: length,
            maximum: limits.frame_bytes,
        });
    }
    let total = length.checked_add(4).ok_or(PeerCodecError::FrameLimit {
        actual: length,
        maximum: limits.frame_bytes,
    })?;
    if input.len() < total {
        return Ok(None);
    }
    let frame = input.split_to(total).freeze();
    if length == 0 {
        return Ok(Some(PeerMessage::KeepAlive));
    }
    decode_payload(&frame, length, total, limits).map(Some)
}

fn decode_payload(
    frame: &Bytes,
    length: usize,
    total: usize,
    limits: PeerCodecLimits,
) -> Result<PeerMessage, PeerCodecError> {
    let id = frame[4];
    let message = match id {
        0 => {
            require_length(id, length, 1)?;
            PeerMessage::Choke
        }
        1 => {
            require_length(id, length, 1)?;
            PeerMessage::Unchoke
        }
        2 => {
            require_length(id, length, 1)?;
            PeerMessage::Interested
        }
        3 => {
            require_length(id, length, 1)?;
            PeerMessage::NotInterested
        }
        4 => {
            require_length(id, length, 5)?;
            PeerMessage::Have(read_u32(frame, 5)?)
        }
        5 => {
            let bytes = length - 1;
            if bytes > limits.bitfield_bytes {
                return Err(PeerCodecError::BitfieldLimit {
                    actual: bytes,
                    maximum: limits.bitfield_bytes,
                });
            }
            PeerMessage::Bitfield(frame.slice(5..total))
        }
        6 => {
            require_length(id, length, 13)?;
            PeerMessage::Request(read_request(frame)?)
        }
        7 => {
            if length < 9 {
                return Err(PeerCodecError::InvalidLength { id, actual: length });
            }
            let block_length = length - 9;
            if block_length > limits.block_bytes {
                return Err(PeerCodecError::BlockLimit {
                    actual: block_length,
                    maximum: limits.block_bytes,
                });
            }
            PeerMessage::Piece {
                piece: read_u32(frame, 5)?,
                begin: read_u32(frame, 9)?,
                block: frame.slice(13..total),
            }
        }
        8 => {
            require_length(id, length, 13)?;
            PeerMessage::Cancel(read_request(frame)?)
        }
        9 => {
            require_length(id, length, 3)?;
            PeerMessage::Port(u16::from_be_bytes([frame[5], frame[6]]))
        }
        16 => {
            require_length(id, length, 13)?;
            PeerMessage::Reject(read_request(frame)?)
        }
        20 => {
            if length < 2 {
                return Err(PeerCodecError::InvalidLength { id, actual: length });
            }
            let payload_length = length - 2;
            if payload_length > limits.extension_bytes {
                return Err(PeerCodecError::ExtensionLimit {
                    actual: payload_length,
                    maximum: limits.extension_bytes,
                });
            }
            PeerMessage::Extended {
                extension_id: frame[5],
                payload: frame.slice(6..total),
            }
        }
        21..=23 => decode_hash_message(frame, id, length, total, limits)?,
        _ => return Err(PeerCodecError::UnknownMessage(id)),
    };
    Ok(message)
}

fn decode_hash_message(
    frame: &Bytes,
    id: u8,
    length: usize,
    total: usize,
    limits: PeerCodecLimits,
) -> Result<PeerMessage, PeerCodecError> {
    if id != 22 {
        require_length(id, length, 49)?;
        let request = read_hash_request(frame)?;
        return Ok(if id == 21 {
            PeerMessage::HashRequest(request)
        } else {
            PeerMessage::HashReject(request)
        });
    }
    if length < 49 || !(length - 49).is_multiple_of(32) {
        return Err(PeerCodecError::InvalidLength { id, actual: length });
    }
    let hash_bytes = length - 49;
    if hash_bytes > limits.hash_bytes {
        return Err(PeerCodecError::HashLimit {
            actual: hash_bytes,
            maximum: limits.hash_bytes,
        });
    }
    Ok(PeerMessage::Hashes {
        request: read_hash_request(frame)?,
        hashes: frame.slice(53..total),
    })
}

pub fn encode_message(message: &PeerMessage) -> Result<Bytes, PeerCodecError> {
    let mut payload = BytesMut::new();
    match message {
        PeerMessage::KeepAlive => payload.put_u32(0),
        PeerMessage::Choke => encode_empty(&mut payload, 0),
        PeerMessage::Unchoke => encode_empty(&mut payload, 1),
        PeerMessage::Interested => encode_empty(&mut payload, 2),
        PeerMessage::NotInterested => encode_empty(&mut payload, 3),
        PeerMessage::Have(piece) => {
            payload.put_u32(5);
            payload.put_u8(4);
            payload.put_u32(*piece);
        }
        PeerMessage::Bitfield(bits) => {
            put_length(
                &mut payload,
                1_usize
                    .checked_add(bits.len())
                    .ok_or(PeerCodecError::EncodedLength)?,
            )?;
            payload.put_u8(5);
            payload.extend_from_slice(bits);
        }
        PeerMessage::Request(request) => encode_request(&mut payload, 6, *request),
        PeerMessage::Piece {
            piece,
            begin,
            block,
        } => {
            put_length(
                &mut payload,
                9_usize
                    .checked_add(block.len())
                    .ok_or(PeerCodecError::EncodedLength)?,
            )?;
            payload.put_u8(7);
            payload.put_u32(*piece);
            payload.put_u32(*begin);
            payload.extend_from_slice(block);
        }
        PeerMessage::Cancel(request) => encode_request(&mut payload, 8, *request),
        PeerMessage::Reject(request) => encode_request(&mut payload, 16, *request),
        PeerMessage::Port(port) => {
            payload.put_u32(3);
            payload.put_u8(9);
            payload.put_u16(*port);
        }
        PeerMessage::Extended {
            extension_id,
            payload: extension,
        } => {
            put_length(
                &mut payload,
                2_usize
                    .checked_add(extension.len())
                    .ok_or(PeerCodecError::EncodedLength)?,
            )?;
            payload.put_u8(20);
            payload.put_u8(*extension_id);
            payload.extend_from_slice(extension);
        }
        PeerMessage::HashRequest(request) => encode_hash_request(&mut payload, 21, *request),
        PeerMessage::Hashes { request, hashes } => {
            if !hashes.len().is_multiple_of(32) {
                return Err(PeerCodecError::InvalidLength {
                    id: 22,
                    actual: 49_usize.saturating_add(hashes.len()),
                });
            }
            put_length(
                &mut payload,
                49_usize
                    .checked_add(hashes.len())
                    .ok_or(PeerCodecError::EncodedLength)?,
            )?;
            payload.put_u8(22);
            encode_hash_fields(&mut payload, *request);
            payload.extend_from_slice(hashes);
        }
        PeerMessage::HashReject(request) => encode_hash_request(&mut payload, 23, *request),
    }
    Ok(payload.freeze())
}

fn read_request(frame: &[u8]) -> Result<BlockRequest, PeerCodecError> {
    Ok(BlockRequest {
        piece: read_u32(frame, 5)?,
        begin: read_u32(frame, 9)?,
        length: read_u32(frame, 13)?,
    })
}

fn read_hash_request(frame: &[u8]) -> Result<HashRequest, PeerCodecError> {
    let root: [u8; 32] = frame
        .get(5..37)
        .ok_or(PeerCodecError::InvalidLength {
            id: frame.get(4).copied().unwrap_or(u8::MAX),
            actual: frame.len().saturating_sub(4),
        })?
        .try_into()
        .map_err(|_| PeerCodecError::InvalidLength {
            id: frame.get(4).copied().unwrap_or(u8::MAX),
            actual: frame.len().saturating_sub(4),
        })?;
    Ok(HashRequest {
        pieces_root: Sha256Hash::from_bytes(root),
        base_layer: read_u32(frame, 37)?,
        index: read_u32(frame, 41)?,
        length: read_u32(frame, 45)?,
        proof_layers: read_u32(frame, 49)?,
    })
}

fn read_u32(frame: &[u8], start: usize) -> Result<u32, PeerCodecError> {
    let end = start.checked_add(4).ok_or(PeerCodecError::InvalidLength {
        id: frame.get(4).copied().unwrap_or(u8::MAX),
        actual: frame.len().saturating_sub(4),
    })?;
    let bytes = frame.get(start..end).ok_or(PeerCodecError::InvalidLength {
        id: frame.get(4).copied().unwrap_or(u8::MAX),
        actual: frame.len().saturating_sub(4),
    })?;
    Ok(u32::from_be_bytes(bytes.try_into().map_err(|_| {
        PeerCodecError::InvalidLength {
            id: frame.get(4).copied().unwrap_or(u8::MAX),
            actual: frame.len().saturating_sub(4),
        }
    })?))
}

fn require_length(id: u8, actual: usize, expected: usize) -> Result<(), PeerCodecError> {
    if actual == expected {
        Ok(())
    } else {
        Err(PeerCodecError::ExactLength {
            id,
            actual,
            expected,
        })
    }
}

fn encode_empty(output: &mut BytesMut, id: u8) {
    output.put_u32(1);
    output.put_u8(id);
}

fn encode_request(output: &mut BytesMut, id: u8, request: BlockRequest) {
    output.put_u32(13);
    output.put_u8(id);
    output.put_u32(request.piece);
    output.put_u32(request.begin);
    output.put_u32(request.length);
}

fn encode_hash_request(output: &mut BytesMut, id: u8, request: HashRequest) {
    output.put_u32(49);
    output.put_u8(id);
    encode_hash_fields(output, request);
}

fn encode_hash_fields(output: &mut BytesMut, request: HashRequest) {
    output.extend_from_slice(request.pieces_root.as_bytes());
    output.put_u32(request.base_layer);
    output.put_u32(request.index);
    output.put_u32(request.length);
    output.put_u32(request.proof_layers);
}

fn put_length(output: &mut BytesMut, length: usize) -> Result<(), PeerCodecError> {
    output.put_u32(u32::try_from(length).map_err(|_| PeerCodecError::EncodedLength)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_round_trip() {
        let handshake = Handshake {
            reserved: [0, 0, 0, 0, 0, 0x10, 0, 1],
            info_hash: Sha1Hash::from_bytes([4; 20]),
            peer_id: PeerId::from_bytes([7; 20]),
        };
        assert_eq!(Handshake::decode(&handshake.encode()), Ok(handshake));
    }

    #[test]
    fn messages_round_trip() -> Result<(), PeerCodecError> {
        let hash_request = HashRequest {
            pieces_root: Sha256Hash::from_bytes([8; 32]),
            base_layer: 3,
            index: 0,
            length: 2,
            proof_layers: 4,
        };
        let messages = [
            PeerMessage::KeepAlive,
            PeerMessage::Choke,
            PeerMessage::Have(42),
            PeerMessage::Request(BlockRequest {
                piece: 4,
                begin: 16_384,
                length: 16_384,
            }),
            PeerMessage::Piece {
                piece: 4,
                begin: 0,
                block: Bytes::from_static(b"block"),
            },
            PeerMessage::Extended {
                extension_id: 1,
                payload: Bytes::from_static(b"extension"),
            },
            PeerMessage::Reject(BlockRequest {
                piece: 1,
                begin: 0,
                length: 16_384,
            }),
            PeerMessage::HashRequest(hash_request),
            PeerMessage::Hashes {
                request: hash_request,
                hashes: Bytes::from(vec![9; 64]),
            },
            PeerMessage::HashReject(hash_request),
        ];
        for expected in messages {
            let encoded = encode_message(&expected)?;
            let mut buffer = BytesMut::from(encoded.as_ref());
            assert_eq!(
                decode_message(&mut buffer, PeerCodecLimits::default())?,
                Some(expected)
            );
            assert!(buffer.is_empty());
        }
        Ok(())
    }

    #[test]
    fn rejects_wrong_fixed_length_without_consuming_following_frame() {
        let mut input = BytesMut::from(&b"\0\0\0\x02\x00\xff\0\0\0\0"[..]);
        assert!(matches!(
            decode_message(&mut input, PeerCodecLimits::default()),
            Err(PeerCodecError::ExactLength {
                id: 0,
                actual: 2,
                expected: 1
            })
        ));
        assert_eq!(input.as_ref(), b"\0\0\0\0");
    }

    #[test]
    fn oversized_declared_frame_is_rejected_before_allocation() {
        let mut input = BytesMut::from(&u32::MAX.to_be_bytes()[..]);
        assert!(matches!(
            decode_message(&mut input, PeerCodecLimits::default()),
            Err(PeerCodecError::FrameLimit { .. })
        ));
    }
}
