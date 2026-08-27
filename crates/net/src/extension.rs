//! Bounded BEP 10 extension-handshake and BEP 9 metadata codecs.

use std::{
    collections::HashSet,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

use bytes::{BufMut as _, Bytes, BytesMut};
use dendrite_metainfo::{BencodeLimits, BencodeValue, DecodeError, decode, decode_prefix};
use thiserror::Error;

pub const LOCAL_METADATA_EXTENSION_ID: u8 = 1;
pub const LOCAL_PEX_EXTENSION_ID: u8 = 2;
pub const LOCAL_HOLEPUNCH_EXTENSION_ID: u8 = 3;
pub const METADATA_BLOCK_BYTES: usize = 16 * 1024;
pub const PEX_PEER_LIMIT: usize = 50;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtensionHandshake {
    pub metadata_extension_id: Option<u8>,
    pub metadata_size: Option<usize>,
    pub pex_extension_id: Option<u8>,
    pub holepunch_extension_id: Option<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HolePunchKind {
    Rendezvous,
    Connect,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HolePunchMessage {
    pub kind: HolePunchKind,
    pub address: SocketAddr,
    pub error_code: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PexPeer {
    pub address: SocketAddr,
    pub flags: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PexMessage {
    pub added: Vec<PexPeer>,
    pub dropped: Vec<SocketAddr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataMessage {
    Request {
        piece: u32,
    },
    Data {
        piece: u32,
        total_size: usize,
        block: Bytes,
    },
    Reject {
        piece: u32,
    },
}

#[derive(Debug, Error)]
pub enum ExtensionCodecError {
    #[error(transparent)]
    Bencode(#[from] DecodeError),
    #[error("extension field {0} is missing or invalid")]
    Field(&'static str),
    #[error("metadata size {actual} exceeds configured limit {maximum}")]
    MetadataLimit { actual: usize, maximum: usize },
    #[error("metadata block has {actual} bytes; maximum is {maximum}")]
    BlockLimit { actual: usize, maximum: usize },
    #[error("unknown metadata message type {0}")]
    MessageType(i64),
    #[error("PEX message has no added or dropped peers")]
    EmptyPex,
    #[error("PEX field {field} has invalid length {actual}")]
    PexLength { field: &'static str, actual: usize },
    #[error("PEX message exceeds {PEX_PEER_LIMIT} peers per update")]
    PexLimit,
    #[error("PEX message contains duplicate peers")]
    DuplicatePexPeer,
    #[error("hole-punch message is malformed")]
    HolePunch,
}

#[must_use]
pub fn encode_extension_handshake(metadata_size: Option<usize>) -> Bytes {
    let mut encoded = BytesMut::from(&b"d1:md12:ut_holepunchi3e11:ut_metadatai1e6:ut_pexi2eee"[..]);
    if let Some(size) = metadata_size {
        encoded.truncate(encoded.len() - 1);
        encoded.extend_from_slice(format!("13:metadata_sizei{size}ee").as_bytes());
    }
    encoded.freeze()
}

pub fn decode_extension_handshake(
    input: &[u8],
    metadata_limit: usize,
) -> Result<ExtensionHandshake, ExtensionCodecError> {
    let root = decode(input, limits(input.len()))?;
    let mappings = dictionary_field(&root.value, b"m").ok_or(ExtensionCodecError::Field("m"))?;
    let metadata_extension_id = mappings
        .dictionary_get(b"ut_metadata")
        .map(|value| {
            value
                .value
                .as_integer()
                .and_then(|value| u8::try_from(value).ok())
                .filter(|value| *value != 0)
                .ok_or(ExtensionCodecError::Field("m.ut_metadata"))
        })
        .transpose()?;
    let metadata_size = root
        .value
        .dictionary_get(b"metadata_size")
        .map(|value| {
            value
                .value
                .as_integer()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(ExtensionCodecError::Field("metadata_size"))
        })
        .transpose()?;
    if let Some(actual) = metadata_size
        && actual > metadata_limit
    {
        return Err(ExtensionCodecError::MetadataLimit {
            actual,
            maximum: metadata_limit,
        });
    }
    let pex_extension_id = mappings
        .dictionary_get(b"ut_pex")
        .map(|value| {
            value
                .value
                .as_integer()
                .and_then(|value| u8::try_from(value).ok())
                .filter(|value| *value != 0)
                .ok_or(ExtensionCodecError::Field("m.ut_pex"))
        })
        .transpose()?;
    let holepunch_extension_id = mappings
        .dictionary_get(b"ut_holepunch")
        .map(|value| {
            value
                .value
                .as_integer()
                .and_then(|value| u8::try_from(value).ok())
                .filter(|value| *value != 0)
                .ok_or(ExtensionCodecError::Field("m.ut_holepunch"))
        })
        .transpose()?;
    Ok(ExtensionHandshake {
        metadata_extension_id,
        metadata_size,
        pex_extension_id,
        holepunch_extension_id,
    })
}

#[must_use]
pub fn encode_metadata_request(piece: u32) -> Bytes {
    Bytes::from(format!("d8:msg_typei0e5:piecei{piece}ee"))
}

#[must_use]
pub fn encode_metadata_reject(piece: u32) -> Bytes {
    Bytes::from(format!("d8:msg_typei2e5:piecei{piece}ee"))
}

#[must_use]
pub fn encode_metadata_data(piece: u32, total_size: usize, block: &[u8]) -> Bytes {
    let mut encoded = BytesMut::from(
        format!("d8:msg_typei1e5:piecei{piece}e10:total_sizei{total_size}ee").as_bytes(),
    );
    encoded.extend_from_slice(block);
    encoded.freeze()
}

pub fn decode_metadata_message(
    input: &[u8],
    metadata_limit: usize,
) -> Result<MetadataMessage, ExtensionCodecError> {
    let (header, consumed) = decode_prefix(input, limits(input.len()))?;
    let kind = integer_field(&header.value, b"msg_type", "msg_type")?;
    let piece = integer_field(&header.value, b"piece", "piece")?;
    let piece = u32::try_from(piece).map_err(|_| ExtensionCodecError::Field("piece"))?;
    match kind {
        0 if consumed == input.len() => Ok(MetadataMessage::Request { piece }),
        1 => {
            let total_size = integer_field(&header.value, b"total_size", "total_size")?;
            let total_size = usize::try_from(total_size)
                .map_err(|_| ExtensionCodecError::Field("total_size"))?;
            if total_size > metadata_limit {
                return Err(ExtensionCodecError::MetadataLimit {
                    actual: total_size,
                    maximum: metadata_limit,
                });
            }
            let block = &input[consumed..];
            if block.len() > METADATA_BLOCK_BYTES {
                return Err(ExtensionCodecError::BlockLimit {
                    actual: block.len(),
                    maximum: METADATA_BLOCK_BYTES,
                });
            }
            Ok(MetadataMessage::Data {
                piece,
                total_size,
                block: Bytes::copy_from_slice(block),
            })
        }
        2 if consumed == input.len() => Ok(MetadataMessage::Reject { piece }),
        0 | 2 => Err(ExtensionCodecError::Field("trailing metadata payload")),
        other => Err(ExtensionCodecError::MessageType(other)),
    }
}

#[must_use]
pub fn encode_pex_message(message: &PexMessage) -> Bytes {
    let mut added4 = Vec::new();
    let mut flags4 = Vec::new();
    let mut added6 = Vec::new();
    let mut flags6 = Vec::new();
    for peer in &message.added {
        match peer.address {
            SocketAddr::V4(address) => {
                added4.extend_from_slice(&address.ip().octets());
                added4.extend_from_slice(&address.port().to_be_bytes());
                flags4.push(peer.flags);
            }
            SocketAddr::V6(address) => {
                added6.extend_from_slice(&address.ip().octets());
                added6.extend_from_slice(&address.port().to_be_bytes());
                flags6.push(peer.flags);
            }
        }
    }
    let mut dropped4 = Vec::new();
    let mut dropped6 = Vec::new();
    for peer in &message.dropped {
        match peer {
            SocketAddr::V4(address) => {
                dropped4.extend_from_slice(&address.ip().octets());
                dropped4.extend_from_slice(&address.port().to_be_bytes());
            }
            SocketAddr::V6(address) => {
                dropped6.extend_from_slice(&address.ip().octets());
                dropped6.extend_from_slice(&address.port().to_be_bytes());
            }
        }
    }
    let mut encoded = BytesMut::from(&b"d"[..]);
    append_bytes_field(&mut encoded, b"added", &added4);
    if !added4.is_empty() {
        append_bytes_field(&mut encoded, b"added.f", &flags4);
    }
    append_bytes_field(&mut encoded, b"added6", &added6);
    if !added6.is_empty() {
        append_bytes_field(&mut encoded, b"added6.f", &flags6);
    }
    append_bytes_field(&mut encoded, b"dropped", &dropped4);
    append_bytes_field(&mut encoded, b"dropped6", &dropped6);
    encoded.put_u8(b'e');
    encoded.freeze()
}

pub fn decode_pex_message(input: &[u8]) -> Result<PexMessage, ExtensionCodecError> {
    let root = decode(input, limits(input.len()))?;
    let BencodeValue::Dictionary(dictionary) = &root.value else {
        return Err(ExtensionCodecError::Field("PEX dictionary"));
    };
    let added4 = optional_bytes(dictionary, b"added");
    let added6 = optional_bytes(dictionary, b"added6");
    let dropped4 = optional_bytes(dictionary, b"dropped");
    let dropped6 = optional_bytes(dictionary, b"dropped6");
    if added4.is_none() && added6.is_none() && dropped4.is_none() && dropped6.is_none() {
        return Err(ExtensionCodecError::EmptyPex);
    }
    let mut added = parse_added_v4(
        added4.unwrap_or_default(),
        optional_bytes(dictionary, b"added.f"),
    )?;
    added.extend(parse_added_v6(
        added6.unwrap_or_default(),
        optional_bytes(dictionary, b"added6.f"),
    )?);
    let mut dropped = parse_compact_v4(dropped4.unwrap_or_default(), "dropped")?;
    dropped.extend(parse_compact_v6(dropped6.unwrap_or_default(), "dropped6")?);
    if added.len() > PEX_PEER_LIMIT || dropped.len() > PEX_PEER_LIMIT {
        return Err(ExtensionCodecError::PexLimit);
    }
    let mut unique = HashSet::with_capacity(added.len().saturating_add(dropped.len()));
    if added
        .iter()
        .map(|peer| peer.address)
        .chain(dropped.iter().copied())
        .any(|address| !unique.insert(address))
    {
        return Err(ExtensionCodecError::DuplicatePexPeer);
    }
    Ok(PexMessage { added, dropped })
}

pub fn encode_holepunch_message(message: HolePunchMessage) -> Result<Bytes, ExtensionCodecError> {
    validate_holepunch(message)?;
    let mut output = BytesMut::with_capacity(if message.address.is_ipv4() { 12 } else { 24 });
    output.put_u8(match message.kind {
        HolePunchKind::Rendezvous => 0,
        HolePunchKind::Connect => 1,
        HolePunchKind::Error => 2,
    });
    match message.address {
        SocketAddr::V4(address) => {
            output.put_u8(0);
            output.extend_from_slice(&address.ip().octets());
            output.put_u16(address.port());
        }
        SocketAddr::V6(address) => {
            output.put_u8(1);
            output.extend_from_slice(&address.ip().octets());
            output.put_u16(address.port());
        }
    }
    output.put_u32(message.error_code);
    Ok(output.freeze())
}

pub fn decode_holepunch_message(input: &[u8]) -> Result<HolePunchMessage, ExtensionCodecError> {
    let kind = match input.first() {
        Some(0) => HolePunchKind::Rendezvous,
        Some(1) => HolePunchKind::Connect,
        Some(2) => HolePunchKind::Error,
        _ => return Err(ExtensionCodecError::HolePunch),
    };
    let (address, error_offset) = match input.get(1) {
        Some(0) if input.len() == 12 => (
            SocketAddr::from((
                Ipv4Addr::new(input[2], input[3], input[4], input[5]),
                u16::from_be_bytes([input[6], input[7]]),
            )),
            8,
        ),
        Some(1) if input.len() == 24 => {
            let octets: [u8; 16] = input[2..18]
                .try_into()
                .map_err(|_| ExtensionCodecError::HolePunch)?;
            (
                SocketAddr::from((
                    Ipv6Addr::from(octets),
                    u16::from_be_bytes([input[18], input[19]]),
                )),
                20,
            )
        }
        _ => return Err(ExtensionCodecError::HolePunch),
    };
    let error_code = u32::from_be_bytes(
        input[error_offset..error_offset + 4]
            .try_into()
            .map_err(|_| ExtensionCodecError::HolePunch)?,
    );
    let message = HolePunchMessage {
        kind,
        address,
        error_code,
    };
    validate_holepunch(message)?;
    Ok(message)
}

fn validate_holepunch(message: HolePunchMessage) -> Result<(), ExtensionCodecError> {
    let error_valid = match message.kind {
        HolePunchKind::Rendezvous | HolePunchKind::Connect => message.error_code == 0,
        HolePunchKind::Error => (1..=4).contains(&message.error_code),
    };
    if message.address.port() == 0 || message.address.ip().is_unspecified() || !error_valid {
        return Err(ExtensionCodecError::HolePunch);
    }
    Ok(())
}

fn append_bytes_field(output: &mut BytesMut, key: &[u8], value: &[u8]) {
    if value.is_empty() {
        return;
    }
    output.extend_from_slice(format!("{}:", key.len()).as_bytes());
    output.extend_from_slice(key);
    output.extend_from_slice(format!("{}:", value.len()).as_bytes());
    output.extend_from_slice(value);
}

fn optional_bytes<'a>(
    dictionary: &'a [(&'a [u8], dendrite_metainfo::SpannedValue<'a>)],
    key: &[u8],
) -> Option<&'a [u8]> {
    dictionary
        .iter()
        .find_map(|(candidate, value)| (*candidate == key).then_some(&value.value))
        .and_then(BencodeValue::as_bytes)
}

fn parse_added_v4(bytes: &[u8], flags: Option<&[u8]>) -> Result<Vec<PexPeer>, ExtensionCodecError> {
    let peers = parse_compact_v4(bytes, "added")?;
    add_flags(peers, flags, "added.f")
}

fn parse_added_v6(bytes: &[u8], flags: Option<&[u8]>) -> Result<Vec<PexPeer>, ExtensionCodecError> {
    let peers = parse_compact_v6(bytes, "added6")?;
    add_flags(peers, flags, "added6.f")
}

fn add_flags(
    peers: Vec<SocketAddr>,
    flags: Option<&[u8]>,
    field: &'static str,
) -> Result<Vec<PexPeer>, ExtensionCodecError> {
    let flags = flags.unwrap_or(&[]);
    if !flags.is_empty() && flags.len() != peers.len() {
        return Err(ExtensionCodecError::PexLength {
            field,
            actual: flags.len(),
        });
    }
    Ok(peers
        .into_iter()
        .enumerate()
        .map(|(index, address)| PexPeer {
            address,
            flags: flags.get(index).copied().unwrap_or(0),
        })
        .collect())
}

fn parse_compact_v4(
    bytes: &[u8],
    field: &'static str,
) -> Result<Vec<SocketAddr>, ExtensionCodecError> {
    if !bytes.len().is_multiple_of(6) {
        return Err(ExtensionCodecError::PexLength {
            field,
            actual: bytes.len(),
        });
    }
    Ok(bytes
        .chunks_exact(6)
        .map(|peer| {
            SocketAddr::from((
                Ipv4Addr::new(peer[0], peer[1], peer[2], peer[3]),
                u16::from_be_bytes([peer[4], peer[5]]),
            ))
        })
        .collect())
}

fn parse_compact_v6(
    bytes: &[u8],
    field: &'static str,
) -> Result<Vec<SocketAddr>, ExtensionCodecError> {
    if !bytes.len().is_multiple_of(18) {
        return Err(ExtensionCodecError::PexLength {
            field,
            actual: bytes.len(),
        });
    }
    bytes
        .chunks_exact(18)
        .map(|peer| {
            let octets: [u8; 16] =
                peer[..16]
                    .try_into()
                    .map_err(|_| ExtensionCodecError::PexLength {
                        field,
                        actual: peer.len(),
                    })?;
            Ok(SocketAddr::from((
                Ipv6Addr::from(octets),
                u16::from_be_bytes([peer[16], peer[17]]),
            )))
        })
        .collect()
}

fn limits(input_bytes: usize) -> BencodeLimits {
    BencodeLimits {
        input_bytes,
        byte_string_bytes: input_bytes,
        depth: 8,
        nodes: 64,
        collection_items: 32,
        canonical_dictionaries: false,
    }
}

fn dictionary_field<'a>(root: &'a BencodeValue<'a>, key: &[u8]) -> Option<&'a BencodeValue<'a>> {
    root.dictionary_get(key).map(|value| &value.value)
}

fn integer_field(
    root: &BencodeValue<'_>,
    key: &[u8],
    field: &'static str,
) -> Result<i64, ExtensionCodecError> {
    root.dictionary_get(key)
        .and_then(|value| value.value.as_integer())
        .ok_or(ExtensionCodecError::Field(field))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_handshake_round_trips() -> Result<(), ExtensionCodecError> {
        let encoded = encode_extension_handshake(Some(32_768));
        assert_eq!(
            decode_extension_handshake(&encoded, 64 * 1024)?,
            ExtensionHandshake {
                metadata_extension_id: Some(LOCAL_METADATA_EXTENSION_ID),
                metadata_size: Some(32_768),
                pex_extension_id: Some(LOCAL_PEX_EXTENSION_ID),
                holepunch_extension_id: Some(LOCAL_HOLEPUNCH_EXTENSION_ID),
            }
        );
        Ok(())
    }

    #[test]
    fn metadata_data_preserves_opaque_block() -> Result<(), ExtensionCodecError> {
        let block = [0, 255, b'e', b'd'];
        let encoded = encode_metadata_data(2, 40_000, &block);
        assert_eq!(
            decode_metadata_message(&encoded, 64 * 1024)?,
            MetadataMessage::Data {
                piece: 2,
                total_size: 40_000,
                block: Bytes::copy_from_slice(&block),
            }
        );
        Ok(())
    }

    #[test]
    fn metadata_limits_are_enforced() {
        let encoded = encode_extension_handshake(Some(65_537));
        assert!(matches!(
            decode_extension_handshake(&encoded, 65_536),
            Err(ExtensionCodecError::MetadataLimit { .. })
        ));
    }

    #[test]
    fn pex_round_trips_ipv4_ipv6_flags_and_drops() -> Result<(), Box<dyn std::error::Error>> {
        let message = PexMessage {
            added: vec![
                PexPeer {
                    address: "192.0.2.1:6881".parse()?,
                    flags: 0x14,
                },
                PexPeer {
                    address: "[2001:db8::1]:6882".parse()?,
                    flags: 0x04,
                },
            ],
            dropped: vec!["198.51.100.2:6883".parse()?],
        };
        assert_eq!(decode_pex_message(&encode_pex_message(&message))?, message);
        Ok(())
    }

    #[test]
    fn pex_rejects_duplicates_and_misaligned_contacts() {
        assert!(matches!(
            decode_pex_message(b"d5:added5:abcdee"),
            Err(ExtensionCodecError::PexLength { .. })
        ));
        let duplicate = b"d5:added12:\x7f\x00\x00\x01\x1a\xe1\x7f\x00\x00\x01\x1a\xe1e";
        assert!(matches!(
            decode_pex_message(duplicate),
            Err(ExtensionCodecError::DuplicatePexPeer)
        ));
    }

    #[test]
    fn pex_rejects_peer_floods() {
        let mut added = Vec::with_capacity((PEX_PEER_LIMIT + 1) * 6);
        for index in 0..=PEX_PEER_LIMIT {
            added.extend_from_slice(&[192, 0, 2, u8::try_from(index + 1).unwrap_or(u8::MAX)]);
            added.extend_from_slice(&6881_u16.to_be_bytes());
        }
        let mut encoded = format!("d5:added{}:", added.len()).into_bytes();
        encoded.extend_from_slice(&added);
        encoded.push(b'e');
        assert!(matches!(
            decode_pex_message(&encoded),
            Err(ExtensionCodecError::PexLimit)
        ));
    }

    #[test]
    fn holepunch_messages_round_trip_and_reject_invalid_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        for message in [
            HolePunchMessage {
                kind: HolePunchKind::Rendezvous,
                address: "192.0.2.2:49001".parse()?,
                error_code: 0,
            },
            HolePunchMessage {
                kind: HolePunchKind::Connect,
                address: "[2001:db8::2]:49002".parse()?,
                error_code: 0,
            },
            HolePunchMessage {
                kind: HolePunchKind::Error,
                address: "198.51.100.4:49003".parse()?,
                error_code: 2,
            },
        ] {
            assert_eq!(
                decode_holepunch_message(&encode_holepunch_message(message)?)?,
                message
            );
        }
        assert!(
            encode_holepunch_message(HolePunchMessage {
                kind: HolePunchKind::Connect,
                address: "192.0.2.2:49001".parse()?,
                error_code: 1,
            })
            .is_err()
        );
        Ok(())
    }
}
