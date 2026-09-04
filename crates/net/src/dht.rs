use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use bytes::Bytes;
use dendrite_core::Sha1Hash;
use dendrite_metainfo::{BencodeLimits, BencodeValue, DecodeError, SpannedValue, decode};
use thiserror::Error;

mod service;

pub use service::{DhtClient, DhtLookup, DhtServiceError};

const MAX_PACKET_BYTES: usize = 65_507;
const MAX_TRANSACTION_BYTES: usize = 16;
const MAX_TOKEN_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NodeId([u8; 20]);

impl NodeId {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeContact {
    pub id: NodeId,
    pub address: SocketAddr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DhtQuery {
    Ping {
        id: NodeId,
    },
    FindNode {
        id: NodeId,
        target: NodeId,
    },
    GetPeers {
        id: NodeId,
        info_hash: Sha1Hash,
    },
    AnnouncePeer {
        id: NodeId,
        info_hash: Sha1Hash,
        port: u16,
        implied_port: bool,
        token: Bytes,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DhtResponse {
    pub id: NodeId,
    pub token: Option<Bytes>,
    pub nodes: Vec<NodeContact>,
    pub peers: Vec<SocketAddr>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DhtMessage {
    Query {
        transaction: Bytes,
        query: DhtQuery,
    },
    Response {
        transaction: Bytes,
        response: DhtResponse,
    },
    Error {
        transaction: Bytes,
        code: i64,
        message: String,
    },
}

#[derive(Debug, Error)]
pub enum DhtCodecError {
    #[error(transparent)]
    Bencode(#[from] DecodeError),
    #[error("DHT field {0} is missing or invalid")]
    Field(&'static str),
    #[error("DHT transaction id must contain 1..={MAX_TRANSACTION_BYTES} bytes")]
    Transaction,
    #[error("DHT token exceeds {MAX_TOKEN_BYTES} bytes")]
    Token,
    #[error("unknown DHT query {0}")]
    UnknownQuery(String),
    #[error("compact DHT node list has invalid length")]
    Nodes,
    #[error("compact DHT peer address has invalid length")]
    Peer,
}

pub fn decode_message(input: &[u8]) -> Result<DhtMessage, DhtCodecError> {
    let root = decode(
        input,
        BencodeLimits {
            input_bytes: MAX_PACKET_BYTES,
            byte_string_bytes: MAX_PACKET_BYTES,
            nodes: 4096,
            collection_items: 2048,
            canonical_dictionaries: false,
            ..BencodeLimits::default()
        },
    )?;
    let transaction = bytes_field(&root.value, b"t", "t")?;
    if transaction.is_empty() || transaction.len() > MAX_TRANSACTION_BYTES {
        return Err(DhtCodecError::Transaction);
    }
    let transaction = Bytes::copy_from_slice(transaction);
    match bytes_field(&root.value, b"y", "y")? {
        b"q" => decode_query(&root.value, transaction),
        b"r" => decode_response(&root.value, transaction),
        b"e" => decode_error(&root.value, transaction),
        _ => Err(DhtCodecError::Field("y")),
    }
}

#[must_use]
pub fn encode_announce_peer_query(
    transaction: &[u8],
    id: NodeId,
    info_hash: Sha1Hash,
    port: u16,
    token: &[u8],
) -> Bytes {
    let mut output = Vec::with_capacity(128 + token.len());
    output.extend_from_slice(b"d1:ad2:id20:");
    output.extend_from_slice(id.as_bytes());
    output.extend_from_slice(b"9:info_hash20:");
    output.extend_from_slice(info_hash.as_bytes());
    output.extend_from_slice(format!("4:porti{port}e5:token{}:", token.len()).as_bytes());
    output.extend_from_slice(token);
    output.extend_from_slice(b"e1:q13:announce_peer1:t");
    output.extend_from_slice(transaction.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(transaction);
    output.extend_from_slice(b"1:y1:qe");
    Bytes::from(output)
}

#[must_use]
pub fn encode_get_peers_query(transaction: &[u8], id: NodeId, info_hash: Sha1Hash) -> Bytes {
    let mut output = Vec::with_capacity(96);
    output.extend_from_slice(b"d1:ad2:id20:");
    output.extend_from_slice(id.as_bytes());
    output.extend_from_slice(b"9:info_hash20:");
    output.extend_from_slice(info_hash.as_bytes());
    output.extend_from_slice(b"e1:q9:get_peers1:t");
    output.extend_from_slice(transaction.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(transaction);
    output.extend_from_slice(b"1:y1:qe");
    Bytes::from(output)
}

fn decode_query(root: &BencodeValue<'_>, transaction: Bytes) -> Result<DhtMessage, DhtCodecError> {
    let method = bytes_field(root, b"q", "q")?;
    let arguments = value_field(root, b"a", "a")?;
    let id = node_id(bytes_field(&arguments.value, b"id", "a.id")?)?;
    let query = match method {
        b"ping" => DhtQuery::Ping { id },
        b"find_node" => DhtQuery::FindNode {
            id,
            target: node_id(bytes_field(&arguments.value, b"target", "a.target")?)?,
        },
        b"get_peers" => DhtQuery::GetPeers {
            id,
            info_hash: info_hash(bytes_field(&arguments.value, b"info_hash", "a.info_hash")?)?,
        },
        b"announce_peer" => {
            let token = bytes_field(&arguments.value, b"token", "a.token")?;
            if token.len() > MAX_TOKEN_BYTES {
                return Err(DhtCodecError::Token);
            }
            let port = integer_field(&arguments.value, b"port", "a.port")?;
            let port = u16::try_from(port).map_err(|_| DhtCodecError::Field("a.port"))?;
            let implied_port = optional_integer(&arguments.value, b"implied_port")?.unwrap_or(0);
            if !matches!(implied_port, 0 | 1) {
                return Err(DhtCodecError::Field("a.implied_port"));
            }
            DhtQuery::AnnouncePeer {
                id,
                info_hash: info_hash(bytes_field(&arguments.value, b"info_hash", "a.info_hash")?)?,
                port,
                implied_port: implied_port == 1,
                token: Bytes::copy_from_slice(token),
            }
        }
        _ => {
            return Err(DhtCodecError::UnknownQuery(
                String::from_utf8_lossy(method).into_owned(),
            ));
        }
    };
    Ok(DhtMessage::Query { transaction, query })
}

fn decode_response(
    root: &BencodeValue<'_>,
    transaction: Bytes,
) -> Result<DhtMessage, DhtCodecError> {
    let response = value_field(root, b"r", "r")?;
    let id = node_id(bytes_field(&response.value, b"id", "r.id")?)?;
    let token = response
        .value
        .dictionary_get(b"token")
        .map(|value| {
            let token = value
                .value
                .as_bytes()
                .ok_or(DhtCodecError::Field("r.token"))?;
            if token.len() > MAX_TOKEN_BYTES {
                return Err(DhtCodecError::Token);
            }
            Ok(Bytes::copy_from_slice(token))
        })
        .transpose()?;
    let mut nodes = Vec::new();
    if let Some(value) = response.value.dictionary_get(b"nodes") {
        nodes.extend(parse_nodes(
            value
                .value
                .as_bytes()
                .ok_or(DhtCodecError::Field("r.nodes"))?,
            false,
        )?);
    }
    if let Some(value) = response.value.dictionary_get(b"nodes6") {
        nodes.extend(parse_nodes(
            value
                .value
                .as_bytes()
                .ok_or(DhtCodecError::Field("r.nodes6"))?,
            true,
        )?);
    }
    let peers = response
        .value
        .dictionary_get(b"values")
        .map(parse_peer_values)
        .transpose()?
        .unwrap_or_default();
    Ok(DhtMessage::Response {
        transaction,
        response: DhtResponse {
            id,
            token,
            nodes,
            peers,
        },
    })
}

fn decode_error(root: &BencodeValue<'_>, transaction: Bytes) -> Result<DhtMessage, DhtCodecError> {
    let error = value_field(root, b"e", "e")?;
    let BencodeValue::List(values) = &error.value else {
        return Err(DhtCodecError::Field("e"));
    };
    if values.len() != 2 {
        return Err(DhtCodecError::Field("e"));
    }
    let code = values[0]
        .value
        .as_integer()
        .ok_or(DhtCodecError::Field("e[0]"))?;
    let message = values[1]
        .value
        .as_bytes()
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .ok_or(DhtCodecError::Field("e[1]"))?;
    Ok(DhtMessage::Error {
        transaction,
        code,
        message,
    })
}

fn parse_nodes(input: &[u8], ipv6: bool) -> Result<Vec<NodeContact>, DhtCodecError> {
    let record_bytes = if ipv6 { 38 } else { 26 };
    let chunks = input.chunks_exact(record_bytes);
    if !chunks.remainder().is_empty() {
        return Err(DhtCodecError::Nodes);
    }
    chunks
        .map(|chunk| {
            let id = node_id(&chunk[..20])?;
            let address = if ipv6 {
                let ip: [u8; 16] = chunk[20..36].try_into().map_err(|_| DhtCodecError::Nodes)?;
                SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(ip),
                    u16::from_be_bytes([chunk[36], chunk[37]]),
                    0,
                    0,
                ))
            } else {
                SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]),
                    u16::from_be_bytes([chunk[24], chunk[25]]),
                ))
            };
            Ok(NodeContact { id, address })
        })
        .collect()
}

fn parse_peer_values(value: &SpannedValue<'_>) -> Result<Vec<SocketAddr>, DhtCodecError> {
    let BencodeValue::List(values) = &value.value else {
        return Err(DhtCodecError::Field("r.values"));
    };
    values
        .iter()
        .map(|value| {
            let compact = value
                .value
                .as_bytes()
                .ok_or(DhtCodecError::Field("r.values[]"))?;
            match compact {
                [a, b, c, d, high, low] => Ok(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(*a, *b, *c, *d),
                    u16::from_be_bytes([*high, *low]),
                ))),
                bytes if bytes.len() == 18 => {
                    let ip: [u8; 16] = bytes[..16].try_into().map_err(|_| DhtCodecError::Peer)?;
                    Ok(SocketAddr::V6(SocketAddrV6::new(
                        Ipv6Addr::from(ip),
                        u16::from_be_bytes([bytes[16], bytes[17]]),
                        0,
                        0,
                    )))
                }
                _ => Err(DhtCodecError::Peer),
            }
        })
        .collect()
}

fn value_field<'a>(
    root: &'a BencodeValue<'a>,
    key: &[u8],
    field: &'static str,
) -> Result<&'a SpannedValue<'a>, DhtCodecError> {
    root.dictionary_get(key).ok_or(DhtCodecError::Field(field))
}

fn bytes_field<'a>(
    root: &'a BencodeValue<'a>,
    key: &[u8],
    field: &'static str,
) -> Result<&'a [u8], DhtCodecError> {
    value_field(root, key, field)?
        .value
        .as_bytes()
        .ok_or(DhtCodecError::Field(field))
}

fn integer_field(
    root: &BencodeValue<'_>,
    key: &[u8],
    field: &'static str,
) -> Result<i64, DhtCodecError> {
    root.dictionary_get(key)
        .and_then(|value| value.value.as_integer())
        .ok_or(DhtCodecError::Field(field))
}

fn optional_integer(root: &BencodeValue<'_>, key: &[u8]) -> Result<Option<i64>, DhtCodecError> {
    root.dictionary_get(key)
        .map(|value| {
            value
                .value
                .as_integer()
                .ok_or(DhtCodecError::Field("integer"))
        })
        .transpose()
}

fn node_id(value: &[u8]) -> Result<NodeId, DhtCodecError> {
    let bytes: [u8; 20] = value
        .try_into()
        .map_err(|_| DhtCodecError::Field("node id"))?;
    Ok(NodeId::from_bytes(bytes))
}

fn info_hash(value: &[u8]) -> Result<Sha1Hash, DhtCodecError> {
    let bytes: [u8; 20] = value
        .try_into()
        .map_err(|_| DhtCodecError::Field("info hash"))?;
    Ok(Sha1Hash::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_ping_query() -> Result<(), DhtCodecError> {
        let input = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";
        assert_eq!(
            decode_message(input)?,
            DhtMessage::Query {
                transaction: Bytes::from_static(b"aa"),
                query: DhtQuery::Ping {
                    id: NodeId::from_bytes(*b"abcdefghij0123456789"),
                },
            }
        );
        Ok(())
    }

    #[test]
    fn rejects_partial_node_records() {
        let input = b"d1:rd2:id20:abcdefghij01234567895:nodes1:xe1:t2:aa1:y1:re";
        assert!(matches!(decode_message(input), Err(DhtCodecError::Nodes)));
    }

    #[test]
    fn transaction_ids_are_bounded() {
        let input = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t17:abcdefghijklmnopq1:y1:qe";
        assert!(matches!(
            decode_message(input),
            Err(DhtCodecError::Transaction)
        ));
    }

    #[test]
    fn encoded_get_peers_query_round_trips() -> Result<(), DhtCodecError> {
        let id = NodeId::from_bytes([3; 20]);
        let hash = Sha1Hash::from_bytes([4; 20]);
        assert_eq!(
            decode_message(&encode_get_peers_query(b"abcd", id, hash))?,
            DhtMessage::Query {
                transaction: Bytes::from_static(b"abcd"),
                query: DhtQuery::GetPeers {
                    id,
                    info_hash: hash,
                },
            }
        );
        Ok(())
    }
}
