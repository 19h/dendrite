use std::{
    collections::HashSet,
    fmt::Write as _,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use dendrite_core::Sha1Hash;
use dendrite_metainfo::{BencodeLimits, BencodeValue, DecodeError, decode};
use futures_util::StreamExt as _;
use percent_encoding::{AsciiSet, CONTROLS, percent_encode};
use reqwest::{Client, header};
use thiserror::Error;
use url::Url;

use crate::{
    peer::PeerId,
    tracker::{TrackerCodecError, parse_compact_ipv4, parse_compact_ipv6},
};

const TRACKER_ENCODE: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'=')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b'~');
const MAX_TRACKER_PEERS: usize = 2_048;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnnounceEvent {
    Started,
    Stopped,
    Completed,
    None,
}

#[derive(Clone, Copy, Debug)]
pub struct TrackerRequest {
    pub info_hash: Sha1Hash,
    pub peer_id: PeerId,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: AnnounceEvent,
    pub numwant: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrackerAnnounce {
    pub interval: Duration,
    pub minimum_interval: Option<Duration>,
    pub complete: Option<u32>,
    pub incomplete: Option<u32>,
    pub warning: Option<String>,
    pub peers: Vec<SocketAddr>,
}

#[derive(Clone, Debug)]
pub struct HttpTrackerClient {
    client: Client,
    response_limit: usize,
}

#[derive(Debug, Error)]
pub enum TrackerServiceError {
    #[error("tracker URL must use HTTP or HTTPS")]
    Scheme,
    #[error("tracker URL is invalid: {0}")]
    Url(#[from] url::ParseError),
    #[error("tracker request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("tracker returned HTTP {0}")]
    Status(reqwest::StatusCode),
    #[error("tracker response exceeds {maximum} bytes")]
    ResponseLimit { maximum: usize },
    #[error("tracker bencode is invalid: {0}")]
    Bencode(#[from] DecodeError),
    #[error("tracker failure: {0}")]
    Failure(String),
    #[error("tracker field {0} is missing or invalid")]
    Field(&'static str),
    #[error("tracker returned more than {maximum} unique peers")]
    PeerLimit { maximum: usize },
    #[error("failed to construct tracker URL")]
    UrlEncoding,
    #[error(transparent)]
    CompactPeers(#[from] TrackerCodecError),
}

impl HttpTrackerClient {
    pub fn new(response_limit: usize) -> Result<Self, TrackerServiceError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::limited(3))
            .user_agent(concat!("dendrite/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            response_limit,
        })
    }

    pub async fn announce(
        &self,
        tracker: &Url,
        request: TrackerRequest,
    ) -> Result<TrackerAnnounce, TrackerServiceError> {
        if !matches!(tracker.scheme(), "http" | "https") {
            return Err(TrackerServiceError::Scheme);
        }
        let url = announce_url(tracker, request)?;
        let response = self
            .client
            .get(url)
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(TrackerServiceError::Status(response.status()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.response_limit as u64)
        {
            return Err(TrackerServiceError::ResponseLimit {
                maximum: self.response_limit,
            });
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if body.len().saturating_add(chunk.len()) > self.response_limit {
                return Err(TrackerServiceError::ResponseLimit {
                    maximum: self.response_limit,
                });
            }
            body.extend_from_slice(&chunk);
        }
        parse_announce(&body, self.response_limit)
    }
}

fn announce_url(base: &Url, request: TrackerRequest) -> Result<Url, TrackerServiceError> {
    let mut value = base.as_str().to_owned();
    value.push(if base.query().is_some() { '&' } else { '?' });
    let info_hash = percent_encode(request.info_hash.as_bytes(), TRACKER_ENCODE);
    let peer_id = percent_encode(request.peer_id.as_bytes(), TRACKER_ENCODE);
    write!(
        value,
        "info_hash={info_hash}&peer_id={peer_id}&port={}&uploaded={}&downloaded={}&left={}&compact=1&numwant={}",
        request.port, request.uploaded, request.downloaded, request.left, request.numwant
    )
    .map_err(|_| TrackerServiceError::UrlEncoding)?;
    match request.event {
        AnnounceEvent::Started => value.push_str("&event=started"),
        AnnounceEvent::Stopped => value.push_str("&event=stopped"),
        AnnounceEvent::Completed => value.push_str("&event=completed"),
        AnnounceEvent::None => {}
    }
    Url::parse(&value).map_err(TrackerServiceError::from)
}

fn parse_announce(input: &[u8], limit: usize) -> Result<TrackerAnnounce, TrackerServiceError> {
    let root = decode(
        input,
        BencodeLimits {
            input_bytes: limit,
            byte_string_bytes: limit,
            canonical_dictionaries: false,
            ..BencodeLimits::default()
        },
    )?;
    if let Some(failure) = root
        .value
        .dictionary_get(b"failure reason")
        .and_then(|value| value.value.as_bytes())
    {
        return Err(TrackerServiceError::Failure(
            String::from_utf8_lossy(failure).into_owned(),
        ));
    }
    let interval = integer_field(&root.value, b"interval")?;
    let interval = u64::try_from(interval).map_err(|_| TrackerServiceError::Field("interval"))?;
    let minimum_interval = optional_integer_field(&root.value, b"min interval")?
        .map(|value| u64::try_from(value).map(Duration::from_secs))
        .transpose()
        .map_err(|_| TrackerServiceError::Field("min interval"))?;
    let mut peers = parse_peers_field(&root.value, b"peers", false)?;
    peers.extend(parse_peers_field(&root.value, b"peers6", true)?);
    let mut unique = HashSet::with_capacity(peers.len().min(MAX_TRACKER_PEERS));
    peers.retain(|address| usable_peer_address(*address) && unique.insert(*address));
    if peers.len() > MAX_TRACKER_PEERS {
        return Err(TrackerServiceError::PeerLimit {
            maximum: MAX_TRACKER_PEERS,
        });
    }
    Ok(TrackerAnnounce {
        interval: Duration::from_secs(interval),
        minimum_interval,
        complete: optional_u32_field(&root.value, b"complete")?,
        incomplete: optional_u32_field(&root.value, b"incomplete")?,
        warning: root
            .value
            .dictionary_get(b"warning message")
            .and_then(|value| value.value.as_bytes())
            .map(|value| String::from_utf8_lossy(value).into_owned()),
        peers,
    })
}

fn usable_peer_address(address: SocketAddr) -> bool {
    address.port() != 0 && !address.ip().is_unspecified() && !address.ip().is_multicast()
}

fn parse_peers_field(
    root: &BencodeValue<'_>,
    key: &[u8],
    ipv6: bool,
) -> Result<Vec<SocketAddr>, TrackerServiceError> {
    let Some(value) = root.dictionary_get(key) else {
        return Ok(Vec::new());
    };
    match &value.value {
        BencodeValue::Bytes(bytes) if ipv6 => Ok(parse_compact_ipv6(bytes)?),
        BencodeValue::Bytes(bytes) => Ok(parse_compact_ipv4(bytes)?),
        BencodeValue::List(entries) => entries
            .iter()
            .map(|entry| {
                let ip = entry
                    .value
                    .dictionary_get(b"ip")
                    .and_then(|value| value.value.as_bytes())
                    .and_then(|value| std::str::from_utf8(value).ok())
                    .and_then(|value| value.parse::<IpAddr>().ok())
                    .ok_or(TrackerServiceError::Field("peers[].ip"))?;
                let port = integer_field(&entry.value, b"port")?;
                let port =
                    u16::try_from(port).map_err(|_| TrackerServiceError::Field("peers[].port"))?;
                Ok(SocketAddr::new(ip, port))
            })
            .collect(),
        _ => Err(TrackerServiceError::Field("peers")),
    }
}

fn integer_field(root: &BencodeValue<'_>, key: &[u8]) -> Result<i64, TrackerServiceError> {
    root.dictionary_get(key)
        .and_then(|value| value.value.as_integer())
        .ok_or(TrackerServiceError::Field("integer"))
}

fn optional_integer_field(
    root: &BencodeValue<'_>,
    key: &[u8],
) -> Result<Option<i64>, TrackerServiceError> {
    root.dictionary_get(key)
        .map(|value| {
            value
                .value
                .as_integer()
                .ok_or(TrackerServiceError::Field("integer"))
        })
        .transpose()
}

fn optional_u32_field(
    root: &BencodeValue<'_>,
    key: &[u8],
) -> Result<Option<u32>, TrackerServiceError> {
    optional_integer_field(root, key)?
        .map(|value| u32::try_from(value).map_err(|_| TrackerServiceError::Field("integer")))
        .transpose()
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };

    use super::*;

    #[test]
    fn parses_bounded_compact_response() -> Result<(), TrackerServiceError> {
        let response =
            b"d8:completei3e10:incompletei2e8:intervali60e5:peers6:\x7f\x00\x00\x01\x1a\xe1e";
        let announce = parse_announce(response, 1024)?;
        assert_eq!(announce.interval, Duration::from_mins(1));
        assert_eq!(announce.peers, [SocketAddr::from(([127, 0, 0, 1], 6881))]);
        Ok(())
    }

    #[test]
    fn combines_ipv4_and_ipv6_peers_without_family_bias() -> Result<(), TrackerServiceError> {
        let response = b"d8:intervali60e5:peers6:\xc0\x00\x02\x01\x1a\xe16:peers618:\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x1a\xe2e";
        let announce = parse_announce(response, response.len())?;
        assert_eq!(
            announce.peers,
            [
                SocketAddr::from(([192, 0, 2, 1], 6881)),
                "[2001:db8::1]:6882"
                    .parse()
                    .map_err(|_| { TrackerServiceError::Field("test IPv6 peer") })?,
            ]
        );
        Ok(())
    }

    #[test]
    fn surfaces_tracker_failures() {
        let response = b"d14:failure reason4:nopee";
        assert!(matches!(
            parse_announce(response, 1024),
            Err(TrackerServiceError::Failure(reason)) if reason == "nope"
        ));
    }

    #[test]
    fn deduplicates_peers_and_discards_unusable_endpoints() -> Result<(), TrackerServiceError> {
        let response = b"d8:intervali60e5:peers24:\x7f\x00\x00\x01\x1a\xe1\x7f\x00\x00\x01\x1a\xe1\x00\x00\x00\x00\x1a\xe1\x7f\x00\x00\x01\x00\x00e";
        let announce = parse_announce(response, response.len())?;
        assert_eq!(announce.peers, [SocketAddr::from(([127, 0, 0, 1], 6881))]);
        Ok(())
    }

    #[test]
    fn rejects_unique_peer_floods_above_the_hard_limit() {
        let mut compact = Vec::with_capacity((MAX_TRACKER_PEERS + 1) * 6);
        for index in 0..=MAX_TRACKER_PEERS {
            compact.extend_from_slice(&[
                10,
                u8::try_from(index >> 8).unwrap_or(u8::MAX),
                u8::try_from(index & 0xff).unwrap_or(u8::MAX),
                1,
            ]);
            let port = u16::try_from(index + 1).unwrap_or(u16::MAX);
            compact.extend_from_slice(&port.to_be_bytes());
        }
        let mut response = format!("d8:intervali60e5:peers{}:", compact.len()).into_bytes();
        response.extend_from_slice(&compact);
        response.push(b'e');
        assert!(matches!(
            parse_announce(&response, response.len()),
            Err(TrackerServiceError::PeerLimit { maximum }) if maximum == MAX_TRACKER_PEERS
        ));
    }

    #[tokio::test]
    async fn rate_limits_malformed_bodies_and_tls_failures_are_reported()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let rate_listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let rate_url = Url::parse(&format!("http://{}/announce", rate_listener.local_addr()?))?;
        let rate_server = tokio::spawn(serve_http_once(
            rate_listener,
            "429 Too Many Requests",
            Vec::new(),
        ));
        let client = HttpTrackerClient::new(1024)?;
        assert!(matches!(
            client.announce(&rate_url, test_request()).await,
            Err(TrackerServiceError::Status(
                reqwest::StatusCode::TOO_MANY_REQUESTS
            ))
        ));
        rate_server.await??;

        let malformed_listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let malformed_url = Url::parse(&format!(
            "http://{}/announce",
            malformed_listener.local_addr()?
        ))?;
        let malformed_server = tokio::spawn(serve_http_once(
            malformed_listener,
            "200 OK",
            b"not bencode".to_vec(),
        ));
        assert!(matches!(
            client.announce(&malformed_url, test_request()).await,
            Err(TrackerServiceError::Bencode(_))
        ));
        malformed_server.await??;

        let tls_listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let tls_url = Url::parse(&format!("https://{}/announce", tls_listener.local_addr()?))?;
        let tls_server = tokio::spawn(async move {
            let (mut stream, _) = tls_listener.accept().await?;
            let mut input = [0_u8; 512];
            let _read = stream.read(&mut input).await?;
            Ok::<(), std::io::Error>(())
        });
        assert!(matches!(
            client.announce(&tls_url, test_request()).await,
            Err(TrackerServiceError::Request(_))
        ));
        tls_server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn redirect_loops_and_dns_failure_are_bounded()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let url = Url::parse(&format!("http://{}/announce", listener.local_addr()?))?;
        let location = url.to_string();
        let redirects = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().await?;
                read_http_headers(&mut stream).await?;
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                stream.write_all(response.as_bytes()).await?;
            }
            Ok::<(), std::io::Error>(())
        });
        let client = HttpTrackerClient::new(1024)?;
        assert!(matches!(
            client.announce(&url, test_request()).await,
            Err(TrackerServiceError::Request(_))
        ));
        redirects.await??;

        let dns_client = HttpTrackerClient {
            client: Client::builder()
                .no_proxy()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(2))
                .build()?,
            response_limit: 1024,
        };
        let missing = Url::parse("http://tracker-does-not-exist.invalid/announce")?;
        assert!(matches!(
            dns_client.announce(&missing, test_request()).await,
            Err(TrackerServiceError::Request(_))
        ));
        Ok(())
    }

    fn test_request() -> TrackerRequest {
        TrackerRequest {
            info_hash: Sha1Hash::from_bytes([7; 20]),
            peer_id: PeerId::from_bytes([8; 20]),
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1,
            event: AnnounceEvent::Started,
            numwant: 10,
        }
    }

    async fn serve_http_once(
        listener: TcpListener,
        status: &'static str,
        body: Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let (mut stream, _) = listener.accept().await?;
        read_http_headers(&mut stream).await?;
        let headers = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(&body).await
    }

    async fn read_http_headers(stream: &mut tokio::net::TcpStream) -> Result<(), std::io::Error> {
        let mut request = Vec::new();
        loop {
            let mut chunk = [0_u8; 512];
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "HTTP request ended before its headers",
                ));
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                return Ok(());
            }
            if request.len() > 16 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "HTTP request headers exceeded test bound",
                ));
            }
        }
    }
}
