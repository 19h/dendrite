use std::str::FromStr as _;

use data_encoding::BASE32_NOPAD;
use dendrite_core::{Sha1Hash, Sha256Hash};
use thiserror::Error;
use url::Url;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Magnet {
    pub v1_info_hash: Option<Sha1Hash>,
    pub v2_info_hash: Option<Sha256Hash>,
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum MagnetError {
    #[error("magnet URI is invalid: {0}")]
    Url(String),
    #[error("URI scheme must be magnet")]
    Scheme,
    #[error("magnet URI does not contain a supported exact topic")]
    MissingTopic,
    #[error("invalid v1 exact topic")]
    InvalidV1,
    #[error("invalid v2 multihash exact topic")]
    InvalidV2,
}

impl Magnet {
    pub fn parse(input: &str) -> Result<Self, MagnetError> {
        let url = Url::parse(input).map_err(|error| MagnetError::Url(error.to_string()))?;
        if url.scheme() != "magnet" {
            return Err(MagnetError::Scheme);
        }
        let mut magnet = Self {
            v1_info_hash: None,
            v2_info_hash: None,
            display_name: None,
            trackers: Vec::new(),
            web_seeds: Vec::new(),
        };
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "xt" if value.starts_with("urn:btih:") => {
                    magnet.v1_info_hash = Some(parse_v1(&value[9..])?);
                }
                "xt" if value.starts_with("urn:btmh:") => {
                    magnet.v2_info_hash = Some(parse_v2(&value[9..])?);
                }
                "dn" => magnet.display_name = Some(value.into_owned()),
                "tr" => magnet.trackers.push(value.into_owned()),
                "ws" => magnet.web_seeds.push(value.into_owned()),
                _ => {}
            }
        }
        if magnet.v1_info_hash.is_none() && magnet.v2_info_hash.is_none() {
            return Err(MagnetError::MissingTopic);
        }
        Ok(magnet)
    }
}

fn parse_v1(value: &str) -> Result<Sha1Hash, MagnetError> {
    if value.len() == 40 {
        return Sha1Hash::from_str(value).map_err(|_| MagnetError::InvalidV1);
    }
    if value.len() == 32 {
        let uppercase = value.to_ascii_uppercase();
        let bytes = BASE32_NOPAD
            .decode(uppercase.as_bytes())
            .map_err(|_| MagnetError::InvalidV1)?;
        let hash: [u8; 20] = bytes.try_into().map_err(|_| MagnetError::InvalidV1)?;
        return Ok(Sha1Hash::from_bytes(hash));
    }
    Err(MagnetError::InvalidV1)
}

fn parse_v2(value: &str) -> Result<Sha256Hash, MagnetError> {
    let decoded = if value.len() == 68 && value.starts_with("1220") {
        hex::decode(value).map_err(|_| MagnetError::InvalidV2)?
    } else {
        multibase::decode(value)
            .map(|(_, bytes)| bytes)
            .map_err(|_| MagnetError::InvalidV2)?
    };
    if decoded.len() != 34 || decoded[0] != 0x12 || decoded[1] != 0x20 {
        return Err(MagnetError::InvalidV2);
    }
    let hash: [u8; 32] = decoded[2..]
        .try_into()
        .map_err(|_| MagnetError::InvalidV2)?;
    Ok(Sha256Hash::from_bytes(hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_v1_hex_and_v2_multihash() -> Result<(), MagnetError> {
        let uri = concat!(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            "&xt=urn:btmh:1220aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "&dn=example&tr=https%3A%2F%2Ftracker.example%2Fannounce"
        );
        let magnet = Magnet::parse(uri)?;
        assert!(magnet.v1_info_hash.is_some());
        assert!(magnet.v2_info_hash.is_some());
        assert_eq!(magnet.display_name.as_deref(), Some("example"));
        assert_eq!(magnet.trackers, ["https://tracker.example/announce"]);
        Ok(())
    }

    #[test]
    fn rejects_missing_topics() {
        assert_eq!(
            Magnet::parse("magnet:?dn=test"),
            Err(MagnetError::MissingTopic)
        );
    }
}
