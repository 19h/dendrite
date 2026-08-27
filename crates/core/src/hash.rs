use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha1Hash([u8; 20]);

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256Hash([u8; 32]);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(tag = "algorithm", content = "hex", rename_all = "snake_case")]
pub enum InfoHash {
    Sha1(Sha1Hash),
    Sha256(Sha256Hash),
}

#[derive(Debug, Error, PartialEq)]
pub enum HashParseError {
    #[error("hash is not valid hexadecimal: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("expected {expected} bytes, got {actual}")]
    Length { expected: usize, actual: usize },
}

macro_rules! fixed_hash {
    ($name:ident, $length:expr) => {
        impl $name {
            #[must_use]
            pub const fn from_bytes(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(self, formatter)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&hex::encode(self.0))
            }
        }

        impl FromStr for $name {
            type Err = HashParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let decoded = hex::decode(value)?;
                let actual = decoded.len();
                let bytes = decoded
                    .try_into()
                    .map_err(|_: Vec<u8>| HashParseError::Length {
                        expected: $length,
                        actual,
                    })?;
                Ok(Self(bytes))
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                if serializer.is_human_readable() {
                    serializer.serialize_str(&self.to_string())
                } else {
                    serializer.serialize_bytes(&self.0)
                }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                if deserializer.is_human_readable() {
                    let value = String::deserialize(deserializer)?;
                    value.parse().map_err(D::Error::custom)
                } else {
                    let value = Vec::<u8>::deserialize(deserializer)?;
                    let actual = value.len();
                    let bytes = value.try_into().map_err(|_: Vec<u8>| {
                        D::Error::custom(HashParseError::Length {
                            expected: $length,
                            actual,
                        })
                    })?;
                    Ok(Self(bytes))
                }
            }
        }
    };
}

fixed_hash!(Sha1Hash, 20);
fixed_hash!(Sha256Hash, 32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_round_trip_hex() {
        let hash = Sha1Hash::from_bytes([0xab; 20]);
        let encoded = hash.to_string();
        assert_eq!(encoded.parse::<Sha1Hash>(), Ok(hash));
    }

    #[test]
    fn hashes_reject_wrong_length() {
        assert!(matches!(
            "ab".parse::<Sha256Hash>(),
            Err(HashParseError::Length {
                expected: 32,
                actual: 1
            })
        ));
    }
}
