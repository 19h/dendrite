use std::ops::Range;

use thiserror::Error;

#[derive(Clone, Copy, Debug)]
pub struct BencodeLimits {
    pub input_bytes: usize,
    pub depth: usize,
    pub nodes: usize,
    pub byte_string_bytes: usize,
    pub collection_items: usize,
    pub canonical_dictionaries: bool,
}

impl Default for BencodeLimits {
    fn default() -> Self {
        Self {
            input_bytes: 64 * 1024 * 1024,
            depth: 64,
            nodes: 1_000_000,
            byte_string_bytes: 64 * 1024 * 1024,
            collection_items: 1_000_000,
            canonical_dictionaries: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpannedValue<'a> {
    pub span: Range<usize>,
    pub value: BencodeValue<'a>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BencodeValue<'a> {
    Integer(i64),
    Bytes(&'a [u8]),
    List(Vec<SpannedValue<'a>>),
    Dictionary(Vec<(&'a [u8], SpannedValue<'a>)>),
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    #[error("bencode input exceeds {maximum} bytes")]
    InputLimit { maximum: usize },
    #[error("unexpected end of bencode input at byte {offset}")]
    UnexpectedEof { offset: usize },
    #[error("invalid bencode marker 0x{marker:02x} at byte {offset}")]
    InvalidMarker { offset: usize, marker: u8 },
    #[error("invalid canonical integer at byte {offset}")]
    InvalidInteger { offset: usize },
    #[error("invalid canonical byte-string length at byte {offset}")]
    InvalidByteStringLength { offset: usize },
    #[error("byte string length exceeds {maximum} bytes")]
    ByteStringLimit { maximum: usize },
    #[error("nesting exceeds {maximum} levels")]
    DepthLimit { maximum: usize },
    #[error("document exceeds {maximum} values")]
    NodeLimit { maximum: usize },
    #[error("collection exceeds {maximum} items")]
    CollectionLimit { maximum: usize },
    #[error("dictionary key is not strictly ordered at byte {offset}")]
    NonCanonicalDictionary { offset: usize },
    #[error("trailing data begins at byte {offset}")]
    TrailingData { offset: usize },
}

pub fn decode(input: &[u8], limits: BencodeLimits) -> Result<SpannedValue<'_>, DecodeError> {
    let (value, consumed) = decode_prefix(input, limits)?;
    if consumed != input.len() {
        return Err(DecodeError::TrailingData { offset: consumed });
    }
    Ok(value)
}

/// Decode one value and return the number of consumed bytes. This is useful
/// for protocols such as BEP 9 that append an opaque payload after a bencoded
/// header.
pub fn decode_prefix(
    input: &[u8],
    limits: BencodeLimits,
) -> Result<(SpannedValue<'_>, usize), DecodeError> {
    if input.len() > limits.input_bytes {
        return Err(DecodeError::InputLimit {
            maximum: limits.input_bytes,
        });
    }
    let mut parser = Parser {
        input,
        limits,
        offset: 0,
        nodes: 0,
    };
    let value = parser.parse_value(0)?;
    Ok((value, parser.offset))
}

struct Parser<'a> {
    input: &'a [u8],
    limits: BencodeLimits,
    offset: usize,
    nodes: usize,
}

impl<'a> Parser<'a> {
    fn parse_value(&mut self, depth: usize) -> Result<SpannedValue<'a>, DecodeError> {
        if depth > self.limits.depth {
            return Err(DecodeError::DepthLimit {
                maximum: self.limits.depth,
            });
        }
        self.nodes = self.nodes.checked_add(1).ok_or(DecodeError::NodeLimit {
            maximum: self.limits.nodes,
        })?;
        if self.nodes > self.limits.nodes {
            return Err(DecodeError::NodeLimit {
                maximum: self.limits.nodes,
            });
        }

        let start = self.offset;
        let marker = self.byte()?;
        let value = match marker {
            b'i' => BencodeValue::Integer(self.parse_integer(start)?),
            b'l' => BencodeValue::List(self.parse_list(depth)?),
            b'd' => BencodeValue::Dictionary(self.parse_dictionary(depth)?),
            b'0'..=b'9' => BencodeValue::Bytes(self.parse_bytes(start, marker)?),
            _ => {
                return Err(DecodeError::InvalidMarker {
                    offset: start,
                    marker,
                });
            }
        };
        Ok(SpannedValue {
            span: start..self.offset,
            value,
        })
    }

    fn parse_integer(&mut self, start: usize) -> Result<i64, DecodeError> {
        let digits_start = self.offset;
        let end = self.find(b'e')?;
        let bytes = &self.input[digits_start..end];
        self.offset = end + 1;
        if bytes.is_empty()
            || bytes == b"-0"
            || (bytes.len() > 1 && bytes[0] == b'0')
            || (bytes.starts_with(b"-0") && bytes.len() > 2)
            || bytes.starts_with(b"+")
        {
            return Err(DecodeError::InvalidInteger { offset: start });
        }
        let text = std::str::from_utf8(bytes)
            .map_err(|_| DecodeError::InvalidInteger { offset: start })?;
        text.parse()
            .map_err(|_| DecodeError::InvalidInteger { offset: start })
    }

    fn parse_bytes(&mut self, start: usize, first: u8) -> Result<&'a [u8], DecodeError> {
        let mut length = usize::from(first - b'0');
        let mut digits = 1_usize;
        loop {
            let next = self.byte()?;
            if next == b':' {
                break;
            }
            if !next.is_ascii_digit() || (digits == 1 && first == b'0') {
                return Err(DecodeError::InvalidByteStringLength { offset: start });
            }
            length = length
                .checked_mul(10)
                .and_then(|value| value.checked_add(usize::from(next - b'0')))
                .ok_or(DecodeError::ByteStringLimit {
                    maximum: self.limits.byte_string_bytes,
                })?;
            digits += 1;
        }
        if length > self.limits.byte_string_bytes {
            return Err(DecodeError::ByteStringLimit {
                maximum: self.limits.byte_string_bytes,
            });
        }
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.input.len())
            .ok_or(DecodeError::UnexpectedEof {
                offset: self.offset,
            })?;
        let value = &self.input[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn parse_list(&mut self, depth: usize) -> Result<Vec<SpannedValue<'a>>, DecodeError> {
        let mut values = Vec::new();
        while self.peek()? != b'e' {
            if values.len() >= self.limits.collection_items {
                return Err(DecodeError::CollectionLimit {
                    maximum: self.limits.collection_items,
                });
            }
            values.push(self.parse_value(depth + 1)?);
        }
        self.offset += 1;
        Ok(values)
    }

    fn parse_dictionary(
        &mut self,
        depth: usize,
    ) -> Result<Vec<(&'a [u8], SpannedValue<'a>)>, DecodeError> {
        let mut values = Vec::new();
        let mut previous: Option<&[u8]> = None;
        while self.peek()? != b'e' {
            if values.len() >= self.limits.collection_items {
                return Err(DecodeError::CollectionLimit {
                    maximum: self.limits.collection_items,
                });
            }
            let key_offset = self.offset;
            let BencodeValue::Bytes(key) = self.parse_value(depth + 1)?.value else {
                return Err(DecodeError::InvalidMarker {
                    offset: key_offset,
                    marker: self.input[key_offset],
                });
            };
            if self.limits.canonical_dictionaries && previous.is_some_and(|value| value >= key) {
                return Err(DecodeError::NonCanonicalDictionary { offset: key_offset });
            }
            previous = Some(key);
            values.push((key, self.parse_value(depth + 1)?));
        }
        self.offset += 1;
        Ok(values)
    }

    fn byte(&mut self) -> Result<u8, DecodeError> {
        let value = *self
            .input
            .get(self.offset)
            .ok_or(DecodeError::UnexpectedEof {
                offset: self.offset,
            })?;
        self.offset += 1;
        Ok(value)
    }

    fn peek(&self) -> Result<u8, DecodeError> {
        self.input
            .get(self.offset)
            .copied()
            .ok_or(DecodeError::UnexpectedEof {
                offset: self.offset,
            })
    }

    fn find(&self, needle: u8) -> Result<usize, DecodeError> {
        self.input[self.offset..]
            .iter()
            .position(|byte| *byte == needle)
            .map(|relative| self.offset + relative)
            .ok_or(DecodeError::UnexpectedEof {
                offset: self.offset,
            })
    }
}

impl<'a> BencodeValue<'a> {
    #[must_use]
    pub const fn as_integer(&self) -> Option<i64> {
        if let Self::Integer(value) = self {
            Some(*value)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn as_bytes(&self) -> Option<&'a [u8]> {
        if let Self::Bytes(value) = self {
            Some(value)
        } else {
            None
        }
    }

    #[must_use]
    pub fn dictionary_get(&self, key: &[u8]) -> Option<&SpannedValue<'a>> {
        let Self::Dictionary(values) = self else {
            return None;
        };
        values
            .iter()
            .find(|(candidate, _)| *candidate == key)
            .map(|(_, value)| value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_value_spans() {
        let input = b"d4:infod4:name4:teste4:spamli1ei2eee";
        let decoded = decode(input, BencodeLimits::default());
        let value = decoded.as_ref().ok().map(|root| &root.value);
        let info = value.and_then(|root| root.dictionary_get(b"info"));
        assert_eq!(
            info.map(|value| &input[value.span.clone()]),
            Some(&b"d4:name4:teste"[..])
        );
    }

    #[test]
    fn rejects_noncanonical_values() {
        for value in [&b"i03e"[..], &b"i-0e"[..], &b"03:abc"[..]] {
            assert!(decode(value, BencodeLimits::default()).is_err());
        }
    }

    #[test]
    fn rejects_unsorted_and_duplicate_keys() {
        assert!(decode(b"d1:bi1e1:ai2ee", BencodeLimits::default()).is_err());
        assert!(decode(b"d1:ai1e1:ai2ee", BencodeLimits::default()).is_err());
    }

    #[test]
    fn enforces_depth_budget() {
        let limits = BencodeLimits {
            depth: 2,
            ..BencodeLimits::default()
        };
        assert!(matches!(
            decode(b"llli1eeee", limits),
            Err(DecodeError::DepthLimit { maximum: 2 })
        ));
    }
}
