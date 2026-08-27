//! Bounded metainfo parsing with exact info-dictionary hashing.

mod bencode;
mod magnet;
mod metainfo;

pub use bencode::{BencodeLimits, BencodeValue, DecodeError, SpannedValue, decode, decode_prefix};
pub use magnet::{Magnet, MagnetError};
pub use metainfo::{FileEntry, Metainfo, MetainfoError, TorrentVersion};
