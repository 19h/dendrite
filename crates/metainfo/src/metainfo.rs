use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
};

use dendrite_core::{Sha1Hash, Sha256Hash, TorrentPath};
use serde::{Deserialize, Serialize};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use thiserror::Error;

use crate::{BencodeLimits, BencodeValue, DecodeError, SpannedValue, decode};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentVersion {
    V1,
    V2,
    Hybrid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: TorrentPath,
    pub length: u64,
    pub pieces_root: Option<Sha256Hash>,
    pub padding: bool,
    #[serde(skip)]
    pub wire_offset: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Metainfo {
    pub raw: Vec<u8>,
    pub name: String,
    pub version: TorrentVersion,
    pub v1_info_hash: Option<Sha1Hash>,
    pub v2_info_hash: Option<Sha256Hash>,
    pub piece_length: NonZeroU32,
    pub v1_piece_hashes: Vec<Sha1Hash>,
    pub piece_layers: BTreeMap<Sha256Hash, Vec<Sha256Hash>>,
    pub files: Vec<FileEntry>,
    pub v1_files: Vec<FileEntry>,
    pub total_length: u64,
    pub piece_space_length: u64,
    pub trackers: Vec<Vec<String>>,
    pub web_seeds: Vec<String>,
    pub private: bool,
}

#[derive(Debug, Error)]
pub enum MetainfoError {
    #[error(transparent)]
    Bencode(#[from] DecodeError),
    #[error("metainfo root must be a dictionary")]
    RootType,
    #[error("missing or invalid field: {0}")]
    Field(&'static str),
    #[error("field {field} is not valid UTF-8")]
    Utf8 { field: &'static str },
    #[error("integer field {field} is outside its permitted range")]
    IntegerRange { field: &'static str },
    #[error("torrent path is invalid: {0}")]
    Path(#[from] dendrite_core::PathError),
    #[error("torrent file list is empty")]
    EmptyFiles,
    #[error("torrent contains duplicate path {0}")]
    DuplicatePath(TorrentPath),
    #[error("torrent total length overflowed u64")]
    TotalLengthOverflow,
    #[error("v1 pieces length is not a multiple of 20")]
    InvalidV1Pieces,
    #[error("v1 piece count is {actual}, expected {expected}")]
    V1PieceCount { expected: u64, actual: usize },
    #[error("v2 piece length must be a power of two and at least 16384")]
    InvalidV2PieceLength,
    #[error("v2 file tree exceeds 64 path levels")]
    FileTreeDepth,
    #[error("symbolic links in torrent metadata are not supported")]
    SymlinkUnsupported,
    #[error("non-empty v2 file is missing its pieces root")]
    MissingPiecesRoot,
    #[error("piece layer for root {0} is missing or malformed")]
    PieceLayer(Sha256Hash),
    #[error("piece layer for root {0} does not reconstruct that root")]
    PieceLayerRoot(Sha256Hash),
    #[error("piece layers contain an entry not referenced by a large file")]
    UnreferencedPieceLayer,
    #[error("hybrid v1 and v2 file layouts describe different content")]
    HybridFileMismatch,
    #[error("hybrid v1 layout does not align every non-empty file to a piece boundary")]
    HybridAlignment,
}

impl Metainfo {
    pub fn parse(input: &[u8], limits: BencodeLimits) -> Result<Self, MetainfoError> {
        Self::parse_internal(input, limits, true)
    }

    /// Parse BEP 9's info-only metadata before BEP 52 piece layers have been
    /// fetched from a peer. Any present layers are still fully validated.
    pub fn parse_allow_missing_piece_layers(
        input: &[u8],
        limits: BencodeLimits,
    ) -> Result<Self, MetainfoError> {
        Self::parse_internal(input, limits, false)
    }

    fn parse_internal(
        input: &[u8],
        limits: BencodeLimits,
        require_piece_layers: bool,
    ) -> Result<Self, MetainfoError> {
        let root = decode(input, limits)?;
        let root_dict = dictionary(&root).ok_or(MetainfoError::RootType)?;
        let info = get(root_dict, b"info").ok_or(MetainfoError::Field("info"))?;
        let info_dict = dictionary(info).ok_or(MetainfoError::Field("info"))?;

        let name = text(
            required_bytes(info_dict, b"name", "info.name")?,
            "info.name",
        )?;
        let raw_info = &input[info.span.clone()];
        let meta_version = optional_integer(info_dict, b"meta version", "info.meta version")?;
        let has_v1 = get(info_dict, b"pieces").is_some();
        let has_v2 = meta_version == Some(2);
        let version = match (has_v1, has_v2) {
            (true, false) => TorrentVersion::V1,
            (false, true) => TorrentVersion::V2,
            (true, true) => TorrentVersion::Hybrid,
            (false, false) => return Err(MetainfoError::Field("info.pieces or info.meta version")),
        };

        let raw_piece_length = required_integer(info_dict, b"piece length", "info.piece length")?;
        let piece_length = u32::try_from(raw_piece_length)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(MetainfoError::IntegerRange {
                field: "info.piece length",
            })?;
        if has_v2 && (piece_length.get() < 16_384 || !piece_length.get().is_power_of_two()) {
            return Err(MetainfoError::InvalidV2PieceLength);
        }

        let v1_piece_hashes = parse_v1_piece_hashes(info_dict, has_v1)?;

        let mut v1_files = if has_v1 {
            parse_v1_files(info_dict, &name)?
        } else {
            Vec::new()
        };
        assign_wire_offsets(&mut v1_files)?;
        let mut files = if has_v2 {
            parse_v2_files(info_dict, &name)?
        } else {
            v1_files.clone()
        };
        assign_wire_offsets(&mut files)?;
        if has_v1 {
            validate_unique_paths(&v1_files)?;
        }
        validate_unique_paths(&files)?;
        let total_length = sum_file_lengths(&files)?;
        let piece_space_length = if has_v1 {
            sum_file_lengths(&v1_files)?
        } else {
            total_length
        };
        if has_v1 && has_v2 {
            validate_hybrid_layout(&v1_files, &files, piece_length)?;
        }
        let piece_layers = if has_v2 {
            parse_piece_layers(root_dict, &files, piece_length, require_piece_layers)?
        } else {
            BTreeMap::new()
        };

        if has_v1 {
            let piece_length_bytes = u64::from(piece_length.get());
            let expected = piece_space_length.div_ceil(piece_length_bytes);
            if u64::try_from(v1_piece_hashes.len()).ok() != Some(expected) {
                return Err(MetainfoError::V1PieceCount {
                    expected,
                    actual: v1_piece_hashes.len(),
                });
            }
        }

        let v1_info_hash = has_v1.then(|| {
            let digest: [u8; 20] = Sha1::digest(raw_info).into();
            Sha1Hash::from_bytes(digest)
        });
        let v2_info_hash = has_v2.then(|| {
            let digest: [u8; 32] = Sha256::digest(raw_info).into();
            Sha256Hash::from_bytes(digest)
        });

        Ok(Self {
            raw: input.to_vec(),
            name,
            version,
            v1_info_hash,
            v2_info_hash,
            piece_length,
            v1_piece_hashes,
            piece_layers,
            files,
            v1_files,
            total_length,
            piece_space_length,
            trackers: parse_trackers(root_dict),
            web_seeds: parse_web_seeds(root_dict),
            private: optional_integer(info_dict, b"private", "info.private")? == Some(1),
        })
    }
}

fn parse_v1_piece_hashes(
    info: &[(&[u8], SpannedValue<'_>)],
    has_v1: bool,
) -> Result<Vec<Sha1Hash>, MetainfoError> {
    if !has_v1 {
        return Ok(Vec::new());
    }
    let pieces = required_bytes(info, b"pieces", "info.pieces")?;
    let chunks = pieces.chunks_exact(20);
    if !chunks.remainder().is_empty() {
        return Err(MetainfoError::InvalidV1Pieces);
    }
    chunks
        .map(|chunk| {
            let bytes: [u8; 20] = chunk
                .try_into()
                .map_err(|_| MetainfoError::InvalidV1Pieces)?;
            Ok(Sha1Hash::from_bytes(bytes))
        })
        .collect()
}

fn parse_v1_files(
    info: &[(&[u8], SpannedValue<'_>)],
    name: &str,
) -> Result<Vec<FileEntry>, MetainfoError> {
    if let Some(files_value) = get(info, b"files") {
        let BencodeValue::List(files) = &files_value.value else {
            return Err(MetainfoError::Field("info.files"));
        };
        if files.is_empty() {
            return Err(MetainfoError::EmptyFiles);
        }
        files
            .iter()
            .map(|file| {
                let dict = dictionary(file).ok_or(MetainfoError::Field("info.files[]"))?;
                let length = nonnegative_u64(
                    required_integer(dict, b"length", "info.files[].length")?,
                    "info.files[].length",
                )?;
                let path_value =
                    get(dict, b"path").ok_or(MetainfoError::Field("info.files[].path"))?;
                let BencodeValue::List(parts) = &path_value.value else {
                    return Err(MetainfoError::Field("info.files[].path"));
                };
                let mut components = Vec::with_capacity(parts.len() + 1);
                components.push(name.to_owned());
                for part in parts {
                    let bytes = part
                        .value
                        .as_bytes()
                        .ok_or(MetainfoError::Field("info.files[].path[]"))?;
                    components.push(text(bytes, "info.files[].path[]")?);
                }
                let padding = get(dict, b"attr")
                    .and_then(|value| value.value.as_bytes())
                    .is_some_and(|attributes| attributes.contains(&b'p'));
                Ok(FileEntry {
                    path: TorrentPath::new(components)?,
                    length,
                    pieces_root: None,
                    padding,
                    wire_offset: 0,
                })
            })
            .collect()
    } else {
        let length = nonnegative_u64(
            required_integer(info, b"length", "info.length")?,
            "info.length",
        )?;
        Ok(vec![FileEntry {
            path: TorrentPath::new([name.to_owned()])?,
            length,
            pieces_root: None,
            padding: false,
            wire_offset: 0,
        }])
    }
}

fn sum_file_lengths(files: &[FileEntry]) -> Result<u64, MetainfoError> {
    files.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.length)
            .ok_or(MetainfoError::TotalLengthOverflow)
    })
}

fn assign_wire_offsets(files: &mut [FileEntry]) -> Result<(), MetainfoError> {
    let mut offset = 0_u64;
    for file in files {
        file.wire_offset = offset;
        offset = offset
            .checked_add(file.length)
            .ok_or(MetainfoError::TotalLengthOverflow)?;
    }
    Ok(())
}

fn validate_hybrid_layout(
    v1_files: &[FileEntry],
    v2_files: &[FileEntry],
    piece_length: NonZeroU32,
) -> Result<(), MetainfoError> {
    let v1_content: Vec<_> = v1_files.iter().filter(|file| !file.padding).collect();
    let v2_content: Vec<_> = v2_files.iter().filter(|file| !file.padding).collect();
    if v1_content.len() != v2_content.len()
        || v1_content
            .iter()
            .zip(&v2_content)
            .any(|(v1, v2)| v1.path != v2.path || v1.length != v2.length)
    {
        return Err(MetainfoError::HybridFileMismatch);
    }
    let alignment = u64::from(piece_length.get());
    let mut offset = 0_u64;
    for file in v1_files {
        if !file.padding && file.length > 0 && !offset.is_multiple_of(alignment) {
            return Err(MetainfoError::HybridAlignment);
        }
        offset = offset
            .checked_add(file.length)
            .ok_or(MetainfoError::TotalLengthOverflow)?;
    }
    Ok(())
}

fn parse_v2_files(
    info: &[(&[u8], SpannedValue<'_>)],
    name: &str,
) -> Result<Vec<FileEntry>, MetainfoError> {
    let tree = get(info, b"file tree").ok_or(MetainfoError::Field("info.file tree"))?;
    let tree_dict = dictionary(tree).ok_or(MetainfoError::Field("info.file tree"))?;
    let mut files = Vec::new();
    let mut components = vec![name.to_owned()];
    walk_v2_tree(tree_dict, &mut components, &mut files, 0)?;
    if files.is_empty() {
        return Err(MetainfoError::EmptyFiles);
    }
    Ok(files)
}

fn walk_v2_tree(
    tree: &[(&[u8], SpannedValue<'_>)],
    components: &mut Vec<String>,
    files: &mut Vec<FileEntry>,
    depth: usize,
) -> Result<(), MetainfoError> {
    if depth > 64 {
        return Err(MetainfoError::FileTreeDepth);
    }
    for (key, value) in tree {
        if key.is_empty() {
            let attributes =
                dictionary(value).ok_or(MetainfoError::Field("info.file tree file"))?;
            let length = nonnegative_u64(
                required_integer(attributes, b"length", "info.file tree.length")?,
                "info.file tree.length",
            )?;
            let attr = get(attributes, b"attr")
                .and_then(|value| value.value.as_bytes())
                .unwrap_or_default();
            if attr.contains(&b'l') {
                return Err(MetainfoError::SymlinkUnsupported);
            }
            let pieces_root = get(attributes, b"pieces root")
                .map(|value| -> Result<Sha256Hash, MetainfoError> {
                    let bytes = value
                        .value
                        .as_bytes()
                        .ok_or(MetainfoError::Field("info.file tree.pieces root"))?;
                    let root: [u8; 32] = bytes
                        .try_into()
                        .map_err(|_| MetainfoError::Field("info.file tree.pieces root"))?;
                    Ok(Sha256Hash::from_bytes(root))
                })
                .transpose()?;
            files.push(FileEntry {
                path: TorrentPath::new(components.clone())?,
                length,
                pieces_root,
                padding: attr.contains(&b'p'),
                wire_offset: 0,
            });
        } else {
            let component = text(key, "info.file tree path")?;
            let subtree =
                dictionary(value).ok_or(MetainfoError::Field("info.file tree directory"))?;
            components.push(component);
            walk_v2_tree(subtree, components, files, depth + 1)?;
            components.pop();
        }
    }
    Ok(())
}

fn parse_piece_layers(
    root: &[(&[u8], SpannedValue<'_>)],
    files: &[FileEntry],
    piece_length: NonZeroU32,
    required: bool,
) -> Result<BTreeMap<Sha256Hash, Vec<Sha256Hash>>, MetainfoError> {
    let mut layers = BTreeMap::new();
    if let Some(value) = get(root, b"piece layers") {
        let dictionary = dictionary(value).ok_or(MetainfoError::Field("piece layers"))?;
        for (key, value) in dictionary {
            let root_bytes: [u8; 32] = (*key)
                .try_into()
                .map_err(|_| MetainfoError::Field("piece layers key"))?;
            let root_hash = Sha256Hash::from_bytes(root_bytes);
            let bytes = value
                .value
                .as_bytes()
                .ok_or(MetainfoError::PieceLayer(root_hash))?;
            let chunks = bytes.chunks_exact(32);
            if !chunks.remainder().is_empty() {
                return Err(MetainfoError::PieceLayer(root_hash));
            }
            let hashes = chunks
                .map(|chunk| {
                    let hash: [u8; 32] = chunk
                        .try_into()
                        .map_err(|_| MetainfoError::PieceLayer(root_hash))?;
                    Ok(Sha256Hash::from_bytes(hash))
                })
                .collect::<Result<Vec<_>, MetainfoError>>()?;
            layers.insert(root_hash, hashes);
        }
    }

    let mut referenced = BTreeSet::new();
    for file in files.iter().filter(|file| !file.padding && file.length > 0) {
        let pieces_root = file.pieces_root.ok_or(MetainfoError::MissingPiecesRoot)?;
        if file.length <= u64::from(piece_length.get()) {
            continue;
        }
        let expected = file.length.div_ceil(u64::from(piece_length.get()));
        let Some(layer) = layers.get(&pieces_root) else {
            if required {
                return Err(MetainfoError::PieceLayer(pieces_root));
            }
            continue;
        };
        if u64::try_from(layer.len()).ok() != Some(expected) {
            return Err(MetainfoError::PieceLayer(pieces_root));
        }
        let base_layer = (piece_length.get() / (16 * 1024)).ilog2();
        if merkle_root_from_layer(layer, base_layer) != pieces_root {
            return Err(MetainfoError::PieceLayerRoot(pieces_root));
        }
        referenced.insert(pieces_root);
    }
    if layers.len() != referenced.len() {
        return Err(MetainfoError::UnreferencedPieceLayer);
    }
    Ok(layers)
}

fn merkle_root_from_layer(layer: &[Sha256Hash], base_layer: u32) -> Sha256Hash {
    let zero = zero_hash(base_layer);
    let width = layer.len().max(1).next_power_of_two();
    let mut hashes = Vec::with_capacity(width);
    hashes.extend_from_slice(layer);
    hashes.resize(width, zero);
    while hashes.len() > 1 {
        hashes = hashes
            .chunks_exact(2)
            .map(|pair| hash_pair(pair[0], pair[1]))
            .collect();
    }
    hashes[0]
}

fn zero_hash(layer: u32) -> Sha256Hash {
    let mut hash = Sha256Hash::from_bytes([0; 32]);
    for _ in 0..layer {
        hash = hash_pair(hash, hash);
    }
    hash
}

fn hash_pair(left: Sha256Hash, right: Sha256Hash) -> Sha256Hash {
    let mut hasher = Sha256::new();
    hasher.update(left.as_bytes());
    hasher.update(right.as_bytes());
    Sha256Hash::from_bytes(hasher.finalize().into())
}

fn validate_unique_paths(files: &[FileEntry]) -> Result<(), MetainfoError> {
    let mut paths: Vec<_> = files
        .iter()
        .map(|file| {
            (
                &file.path,
                file.path
                    .components()
                    .iter()
                    .map(|component| component.to_lowercase())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    paths.sort_unstable_by(|left, right| left.1.cmp(&right.1));
    for pair in paths.windows(2) {
        if pair[0].1 == pair[1].1
            || pair[0].1.starts_with(&pair[1].1)
            || pair[1].1.starts_with(&pair[0].1)
        {
            return Err(MetainfoError::DuplicatePath(pair[1].0.clone()));
        }
    }
    Ok(())
}

fn parse_trackers(root: &[(&[u8], SpannedValue<'_>)]) -> Vec<Vec<String>> {
    if let Some(BencodeValue::List(tiers)) = get(root, b"announce-list").map(|value| &value.value) {
        let parsed: Vec<Vec<String>> = tiers
            .iter()
            .filter_map(|tier| match &tier.value {
                BencodeValue::List(values) => Some(
                    values
                        .iter()
                        .filter_map(|value| value.value.as_bytes())
                        .filter_map(|value| std::str::from_utf8(value).ok())
                        .map(str::to_owned)
                        .collect(),
                ),
                _ => None,
            })
            .filter(|tier: &Vec<String>| !tier.is_empty())
            .collect();
        if !parsed.is_empty() {
            return parsed;
        }
    }
    get(root, b"announce")
        .and_then(|value| value.value.as_bytes())
        .and_then(|value| std::str::from_utf8(value).ok())
        .map(|value| vec![vec![value.to_owned()]])
        .unwrap_or_default()
}

fn parse_web_seeds(root: &[(&[u8], SpannedValue<'_>)]) -> Vec<String> {
    let Some(value) = get(root, b"url-list") else {
        return Vec::new();
    };
    match &value.value {
        BencodeValue::Bytes(value) => std::str::from_utf8(value)
            .ok()
            .map(|value| vec![value.to_owned()])
            .unwrap_or_default(),
        BencodeValue::List(values) => values
            .iter()
            .filter_map(|value| value.value.as_bytes())
            .filter_map(|value| std::str::from_utf8(value).ok())
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn dictionary<'a>(value: &'a SpannedValue<'a>) -> Option<&'a [(&'a [u8], SpannedValue<'a>)]> {
    if let BencodeValue::Dictionary(values) = &value.value {
        Some(values)
    } else {
        None
    }
}

fn get<'a>(
    dictionary: &'a [(&'a [u8], SpannedValue<'a>)],
    key: &[u8],
) -> Option<&'a SpannedValue<'a>> {
    dictionary
        .iter()
        .find(|(candidate, _)| *candidate == key)
        .map(|(_, value)| value)
}

fn required_bytes<'a>(
    dictionary: &'a [(&'a [u8], SpannedValue<'a>)],
    key: &[u8],
    field: &'static str,
) -> Result<&'a [u8], MetainfoError> {
    get(dictionary, key)
        .and_then(|value| value.value.as_bytes())
        .ok_or(MetainfoError::Field(field))
}

fn required_integer(
    dictionary: &[(&[u8], SpannedValue<'_>)],
    key: &[u8],
    field: &'static str,
) -> Result<i64, MetainfoError> {
    get(dictionary, key)
        .and_then(|value| value.value.as_integer())
        .ok_or(MetainfoError::Field(field))
}

fn optional_integer(
    dictionary: &[(&[u8], SpannedValue<'_>)],
    key: &[u8],
    field: &'static str,
) -> Result<Option<i64>, MetainfoError> {
    get(dictionary, key)
        .map(|value| value.value.as_integer().ok_or(MetainfoError::Field(field)))
        .transpose()
}

fn nonnegative_u64(value: i64, field: &'static str) -> Result<u64, MetainfoError> {
    u64::try_from(value).map_err(|_| MetainfoError::IntegerRange { field })
}

fn text(value: &[u8], field: &'static str) -> Result<String, MetainfoError> {
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| MetainfoError::Utf8 { field })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_safe_single_file_v1_and_hashes_original_info() {
        let input = b"d8:announce14:http://tracker4:infod6:lengthi4e4:name4:test12:piece lengthi4e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        let parsed = Metainfo::parse(input, BencodeLimits::default());
        let metainfo = parsed.as_ref().ok();
        assert_eq!(metainfo.map(|value| value.name.as_str()), Some("test"));
        assert_eq!(metainfo.map(|value| value.total_length), Some(4));
        assert!(metainfo.and_then(|value| value.v1_info_hash).is_some());
    }

    #[test]
    fn rejects_negative_lengths() {
        let input = b"d4:infod6:lengthi-1e4:name4:test12:piece lengthi4e6:pieces0:ee";
        assert!(matches!(
            Metainfo::parse(input, BencodeLimits::default()),
            Err(MetainfoError::IntegerRange {
                field: "info.length"
            })
        ));
    }

    #[test]
    fn rejects_traversal_components() {
        let input = b"d4:infod5:filesld6:lengthi1e4:pathl2:..1:aeee4:name4:root12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        assert!(matches!(
            Metainfo::parse(input, BencodeLimits::default()),
            Err(MetainfoError::Path(_))
        ));
    }

    #[test]
    fn rejects_case_and_file_directory_collisions_on_portable_filesystems() {
        let case_collision = b"d4:infod5:filesld6:lengthi1e4:pathl1:Aeed6:lengthi1e4:pathl1:aeee4:name4:root12:piece lengthi4e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        assert!(matches!(
            Metainfo::parse(case_collision, BencodeLimits::default()),
            Err(MetainfoError::DuplicatePath(_))
        ));
        let prefix_collision = b"d4:infod5:filesld6:lengthi1e4:pathl3:direed6:lengthi1e4:pathl3:dir6:nestedeee4:name4:root12:piece lengthi4e6:pieces20:aaaaaaaaaaaaaaaaaaaaee";
        assert!(matches!(
            Metainfo::parse(prefix_collision, BencodeLimits::default()),
            Err(MetainfoError::DuplicatePath(_))
        ));
    }

    #[test]
    fn validates_v2_piece_layers_against_file_roots() -> Result<(), MetainfoError> {
        let first = Sha256Hash::from_bytes([1; 32]);
        let second = Sha256Hash::from_bytes([2; 32]);
        let root = hash_pair(first, second);
        let input = v2_metainfo(root, first, second);
        let parsed = Metainfo::parse(&input, BencodeLimits::default())?;
        assert_eq!(parsed.piece_layers.get(&root), Some(&vec![first, second]));

        let corrupted = v2_metainfo(root, first, Sha256Hash::from_bytes([3; 32]));
        assert!(matches!(
            Metainfo::parse(&corrupted, BencodeLimits::default()),
            Err(MetainfoError::PieceLayerRoot(value)) if value == root
        ));
        Ok(())
    }

    fn v2_metainfo(root: Sha256Hash, first: Sha256Hash, second: Sha256Hash) -> Vec<u8> {
        let mut input = b"d4:infod9:file treed4:filed0:d6:lengthi32768e11:pieces root32:".to_vec();
        input.extend_from_slice(root.as_bytes());
        input.extend_from_slice(
            b"eee12:meta versioni2e4:name4:test12:piece lengthi16384ee12:piece layersd32:",
        );
        input.extend_from_slice(root.as_bytes());
        input.extend_from_slice(b"64:");
        input.extend_from_slice(first.as_bytes());
        input.extend_from_slice(second.as_bytes());
        input.extend_from_slice(b"ee");
        input
    }
}
