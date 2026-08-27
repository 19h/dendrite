use std::{fmt, path::PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use unicode_normalization::UnicodeNormalization as _;

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TorrentPath(Vec<String>);

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PathError {
    #[error("torrent path must contain at least one component")]
    Empty,
    #[error("path component {index} is empty")]
    EmptyComponent { index: usize },
    #[error("path component {index} is unsafe")]
    UnsafeComponent { index: usize },
    #[error("path component {index} is not portable across supported filesystems")]
    NonPortableComponent { index: usize },
    #[error("path component {index} exceeds 255 UTF-8 bytes")]
    ComponentTooLong { index: usize },
    #[error("torrent path exceeds 4096 UTF-8 bytes")]
    PathTooLong,
}

impl TorrentPath {
    pub fn new(components: impl IntoIterator<Item = String>) -> Result<Self, PathError> {
        let components: Vec<String> = components
            .into_iter()
            .map(|component| component.nfc().collect())
            .collect();
        if components.is_empty() {
            return Err(PathError::Empty);
        }

        let mut total = 0_usize;
        for (index, component) in components.iter().enumerate() {
            if component.is_empty() {
                return Err(PathError::EmptyComponent { index });
            }
            if component == "."
                || component == ".."
                || component.contains('/')
                || component.contains('\\')
                || component.contains('\0')
            {
                return Err(PathError::UnsafeComponent { index });
            }
            if !portable_component(component) {
                return Err(PathError::NonPortableComponent { index });
            }
            if component.len() > 255 {
                return Err(PathError::ComponentTooLong { index });
            }
            total = total
                .checked_add(component.len())
                .and_then(|length| length.checked_add(1))
                .ok_or(PathError::PathTooLong)?;
        }
        if total > 4096 {
            return Err(PathError::PathTooLong);
        }
        Ok(Self(components))
    }

    #[must_use]
    pub fn components(&self) -> &[String] {
        &self.0
    }

    #[must_use]
    pub fn to_relative_path_buf(&self) -> PathBuf {
        self.0.iter().collect()
    }
}

fn portable_component(component: &str) -> bool {
    if component.ends_with(['.', ' '])
        || component
            .chars()
            .any(|character| character.is_ascii_control() || "<>:\"|?*".contains(character))
    {
        return false;
    }
    let stem = component.split('.').next().unwrap_or(component);
    let upper = stem.to_ascii_uppercase();
    !matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        && !(upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && matches!(upper.as_bytes()[3], b'1'..=b'9'))
}

impl fmt::Debug for TorrentPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("TorrentPath").field(&self.0).finish()
    }
}

impl fmt::Display for TorrentPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.join("/"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_path_is_relative() {
        let path = TorrentPath::new(["linux".to_owned(), "image.iso".to_owned()]);
        assert_eq!(
            path.map(|value| value.to_relative_path_buf()),
            Ok(PathBuf::from("linux/image.iso"))
        );
    }

    #[test]
    fn traversal_is_rejected() {
        for component in ["..", ".", "/tmp", "a/b", "a\\b", "bad\0name"] {
            assert!(TorrentPath::new([component.to_owned()]).is_err());
        }
    }

    #[test]
    fn paths_are_unicode_normalized_and_windows_safe() {
        let normalized = TorrentPath::new(["cafe\u{301}.txt".to_owned()]);
        assert_eq!(
            normalized.map(|path| path.to_string()),
            Ok("caf\u{e9}.txt".to_owned())
        );
        for component in [
            "CON",
            "con.txt",
            "LPT1.log",
            "COM9",
            "name.",
            "name ",
            "stream:fork",
            "wild*card",
            "control\u{1f}",
        ] {
            assert!(matches!(
                TorrentPath::new([component.to_owned()]),
                Err(PathError::NonPortableComponent { .. })
            ));
        }
    }

    #[test]
    fn component_and_total_path_limits_match_portable_filesystems() {
        assert!(TorrentPath::new(["x".repeat(255)]).is_ok());
        assert!(matches!(
            TorrentPath::new(["x".repeat(256)]),
            Err(PathError::ComponentTooLong { .. })
        ));
        assert!(matches!(
            TorrentPath::new((0..17).map(|_| "x".repeat(255))),
            Err(PathError::PathTooLong)
        ));
    }
}
