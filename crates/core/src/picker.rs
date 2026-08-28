use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectionMode {
    RarestFirst,
    Sequential,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PieceState {
    Missing,
    Requested(u8),
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Piece {
    availability: u32,
    enabled: bool,
    state: PieceState,
}

#[derive(Clone, Debug)]
pub struct PiecePicker {
    pieces: Vec<Piece>,
    remaining: usize,
    cursor: usize,
    endgame_threshold: usize,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum BitfieldError {
    #[error("bitfield has {actual} bytes, expected {expected}")]
    Length { expected: usize, actual: usize },
    #[error("bitfield sets spare bits beyond the torrent piece count")]
    SpareBits,
    #[error("piece index {index} is outside 0..{pieces}")]
    Index { index: usize, pieces: usize },
    #[error("piece range {start}..{end} is outside 0..{pieces}")]
    Range {
        start: usize,
        end: usize,
        pieces: usize,
    },
}

impl PiecePicker {
    #[must_use]
    pub fn new(piece_count: usize, endgame_threshold: usize) -> Self {
        Self {
            pieces: vec![
                Piece {
                    availability: 0,
                    enabled: true,
                    state: PieceState::Missing,
                };
                piece_count
            ],
            remaining: piece_count,
            cursor: 0,
            endgame_threshold,
        }
    }

    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    pub fn add_peer_bitfield(&mut self, bitfield: &[u8]) -> Result<(), BitfieldError> {
        self.update_availability(bitfield, true)
    }

    pub fn remove_peer_bitfield(&mut self, bitfield: &[u8]) -> Result<(), BitfieldError> {
        self.update_availability(bitfield, false)
    }

    pub fn add_peer_piece(&mut self, index: usize) -> Result<(), BitfieldError> {
        let pieces = self.pieces.len();
        let piece = self
            .pieces
            .get_mut(index)
            .ok_or(BitfieldError::Index { index, pieces })?;
        piece.availability = piece.availability.saturating_add(1);
        Ok(())
    }

    pub fn set_enabled(
        &mut self,
        start: usize,
        end: usize,
        enabled: bool,
    ) -> Result<(), BitfieldError> {
        let pieces = self.pieces.len();
        let range = self
            .pieces
            .get_mut(start..end)
            .ok_or(BitfieldError::Range { start, end, pieces })?;
        for piece in range {
            piece.enabled = enabled;
        }
        Ok(())
    }

    pub fn mark_complete(&mut self, index: usize) -> Result<(), BitfieldError> {
        let pieces = self.pieces.len();
        let piece = self
            .pieces
            .get_mut(index)
            .ok_or(BitfieldError::Index { index, pieces })?;
        if piece.state != PieceState::Complete {
            piece.state = PieceState::Complete;
            self.remaining = self.remaining.saturating_sub(1);
        }
        Ok(())
    }

    pub fn mark_request_failed(&mut self, index: usize) -> Result<(), BitfieldError> {
        let pieces = self.pieces.len();
        let piece = self
            .pieces
            .get_mut(index)
            .ok_or(BitfieldError::Index { index, pieces })?;
        piece.state = match piece.state {
            PieceState::Requested(copies) if copies > 1 => PieceState::Requested(copies - 1),
            PieceState::Requested(_) => PieceState::Missing,
            state => state,
        };
        Ok(())
    }

    pub fn select(
        &mut self,
        peer_bitfield: &[u8],
        mode: SelectionMode,
    ) -> Result<Option<usize>, BitfieldError> {
        validate_bitfield(peer_bitfield, self.pieces.len())?;
        if self.pieces.is_empty() {
            return Ok(None);
        }
        let endgame = self.remaining <= self.endgame_threshold;
        let selected = match mode {
            SelectionMode::Sequential => self.select_sequential(peer_bitfield, endgame),
            SelectionMode::RarestFirst => self.select_rarest(peer_bitfield, endgame),
        };
        if let Some(index) = selected {
            let piece = &mut self.pieces[index];
            piece.state = match piece.state {
                PieceState::Missing => PieceState::Requested(1),
                PieceState::Requested(copies) => PieceState::Requested(copies.saturating_add(1)),
                PieceState::Complete => PieceState::Complete,
            };
            self.cursor = (index + 1) % self.pieces.len();
        }
        Ok(selected)
    }

    fn select_sequential(&self, peer_bitfield: &[u8], endgame: bool) -> Option<usize> {
        self.pieces
            .iter()
            .enumerate()
            .find(|(index, piece)| selectable(*index, **piece, peer_bitfield, endgame))
            .map(|(index, _)| index)
    }

    fn select_rarest(&self, peer_bitfield: &[u8], endgame: bool) -> Option<usize> {
        (0..self.pieces.len())
            .map(|relative| (self.cursor + relative) % self.pieces.len())
            .filter(|index| selectable(*index, self.pieces[*index], peer_bitfield, endgame))
            .min_by_key(|index| self.pieces[*index].availability)
    }

    fn update_availability(&mut self, bitfield: &[u8], add: bool) -> Result<(), BitfieldError> {
        validate_bitfield(bitfield, self.pieces.len())?;
        for (index, piece) in self.pieces.iter_mut().enumerate() {
            if bit_is_set(bitfield, index) {
                piece.availability = if add {
                    piece.availability.saturating_add(1)
                } else {
                    piece.availability.saturating_sub(1)
                };
            }
        }
        Ok(())
    }
}

fn selectable(index: usize, piece: Piece, bitfield: &[u8], endgame: bool) -> bool {
    piece.enabled
        && piece.availability > 0
        && bit_is_set(bitfield, index)
        && match piece.state {
            PieceState::Missing => true,
            PieceState::Requested(copies) => endgame && copies < 2,
            PieceState::Complete => false,
        }
}

fn validate_bitfield(bitfield: &[u8], pieces: usize) -> Result<(), BitfieldError> {
    let expected = pieces.div_ceil(8);
    if bitfield.len() != expected {
        return Err(BitfieldError::Length {
            expected,
            actual: bitfield.len(),
        });
    }
    let spare = expected.saturating_mul(8).saturating_sub(pieces);
    if spare > 0
        && bitfield
            .last()
            .is_some_and(|last| last & ((1_u8 << spare) - 1) != 0)
    {
        return Err(BitfieldError::SpareBits);
    }
    Ok(())
}

fn bit_is_set(bitfield: &[u8], index: usize) -> bool {
    bitfield
        .get(index / 8)
        .is_some_and(|byte| byte & (0x80 >> (index % 8)) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rarest_piece_is_selected() -> Result<(), BitfieldError> {
        let mut picker = PiecePicker::new(4, 1);
        picker.add_peer_bitfield(&[0b1111_0000])?;
        picker.add_peer_bitfield(&[0b1010_0000])?;
        assert_eq!(
            picker.select(&[0b1111_0000], SelectionMode::RarestFirst)?,
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn have_updates_piece_availability() -> Result<(), BitfieldError> {
        let mut picker = PiecePicker::new(4, 1);
        let bitfield = [0b0100_0000];
        picker.add_peer_piece(1)?;
        assert_eq!(
            picker.select(&bitfield, SelectionMode::RarestFirst)?,
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn completed_and_disabled_pieces_are_skipped() -> Result<(), BitfieldError> {
        let mut picker = PiecePicker::new(3, 1);
        picker.add_peer_bitfield(&[0b1110_0000])?;
        picker.mark_complete(0)?;
        picker.set_enabled(1, 2, false)?;
        assert_eq!(
            picker.select(&[0b1110_0000], SelectionMode::Sequential)?,
            Some(2)
        );
        Ok(())
    }

    #[test]
    fn endgame_allows_at_most_two_requests() -> Result<(), BitfieldError> {
        let mut picker = PiecePicker::new(1, 1);
        picker.add_peer_bitfield(&[0b1000_0000])?;
        assert_eq!(
            picker.select(&[0b1000_0000], SelectionMode::RarestFirst)?,
            Some(0)
        );
        assert_eq!(
            picker.select(&[0b1000_0000], SelectionMode::RarestFirst)?,
            Some(0)
        );
        assert_eq!(
            picker.select(&[0b1000_0000], SelectionMode::RarestFirst)?,
            None
        );
        Ok(())
    }

    #[test]
    fn rejects_spare_bits() {
        let mut picker = PiecePicker::new(3, 1);
        assert_eq!(
            picker.add_peer_bitfield(&[0b1110_0001]),
            Err(BitfieldError::SpareBits)
        );
    }
}
