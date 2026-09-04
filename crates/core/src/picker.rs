use thiserror::Error;

const RAREST_CANDIDATE_SAMPLE: usize = 256;
const WORD_BITS: usize = 64;

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

/// Rarest-first piece selection over a torrent.
///
/// Selection cost is proportional to the number of candidate pieces actually
/// examined rather than to the torrent size: two word-level bitsets track the
/// pieces that are currently selectable outside and inside endgame, and a
/// peer's bitfield is intersected with them eight pieces at a time. A peer that
/// has nothing selectable costs one sequential pass over `pieces / 64` words.
#[derive(Clone, Debug)]
pub struct PiecePicker {
    pieces: Vec<Piece>,
    /// Bit set for every enabled piece in `PieceState::Missing`. Bits are
    /// most-significant-first within each word so that word `w` has the same
    /// layout as the eight peer-bitfield bytes starting at `w * 8`.
    missing: Vec<u64>,
    /// Bit set for every enabled piece in `PieceState::Requested(1)`, which is
    /// selectable only during endgame.
    requested_once: Vec<u64>,
    remaining: usize,
    cursor: usize,
    endgame_threshold: usize,
    generation: u64,
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
        let words = piece_count.div_ceil(WORD_BITS);
        let mut missing = vec![u64::MAX; words];
        let spare = words.saturating_mul(WORD_BITS).saturating_sub(piece_count);
        if spare > 0
            && let Some(last) = missing.last_mut()
        {
            *last &= u64::MAX << spare;
        }
        Self {
            pieces: vec![
                Piece {
                    availability: 0,
                    enabled: true,
                    state: PieceState::Missing,
                };
                piece_count
            ],
            missing,
            requested_once: vec![0; words],
            remaining: piece_count,
            cursor: 0,
            endgame_threshold,
            generation: 0,
        }
    }

    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.remaining
    }

    /// Selectability generation.
    ///
    /// The value changes only when a piece may have become selectable for a
    /// registered peer whose own bitfield did not change: a request failure
    /// that returns a piece to the pool, re-enabling a range, or entering
    /// endgame. If `select` returned `None` for a bitfield that was registered
    /// with `add_peer_bitfield` (so every piece it holds has availability of at
    /// least one), the generation is unchanged, and that bitfield gained no
    /// bits, `select` would still return `None`. Availability changes caused by
    /// other peers cannot change that answer, so they do not bump the
    /// generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
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
        for index in start..end {
            self.refresh_candidate(index);
        }
        if enabled && start < end {
            self.generation = self.generation.wrapping_add(1);
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
            let was_endgame = self.remaining <= self.endgame_threshold;
            self.remaining = self.remaining.saturating_sub(1);
            self.refresh_candidate(index);
            if !was_endgame && self.remaining <= self.endgame_threshold {
                self.generation = self.generation.wrapping_add(1);
            }
        }
        Ok(())
    }

    pub fn mark_request_failed(&mut self, index: usize) -> Result<(), BitfieldError> {
        let pieces = self.pieces.len();
        let piece = self
            .pieces
            .get_mut(index)
            .ok_or(BitfieldError::Index { index, pieces })?;
        let previous = piece.state;
        piece.state = match piece.state {
            PieceState::Requested(copies) if copies > 1 => PieceState::Requested(copies - 1),
            PieceState::Requested(_) => PieceState::Missing,
            state => state,
        };
        if piece.state != previous {
            self.refresh_candidate(index);
            self.generation = self.generation.wrapping_add(1);
        }
        Ok(())
    }

    pub fn select(
        &mut self,
        peer_bitfield: &[u8],
        mode: SelectionMode,
    ) -> Result<Option<usize>, BitfieldError> {
        self.select_where(peer_bitfield, mode, |_| true)
    }

    /// Like `select`, but only pieces accepted by `accept` are candidates.
    /// Rejected pieces are skipped without affecting the candidate sample or
    /// the cursor, so a caller can exclude pieces it already holds during
    /// endgame without perturbing the rarest-first order.
    pub fn select_where(
        &mut self,
        peer_bitfield: &[u8],
        mode: SelectionMode,
        accept: impl Fn(usize) -> bool,
    ) -> Result<Option<usize>, BitfieldError> {
        validate_bitfield(peer_bitfield, self.pieces.len())?;
        if self.pieces.is_empty() {
            return Ok(None);
        }
        let endgame = self.remaining <= self.endgame_threshold;
        let selected = match mode {
            SelectionMode::Sequential => self.select_sequential(peer_bitfield, endgame, &accept),
            SelectionMode::RarestFirst => self.select_rarest(peer_bitfield, endgame, &accept),
        };
        if let Some(index) = selected {
            let piece = &mut self.pieces[index];
            piece.state = match piece.state {
                PieceState::Missing => PieceState::Requested(1),
                PieceState::Requested(copies) => PieceState::Requested(copies.saturating_add(1)),
                PieceState::Complete => PieceState::Complete,
            };
            self.refresh_candidate(index);
            self.cursor = (index + 1) % self.pieces.len();
        }
        Ok(selected)
    }

    fn candidate_word(&self, word: usize, endgame: bool) -> u64 {
        let missing = self.missing.get(word).copied().unwrap_or(0);
        if endgame {
            missing | self.requested_once.get(word).copied().unwrap_or(0)
        } else {
            missing
        }
    }

    fn select_sequential(
        &self,
        peer_bitfield: &[u8],
        endgame: bool,
        accept: &impl Fn(usize) -> bool,
    ) -> Option<usize> {
        for word in 0..self.missing.len() {
            let mut bits = self.candidate_word(word, endgame) & peer_word(peer_bitfield, word);
            while bits != 0 {
                let lane = bits.leading_zeros();
                bits &= !(1_u64 << (63 - lane));
                let index = word * WORD_BITS + lane as usize;
                if self.pieces[index].availability > 0 && accept(index) {
                    return Some(index);
                }
            }
        }
        None
    }

    /// Visits pieces in ascending index order starting at the cursor, wrapping
    /// once, and returns the first piece with the lowest availability among the
    /// first `RAREST_CANDIDATE_SAMPLE` selectable candidates. A piece held by a
    /// single peer wins immediately.
    fn select_rarest(
        &self,
        peer_bitfield: &[u8],
        endgame: bool,
        accept: &impl Fn(usize) -> bool,
    ) -> Option<usize> {
        let words = self.missing.len();
        let start_word = self.cursor / WORD_BITS;
        let start_lane = u32::try_from(self.cursor % WORD_BITS).unwrap_or(0);
        let head_mask = u64::MAX >> start_lane;
        let mut best: Option<(usize, u32)> = None;
        let mut candidates = 0_usize;
        for step in 0..=words {
            let mut word = start_word + step;
            if word >= words {
                word -= words;
            }
            let mut bits = self.candidate_word(word, endgame) & peer_word(peer_bitfield, word);
            if step == 0 {
                bits &= head_mask;
            } else if step == words {
                bits &= !head_mask;
            }
            while bits != 0 {
                let lane = bits.leading_zeros();
                bits &= !(1_u64 << (63 - lane));
                let index = word * WORD_BITS + lane as usize;
                let availability = self.pieces[index].availability;
                if availability == 0 || !accept(index) {
                    continue;
                }
                candidates += 1;
                if best.is_none_or(|(_, current)| availability < current) {
                    best = Some((index, availability));
                    if availability == 1 {
                        return Some(index);
                    }
                }
                if candidates >= RAREST_CANDIDATE_SAMPLE {
                    return best.map(|(index, _)| index);
                }
            }
        }
        best.map(|(index, _)| index)
    }

    fn refresh_candidate(&mut self, index: usize) {
        let Some(piece) = self.pieces.get(index).copied() else {
            return;
        };
        let word = index / WORD_BITS;
        let mask = 1_u64 << (63 - (index % WORD_BITS));
        let missing = piece.enabled && piece.state == PieceState::Missing;
        let requested_once = piece.enabled && piece.state == PieceState::Requested(1);
        if let Some(slot) = self.missing.get_mut(word) {
            if missing {
                *slot |= mask;
            } else {
                *slot &= !mask;
            }
        }
        if let Some(slot) = self.requested_once.get_mut(word) {
            if requested_once {
                *slot |= mask;
            } else {
                *slot &= !mask;
            }
        }
    }

    fn update_availability(&mut self, bitfield: &[u8], add: bool) -> Result<(), BitfieldError> {
        validate_bitfield(bitfield, self.pieces.len())?;
        for word in 0..self.missing.len() {
            let mut bits = peer_word(bitfield, word);
            while bits != 0 {
                let lane = bits.leading_zeros();
                bits &= !(1_u64 << (63 - lane));
                let index = word * WORD_BITS + lane as usize;
                if let Some(piece) = self.pieces.get_mut(index) {
                    piece.availability = if add {
                        piece.availability.saturating_add(1)
                    } else {
                        piece.availability.saturating_sub(1)
                    };
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn check_invariants(&self) {
        let mut remaining = 0;
        for (index, piece) in self.pieces.iter().enumerate() {
            let word = index / WORD_BITS;
            let mask = 1_u64 << (63 - (index % WORD_BITS));
            let missing = self.missing[word] & mask != 0;
            let requested_once = self.requested_once[word] & mask != 0;
            assert_eq!(
                missing,
                piece.enabled && piece.state == PieceState::Missing,
                "missing bit for piece {index}"
            );
            assert_eq!(
                requested_once,
                piece.enabled && piece.state == PieceState::Requested(1),
                "requested-once bit for piece {index}"
            );
            if piece.state != PieceState::Complete {
                remaining += 1;
            }
        }
        assert_eq!(remaining, self.remaining);
        let spare = self.missing.len() * WORD_BITS - self.pieces.len();
        if spare > 0 {
            let tail = u64::MAX >> (WORD_BITS - spare);
            assert_eq!(self.missing.last().copied().unwrap_or(0) & tail, 0);
            assert_eq!(self.requested_once.last().copied().unwrap_or(0) & tail, 0);
        }
    }
}

/// Eight bytes of a peer bitfield as one most-significant-first word; bytes
/// past the end of the bitfield read as zero.
fn peer_word(bitfield: &[u8], word: usize) -> u64 {
    let start = word * 8;
    let mut bytes = [0_u8; 8];
    if let Some(source) = bitfield.get(start..) {
        let length = source.len().min(8);
        if let (Some(target), Some(source)) = (bytes.get_mut(..length), source.get(..length)) {
            target.copy_from_slice(source);
        }
    }
    u64::from_be_bytes(bytes)
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

    #[test]
    fn cursor_wraps_across_word_boundaries() -> Result<(), BitfieldError> {
        for pieces in [64_usize, 65, 129, 200] {
            let mut picker = PiecePicker::new(pieces, 1);
            let bitfield = full_bitfield(pieces);
            picker.add_peer_bitfield(&bitfield)?;
            picker.add_peer_bitfield(&bitfield)?;
            let mut seen = Vec::new();
            for _ in 0..pieces {
                let selected = picker.select(&bitfield, SelectionMode::RarestFirst)?;
                seen.push(selected);
            }
            let expected: Vec<_> = (0..pieces).map(Some).collect();
            assert_eq!(seen, expected, "pieces={pieces}");
            assert_eq!(
                picker.select(&bitfield, SelectionMode::RarestFirst)?,
                None,
                "pieces={pieces}"
            );
        }
        Ok(())
    }

    #[test]
    fn sparse_peer_with_only_complete_pieces_yields_none() -> Result<(), BitfieldError> {
        let pieces = 100_000;
        let mut picker = PiecePicker::new(pieces, 4);
        let seed = full_bitfield(pieces);
        picker.add_peer_bitfield(&seed)?;
        let mut sparse = vec![0_u8; pieces.div_ceil(8)];
        sparse[7] = 0b0000_1000;
        picker.add_peer_bitfield(&sparse)?;
        picker.mark_complete(60)?;
        assert_eq!(picker.select(&sparse, SelectionMode::RarestFirst)?, None);
        assert_eq!(picker.select(&sparse, SelectionMode::Sequential)?, None);
        picker.check_invariants();
        Ok(())
    }

    #[test]
    fn generation_changes_only_on_selectability_gains() -> Result<(), BitfieldError> {
        let mut picker = PiecePicker::new(8, 2);
        let bitfield = [0b1111_1111];
        let start = picker.generation();
        picker.add_peer_bitfield(&bitfield)?;
        picker.add_peer_piece(3)?;
        picker.remove_peer_bitfield(&bitfield)?;
        picker.add_peer_bitfield(&bitfield)?;
        assert_eq!(picker.generation(), start, "availability must not bump");
        let selected = picker
            .select(&bitfield, SelectionMode::RarestFirst)?
            .ok_or(BitfieldError::Index {
                index: 0,
                pieces: 8,
            })?;
        assert_eq!(picker.generation(), start, "select must not bump");
        picker.mark_complete(selected)?;
        assert_eq!(
            picker.generation(),
            start,
            "non-endgame completion must not bump"
        );
        picker.set_enabled(0, 4, false)?;
        assert_eq!(picker.generation(), start, "disabling must not bump");
        picker.set_enabled(0, 4, true)?;
        assert_eq!(picker.generation(), start + 1, "enabling bumps");
        let requested = picker
            .select(&bitfield, SelectionMode::RarestFirst)?
            .ok_or(BitfieldError::Index {
                index: 0,
                pieces: 8,
            })?;
        picker.mark_request_failed(requested)?;
        assert_eq!(picker.generation(), start + 2, "request failure bumps");
        picker.mark_request_failed(requested)?;
        assert_eq!(
            picker.generation(),
            start + 2,
            "no-op failure does not bump"
        );
        for index in 0..8 {
            if index != requested {
                picker.mark_complete(index)?;
            }
        }
        assert_eq!(picker.remaining(), 1);
        assert_eq!(
            picker.generation(),
            start + 3,
            "entering endgame bumps once"
        );
        picker.check_invariants();
        Ok(())
    }

    #[test]
    fn skip_contract_holds_while_generation_is_stable() -> Result<(), BitfieldError> {
        let mut random = XorShift64(0x9e37_79b9_7f4a_7c15);
        for pieces in [1_usize, 7, 63, 64, 65, 200] {
            for _ in 0..40 {
                let mut picker = PiecePicker::new(pieces, 3);
                let peers: Vec<_> = (0..4)
                    .map(|_| random_bitfield(&mut random, pieces))
                    .collect();
                for peer in &peers {
                    picker.add_peer_bitfield(peer)?;
                }
                for _ in 0..pieces * 2 {
                    let peer = &peers[random.below(peers.len())];
                    if let Some(piece) = picker.select(peer, SelectionMode::RarestFirst)?
                        && random.below(3) == 0
                    {
                        picker.mark_complete(piece)?;
                    }
                }
                let probe = random_bitfield(&mut random, pieces);
                picker.add_peer_bitfield(&probe)?;
                if picker.select(&probe, SelectionMode::RarestFirst)?.is_some() {
                    continue;
                }
                let generation = picker.generation();
                for _ in 0..pieces {
                    match random.below(4) {
                        0 => picker.add_peer_bitfield(&peers[random.below(peers.len())])?,
                        1 => picker.add_peer_piece(random.below(pieces))?,
                        2 => {
                            let other = &peers[random.below(peers.len())];
                            let _ = picker.select(other, SelectionMode::RarestFirst)?;
                        }
                        _ => {
                            let index = random.below(pieces);
                            if picker.remaining() > 4 {
                                picker.mark_complete(index)?;
                            }
                        }
                    }
                    if picker.generation() != generation {
                        break;
                    }
                    assert_eq!(
                        picker.select(&probe, SelectionMode::RarestFirst)?,
                        None,
                        "pieces={pieces}"
                    );
                }
                picker.check_invariants();
            }
        }
        Ok(())
    }

    #[test]
    fn matches_reference_implementation_on_random_sequences() -> Result<(), BitfieldError> {
        let mut random = XorShift64(0x2545_f491_4f6c_dd1d);
        for pieces in [1_usize, 7, 63, 64, 65, 200, 4097] {
            let rounds = if pieces > 1000 { 2 } else { 12 };
            for round in 0..rounds {
                let threshold = random.below(8);
                let mut picker = PiecePicker::new(pieces, threshold);
                let mut reference = reference::PiecePicker::new(pieces, threshold);
                let peers: Vec<_> = (0..=random.below(8))
                    .map(|_| random_bitfield(&mut random, pieces))
                    .collect();
                let mut registered = vec![false; peers.len()];
                let mut selected = Vec::new();
                for step in 0..pieces * 2 + 32 {
                    let peer = random.below(peers.len());
                    match random.below(10) {
                        0 => {
                            registered[peer] = true;
                            picker.add_peer_bitfield(&peers[peer])?;
                            reference.add_peer_bitfield(&peers[peer])?;
                        }
                        1 if registered[peer] => {
                            registered[peer] = false;
                            picker.remove_peer_bitfield(&peers[peer])?;
                            reference.remove_peer_bitfield(&peers[peer])?;
                        }
                        2 => {
                            let index = random.below(pieces);
                            picker.add_peer_piece(index)?;
                            reference.add_peer_piece(index)?;
                        }
                        3 if !selected.is_empty() => {
                            let index = selected[random.below(selected.len())];
                            picker.mark_complete(index)?;
                            reference.mark_complete(index)?;
                        }
                        4 if !selected.is_empty() => {
                            let index = selected[random.below(selected.len())];
                            picker.mark_request_failed(index)?;
                            reference.mark_request_failed(index)?;
                        }
                        5 => {
                            let start = random.below(pieces + 1);
                            let end = start + random.below(pieces + 1 - start);
                            let enabled = random.below(2) == 0;
                            picker.set_enabled(start, end, enabled)?;
                            reference.set_enabled(start, end, enabled)?;
                        }
                        _ => {
                            let mode = if random.below(4) == 0 {
                                SelectionMode::Sequential
                            } else {
                                SelectionMode::RarestFirst
                            };
                            let reference_mode = match mode {
                                SelectionMode::Sequential => reference::SelectionMode::Sequential,
                                SelectionMode::RarestFirst => reference::SelectionMode::RarestFirst,
                            };
                            let actual = picker.select(&peers[peer], mode)?;
                            let expected = reference.select(&peers[peer], reference_mode)?;
                            assert_eq!(
                                actual, expected,
                                "pieces={pieces} round={round} step={step} mode={mode:?}"
                            );
                            if let Some(index) = actual {
                                selected.push(index);
                            }
                        }
                    }
                    assert_eq!(picker.remaining(), reference.remaining());
                    picker.check_invariants();
                }
            }
        }
        Ok(())
    }

    #[test]
    fn select_where_skips_rejected_pieces_in_endgame() -> Result<(), BitfieldError> {
        let mut picker = PiecePicker::new(2, 4);
        let bitfield = [0b1100_0000];
        picker.add_peer_bitfield(&bitfield)?;
        let first = picker.select(&bitfield, SelectionMode::RarestFirst)?;
        assert_eq!(first, Some(0));
        let second =
            picker.select_where(&bitfield, SelectionMode::RarestFirst, |piece| piece != 0)?;
        assert_eq!(second, Some(1));
        let third = picker.select_where(&bitfield, SelectionMode::RarestFirst, |piece| {
            piece != 0 && piece != 1
        })?;
        assert_eq!(third, None);
        assert_eq!(
            picker.select(&bitfield, SelectionMode::RarestFirst)?,
            Some(0)
        );
        picker.check_invariants();
        Ok(())
    }

    fn full_bitfield(pieces: usize) -> Vec<u8> {
        let mut bitfield = vec![0_u8; pieces.div_ceil(8)];
        for index in 0..pieces {
            bitfield[index / 8] |= 0x80 >> (index % 8);
        }
        bitfield
    }

    fn random_bitfield(random: &mut XorShift64, pieces: usize) -> Vec<u8> {
        let density = random.below(4);
        let mut bitfield = vec![0_u8; pieces.div_ceil(8)];
        for index in 0..pieces {
            let set = match density {
                0 => true,
                1 => random.below(2) == 0,
                2 => random.below(16) == 0,
                _ => random.below(pieces.max(2) * 2) == 0,
            };
            if set {
                bitfield[index / 8] |= 0x80 >> (index % 8);
            }
        }
        bitfield
    }

    struct XorShift64(u64);

    impl XorShift64 {
        fn next(&mut self) -> u64 {
            let mut value = self.0;
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;
            self.0 = value;
            value
        }

        fn below(&mut self, bound: usize) -> usize {
            if bound == 0 {
                return 0;
            }
            usize::try_from(self.next() % bound as u64).unwrap_or(0)
        }
    }

    /// The pre-bitset picker, kept verbatim as the behavioural oracle.
    mod reference {
        use super::super::BitfieldError;

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

        pub struct PiecePicker {
            pieces: Vec<Piece>,
            remaining: usize,
            cursor: usize,
            endgame_threshold: usize,
        }

        impl PiecePicker {
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

            pub fn remaining(&self) -> usize {
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
                    PieceState::Requested(copies) if copies > 1 => {
                        PieceState::Requested(copies - 1)
                    }
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
                        PieceState::Requested(copies) => {
                            PieceState::Requested(copies.saturating_add(1))
                        }
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
                let mut selected: Option<usize> = None;
                let mut candidates = 0_usize;
                for relative in 0..self.pieces.len() {
                    let index = (self.cursor + relative) % self.pieces.len();
                    let piece = self.pieces[index];
                    if !selectable(index, piece, peer_bitfield, endgame) {
                        continue;
                    }
                    candidates += 1;
                    if selected.is_none_or(|current| {
                        piece.availability < self.pieces[current].availability
                    }) {
                        selected = Some(index);
                        if piece.availability == 1 {
                            break;
                        }
                    }
                    if candidates >= super::super::RAREST_CANDIDATE_SAMPLE {
                        break;
                    }
                }
                selected
            }

            fn update_availability(
                &mut self,
                bitfield: &[u8],
                add: bool,
            ) -> Result<(), BitfieldError> {
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
            super::super::validate_bitfield(bitfield, pieces)
        }

        fn bit_is_set(bitfield: &[u8], index: usize) -> bool {
            bitfield
                .get(index / 8)
                .is_some_and(|byte| byte & (0x80 >> (index % 8)) != 0)
        }
    }
}
