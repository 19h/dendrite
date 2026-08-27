//! Reproducible fault simulation for scheduler invariants.

use dendrite_core::{BitfieldError, PiecePicker, SelectionMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct SimulationConfig {
    pub seed: u64,
    pub pieces: usize,
    pub peers: usize,
    pub maximum_steps: usize,
    pub corruption_per_mille: u16,
    pub churn_per_mille: u16,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            pieces: 1_024,
            peers: 32,
            maximum_steps: 1_000_000,
            corruption_per_mille: 10,
            churn_per_mille: 5,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SimulationReport {
    pub seed: u64,
    pub steps: usize,
    pub completed_pieces: usize,
    pub rejected_corrupt_pieces: usize,
    pub churn_events: usize,
    pub complete: bool,
}

#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("pieces and peers must both be nonzero")]
    Empty,
    #[error("fault probabilities must not exceed 1000 per mille")]
    Probability,
    #[error(transparent)]
    Bitfield(#[from] BitfieldError),
}

pub fn run(config: SimulationConfig) -> Result<SimulationReport, SimulationError> {
    if config.pieces == 0 || config.peers == 0 {
        return Err(SimulationError::Empty);
    }
    if config.corruption_per_mille > 1_000 || config.churn_per_mille > 1_000 {
        return Err(SimulationError::Probability);
    }
    let mut random = XorShift64::new(config.seed);
    let mut peers = make_peers(config.pieces, config.peers, &mut random);
    let mut active = vec![true; peers.len()];
    let mut picker = PiecePicker::new(config.pieces, 16.min(config.pieces));
    for bitfield in &peers {
        picker.add_peer_bitfield(bitfield)?;
    }
    let mut rejected = 0;
    let mut churn = 0;
    let mut steps = 0;
    while picker.remaining() > 0 && steps < config.maximum_steps {
        steps += 1;
        let peer = random.index(peers.len());
        if peer != 0 && random.per_mille(config.churn_per_mille) {
            if active[peer] {
                picker.remove_peer_bitfield(&peers[peer])?;
            } else {
                randomize_bitfield(&mut peers[peer], config.pieces, &mut random);
                picker.add_peer_bitfield(&peers[peer])?;
            }
            active[peer] = !active[peer];
            churn += 1;
            continue;
        }
        if !active[peer] {
            continue;
        }
        if let Some(piece) = picker.select(&peers[peer], SelectionMode::RarestFirst)? {
            if random.per_mille(config.corruption_per_mille) {
                picker.mark_request_failed(piece)?;
                rejected += 1;
            } else {
                picker.mark_complete(piece)?;
            }
        }
    }
    Ok(SimulationReport {
        seed: config.seed,
        steps,
        completed_pieces: config.pieces - picker.remaining(),
        rejected_corrupt_pieces: rejected,
        churn_events: churn,
        complete: picker.remaining() == 0,
    })
}

fn make_peers(pieces: usize, count: usize, random: &mut XorShift64) -> Vec<Vec<u8>> {
    let mut peers = Vec::with_capacity(count);
    peers.push(full_bitfield(pieces));
    for _ in 1..count {
        let mut bitfield = vec![0; pieces.div_ceil(8)];
        randomize_bitfield(&mut bitfield, pieces, random);
        peers.push(bitfield);
    }
    peers
}

fn full_bitfield(pieces: usize) -> Vec<u8> {
    let mut bitfield = vec![u8::MAX; pieces.div_ceil(8)];
    clear_spare_bits(&mut bitfield, pieces);
    bitfield
}

fn randomize_bitfield(bitfield: &mut [u8], pieces: usize, random: &mut XorShift64) {
    for byte in bitfield.iter_mut() {
        *byte = random.next().to_le_bytes()[0];
    }
    clear_spare_bits(bitfield, pieces);
}

fn clear_spare_bits(bitfield: &mut [u8], pieces: usize) {
    if let Some(last) = bitfield.last_mut()
        && !pieces.is_multiple_of(8)
    {
        *last &= u8::MAX << (8 - pieces % 8);
    }
}

struct XorShift64(u64);

impl XorShift64 {
    const fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        })
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn index(&mut self, length: usize) -> usize {
        usize::try_from(self.next()).unwrap_or(usize::MAX) % length
    }

    fn per_mille(&mut self, probability: u16) -> bool {
        self.next() % 1_000 < u64::from(probability)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulation_is_reproducible_and_completes_despite_faults() -> Result<(), SimulationError> {
        let config = SimulationConfig {
            seed: 0x5eed,
            pieces: 257,
            peers: 12,
            maximum_steps: 100_000,
            corruption_per_mille: 100,
            churn_per_mille: 100,
        };
        let first = run(config)?;
        let second = run(config)?;
        assert_eq!(first, second);
        assert!(first.complete);
        assert_eq!(first.completed_pieces, config.pieces);
        assert!(first.rejected_corrupt_pieces > 0);
        assert!(first.churn_events > 0);
        Ok(())
    }

    #[test]
    fn rejects_invalid_scenarios() {
        assert!(matches!(
            run(SimulationConfig {
                pieces: 0,
                ..SimulationConfig::default()
            }),
            Err(SimulationError::Empty)
        ));
        assert!(matches!(
            run(SimulationConfig {
                corruption_per_mille: 1_001,
                ..SimulationConfig::default()
            }),
            Err(SimulationError::Probability)
        ));
    }

    #[test]
    fn accelerated_fault_soak_covers_many_deterministic_schedules() -> Result<(), SimulationError> {
        run_fault_matrix(512)
    }

    #[test]
    fn virtual_week_keeps_many_torrents_fair_and_bounded() -> Result<(), SimulationError> {
        const TORRENTS: usize = 32;
        const PIECES: usize = 64;
        const MINUTES_PER_WEEK: usize = 7 * 24 * 60;
        let bitfield = full_bitfield(PIECES);
        let mut pickers = Vec::with_capacity(TORRENTS);
        for _ in 0..TORRENTS {
            let mut picker = PiecePicker::new(PIECES, 4);
            picker.add_peer_bitfield(&bitfield)?;
            pickers.push(picker);
        }
        let mut completed_downloads = vec![0_usize; TORRENTS];
        let mut random = XorShift64::new(0xfeed_f00d_dead_beef);

        for _minute in 0..MINUTES_PER_WEEK {
            for (torrent, picker) in pickers.iter_mut().enumerate() {
                let Some(piece) = picker.select(&bitfield, SelectionMode::RarestFirst)? else {
                    continue;
                };
                if random.per_mille(75) {
                    picker.mark_request_failed(piece)?;
                } else {
                    picker.mark_complete(piece)?;
                }
                if picker.remaining() == 0 {
                    completed_downloads[torrent] += 1;
                    *picker = PiecePicker::new(PIECES, 4);
                    picker.add_peer_bitfield(&bitfield)?;
                }
            }
        }

        let minimum = completed_downloads.iter().copied().min().unwrap_or(0);
        let maximum = completed_downloads.iter().copied().max().unwrap_or(0);
        assert!(minimum > 100, "every torrent must make sustained progress");
        assert!(maximum.saturating_sub(minimum) <= 2);
        assert_eq!(pickers.len(), TORRENTS);
        assert_eq!(completed_downloads.len(), TORRENTS);
        Ok(())
    }

    #[test]
    #[ignore = "extended soak; run explicitly or from the scheduled CI job"]
    fn extended_fault_soak() -> Result<(), SimulationError> {
        let cases = std::env::var("DENDRITE_SOAK_CASES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(100_000);
        run_fault_matrix(cases)
    }

    fn run_fault_matrix(cases: u64) -> Result<(), SimulationError> {
        for seed in 1..=cases {
            let config = SimulationConfig {
                seed: seed.wrapping_mul(0x9e37_79b9_7f4a_7c15),
                pieces: 33 + usize::try_from(seed % 224).unwrap_or(0),
                peers: 4 + usize::try_from(seed % 20).unwrap_or(0),
                maximum_steps: 100_000,
                corruption_per_mille: 25 + u16::try_from(seed % 276).unwrap_or(0),
                churn_per_mille: 25 + u16::try_from(seed.wrapping_mul(17) % 276).unwrap_or(0),
            };
            let report = run(config)?;
            assert!(
                report.complete,
                "seed {} stalled after {} steps with {} of {} pieces complete",
                report.seed, report.steps, report.completed_pieces, config.pieces
            );
        }
        Ok(())
    }
}
