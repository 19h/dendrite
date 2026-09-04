use std::hint::black_box;

use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use dendrite_core::{PiecePicker, SelectionMode};
use dendrite_metainfo::{BencodeLimits, decode};
use dendrite_net::peer::{PeerCodecLimits, PeerMessage, decode_message, encode_message};

fn bencode_decode(criterion: &mut Criterion) {
    let input = b"d8:announce14:http://tracker4:infod6:lengthi1048576e4:name7:file.bin12:piece lengthi16384e6:pieces20:01234567890123456789ee";
    criterion.bench_function("bencode_decode_strict", |bencher| {
        bencher.iter(|| decode(black_box(input), BencodeLimits::default()));
    });
}

fn peer_codec(criterion: &mut Criterion) {
    let message = PeerMessage::Piece {
        piece: 42,
        begin: 16_384,
        block: Bytes::from(vec![0x5a; 16 * 1024]),
    };
    let Ok(encoded) = encode_message(&message) else {
        return;
    };
    criterion.bench_function("peer_piece_decode_16k", |bencher| {
        bencher.iter_batched(
            || BytesMut::from(encoded.as_ref()),
            |mut bytes| decode_message(black_box(&mut bytes), PeerCodecLimits::default()),
            BatchSize::SmallInput,
        );
    });
}

fn rarest_first(criterion: &mut Criterion) {
    const PIECES: usize = 1_000_000;
    let bitfield = vec![u8::MAX; PIECES.div_ceil(8)];
    let mut populated = PiecePicker::new(PIECES, 16);
    for _ in 0..256 {
        let _ = populated.add_peer_bitfield(&bitfield);
    }
    criterion.bench_function("rarest_first_1m_pieces_256_peers", |bencher| {
        bencher.iter_batched(
            || populated.clone(),
            |mut picker| picker.select(black_box(&bitfield), SelectionMode::RarestFirst),
            BatchSize::LargeInput,
        );
    });

    // A peer whose only piece is already complete: the scan must visit every
    // candidate word and return `None`. This was the O(pieces) case that
    // dominated live profiles.
    let mut sparse = vec![0_u8; PIECES.div_ceil(8)];
    sparse[0] = 0x80;
    let mut seeded = PiecePicker::new(PIECES, 16);
    for _ in 0..8 {
        let _ = seeded.add_peer_bitfield(&bitfield);
    }
    let _ = seeded.add_peer_bitfield(&sparse);
    let _ = seeded.mark_complete(0);
    criterion.bench_function("rarest_first_1m_pieces_sparse_miss", |bencher| {
        bencher.iter_batched(
            || seeded.clone(),
            |mut picker| picker.select(black_box(&sparse), SelectionMode::RarestFirst),
            BatchSize::LargeInput,
        );
    });

    // Endgame with a handful of pieces left, most of them already requested.
    let mut endgame = PiecePicker::new(PIECES, 16);
    let _ = endgame.add_peer_bitfield(&bitfield);
    for piece in 8..PIECES {
        let _ = endgame.mark_complete(piece);
    }
    for _ in 0..4 {
        let _ = endgame.select(&bitfield, SelectionMode::RarestFirst);
    }
    criterion.bench_function("rarest_first_1m_pieces_endgame", |bencher| {
        bencher.iter_batched(
            || endgame.clone(),
            |mut picker| picker.select(black_box(&bitfield), SelectionMode::RarestFirst),
            BatchSize::LargeInput,
        );
    });

    let empty = PiecePicker::new(PIECES, 16);
    criterion.bench_function("add_peer_bitfield_1m_seed", |bencher| {
        bencher.iter_batched(
            || empty.clone(),
            |mut picker| picker.add_peer_bitfield(black_box(&bitfield)),
            BatchSize::LargeInput,
        );
    });
    criterion.bench_function("add_peer_bitfield_1m_sparse", |bencher| {
        bencher.iter_batched(
            || empty.clone(),
            |mut picker| picker.add_peer_bitfield(black_box(&sparse)),
            BatchSize::LargeInput,
        );
    });
}

criterion_group!(hot_paths, bencode_decode, peer_codec, rarest_first);
criterion_main!(hot_paths);
