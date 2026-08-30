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
}

criterion_group!(hot_paths, bencode_decode, peer_codec, rarest_first);
criterion_main!(hot_paths);
