#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use dendrite_net::peer::{PeerCodecLimits, decode_message};

fuzz_target!(|data: &[u8]| {
    let mut input = BytesMut::from(data);
    let limits = PeerCodecLimits {
        frame_bytes: 256 * 1024,
        block_bytes: 16 * 1024,
        bitfield_bytes: 64 * 1024,
        extension_bytes: 64 * 1024,
        hash_bytes: 64 * 1024,
    };
    for _ in 0..128 {
        let before = input.len();
        match decode_message(&mut input, limits) {
            Ok(Some(_)) if input.len() < before => {}
            _ => break,
        }
    }
});
