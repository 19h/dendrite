#![no_main]

use libfuzzer_sys::fuzz_target;
use dendrite_net::{
    dht,
    extension::{
        decode_extension_handshake, decode_holepunch_message, decode_metadata_message,
        decode_pex_message,
    },
    lsd,
};

fuzz_target!(|data: &[u8]| {
    let _ = dht::decode_message(data);
    let _ = lsd::decode_announce(data);
    let _ = decode_extension_handshake(data, 1024 * 1024);
    let _ = decode_metadata_message(data, 1024 * 1024);
    let _ = decode_pex_message(data);
    let _ = decode_holepunch_message(data);
});
