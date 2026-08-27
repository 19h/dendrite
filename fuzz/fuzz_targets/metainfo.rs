#![no_main]

use libfuzzer_sys::fuzz_target;
use dendrite_metainfo::{BencodeLimits, Metainfo, decode};

fuzz_target!(|data: &[u8]| {
    let limit = data.len().min(1024 * 1024);
    let limits = BencodeLimits {
        input_bytes: limit,
        byte_string_bytes: limit,
        nodes: 4096,
        collection_items: 2048,
        depth: 64,
        canonical_dictionaries: true,
    };
    let _ = decode(data, limits);
    let _ = Metainfo::parse(data, limits);
});
