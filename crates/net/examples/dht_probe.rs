//! Ad-hoc DHT lookup probe: `cargo run -p dendrite-net --example dht_probe -- <hex info hash>`.
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hash_hex = std::env::args()
        .nth(1)
        .ok_or("usage: dht_probe <hex info hash>")?;
    let mut bytes = [0_u8; 20];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hash_hex[index * 2..index * 2 + 2], 16)?;
    }
    let hash = dendrite_core::Sha1Hash::from_bytes(bytes);
    let client = dendrite_net::dht::DhtClient::bind(
        "0.0.0.0:0".parse()?,
        512,
        65_507,
        Duration::from_secs(2),
    )
    .await?;
    let bootstrap = ["87.98.162.88:6881".parse()?, "212.129.33.59:6881".parse()?];
    for round in 0..2 {
        let started = Instant::now();
        match client.get_peers(hash, &bootstrap).await {
            Ok(peers) => println!(
                "round {round}: {} peers in {:.1}s",
                peers.len(),
                started.elapsed().as_secs_f64()
            ),
            Err(error) => println!(
                "round {round}: error {error} after {:.1}s",
                started.elapsed().as_secs_f64()
            ),
        }
    }
    Ok(())
}
