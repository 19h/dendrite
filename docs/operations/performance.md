[← Documentation home](../../README.md)

# Performance and benchmarking

Dendrite keeps performance claims reproducible and path-specific. Parser,
peer-codec, piece-picker, storage, simulated-swarm, and real-network throughput
measure different layers; none is a universal “torrent speed” number.

## Included Criterion benchmarks

```sh
cargo bench --locked -p dendrite-benchmarks
```

Current cases:

| Benchmark | Workload |
|---|---|
| `bencode_decode_strict` | strict decode of one small metainfo-shaped dictionary |
| `peer_piece_decode_16k` | decode a 16 KiB peer-wire piece block |
| `rarest_first_1m_pieces_256_peers` | select from a populated million-piece, 256-peer availability model |

Criterion stores sampled results under `target/criterion`. CI compiles these
benchmarks but does not enforce wall-clock thresholds.

## Save and compare a baseline

On an otherwise idle, fixed host:

```sh
cargo bench --locked -p dendrite-benchmarks -- --save-baseline main
cargo bench --locked -p dendrite-benchmarks -- --baseline main
```

Record raw Criterion output, not only the direction arrow. CPU frequency,
contention, compiler patch level, kernel, allocator, and thermal state can exceed
small code effects.

## Deterministic swarm simulation

```sh
cargo run --locked -p dendrite-simulator -- \
  --seed 24301 \
  --pieces 4096 \
  --peers 64 \
  --maximum-steps 1000000 \
  --corruption-per-mille 100 \
  --churn-per-mille 100
```

The simulator exercises rarest-first scheduling, bounded endgame behavior,
corruption rejection, peer churn, and termination under a deterministic model.
It prints JSON and exits successfully only when the report is complete.

This is not a socket, tracker, filesystem, crypto, or remote-client benchmark.
Use it to compare algorithmic decisions and fault behavior under identical seeds.

The scheduled CI lane can run a longer ignored matrix through:

```sh
DENDRITE_SOAK_CASES=100000 \
cargo test --locked -p dendrite-simulator \
  tests::extended_fault_soak -- --ignored --exact
```

## DHT lookup probe

```sh
cargo run --locked -p dendrite-net --example dht_probe -- <hex info hash>
```

Runs two iterative lookups against the public bootstrap nodes and prints the
peer count and wall time of each; the second run exercises the node cache.

## End-to-end measurement

For real transfers, report at least:

- Dendrite commit and daemon version;
- host CPU, memory, OS/kernel, filesystem, and storage device;
- selected `portable` or `io_uring` backend;
- release profile and Rust version;
- torrent version, piece length, file count, and total bytes;
- local fixture or external network topology;
- peer count, TCP/uTP mix, encryption mode, tracker/DHT/LSD/PEX inputs;
- warm/cold page cache and preexisting payload state;
- elapsed distribution, downloaded/uploaded counters, and integrity result;
- logging level and concurrent torrents.

Use a controlled local peer for engine/storage throughput. Public internet speed
is dominated by swarm availability, remote policy, routing, NAT, and tracker
state, and is not a stable microbenchmark.

## Interpreting API rates

Torrent summary rates are sampled when summaries are requested. Download and
upload rates come from accepted peer blocks, so they respond before the compact
durable progress counters are flushed. Rates update after at least 250 ms and
use saturating arithmetic. The first sample is zero. Do not use one response as
a high-resolution benchmark or billing counter.

## Correctness gates for optimization

A throughput change is acceptable only with unchanged or improved correctness
evidence:

```sh
cargo test --workspace --all-targets --all-features --locked
cargo run --locked -p dendrite-simulator -- --seed 24301
```

For parser, peer-wire, tracker, extension, DHT, MSE, uTP, storage, or actor changes,
run the owning tests and relevant fuzz smoke target in addition to the benchmark.

Piece writes are concurrent across peers, and verified peers resume downloading
before a one-second group durability barrier commits their completion bits.
Full piece buffers are bounded globally by `limits.download_buffer_bytes`.
Piece verification runs on the blocking pool with at most half the CPU
threads hashing at once, using OpenSSL's assembly SHA-1 and SHA-256, which on
CPUs without SHA extensions run about twice as fast as the pure-Rust
implementations (1.1 GB/s versus 0.5 GB/s per core on a Skylake Xeon). Mutable counters, the completion bitfield, and the
immutable metainfo are stored in separate tables, upload counters are flushed
in per-torrent batches without a durable commit of their own, and summaries and
info-hash lookups are served from an in-memory mirror; these invariants prevent
metadata size, piece count, or peer block rate from amplifying state database
work.

Peer discovery runs an iterative DHT lookup (eight queries in flight,
converging on the sixteen closest nodes, collecting peers from every
responder, remembering responsive nodes for the next lookup) and announces the
listening port every fifteen minutes. Trackers are re-announced only when the
interval they returned has elapsed (clamped to one minute–four hours), while
DHT, local discovery, and PEX refresh every minute. Announces ask for 200
peers.

Candidate addresses are deduplicated for ten minutes rather than forever, so a
peer that dropped is dialled again when discovery reports it. Connected
addresses are never re-dialled.

Outgoing block requests and cancels are queued without waiting for the socket
write, and the writer coalesces every queued message into one write; socket
reads land directly in the frame buffer.

Piece selection uses word-level candidate bitsets and a selectability
generation: a peer that has nothing selectable costs one pass over
`pieces / 64` words and is not asked again until the generation changes. Each
peer keeps a rate-sized queue of assigned pieces and a request pipeline that
spans piece boundaries (128–1024 blocks, bounded by the remote `reqq`).

Outbound connection establishment is limited to 128 concurrent handshakes per
torrent even when the ready-peer ceiling is higher. This avoids synchronized
timeout waves consuming the entire candidate pool.

Incoming TCP and uTP handshakes share a separate 256-session admission bound.
The accept loops continue draining excess connections, while a reserved slice
of the global peer limit prevents outbound workers from starving inbound peer
admission.

Magnet metadata discovery can probe 32 candidates per torrent, while metadata
payload transfer is separately limited to four concurrent sessions across the
daemon. Each session pipelines up to 16 metadata block requests. Advertised
metadata size is allocated incrementally as validated blocks arrive. This
preserves broad discovery without letting simultaneous 64 MiB-limit magnets
multiply memory into gigabytes or one round trip serialize every block.

While downloading, verified byte rate, useful-piece availability, failures,
and upload/download reciprocity influence peer retention. Full seeds receive a
retention preference and scheduling priority; stale or choked non-seeds are
rotated so queued candidates can be classified. During download, regular
upload slots go to peers that hold pieces we still need, ordered by the rate at
which they deliver verified data; upload credit per peer is
`reciprocal_bootstrap_bytes` per connected hour plus `reciprocal_ratio` times
the verified bytes received from it. Optimistic slots rotate through the
remaining interesting peers with credit. In `seeding`, reciprocal credit is
disabled and recent upload rate supplies the fair slot ordering instead. A
global `upload_rate_limit_bytes` and a per-torrent `torrent_max_upload_ratio`
cap egress independently of slot policy; see the `[transfer]` section of the
configuration reference.

## Reproducible result template

```text
Dendrite commit/version:
date:
host CPU / memory:
host OS / kernel:
filesystem / storage:
Rust version:
Cargo command/profile/features:
storage backend:
workload and input hash:
peer/network topology:
configuration differences:
warm-up:
repetitions:
raw timings/distribution:
correctness checks:
```

## Performance non-claims

- A 16 KiB codec decode does not predict whole-swarm throughput.
- Picker selection over all-ones bitfields does not represent churn or sparse
  availability.
- io_uring initialization does not prove it outperforms portable I/O for a given
  filesystem/workload.
- API sampled rates are not kernel network counters.
- Simulator completion does not establish network interoperability.
- The release profile is optimized, but no repository-wide throughput SLA is
  defined.

## Related pages

- [Verification model](../development/verification.md)
- [Torrent lifecycle](../architecture/torrent-lifecycle.md)
- [Storage and security](../architecture/storage-security.md)
