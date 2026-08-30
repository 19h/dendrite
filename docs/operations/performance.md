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

Piece finalization is concurrent across peers but remains bounded to one
pending durable write per peer. Mutable completion state is stored separately
from immutable metainfo, and upload counters are flushed in per-torrent batches;
these invariants prevent metadata size or peer block rate from amplifying state
database writes.

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
