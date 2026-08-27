[← Documentation home](../../README.md)

# Verification

The repository uses layered evidence. A unit test can prove a parser invariant;
an engine integration test can prove components work together locally; a
simulation can prove behavior under its model; none alone proves universal
internet interoperability or production readiness.

## Fast edit loop

Format and test the owning package first:

```sh
cargo fmt --all -- --check
cargo test --locked -p dendrite-config
```

Replace the package with the owner of the change. For daemon/client contract
changes, test both and compare live help:

```sh
cargo test --locked -p dendrite-daemon -p dendrite-cli
cargo run --quiet --locked -p dendrite-daemon -- --help
cargo run --quiet --locked -p dendrite-cli -- --help
```

## Full quality lane

The repository CI quality job attempts:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo run --locked -p dendrite-simulator -- \
  --seed 24301 \
  --pieces 4096 \
  --peers 64 \
  --corruption-per-mille 100 \
  --churn-per-mille 100
cargo bench --locked -p dendrite-benchmarks --no-run
cargo check --locked --manifest-path fuzz/Cargo.toml --bins
cargo build --release -p dendrite-daemon -p dendrite-cli --locked
```

Dependency/license/source policy runs separately through `cargo deny check` in
CI. Use the pinned Rust 1.94.0 toolchain when reproducing results.

## What the test layers cover

| Layer | Examples of evidence | Does not establish |
|---|---|---|
| core unit tests | UUID/hash encoding, portable paths, rarest-first/endgame rules | sockets, persistence, filesystem behavior |
| parser/codec unit tests | bounded bencode, magnets, metainfo, peer/tracker/DHT/extension frames | a complete remote transfer |
| persistence tests | atomic records/indexes, version checks, quarantine, fault-injected commit behavior | payload durability |
| storage tests | positional I/O, backend parity, sync/restart, ENOSPC/EIO, cancellation, link rejection | metadata or swarm correctness |
| daemon tests | route auth, CSRF, rotation, pagination, metrics, admission limits | public-network interoperability |
| engine integration tests | local TCP/uTP/MSE transfers, magnet/v2 exchange, trackers/LSD/PEX, web seeds, incoming seeding, cancellation, restart/recheck/crash ordering | every external implementation/topology |
| simulator | deterministic rarest-first, corruption, churn, endgame and termination model | real network, storage or crypto execution |
| fuzz targets | parser robustness over generated inputs while actually run | absence of bugs after compilation alone |
| Criterion | repeatable microbench distributions for named hot paths | end-to-end throughput or latency SLA |

## Deterministic simulation

```sh
cargo run --locked -p dendrite-simulator -- \
  --seed 24301 \
  --pieces 4096 \
  --peers 64 \
  --maximum-steps 1000000 \
  --corruption-per-mille 100 \
  --churn-per-mille 100
```

Keep the seed and every parameter in a regression report. Scheduled/manual CI
can execute the ignored 100,000-case matrix:

```sh
DENDRITE_SOAK_CASES=100000 \
cargo test --locked -p dendrite-simulator \
  tests::extended_fault_soak -- --ignored --exact
```

This can be expensive; it is not part of every push job.

## Fuzzing

Install `cargo-fuzz` through the Rust tooling path appropriate for the
development host, then run a bounded smoke session or a longer campaign:

```sh
cargo fuzz run metainfo -- -max_total_time=60
cargo fuzz run peer_wire -- -max_total_time=60
cargo fuzz run discovery_extensions -- -max_total_time=60
```

Targets:

| Target | Input boundary |
|---|---|
| `metainfo` | strict bencode and full metainfo parsing with explicit budgets |
| `peer_wire` | repeated bounded peer-frame decoding |
| `discovery_extensions` | DHT, LSD, extension handshake, metadata, PEX, and hole-punch decoders |

CI checks that targets compile; it does not run a time-based corpus campaign.
Record target, commit, toolchain, corpus seed, duration, sanitizer/output, and
crash artifact when making a fuzzing claim.

## Benchmarks

```sh
cargo bench --locked -p dendrite-benchmarks
```

Current Criterion cases cover strict bencode decoding, 16 KiB peer-piece frame
decoding, and rarest-first selection over 1,024 pieces/32 peers. See
[Performance](../operations/performance.md) for a reproducible report template.

## Documentation verification

For a public change:

1. update the exact owning reference;
2. update affected playbooks and troubleshooting branches;
3. compare command docs with live `--help`;
4. load `example.toml` through `dendrite doctor` in isolated temporary
   directories and ephemeral listener ports;
5. resolve every relative Markdown link;
6. parse every maintained Markdown file;
7. run `git diff --check` and search for superseded names/claims.

Doctor is stateful even in a temporary environment: it creates token/database
and probe files. Never point a documentation test at production paths.

## Reporting a result

Include:

```text
Dendrite commit:
Rust version and target:
OS/kernel/filesystem:
exact command and features:
input/fixture identity:
result and elapsed time:
repetitions/seed/corpus where applicable:
expected boundary:
sanitized logs or failure artifact:
```

Do not generalize beyond the executed layer. “The local v2 engine fixture
reached seeding over TCP” is evidence for that path, not all v2 clients.

## Related pages

- [Documentation policy](../documentation-policy.md)
- [Performance](../operations/performance.md)
- [Repository layout](repository-layout.md)
- [Status and limitations](../reference/status-limitations.md)
