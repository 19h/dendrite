[← Documentation home](../../README.md)

# Building and installation

The repository currently supplies source, a Dockerfile, and a systemd unit. It
does not contain a prebuilt-release installer, OS package, or evidence that the
workspace crates have been published as a coordinated Cargo installation.

## Toolchain

`rust-toolchain.toml` pins Rust 1.94.0 with the minimal profile plus Clippy and
rustfmt. A rustup-enabled checkout selects it automatically:

```sh
rustc --version
cargo --version
```

Use the checked-in lockfile for operator and CI builds:

```sh
cargo build --release --locked -p dendrite-daemon -p dendrite-cli
```

Artifacts:

```text
target/release/dendrite
target/release/dendritectl
```

The simulator is a development binary, not part of the daemon installation:

```sh
cargo build --release --locked -p dendrite-simulator
target/release/dendrite-sim --help
```

## Run from the checkout

The least surprising evaluation path is:

```sh
target/release/dendrite --config example.toml
```

The revised example uses repository-relative state and payload directories. Run
the client from the same directory so its default token path resolves correctly:

```sh
target/release/dendritectl status
```

## Install source-built binaries

For a host-local installation, copy the two release binaries to a directory on
the administrator's `PATH`. Installing under `/usr/local/bin` is conventional:

```sh
sudo install -m 0755 target/release/dendrite /usr/local/bin/dendrite
sudo install -m 0755 target/release/dendritectl /usr/local/bin/dendritectl
```

This installs executables only. It does not create a service account,
configuration, state directory, token, firewall policy, or service manager unit.
Use the [systemd playbook](../playbooks/systemd.md) for those steps.

## Release profile consequences

Release builds use thin LTO, one codegen unit, optimization level 3, stripped
debuginfo, and `panic = "abort"`. These settings favor a small optimized daemon;
they also mean a release panic terminates the process rather than unwinding.

Every workspace crate forbids unsafe Rust. This is a source policy, not a claim
that dependencies, the operating system, or untrusted peers cannot trigger a
logic error or denial of service.

## Host and storage behavior

On Linux, automatic storage startup probes an io_uring worker. If initialization
is unavailable, it honestly reports and uses the portable positional-I/O backend.
Check the selected backend through `dendritectl status` or `dendrite doctor`.

The Dockerfile pins an x86-64 Rust toolchain name and should currently be treated
as the repository's Linux/x86-64 container path. The systemd unit is Linux-only.

## Development build and checks

```sh
cargo check --workspace --all-targets --all-features --locked
cargo test --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

The `fault-injection` features belong to internal persistence/storage/engine test
paths. They are not daemon runtime features.

Nightly Rust and `cargo-fuzz` are required only to execute fuzz targets:

```sh
cargo +nightly fuzz run metainfo -- -max_total_time=30
```

## Updating

Build the new binaries from the desired commit, stop the daemon gracefully, keep
a backup of `data_dir`, replace both binaries as one versioned pair, and restart.
The current alpha repository has no documented downgrade migration. Do not assume
that a database opened by a future binary can be reopened by an older one.

## Next steps

- Local import: [First torrent](../playbooks/first-torrent.md)
- Persistent service: [systemd](../playbooks/systemd.md)
- Container: [Docker](../playbooks/docker.md)
- Exact build evidence: [Verification model](../development/verification.md)
