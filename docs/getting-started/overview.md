[← Documentation home](../../README.md)

# Getting started

Dendrite has two operator-facing binaries and no bundled UI:

```text
dendrite      long-running daemon, network engine, storage, database, HTTP API
dendritectl   authenticated client that translates commands into HTTP requests
```

Most first-run confusion comes from mixing their paths, users, or working
directories. Choose one installation workflow, keep the daemon's data and token
together, and point the client at that token and API.

## What you need

- a 64-bit Linux host for the repository's validated path;
- Rust 1.94.0 through the checked-in toolchain file;
- Cargo and a C toolchain/linker usable by Rust dependencies;
- Git if building from a checkout;
- a `.torrent` file or magnet URI you are authorized to transfer;
- write access to the chosen state and download directories;
- free TCP/UDP listener ports.

Linux is the only CI host. The portable storage backend is intended to work
elsewhere, but other operating systems are currently an unverified source-build
path rather than an advertised support target.

## Choose a path

| Goal | Start here |
|---|---|
| Evaluate locally with minimum configuration | [First torrent](../playbooks/first-torrent.md) |
| Understand or install source-built binaries | [Building and installation](building.md) |
| Run an isolated container and administer it from inside | [Docker](../playbooks/docker.md) |
| Run continuously as a dedicated Linux user | [systemd](../playbooks/systemd.md) |
| Expose the API to another machine | [Remote API](../playbooks/remote-api.md) |
| Decide what to configure before starting | [Configuration guide](configuration.md) |
| Diagnose an existing installation | [Torrent recovery](../playbooks/recover-torrent.md) |

## Clean-checkout baseline

This path deliberately avoids root-owned directories, TLS, remote API exposure,
NAT-PMP, DHT bootstrap assumptions, and external packaging:

```sh
cargo build --release --locked -p dendrite-daemon -p dendrite-cli
target/release/dendrite
```

In a second terminal from the same working directory:

```sh
target/release/dendritectl status
target/release/dendritectl list
```

Expected baseline:

- API: `127.0.0.1:8412`;
- peer listener: TCP and uTP on port `16493`;
- DHT listener: UDP port `16309`, with no configured bootstrap nodes;
- state/token: `./dendrite-data`;
- payloads: `./downloads`;
- `status` reports API `2.0` and zero loaded torrents.

If startup fails before the API appears, stop and run:

```sh
target/release/dendrite doctor
```

`doctor` is not read-only: it creates missing directories, creates or validates
the administrator token, opens the database, initializes storage, and briefly
binds each configured listener to prove availability.

## Mental model before the first import

Adding a torrent creates a durable service record. `--start` additionally asks
the engine to start a torrent actor. The actor acquires metadata if needed,
claims payload paths, discovers peers, verifies pieces, writes them below the
single download root, synchronizes affected files, and only then persists piece
completion.

The administrator token is service-wide. `dendritectl` reads it from a file and
sends it as an HTTP bearer credential. The default client paths match the default
daemon paths only when both commands use the same working directory.

## Next steps

- Import and control a torrent: [First torrent](../playbooks/first-torrent.md)
- Select safe settings: [Configuration guide](configuration.md)
- Understand the processes: [Architecture overview](../architecture/overview.md)
