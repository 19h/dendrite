[← Documentation home](../README.md)

# Troubleshooting

Start with the symptom below. Preserve the first exact error and current state
before restarting: the API summary does not retain actor error detail, and event
sequence/history is process-local.

## First triage

```sh
dendritectl status
dendritectl list
```

Then inspect daemon logs. For systemd:

```sh
journalctl -u dendrite.service -n 200 --no-pager
```

Do not run doctor beside a live daemon: its listener probes will collide.

## The build fails

Confirm the checkout-selected compiler and the locked dependency graph:

```sh
rustc --version
cargo build --release --locked -p dendrite-daemon -p dendrite-cli
```

The repository pins Rust 1.94.0. Install a usable linker/C toolchain for Rust
dependencies and preserve the full first Cargo error; later messages are often
consequences. A `--locked` failure after intentionally changing dependencies
means `Cargo.lock` must be reviewed and updated as part of that source change,
not bypassed in an operator build.

## The daemon does not start

Use the first configuration/server error printed on stderr. With the daemon
stopped, run the same binary, user, and configuration through doctor:

```sh
dendrite --config /etc/dendrite/dendrite.toml doctor
```

Doctor identifies configuration, directory, token, database, storage, and
listener failures separately. It creates/probes state, so do not point an
experiment at production directories or run it concurrently with the service.

## `dendritectl` cannot read the token

Typical error:

```text
failed to read administrator token: ...
```

The default path is relative to the client's working directory, not discovered
from the daemon. Point it at the daemon's actual state root:

```sh
dendritectl --token-file /var/lib/dendrite/admin.token status
```

Check the invoking user's read permission without making the token broadly
readable. Under Docker, run the client inside the container or mount/copy the
token through an intentional secret path.

## HTTP 401 `unauthorized`

The token can be from another data directory, stale after rotation, malformed,
or sent to another daemon. Confirm `--api` and `--token-file` refer to the same
service. Browser clients must also provide the session cookie; mutations made
with that cookie require `X-CSRF-Token`.

If `rotate-token` succeeded remotely but the client failed to replace its local
file, retrieve `data_dir/admin.token` through authorized host access.

## Connection refused or timeout

Check whether the daemon is running and which address it actually uses:

```sh
curl --fail --silent --show-error \
  http://127.0.0.1:8412/healthz \
  --output /dev/null
```

A TLS listener requires `https://`. From a container, host loopback is not
container loopback. For a remote client, verify host firewall/routing after
confirming the configured non-loopback address, certificate, and allowed
origins.

`/healthz` is only HTTP liveness. Continue with authenticated status to exercise
the protected control plane.

## Daemon rejects configuration

Common causes:

- unknown TOML fields (strictly rejected);
- invalid socket-address syntax;
- non-loopback API without both TLS files and an allowed origin;
- non-IPv4 or zero-port NAT-PMP gateway;
- a limit outside its accepted range;
- `active_torrents` greater than `loaded_torrents`;
- an environment override with the wrong type;
- attempts to encode array settings through the scalar environment source.

Compare with [`example.toml`](../example.toml) and the
[configuration reference](reference/configuration.md). Keep `dht_bootstrap` and
`allowed_origins` arrays in TOML.

## Address already in use

The peer address needs both TCP and UDP/uTP; DHT needs its own UDP address; API
needs TCP. Find the conflicting service or choose another port. When doctor
alone reports every daemon port busy, stop the running daemon and retry doctor
as the same user.

Inside one host, two Dendrite instances need distinct API, peer, and DHT
addresses as well as distinct data and download roots.

## Import fails

| Error class | First check |
|---|---|
| request/body too large | `limits.metainfo_bytes` and file size |
| invalid metainfo/bencode | source integrity, v1/v2 geometry, paths, piece layers |
| invalid magnet | at least one supported `btih`/`btmh` identity and URI escaping |
| conflict | same v1 or v2 identity already loaded |
| configured limit reached | loaded count; active count when using `--start` |
| payload path conflict | another loaded torrent claims the same relative path |

The CLI treats any source not beginning exactly with `magnet:` as a local file.
Quote magnets so `&` is not interpreted by the shell.

## Torrent has zero peers

Check each discovery input:

1. Does it declare a reachable HTTP/HTTPS/UDP tracker?
2. Did the tracker return usable peers?
3. Is `dht_bootstrap` nonempty and reachable?
4. Is it private, which intentionally disables DHT, LSD, and PEX?
5. Can the host make outbound peer connections?
6. Does `peer_encryption = "required"` exclude available peers?

LSD is LAN-local. NAT-PMP and an open incoming port can improve reachability but
do not manufacture tracker/DHT results. A zero sampled peer count can also be
transient; use logs over time.

Current builds continuously drain the TCP/uTP listener even when connection or
handshake admission is full. Stalled handshakes are bounded separately, and
outbound workers leave reserved capacity for incoming sessions. A temporary
connection flood can cause excess sockets to be rejected, but the listener must
admit new peers automatically as soon as capacity returns; it must not require
a daemon restart.

## Torrent has one peer and is extremely slow

The sampled `peers` value counts connected sessions, not the tracker population.
Use `inbound_peers`, `outbound_peers`, `seed_peers`, and `active_downloaders` to
separate reachable sessions from full sources that are actually sending data.
One connection can therefore mean that discovery found only one usable
endpoint, or that every other candidate failed during connect or negotiation.

On a dual-stack host, Dendrite prefers IPv6 for HTTP tracker announces and falls
back to IPv4. This matters because some trackers return different IPv4 and IPv6
peer populations. With debug logging enabled, compare `tracker announce
succeeded ... peers=N`, `peer connection ready`, and connection failures. If a
tracker returns several candidates but only one connects, investigate routing,
firewall, encryption policy, and remote churn. If it returns one candidate,
check the other tracker tiers and configured DHT bootstrap nodes.

Tracker tiers, DHT, and local discovery run concurrently. Usable results are
connected immediately, while later results remain available as replacements for
failed sessions. While downloading, discovery refreshes periodically, inbound
sessions can join the active swarm, full seeds are preferentially retained, and
idle or choked non-seeds are rotated when unused candidates remain. A long list
of dead trackers should therefore increase log noise but must not delay a
healthy tracker or an already discovered peer.

Measure progress with two summary requests at least 250 ms apart. The live
`download_rate` includes accepted peer blocks, including partial pieces; the
durable `downloaded` counter advances only after complete pieces are verified,
synced, and committed.

## Torrent is stuck in `starting`

For a magnet, metadata must be obtained from a compatible peer and match every
declared identity. Logs can distinguish no metadata peers, repeated identity
mismatches, missing v2 piece layers, path conflicts, or storage setup failures.
During this phase `total_length` remains zero; outbound peer telemetry counts
live metadata sessions. Exhausting a round of missing, incompatible, or
disconnecting metadata peers does not move the torrent to `error`; Dendrite
backs off to at most 30 seconds and automatically starts another discovery
round until metadata succeeds or the torrent is cancelled.

For a `.torrent`, preparation can still fail while claiming paths or opening
storage. Do not repeatedly resume in a tight loop; capture the actor error,
correct the prerequisite, then resume once.

## Torrent is in `error`

The summary contains the state but not its error detail. Read daemon logs.
Magnet discovery and peer-metadata failures recover while remaining in
`starting`; an `error` now indicates a local/configuration failure or a transfer
failure after metadata acquisition. After external payload changes or a storage
failure, recheck first:

```sh
dendritectl recheck <TORRENT_ID>
```

If it becomes `stopped`, missing/invalid pieces were found; resume to reacquire
them. If it remains `error`, fix the logged metadata/path/I/O failure first.

Follow the [recovery playbook](playbooks/recover-torrent.md) for a complete safe
sequence.

## Download rate is zero

The first summary rate sample is zero and later rates update only when summaries
are separated by at least 250 ms. A quiet interval, choking peers, discovery,
metadata acquisition, verification, or completed seeding can all produce zero.

Check state, durable byte counters, `seed_peers`, `active_downloaders`, and logs
over time. A large `peers` total with zero active downloaders usually means the
connected population is choked or currently has no useful pieces. Do not infer
corruption or completion from a single rate field.

While downloading, a peer earns `reciprocal_bootstrap_bytes` of upload per
connected hour plus `reciprocal_ratio` times the bytes it has delivered and
that verified, and only peers holding pieces the torrent still needs occupy
regular upload slots. If every peer is choking you, raise the bootstrap or
ratio in `[transfer]` so remote tit-for-tat clients see you reciprocating; if
`dendrite_uploaded_bytes_total` climbs faster than your egress budget allows,
lower them or set `upload_rate_limit_bytes` / `torrent_max_upload_ratio`. A
full seed cannot be rewarded with payload because it already has every piece
and will not request any; Dendrite instead retains full seeds preferentially
and schedules them before partial sources.

If `peers` includes connections from the daemon's own public address, those
are self-connections; the daemon rejects them by peer id and never dials
addresses it has learned are its own, so a persistent count means a second
daemon is running with the same state directory.

## Recheck ends in `stopped`

At least one required piece is absent or does not match its cryptographic
metadata. This is a successful integrity result, not a recheck crash. Resume to
download missing data. Recheck never trusts file size/timestamp and never repairs
bytes by itself.

## Storage permission, link, or I/O failure

Confirm free bytes/inodes, mount health, service-user ownership, and systemd
`ReadWritePaths`. Dendrite deliberately rejects symlink traversal and, on Unix,
payload files with multiple hard links. Move intended content into normal files
below `download_dir`; do not weaken state/token permissions to solve a payload
problem.

`storage_backend: "portable"` on Linux is not itself an error: automatic
initialization falls back when `io_uring` is unavailable.

## HTTP 429 `limit_reached`

The server rejected API concurrency/rate, browser-session capacity, or a torrent
capacity operation. Read the problem `detail`, reduce parallel work, and retry
with exponential backoff plus jitter. Increasing limits without measuring
memory, file descriptors, and load can move the failure elsewhere.

## List appears incomplete

`dendritectl list` retrieves only the first default page. The response's
`next_cursor` shows whether more records exist. Use `GET /api/v2/torrents` with
`cursor` and `limit` until `next_cursor` is null.

## Events disconnect or contain `resync_required`

The subscriber fell behind the bounded process-local broadcast buffer. Fetch a
fresh paginated torrent snapshot, discard assumptions based on the old sequence,
then reconnect. There is no durable replay endpoint. A send stalled for 10
seconds is also disconnected.

## `quarantined_records` is nonzero

Persistence found a torrent record it could not decode and moved its raw value
out of the active table. Stop mutation-heavy recovery, preserve a copy of
`state.redb` with the daemon stopped, record the exact Dendrite build, and
investigate. There is no supported API/CLI restore operation for quarantine.

Do not edit the redb file with a text or hex editor.

## Remove did not delete files

This is expected. Remove forgets service state and identity indexes; it retains
payloads. Resolve and delete intended data separately with the daemon no longer
managing it. Be especially careful with similarly named torrent roots.

## Docker starts with empty state

The state volume was likely not remounted at
`/home/dendrite/dendrite-data`, or the container is using another working/config
path. Mount state and downloads separately and persist both. A fresh state root
creates a new administrator token.

## systemd service cannot access a path

The packaged unit uses `ProtectSystem=strict`, `ProtectHome=true`, a dedicated
`dendrite` account, and grants writes only to `/var/lib/dendrite` and
`/srv/dendrite/downloads`. Keep configured writable paths there or deliberately
adjust `ReadWritePaths` in a reviewed drop-in. Also ensure the daemon can read
the configuration and TLS files.

## Escalation checklist

Capture the version/commit, exact command, sanitized configuration, host
OS/kernel/filesystem, status/list output, storage backend, relevant logs, doctor
report with the daemon stopped, torrent identity form, network prerequisites,
and reproduction steps. Never publish the token, TLS private key, private
tracker URL, or copyrighted payload.

## Related pages

- [Recovery playbook](playbooks/recover-torrent.md)
- [Observability](operations/observability.md)
- [Status and limitations](reference/status-limitations.md)
- [Storage and security](architecture/storage-security.md)
