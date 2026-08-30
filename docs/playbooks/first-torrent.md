[← Documentation home](../../README.md)

# Playbook: import your first torrent

This playbook takes a clean checkout to one durable torrent record and a running
transfer. It uses local directories and a loopback-only administrator API.

## Outcome

At the end you will know how to:

- start the daemon and authenticate the client;
- import a `.torrent` file or magnet;
- distinguish importing from starting;
- interpret torrent identity, state, rates, and payload location;
- pause, resume, recheck, and remove the record safely.

## 1. Build both binaries

From the repository root:

```sh
cargo build --release --locked -p dendrite-daemon -p dendrite-cli
```

Checkpoint:

```sh
test -x target/release/dendrite
test -x target/release/dendritectl
```

If the build fails, confirm `rustc --version` reports 1.94.0 and see
[Troubleshooting](../troubleshooting.md#the-build-fails).

## 2. Check the environment

Do not start another daemon while running the doctor. It needs to bind the same
listeners the service will use.

```sh
target/release/dendrite --config example.toml doctor
```

Doctor prints JSON. A healthy report has `"healthy": true` and ten successful
checks covering configuration, both directories, token, database, storage, API,
peer TCP, peer uTP, and DHT UDP.

Doctor creates `./dendrite-data`, `./downloads`, `admin.token`, and `state.redb`
if they do not already exist. It is a validation command with local filesystem
side effects, not a purely observational health probe.

## 3. Start the daemon

Keep this terminal open:

```sh
target/release/dendrite --config example.toml
```

Expected log outcome: the API starts on `127.0.0.1:8412` and identifies either
the `portable` or `io_uring` storage backend. The process remains in the
foreground until `Ctrl-C` or `SIGTERM`.

If it exits immediately, use the emitted configuration or server error and the
[startup troubleshooting branches](../troubleshooting.md#the-daemon-does-not-start).

## 4. Authenticate from a second terminal

Run the client from the repository root so its default token path points to the
daemon's token:

```sh
target/release/dendritectl status
```

Expected shape:

```json
{
  "api_version": "2.0",
  "daemon_version": "2.0.0-alpha.1",
  "loaded_torrents": 0,
  "active_torrents": 0,
  "storage_backend": "portable"
}
```

The actual response also contains uptime, connected-peer, and quarantine fields;
the storage backend can be `io_uring` on a usable Linux host.

Authentication failure usually means the client is reading a different token
file. Make both inputs explicit:

```sh
target/release/dendritectl \
  --api http://127.0.0.1:8412/api/v2 \
  --token-file ./dendrite-data/admin.token \
  status
```

## 5A. Import a `.torrent` file

Use a metainfo file you are authorized to transfer:

```sh
target/release/dendritectl add ./example.torrent --start
```

The client reads the whole file, uploads it as multipart field `metainfo`, and
sends JSON add options in multipart field `options`. The daemon parses and
validates the metainfo before creating the record.

Successful output resembles:

```json
{
  "id": "019…",
  "name": "example",
  "state": "starting",
  "v1_info_hash": "…",
  "v2_info_hash": null,
  "total_length": 123456,
  "downloaded": 0,
  "uploaded": 0,
  "download_rate": 0,
  "upload_rate": 0,
  "peers": 0
}
```

Copy the complete `id`; lifecycle commands use the UUID, not the info hash or
display name.

Without `--start`, import stops after durable registration:

```sh
target/release/dendritectl add ./example.torrent
```

The returned state is `stopped`. Start it later with `resume`.

## 5B. Import a magnet instead

Always quote magnets in a shell:

```sh
target/release/dendritectl add \
  'magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=example&tr=https%3A%2F%2Ftracker.example%2Fannounce' \
  --start
```

That is a syntax template using a placeholder identity and reserved example
tracker; replace the entire URI with a real authorized magnet before expecting
metadata or payload transfer.

Supported identities are v1 `urn:btih` in 40-character hexadecimal or
32-character base32 form and v2 `urn:btmh` SHA-256 multihashes. A hybrid magnet
may contain both.

At import time a magnet can report `total_length: 0` and a name derived from
`dn` or the info hash. The actor discovers peers, downloads BEP 9 metadata,
verifies the exact advertised identity, fetches required v2 piece layers, then
replaces those provisional fields with parsed metainfo.

## 6. Observe progress

```sh
target/release/dendritectl list
target/release/dendritectl status
```

Typical transfer progression:

```text
stopped --resume/--start--> starting --> downloading --> seeding
                                   \--> error
```

`downloaded` and `uploaded` are durable byte counters. `download_rate` samples
accepted peer blocks, including partial pieces, while `upload_rate` samples the
durable upload counter. Rates are calculated when summaries are requested and
may be zero on the first or closely spaced request. `peers` is the actor's
current connected peer count.

The client `list` command requests one server-default page and has no cursor or
limit flags. For more than the configured page size, use the paginated HTTP API.

## 7. Find the payload

All non-padding torrent paths are resolved below:

```text
./downloads
```

Single-file and multi-file names come from validated metainfo. Dendrite rejects
absolute paths, separators inside components, dot components, NULs, excessive
component lengths, duplicate/colliding paths, symlink traversal, and external
hard-link targets.

A file's existence does not mean every piece is verified. Use torrent state and
a completed recheck rather than file size alone.

## 8. Manage the record

Replace `<TORRENT_ID>` with the returned UUID:

```sh
target/release/dendritectl pause <TORRENT_ID>
target/release/dendritectl resume <TORRENT_ID>
target/release/dendritectl recheck <TORRENT_ID>
```

- `pause` waits for the active actor to stop and persists `stopped`.
- `resume` sets `starting` and creates a new actor.
- `recheck` reads payload pieces and rebuilds completion state. A completely
  valid payload ends in `seeding`; an incomplete or corrupt payload ends in
  `stopped`, ready for `resume`.

## 9. Remove service state

```sh
target/release/dendritectl remove <TORRENT_ID>
```

Success produces no JSON because the API returns HTTP 204. The daemon cancels the
actor and transactionally removes its record and hash indexes.

**Removal does not delete payload files.** Delete them separately only after
resolving the exact paths and confirming that no other data should be retained.

## Common failure checkpoints

| Symptom | First check |
|---|---|
| `failed to read administrator token` | client and daemon working directories or `--token-file` |
| HTTP 401 | token contents, token rotation, client `--api` target |
| HTTP 400 on add | strict metainfo/magnet parse error or unsupported add option |
| HTTP 409 | the same v1 or v2 info hash is already registered |
| HTTP 429 | loaded/active/API rate/API concurrency limit reached |
| state becomes `error` | daemon log for tracker, metadata, peer, web-seed, path-conflict, or storage detail |
| no peers | usable tracker URL, configured DHT bootstrap, private flag, firewall, and LSD scope |

## Next steps

- Day-to-day commands: [Torrent management](../operations/torrent-management.md)
- Exact command behavior: [Command-line reference](../reference/command-line.md)
- State mechanics: [Torrent lifecycle](../architecture/torrent-lifecycle.md)
- Failure diagnosis: [Torrent recovery](recover-torrent.md)
