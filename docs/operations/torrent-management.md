[← Documentation home](../../README.md)

# Torrent management

Day-to-day administration happens through `dendritectl`. The client reads one
administrator token, calls `/api/v2`, prints successful JSON to stdout, and
prints request/input/encoding errors to stderr with a nonzero exit status.

## Establish the target

Local defaults:

```sh
dendritectl \
  --api http://127.0.0.1:8412/api/v2 \
  --token-file ./dendrite-data/admin.token \
  status
```

The flags can be replaced by `DENDRITE_API` and `DENDRITE_TOKEN_FILE`. Always
make them explicit in automation and service administration.

## Inspect the daemon

```sh
dendritectl status
```

| Field | Meaning |
|---|---|
| `api_version` | control-plane version, currently `2.0` |
| `daemon_version` | package version of the running daemon |
| `uptime_seconds` | process uptime, not host uptime |
| `loaded_torrents` | records currently readable in the active table |
| `active_torrents` | records whose state is neither `stopped` nor `error` |
| `connected_peers` | current engine-wide peer connection count |
| `quarantined_records` | corrupt/undecodable records moved aside by persistence |
| `storage_backend` | `portable` or Linux `io_uring` |

`active_torrents` is state-based. It is not a task-list count and can include a
state whose actor has just completed or is transitioning.

## List torrents

```sh
dendritectl list
```

The client returns `items` and `next_cursor`. It does not expose `--limit` or
`--cursor`, so it only retrieves the first server-default page. Use the HTTP API
for complete pagination when more than `limits.list_page_size` records exist.

Each summary contains:

- UUID identity and display name;
- lifecycle state;
- optional v1 and v2 info hashes;
- total payload length;
- cumulative downloaded/uploaded counters;
- sampled byte-per-second rates;
- current peer count.

The first sample for a torrent normally reports zero rates. Samples update only
after at least 250 ms between summary calculations.

## Import and start

Metainfo file:

```sh
dendritectl add ./release.torrent --start
dendritectl add ./release.torrent --start --stop-on-complete
```

Magnet:

```sh
dendritectl add 'magnet:?xt=urn:btih:…&tr=…' --start
```

Without `--start`, the record is created in `stopped`. With it, the record is
created as `starting` and scheduled after the transaction commits. Add
`--stop-on-complete` to persist a mode that stops the torrent once every piece
verifies instead of keeping it active for seeding.

The CLI exposes no destination or sequential option because API v2.0 rejects
both. All payloads use the daemon's global `download_dir`; scheduling remains
rarest-first with an endgame mode.

Duplicate v1 or v2 identities return HTTP 409 even if the other identity or UUID
differs. Hash indexes are unique and updated transactionally.

## Pause

```sh
dendritectl pause <TORRENT_ID>
```

The daemon serializes the mutation, sets the durable state to `stopped`, cancels
the actor, waits for it to finish, and persists `stopped` again through the
supervisor acknowledgement path. Late actor writes use replace-only persistence
and cannot recreate a record deleted concurrently.

Incoming seeding is selected from durable `downloading` or `seeding` records;
pause to `stopped` therefore also removes eligibility for new incoming seed
sessions.

## Resume

```sh
dendritectl resume <TORRENT_ID>
```

Resume enforces the active-torrent limit when starting from `stopped` or `error`,
persists `starting`, cancels any previous generation, and creates one new actor.
The actor reuses persisted completion bits and byte counters.

## Recheck

```sh
dendritectl recheck <TORRENT_ID>
```

Recheck cancels the existing actor, sets `checking`, reads payload pieces, and
reconstructs completion from v1 hashes, v2 Merkle roots/proofs, or both for a
hybrid. It does not contact peers to “trust” their state.

| Result | Final state |
|---|---|
| every required piece verifies | `seeding`, or `stopped` with stop-on-complete |
| at least one piece is absent/corrupt | `stopped` |
| metadata/path/storage operation fails | `error` |

Resume after an incomplete result to reacquire missing pieces.

## Remove

```sh
dendritectl remove <TORRENT_ID>
```

The daemon waits for actor cancellation, removes the record and hash-index
entries transactionally, publishes a `torrent_removed` event, and returns no
body. **Payload files are retained.** There is no CLI or API “delete data” flag.

## API-only announce action

The API type accepts an `announce` action, but `dendritectl` has no corresponding
command. The current server keeps the stored state, then submits the same engine
start command used by resume. That can replace an active actor and cause normal
start/download state transitions; do not treat it as a side-effect-free tracker
ping.

## State guide

| State | Operator interpretation |
|---|---|
| `stopped` | durable record, no active transfer actor requested |
| `starting` | actor requested; may be acquiring metadata or preparing paths |
| `downloading` | actor is acquiring/verifying missing payload |
| `seeding` | all pieces verified; durable record can serve incoming requests |
| `checking` | payload verification/reconstruction in progress |
| `error` | actor failed; inspect logs/events, then resume or recheck deliberately |
| `stopping` | public state value exists, but current runtime paths do not persist it during pause/shutdown |

## Automation cautions

- Parse JSON rather than human log text.
- Store and pass the UUID exactly.
- Follow `next_cursor` through the HTTP API for exhaustive listing.
- Treat HTTP 429 as an explicit configured-capacity result, not a retry-at-full-
  speed signal.
- Do not infer completion from a quiet rate or file length.
- Do not make token files group/world readable for convenience.

## Related pages

- [First torrent playbook](../playbooks/first-torrent.md)
- [Command-line reference](../reference/command-line.md)
- [HTTP API reference](../reference/http-api.md)
- [Torrent lifecycle](../architecture/torrent-lifecycle.md)
