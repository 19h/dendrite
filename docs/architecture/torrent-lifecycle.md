[← Documentation home](../../README.md)

# Torrent lifecycle

A torrent is a durable record plus, when appropriate, one supervised actor
generation. Its UUID is the control-plane identity; its v1 and/or v2 info hash
is the content identity.

```text
                    import --start
                          │
                          ▼
stopped ── resume ──► starting ── metadata/paths ready ──► downloading
   ▲                     │                                  │
   │                     │ failure                          │ all pieces verified
   │                     ▼                                  ▼
   ├──── incomplete ◄─ checking ◄────── recheck ──────── seeding
   │                     │                                  │
   │                     └──────── failure ─────────► error │
   │                                                        │
   └──────────────────────── pause ──────────────────────────┘

remove: any persisted state → record/index deletion; payloads remain
```

This is an operator model, not a promise that every intermediate value is
observable. The public `stopping` value exists but current pause and shutdown
paths do not persist it.

## Importing metainfo

For a `.torrent` upload the daemon:

1. enforces the metainfo body limit;
2. parses bounded bencode and validates v1, v2, or hybrid metadata;
3. derives and checks identities, paths, piece geometry, trackers, and web seeds;
4. rejects duplicate v1 or v2 identities;
5. writes the record and unique hash indexes in one transaction;
6. publishes `torrent_added`;
7. if start was requested, submits the actor command after commit.

The record is `stopped` without `--start` and `starting` with it. All torrents
write below the configured global `download_dir`; API v2.0 rejects per-import
destination and sequential-download fields even though reserved shared types
exist for them.

## Importing a magnet

A magnet supplies an expected v1 identity, v2 identity, or both, but may not
contain full file and piece metadata. Dendrite stores a provisional record and
the actor obtains the info dictionary through peer metadata exchange. The
received metadata must match every identity supplied by the magnet.

For v2 torrents the actor also obtains the required piece layers before normal
verification and transfer. Until metadata is accepted, provisional fields such
as total length can be zero and the display name can come from `dn`.

## Actor preparation

One actor generation owns active work for one UUID. It loads durable metadata,
claims normalized payload paths, constructs storage layout, and establishes
completion state. A new start or recheck cancels and joins the previous
generation, preventing two generations from intentionally writing the same
torrent concurrently.

Path ownership also prevents distinct loaded torrents from claiming the same
relative payload path. This matters when torrents happen to contain identical
top-level names but do not have identical identities.

## Discovery

For public torrents, initial discovery runs all sources concurrently:

1. announce concurrently to every valid tracker tier;
2. query DHT at the same time when bootstrap nodes are configured;
3. query local service discovery without waiting for either wide-area source.

Each result streams immediately into a deduplicated candidate queue. PEX adds
more candidates after compatible peer sessions exist. Unused candidates remain
available to replace failed sessions, and discovery is repeated only after the
live swarm and candidate queue are exhausted. Private torrents use their
declared trackers and suppress DHT, local discovery, and PEX.

With the default empty DHT bootstrap list, a torrent that has no working
tracker cannot rely on DHT discovery until the operator supplies bootstrap
nodes.

## Scheduling and transfer

The actor connects to a bounded set of discovered peers over TCP or uTP,
performs protocol negotiation, and chooses pieces rarest-first. The current
transfer scheduler uses up to 256 peers, with a 64-block request pipeline per
peer. Near completion, endgame scheduling can duplicate outstanding requests
and cancel redundant work when one copy arrives.

Peer availability, choke state, timeouts, invalid messages, failed hashes, and
global connection admission all influence useful concurrency. The constants
above are upper implementation bounds, not throughput guarantees.

Web seeds are a fallback data source for declared HTTP(S) URLs. Requests enforce
range and response limits. Private-address web seeds are rejected in the daemon
path to avoid turning metainfo into an implicit internal-network fetch.

## Verification and durable completion

Received data is never marked complete solely because the expected number of
bytes arrived:

- v1 pieces are checked against SHA-1 piece hashes;
- v2 blocks/pieces are checked through the file tree and piece-layer Merkle
  roots/proofs;
- hybrid metadata must satisfy the identities and layouts required by both
  forms.

After verification, storage writes are synchronized before the completion bit
is committed. A crash can therefore lose recent progress, but the intended
ordering avoids claiming data durable before the filesystem has received it.
The completion bitmap and counters live in a compact progress record, so a
piece commit never rewrites the torrent's potentially multi-megabyte metainfo.
Verified pieces from different peers are finalized concurrently. Each peer is
released for another assignment only after its previous piece is durable,
bounding finalization memory to the active peer set while avoiding a global
single-piece storage barrier.

When every required piece is verified the record transitions to `seeding`.

## Recheck

Recheck cancels active transfer, reads the payload through the confined storage
root, and rebuilds completion from cryptographic metadata. Its result is:

- `seeding` if every required piece verifies;
- `stopped` if any required piece is absent or invalid;
- `error` if metadata, path, or storage operations fail.

Recheck repairs completion bookkeeping; it does not repair corrupt bytes.
Resume an incomplete torrent to reacquire them.

## Incoming seeding

The incoming peer service accepts globally admitted connections and matches
their handshake identity to durable torrents in `downloading` or `seeding`.
Only verified completed blocks can be served. A persisted `seeding` record does
not need a download actor after restart to remain eligible for this service.
Immutable parsed metainfo and metadata-exchange bytes are shared by concurrent
incoming sessions. Upload accounting is sampled live and durably flushed once
per second in a per-torrent batch.

## Pause, resume, remove, restart

Pause persists `stopped`, cancels and joins the actor, and persists the
supervisor acknowledgement. Resume checks the active limit, persists
`starting`, and installs a fresh actor generation using durable completion and
counters.

Remove cancels active work and deletes the record and hash indexes in one
transaction. Actor state writes are replace-only, so a late write cannot
resurrect the deleted record. Files below `download_dir` are deliberately left
untouched.

After a daemon restart:

- `starting` and `downloading` records are resumed;
- `checking` records are rechecked;
- `stopped`, `seeding`, and `error` records do not spawn download actors;
- durable `downloading` and `seeding` records remain candidates for incoming
  seeding.

## Related pages

- [Torrent management](../operations/torrent-management.md)
- [Protocol scope](protocols.md)
- [Storage and security](storage-security.md)
- [Recover a torrent](../playbooks/recover-torrent.md)
