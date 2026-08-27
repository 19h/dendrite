[← Documentation home](../../README.md)

# Architecture overview

Dendrite is a headless BitTorrent service. One daemon owns configuration,
authentication, durable torrent state, payload storage, network listeners, and
supervised torrent actors. `dendritectl` is only an HTTP client; it never opens
the state database or payload files.

```text
operator / automation
        │
        ▼
  dendritectl ───── bearer token ─────┐
                                     ▼
                               HTTP API v2
                                     │
                         serialized mutations
                                     │
        ┌────────────────────────────┼──────────────────────────┐
        ▼                            ▼                          ▼
 transactional state         engine supervisor          event stream
   and hash indexes           │ one actor/torrent         and metrics
        │                     │
        │              discovery + peer sessions
        │                     │
        └──── completion ─────┴──── capability-confined storage
```

## Startup sequence

For a normal `dendrite run`, the daemon:

1. merges built-in defaults, an optional TOML file, and `DENDRITE__...`
   environment overrides;
2. validates relationships between addresses, TLS, CORS, and resource limits;
3. initializes structured logging;
4. creates or opens `data_dir`, the administrator token, and `state.redb`;
5. opens the storage root and selects the portable or Linux `io_uring` backend;
6. binds peer TCP, peer uTP/UDP, and DHT/UDP listeners;
7. starts the engine supervisor, incoming services, event bridge,
   restart-eligible work, and optional NAT-PMP maintenance;
8. builds the HTTP router, loads remote-listener TLS when configured, binds the
   API listener, and serves requests;
9. waits for a termination signal, then coordinates shutdown.

Initialization fails rather than silently weakening an invalid security or
resource setting. Use `dendrite doctor` with the daemon stopped to exercise the
same directories, token, database, storage, and listener setup without running
the service indefinitely.

## Control plane and data plane

The control plane is the authenticated API, persistence layer, supervisor, and
event bus. It creates durable intent first, then asks the data plane to perform
work. The data plane is made of torrent actors, discovery clients, peer
sessions, verification, and storage I/O.

This division explains several observable behaviors:

- an import can be durable before its actor begins;
- `pause`, `resume`, `recheck`, and `remove` are serialized API mutations;
- rates and peer counts are live samples, while identity and completion are
  durable record fields;
- a daemon restart reconstructs actors from selected durable states instead of
  serializing running tasks;
- deleting a record does not delete payload data.

See [Control plane](control-plane.md) and
[Torrent lifecycle](torrent-lifecycle.md) for those boundaries in detail.

## Supervision and bounds

The engine has a bounded command channel of 256 messages and a bounded event
channel of 4,096 messages. It owns at most one current actor generation for each
torrent UUID. Starting or rechecking a torrent cancels and joins the preceding
generation before installing the replacement.

The daemon separately enforces configured limits for loaded and active
torrents, peer connections, API concurrency, API request rate, browser
sessions, response page size, and accepted body sizes. Per-torrent download
work is also bounded: the current scheduler uses at most 32 peers and an
eight-block request pipeline per peer.

Bounds are part of correctness, not merely tuning. A full queue or exhausted
limit becomes an explicit error or rejection instead of unbounded work.

## Durable state

`state.redb` contains versioned torrent records and unique v1/v2 hash indexes.
Mutations that affect a record and its indexes are transactional. Records are
encoded separately from the database schema so their format can be versioned.
Undecodable records are quarantined instead of being returned as healthy
torrents.

Actor updates use replace-only persistence: a late update may replace an
existing record but cannot recreate one removed at the same time. Completion is
persisted only after payload data is synchronized.

See [Storage and security](storage-security.md) and
[Data layout](../reference/data-layout.md).

## Crate map

| Crate | Responsibility |
|---|---|
| `dendrite-api-types` | versioned request, response, event, and problem types shared by daemon and client |
| `dendrite-config` | defaults, TOML/environment merge, and cross-field validation |
| `dendrite-core` | pure identities, paths, lifecycle values, piece selection, and domain rules |
| `dendrite-metainfo` | bounded bencode, magnet, v1, v2, and hybrid metadata parsing |
| `dendrite-net` | peer wire codecs, TCP/uTP, MSE, trackers, DHT, LSD, PEX, metadata exchange, and related network services |
| `dendrite-persistence` | redb schema, transactions, indexes, record versions, and quarantine |
| `dendrite-storage` | capability-confined file access and portable/`io_uring` I/O |
| `dendrite-engine` | supervisor, torrent actors, discovery, scheduling, verification, web seeds, and seeding |
| `dendrite-daemon` | process assembly, listeners, API, authentication, metrics, and shutdown |
| `dendrite-cli` | `dendritectl` HTTP client |
| `dendrite-simulator` | deterministic swarm/fault model used during development |
| `dendrite-benchmarks` | Criterion hot-path baselines |

Dependencies generally point inward toward pure domain types. The daemon is the
composition root; protocol and storage crates do not know about the HTTP API.

## Shutdown and restart

Shutdown stops accepting new control-plane work, cancels supervised actors and
background services, and gives them a bounded period to finish. The supplied
systemd unit allows 35 seconds before service-manager escalation.

On startup, durable records in `starting` or `downloading` are resumed and
records in `checking` are rechecked. `stopped`, `seeding`, and `error` records
do not create download actors. Persisted `seeding` records remain eligible for
the separate incoming-seeding service, which reads durable `downloading` and
`seeding` state.

## Read next

- [Control plane](control-plane.md)
- [Torrent lifecycle](torrent-lifecycle.md)
- [Protocol scope](protocols.md)
- [Storage and security](storage-security.md)
- [Repository layout](../development/repository-layout.md)
