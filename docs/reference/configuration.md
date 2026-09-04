[← Documentation home](../../README.md)

# Configuration reference

Dendrite deserializes one strict TOML settings tree. Unknown fields, invalid
types, malformed socket addresses, and invalid cross-field relationships stop
startup.

Precedence, highest first:

```text
DENDRITE__... environment setting
TOML selected by --config or DENDRITE_CONFIG
built-in default
```

Paths are interpreted by the daemon process. Relative paths therefore resolve
from its working directory.

## Top-level settings

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `data_dir` | path | `./dendrite-data` | administrator token and redb state |
| `download_dir` | path | `./downloads` | capability root for every torrent payload |

Both directories are created when needed. The daemon account needs create,
read, write, rename, and synchronization access.

## `[listen]`

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `api` | socket address | `127.0.0.1:8412` | HTTP(S) administrator API |
| `peer` | socket address | `0.0.0.0:16493` | peer TCP listener and uTP UDP endpoint |
| `dht` | socket address | `0.0.0.0:16309` | DHT UDP endpoint |
| `dht_bootstrap` | socket-address array | `[]` | literal bootstrap nodes for DHT lookup |
| `nat_pmp_gateway` | socket address or absent | absent | explicit IPv4 NAT-PMP gateway |
| `peer_encryption` | enum | `preferred` | `disabled`, `preferred`, `plaintext_preferred`, or `required` MSE policy; `plaintext_preferred` dials plaintext first and falls back to encryption, accepting both inbound |
| `tls_certificate` | path or absent | absent | PEM certificate chain for API TLS |
| `tls_private_key` | path or absent | absent | PEM private key for API TLS |
| `allowed_origins` | string array | `[]` | exact browser CORS origins |

Cross-field rules:

- a non-loopback `api` address requires both TLS paths;
- a non-loopback `api` address requires at least one allowed origin;
- `nat_pmp_gateway`, when present, must be IPv4 and have a nonzero port;
- the peer port must be available to both TCP and UDP/uTP;
- the DHT address is a separate UDP listener.

The TLS and origin invariant protects the non-loopback bind case. Certificate
validity, key permissions, hostname matching, client trust, and firewall policy
remain operator responsibilities.

## `[limits]`

| Setting | Default | Accepted range | Enforced at |
|---|---:|---:|---|
| `loaded_torrents` | 10,000 | 1–100,000 | import capacity |
| `active_torrents` | 1,000 | 1–10,000 and no more than loaded | started imports/resumes |
| `peer_connections` | 10,000 | 1–100,000 | engine-wide peer admission |
| `metainfo_bytes` | 67,108,864 (64 MiB) | 1,024–67,108,864 | protected HTTP body and metainfo parser |
| `tracker_response_bytes` | 8,388,608 (8 MiB) | 1,024–8,388,608 | HTTP/UDP tracker response parsing |
| `websocket_message_bytes` | 8,388,608 (8 MiB) | 1,024–8,388,608 | event WebSocket frame/message |
| `api_concurrency` | 256 | 1–10,000 | protected requests in flight |
| `api_requests_per_second` | 1,000 | 1–1,000,000 | fixed one-second admission window |
| `browser_sessions` | 1,024 | 1–100,000 | in-memory browser sessions |
| `list_page_size` | 200 | 1–10,000 | maximum/default torrent list page |
| `download_buffer_bytes` | 2,147,483,648 (2 GiB) | 16 MiB–64 GiB | piece buffers assigned to downloading peers across all torrents |
| `piece_cache_bytes` | 536,870,912 (512 MiB) | 16 MiB–64 GiB | verified pieces cached for upload across all torrents |

These are ceilings, not allocations or recommended values for every host.
Configured body sizes do not relax lower-level structural/message limits in the
owning parsers.

## `[storage]`

| Setting | Type | Default | Meaning |
|---|---|---:|---|
| `flush_interval_seconds` | integer 1–300 | `1` | seconds between the group fsync barriers that commit verified pieces |

Completion bits are committed only after the payload files are synchronized,
so a longer interval means more verified pieces may need to be re-downloaded
after a crash, never that unverified data is trusted. On ZFS with a separate
log device, intervals longer than the transaction-group timeout keep most
payload out of the log device.

## `[transfer]`

Upload economics. Regular slots reward peers by the rate at which they deliver
verified data; every other lever bounds egress.

| Setting | Type | Default | Meaning |
|---|---|---:|---|
| `upload_slots` | integer 1–1,000 | `16` | regular upload slots per torrent |
| `optimistic_upload_slots` | integer 0–100 | `4` | rotating audition slots per torrent |
| `reciprocal_ratio` | non-negative number | `1.0` | bytes a downloading torrent may upload to a peer per verified byte received from it; `0` disables the cap |
| `reciprocal_bootstrap_bytes` | integer | `8388608` (8 MiB) | allowance granted to each peer per hour of connection before it has delivered anything |
| `upload_rate_limit_bytes` | integer | `0` | upload ceiling per torrent in bytes per second; `0` is unlimited |
| `torrent_max_upload_ratio` | non-negative number | `0` | uploaded/downloaded ratio at which a torrent chokes every peer; `0` is unlimited |

While a torrent is downloading, regular slots go only to peers that hold
pieces the torrent still needs, because upload to anyone else cannot be repaid
in kind. While seeding, interest is ignored and recent upload rate orders the
slots. The credit cap and the ratio cap are independent: the first is per peer,
the second per torrent.

## `[logging]`

| Setting | Type | Default | Meaning |
|---|---|---|---|
| `filter` | string | `dendrite=info` | `tracing_subscriber` environment-filter expression |
| `json` | boolean | `false` | JSON rather than human-readable event formatting |

An invalid filter does not stop startup; logging falls back to `dendrite=info`.

## Complete local example

```toml
data_dir = "./dendrite-data"
download_dir = "./downloads"

[listen]
api = "127.0.0.1:8412"
peer = "0.0.0.0:16493"
dht = "0.0.0.0:16309"
dht_bootstrap = []
peer_encryption = "preferred"
allowed_origins = []

[limits]
loaded_torrents = 10000
active_torrents = 1000
peer_connections = 10000
metainfo_bytes = 67108864
tracker_response_bytes = 8388608
websocket_message_bytes = 8388608
api_concurrency = 256
api_requests_per_second = 1000
browser_sessions = 1024
list_page_size = 200
download_buffer_bytes = 2147483648
piece_cache_bytes = 536870912

[storage]
flush_interval_seconds = 1

[transfer]
upload_slots = 16
optimistic_upload_slots = 4
reciprocal_ratio = 1.0
reciprocal_bootstrap_bytes = 8388608
upload_rate_limit_bytes = 0
torrent_max_upload_ratio = 0.0

[logging]
filter = "dendrite=info"
json = false
```

The checked-in [`example.toml`](../../example.toml) contains the same runnable
shape with operational comments.

## Complete remote-listener shape

```toml
data_dir = "/var/lib/dendrite"
download_dir = "/srv/dendrite/downloads"

[listen]
api = "0.0.0.0:8412"
peer = "0.0.0.0:16493"
dht = "0.0.0.0:16309"
dht_bootstrap = ["203.0.113.10:6881"]
nat_pmp_gateway = "192.168.1.1:5351"
peer_encryption = "preferred"
tls_certificate = "/etc/dendrite/tls/cert.pem"
tls_private_key = "/etc/dendrite/tls/key.pem"
allowed_origins = ["https://torrents.example.com"]

[limits]
loaded_torrents = 10000
active_torrents = 1000
peer_connections = 10000
metainfo_bytes = 67108864
tracker_response_bytes = 8388608
websocket_message_bytes = 8388608
api_concurrency = 256
api_requests_per_second = 1000
browser_sessions = 1024
list_page_size = 200

[logging]
filter = "dendrite=info"
json = true
```

The documentation-only `203.0.113.10` address is not a real bootstrap service;
replace it with a trusted reachable node.

## Validation

```sh
dendrite --config /etc/dendrite/dendrite.toml doctor
```

Doctor performs writes and listener binds, so stop the active daemon and run it
as the same account. See [Configuration guide](../getting-started/configuration.md)
for choosing settings and [Environment variables](environment-variables.md) for
deployment overrides.
