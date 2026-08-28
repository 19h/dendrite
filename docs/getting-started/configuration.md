[← Documentation home](../../README.md)

# Configuration guide

Dendrite loads built-in defaults, then an optional TOML file, then matching
`DENDRITE__…` setting overrides. The daemon exposes only `--config`; individual
settings are not CLI flags.

```text
DENDRITE__ setting > TOML file > built-in default
```

`DENDRITE_CONFIG` selects the file itself. It is a daemon CLI environment
variable, not a nested setting.

## Safe local configuration

The checked-in [`example.toml`](../../example.toml) is runnable without root:

```sh
target/release/dendrite --config example.toml
```

It keeps the administrator API on loopback, configures public IPv4 DHT bootstrap
nodes, leaves NAT-PMP opt-in, prefers MSE with plaintext fallback, and writes
into ignored checkout directories.

## Choose directories first

```toml
data_dir = "/var/lib/dendrite"
download_dir = "/srv/dendrite/downloads"
```

`data_dir` contains the administrator token and database. `download_dir` is the
single capability root below which all torrent paths are resolved. These paths
can be on different filesystems. The daemon user must be able to create and
synchronize files in both.

Per-torrent destinations are deliberately rejected in API v2.0, so directory
layout must be decided at service scope.

## Listener roles

```toml
[listen]
api = "127.0.0.1:8412"
peer = "0.0.0.0:16493"
dht = "0.0.0.0:16309"
dht_bootstrap = []
```

- `api` is the administrator control plane.
- `peer` is both the TCP peer listener and the local uTP UDP endpoint.
- `dht` is a separate UDP socket for DHT queries/service.
- `dht_bootstrap` supplies literal `IP:port` socket addresses used for iterative
  lookup. With an empty list, tracker and local discovery still work, but DHT is
  not a fallback source of peers.

Open the peer port for both TCP and UDP when accepting incoming peers. The DHT
port is UDP. Do not expose the administrator API merely because peer traffic must
be public.

## Remote API configuration

A non-loopback API address fails validation unless a certificate, private key,
and at least one allowed browser origin are all configured:

```toml
[listen]
api = "0.0.0.0:8412"
peer = "0.0.0.0:16493"
dht = "0.0.0.0:16309"
dht_bootstrap = []
peer_encryption = "preferred"
tls_certificate = "/etc/dendrite/tls/cert.pem"
tls_private_key = "/etc/dendrite/tls/key.pem"
allowed_origins = ["https://torrents.example.com"]
```

Allowed origins control browser CORS. They are not an authentication mechanism
and should be exact origins, including scheme and port when non-default. Follow
the [remote API playbook](../playbooks/remote-api.md) before using this shape.

## Peer encryption

```toml
peer_encryption = "preferred" # default: try MSE, fall back to plaintext
peer_encryption = "disabled" # plaintext peer transport
peer_encryption = "required" # MSE failure rejects the connection
```

This controls peer transport, not HTTP API TLS. Requiring MSE can reduce the
reachable peer set; it does not anonymize traffic or hide tracker/DHT metadata.

## NAT-PMP

```toml
nat_pmp_gateway = "192.168.1.1:5351"
```

The gateway must be a nonzero IPv4 socket address. When set, the daemon renews
TCP and UDP mappings for the peer port and advertises a correlated external port.
There is no automatic gateway discovery or UPnP fallback. An incorrect gateway
does not make the administrator API safe to expose.

## Limits

Start with defaults. Lower limits to match a small host or raise them only after
measuring file descriptors, memory, API clients, and swarm behavior. Startup
rejects zero, excessive, or cross-inconsistent values, including
`active_torrents > loaded_torrents`.

```toml
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
```

These are enforcement ceilings, not preallocated capacities or performance
targets. The exact accepted ranges are in the
[configuration reference](../reference/configuration.md).

## Logging

```toml
[logging]
filter = "dendrite=info"
json = false
```

The filter uses `tracing_subscriber` environment-filter syntax. If it is invalid,
startup falls back to `dendrite=info`. JSON is convenient for a log collector;
plain formatting is easier for local terminals and journald.

## Environment overrides

Nested segments use a double underscore, including after the prefix:

```sh
DENDRITE__LOGGING__FILTER='dendrite=debug' \
DENDRITE__LIMITS__ACTIVE_TORRENTS=64 \
target/release/dendrite --config example.toml
```

The current environment source treats values as strings and does not configure
list parsing. Keep `dht_bootstrap` and `allowed_origins` in TOML rather than
trying to encode arrays in environment variables.

## Validate before deployment

```sh
target/release/dendrite --config /etc/dendrite/dendrite.toml doctor
```

Doctor validates settings, writes probes in both directories, creates or checks
the token, opens the state database, initializes storage, and temporarily binds
the API, TCP peer, uTP peer, and DHT listeners. Stop the running daemon first or
the listener probes correctly report address conflicts.

## Related pages

- [Exact configuration reference](../reference/configuration.md)
- [Environment variables](../reference/environment-variables.md)
- [Data layout](../reference/data-layout.md)
- [Remote API playbook](../playbooks/remote-api.md)
