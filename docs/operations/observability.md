[← Documentation home](../../README.md)

# Observability and maintenance

Dendrite exposes four different operational views: liveness, authenticated
status, structured logs, and streaming/counter interfaces. They answer different
questions and should not be collapsed into one “healthy” signal.

## Liveness

```sh
curl --fail --silent --show-error \
  http://127.0.0.1:8412/healthz \
  --output /dev/null
```

`GET /healthz` returns HTTP 204 without authentication. It proves that the Axum
router is responding. It does not query the database, storage, trackers, peers,
or individual torrent actors.

## Authenticated status

```sh
dendritectl status
```

Status reads torrent records and the quarantine count, samples current peer/rate
state, and reports the selected storage backend. A database listing failure is
currently converted to an empty list in status, so combine surprising zero
counts with logs and doctor rather than treating them as proof of an empty
database.

## Doctor

```sh
dendrite --config /etc/dendrite/dendrite.toml doctor
```

Doctor returns pretty JSON and exits nonzero when any check fails.

| Check | What it exercises |
|---|---|
| `configuration` | complete settings validation |
| `data_directory` | create directory and synchronized write/delete probe |
| `download_directory` | create directory and synchronized write/delete probe |
| `administrator_token` | create or decode token and enforce private Unix mode |
| `state_database` | open/create schema and writable database |
| `payload_storage` | initialize automatic backend with queue capacity 8 |
| `api_listener` | bind configured API TCP address |
| `peer_tcp_listener` | bind configured peer TCP address |
| `peer_utp_listener` | bind uTP UDP endpoint on peer address |
| `dht_udp_listener` | bind configured DHT UDP address |

Run it as the daemon user with the daemon stopped. It mutates configured
directories and database/token state and will conflict with listeners already in
use.

## Logging

Configuration:

```toml
[logging]
filter = "dendrite=info"
json = true
```

The filter is parsed as a `tracing_subscriber` environment filter. Invalid input
falls back to `dendrite=info`. JSON changes formatting, not event selection.

Operationally useful events include API start/stop, storage backend, NAT-PMP
renewal or failure, actor failures, tracker announce failures at debug level,
event-bridge lag, and graceful-shutdown problems. Debug logs can expose peer
addresses, tracker URLs, torrent UUIDs, paths, and failure details; sanitize them
before sharing.

Under systemd:

```sh
journalctl -u dendrite.service --since today
journalctl -u dendrite.service --follow
```

## Prometheus text metrics

`GET /api/v2/metrics` is authenticated. It currently exports:

| Metric | Type | Meaning |
|---|---|---|
| `dendrite_api_requests_total` | counter | protected requests entering authentication middleware |
| `dendrite_api_authentication_failures_total` | counter | invalid bearer/session/CSRF attempts |
| `dendrite_api_rejected_requests_total` | counter | API concurrency or rate-window rejections |
| `dendrite_token_rotations_total` | counter | successful administrator token rotations |
| `dendrite_browser_sessions_created_total` | counter | sessions created since process start |
| `dendrite_torrents` | gauge | currently readable records |
| `dendrite_active_torrents` | gauge | records not in `stopped` or `error` |

Counters are process-local and reset on restart. There are no per-torrent labels,
byte totals, latency histograms, tracker metrics, or persistent metric state in
the current endpoint.

## WebSocket events

Authenticated clients upgrade `GET /api/v2/events`. Every JSON envelope contains:

```json
{
  "schema_version": 1,
  "sequence": 42,
  "timestamp_unix_ms": 0,
  "resource_id": "torrent UUID or null",
  "kind": "torrent_state_changed",
  "payload": {}
}
```

Current event kinds:

- `torrent_added` with a summary payload;
- `torrent_state_changed` from API actions or engine state events;
- `torrent_removed` with a null payload;
- `resync_required` when the subscriber lags the bounded broadcast buffer.

Sequence numbers are process-local and not durable. The stream has no replay.
After `resync_required`, the server closes the socket; fetch status/torrent pages,
then reconnect. Each send has a 10-second timeout, and configured WebSocket
message/frame limits apply.

## OpenAPI discovery

`GET /api/v2/openapi.json` is public and declares OpenAPI 3.1, route names, basic
security schemes, and a small schema subset. It is not currently generated from
Rust types and does not exhaustively describe request bodies, response fields, or
all error responses. Use the [HTTP API reference](../reference/http-api.md) and
source types for exact integration.

## Token maintenance

```sh
dendritectl rotate-token
```

The daemon writes a new random 256-bit token through a new mode-0600 temporary
file, synchronizes it, renames it atomically, and then swaps the in-memory token.
The client similarly replaces its configured token file. Browser sessions are
cleared. Keep an out-of-band way to read the daemon's token file if the client
cannot persist its returned credential.

## Graceful shutdown and restart

`Ctrl-C` and Unix `SIGTERM` stop HTTP serving and ask the engine to cancel all
actors/background services. The daemon waits up to 30 seconds for engine
acknowledgement; the systemd unit allows 35 seconds.

Restart automatically submits actors only for records persisted as `starting`,
`downloading`, or `checking`. It does not automatically submit `seeding`,
`stopped`, or `error` records. Incoming seeding eligibility is nevertheless read
from durable `seeding` state by the shared incoming-peer service after startup.

## Quarantine

Persistence checks record and schema versions and moves undecodable torrent
records into a quarantine table rather than failing every healthy record.
`quarantined_records` exposes only a count. There is no public API to inspect,
repair, or restore quarantined bytes. Preserve `state.redb` and investigate with
the exact binary/version that observed the problem.

## Related pages

- [Recovery playbook](../playbooks/recover-torrent.md)
- [HTTP API reference](../reference/http-api.md)
- [Data layout](../reference/data-layout.md)
- [Performance](performance.md)
