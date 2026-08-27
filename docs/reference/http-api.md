[← Documentation home](../../README.md)

# HTTP API reference

API version `2.0` is served below `/api/v2`. The default origin is
`http://127.0.0.1:8412`. Except for liveness and the OpenAPI discovery document,
routes require the administrator bearer token or a valid browser session.

```sh
API=http://127.0.0.1:8412/api/v2
TOKEN=$(tr -d '\n' < ./dendrite-data/admin.token)
curl --fail --show-error \
  -H "Authorization: Bearer $TOKEN" \
  "$API/status"
```

Do not place real tokens in shell history, logs, or shared scripts. The variables
above are for a short local illustration.

## Route table

| Method | Path | Auth | Success | Purpose |
|---|---|---|---|---|
| `GET` | `/healthz` | public | 204 | HTTP router liveness only |
| `GET` | `/api/v2/openapi.json` | public | 200 JSON | incomplete OpenAPI 3.1 discovery document |
| `GET` | `/api/v2/status` | required | 200 JSON | daemon/process summary |
| `GET` | `/api/v2/torrents` | required | 200 JSON | cursor-paginated torrent summaries |
| `POST` | `/api/v2/torrents` | required | 201 JSON | multipart metainfo import |
| `POST` | `/api/v2/torrents/magnet` | required | 201 JSON | magnet import |
| `GET` | `/api/v2/torrents/{id}` | required | 200 JSON | one torrent summary |
| `DELETE` | `/api/v2/torrents/{id}` | required | 204 | remove record, retain payload |
| `POST` | `/api/v2/torrents/{id}/actions` | required | 200 JSON | pause, resume, recheck, or announce |
| `GET` | `/api/v2/events` | required | 101 WebSocket | process-local event stream |
| `POST` | `/api/v2/auth/session` | required; bearer for the first session | 200 JSON + cookie | create browser session |
| `POST` | `/api/v2/auth/session/logout` | required | 204 + expired cookie | destroy browser session |
| `POST` | `/api/v2/auth/token/rotate` | required | 200 JSON | replace administrator token |
| `GET` | `/api/v2/metrics` | required | 200 text | Prometheus exposition |

## Authentication

Bearer form:

```http
Authorization: Bearer <unpadded-base64url-32-byte-token>
```

The token is stored at `data_dir/admin.token`. Every protected route passes API
concurrency and rate admission before authentication; failed authentication is
counted.

### Browser session

Create a session with the bearer token:

```sh
curl --fail --show-error \
  -c cookies.txt \
  -H "Authorization: Bearer $TOKEN" \
  -X POST "$API/auth/session"
```

Response:

```json
{
  "csrf_token": "base64url value",
  "expires_in_seconds": 43200
}
```

The `dendrite_session` cookie is HTTP-only, SameSite=Strict, scoped to
`/api/v2`, and marked Secure for a non-loopback API bind. Session-authenticated
`POST`, `PATCH`, and `DELETE` requests must include the exact returned value in
`X-CSRF-Token`; safe methods do not. Sessions live in memory for 12 hours and
are lost on restart or token rotation.

## Status

`GET /api/v2/status` returns:

```json
{
  "api_version": "2.0",
  "daemon_version": "2.0.0-alpha.1",
  "uptime_seconds": 123,
  "loaded_torrents": 1,
  "active_torrents": 1,
  "connected_peers": 4,
  "quarantined_records": 0,
  "storage_backend": "io_uring"
}
```

`storage_backend` is `portable` or, on Linux when successfully initialized,
`io_uring`. Status currently substitutes zero/empty values for a failed database
listing or quarantine-count query; correlate unexpected zeros with logs.

## Torrent summaries

Add, get, list items, and action responses use:

```json
{
  "id": "0190f000-0000-7000-8000-000000000000",
  "name": "example",
  "state": "downloading",
  "v1_info_hash": "40 lowercase hex characters or null",
  "v2_info_hash": "64 lowercase hex characters or null",
  "total_length": 1048576,
  "downloaded": 524288,
  "uploaded": 0,
  "download_rate": 131072,
  "upload_rate": 0,
  "peers": 4
}
```

States are `stopped`, `starting`, `downloading`, `seeding`, `checking`, `error`,
and `stopping`. Rates are bytes per second sampled in process; counters are
bytes. A newly added magnet can have `total_length: 0` until metadata arrives.

## List and pagination

```http
GET /api/v2/torrents?limit=100&cursor=<torrent-uuid>
```

`limit` defaults to and cannot exceed `limits.list_page_size`; zero is invalid.
`cursor` is the last UUID from the preceding page. The result is ordered by
UUID and shaped as:

```json
{
  "items": [],
  "next_cursor": null
}
```

Repeat with `next_cursor` until it is null. Treat a cursor as opaque even though
the current encoding is the UUID string.

## Import metainfo

Send `multipart/form-data` with:

- `metainfo`: required `.torrent` bytes;
- `options`: optional JSON text, defaulting to all false/absent.

```sh
curl --fail --show-error \
  -H "Authorization: Bearer $TOKEN" \
  -F 'metainfo=@./example.torrent' \
  -F 'options={"start":true};type=application/json' \
  "$API/torrents"
```

Supported option:

```json
{"start": true}
```

The shared options type also contains `destination` and `sequential`, but API
v2.0 rejects a non-null destination or `sequential: true`. All payloads use the
global download root and the engine's normal scheduler.

## Import magnet

```sh
curl --fail --show-error \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"source":"magnet","uri":"magnet:?xt=urn:btih:…","options":{"start":true}}' \
  "$API/torrents/magnet"
```

The tagged request shape is:

```json
{
  "source": "magnet",
  "uri": "magnet URI",
  "options": {"start": false}
}
```

## Actions

```sh
curl --fail --show-error \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"action":"recheck"}' \
  "$API/torrents/$ID/actions"
```

Allowed values: `pause`, `resume`, `recheck`, `announce`. `announce` is not
exposed by `dendritectl`; the current handler preserves durable state and sends
the engine's start/resume command, which can replace an existing actor. Do not
model it as a tracker-only no-op.

## Events

Each WebSocket text frame is:

```json
{
  "schema_version": 1,
  "sequence": 42,
  "timestamp_unix_ms": 1710000000000,
  "resource_id": "torrent UUID or null",
  "kind": "torrent_state_changed",
  "payload": {}
}
```

Kinds are `torrent_added`, `torrent_state_changed`, `torrent_removed`, and
`resync_required`. Sequence is process-local, not a replay offset. On subscriber
lag the daemon sends `resync_required`, closes the socket, and expects the client
to retrieve a fresh paginated snapshot.

## Metrics

`GET /api/v2/metrics` returns Prometheus text format. Exact names:

```text
dendrite_api_requests_total
dendrite_api_authentication_failures_total
dendrite_api_rejected_requests_total
dendrite_token_rotations_total
dendrite_browser_sessions_created_total
dendrite_torrents
dendrite_active_torrents
```

Counters reset on process restart; there are no per-torrent metric labels.

## Token rotation

`POST /api/v2/auth/token/rotate` returns the new secret once:

```json
{"token":"new unpadded base64url value"}
```

It uses `Cache-Control: no-store`, atomically replaces the daemon token file,
switches the in-memory bearer secret, and clears every browser session. The
request's old credential is invalid after success.

## Problem responses

Errors have content type `application/problem+json`:

```json
{
  "type": "https://dendrite-bt.org/problems/invalid_request",
  "title": "Invalid request",
  "status": 400,
  "code": "invalid_request",
  "detail": "human-readable detail",
  "instance": null
}
```

| HTTP | Code | Meaning |
|---:|---|---|
| 400 | `invalid_request` | malformed ID/body/options/metadata or invalid page request |
| 401 | `unauthorized` | bearer/session/CSRF proof missing or invalid |
| 404 | `not_found` | UUID has no current record |
| 409 | `conflict` | v1 or v2 identity already registered |
| 429 | `limit_reached` | configured request/torrent/session capacity reached |
| 500 | `internal_error` | persistence, engine, encoding, or internal failure |

Use status and `code` for program flow. `detail` is not a stable parser contract.
HTTP infrastructure can also emit responses such as 413 for a body larger than
the configured route limit without the application problem shape.

## Description limitations

The public OpenAPI document lists principal paths and a small required-field
subset. It is hand-maintained and not sufficient by itself for exact client
generation. The versioned types in `dendrite-api-types`, route handlers, and
this page define the current practical contract. `FilePriorityUpdate` and
`Operation` are defined shared types but have no current routes.

## Related pages

- [Remote API playbook](../playbooks/remote-api.md)
- [Control plane](../architecture/control-plane.md)
- [Observability](../operations/observability.md)
- [Status and limitations](status-limitations.md)
