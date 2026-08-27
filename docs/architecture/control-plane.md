[← Documentation home](../../README.md)

# Control plane

The control plane is a versioned HTTP API around durable state and the engine
supervisor. It is intentionally small: there is no embedded web UI, multi-user
authorization model, or direct filesystem access through the API.

## Request path

```text
request
  → request ID / tracing / panic containment
  → global concurrency and rate admission
  → bearer or browser-session authentication
  → CSRF check for session-authenticated mutations
  → route body and input limits
  → serialized mutation when state changes
  → persistence and/or engine command
  → JSON response, problem document, or event frame
```

`GET /healthz` and `GET /api/v2/openapi.json` are public. Every operational
route below `/api/v2` is protected, including metrics and creation of a browser
session.

## Authentication

At first initialization the daemon generates 32 random bytes, encodes them as
unpadded base64url, and writes `data_dir/admin.token`. On Unix the token file is
created with mode `0600`. API bearer comparison is constant-time.

```http
Authorization: Bearer <contents-of-admin.token>
```

The token is a service-wide administrator secret: possession authorizes all
available operations. Rotation atomically replaces it and invalidates all
in-memory browser sessions. Existing bearer clients must reload the new file.

## Browser sessions and CSRF

An already authenticated bearer client can create a browser session. The
daemon returns an HTTP-only, SameSite=Strict cookie and a CSRF value. Mutating
requests authenticated by that cookie must also send the CSRF value in
`X-CSRF-Token`.

Sessions are process-local, expire after 12 hours, and are bounded by
`limits.browser_sessions`. Restart and token rotation clear them. The cookie is
marked `Secure` when the configured API listener is non-loopback. A client must
therefore use HTTPS for remotely exposed browser sessions.

This mechanism is for a trusted administration client; it is not user account
management or tenant isolation.

## Remote exposure invariant

If `listen.api` is not a loopback address, configuration validation requires:

- both `api.tls_cert_path` and `api.tls_key_path`; and
- at least one exact `api.allowed_origins` entry.

These checks prevent the common accidental case of publishing an administrator
API in cleartext with unrestricted browser origins. They do not configure a
firewall, certificate renewal, DNS, or a reverse proxy for you. See the
[remote API playbook](../playbooks/remote-api.md).

## Capacity admission

The control plane rejects excess work using configured bounds:

- `limits.api_concurrency` caps concurrent admitted requests;
- `limits.api_requests_per_second` caps request admission rate;
- one mutation semaphore serializes state-changing torrent operations;
- body limits separately cap metainfo, tracker, and WebSocket payloads;
- `limits.list_page_size` caps one listing page;
- loaded/active torrent limits are enforced at the relevant mutations.

HTTP 429 means a configured capacity was reached. Clients should back off with
jitter and should not convert overload into a tight retry loop.

## Reads, mutations, and engine commands

Read handlers query persistence and live engine counters. Mutation handlers
perform validation, acquire the mutation permit, update durable state, and then
send an engine command where runtime work is needed.

Durable intent and asynchronous execution are separate. For example, a started
import writes its record transactionally before submitting `Start`; the API can
return while discovery and transfer are still beginning. Consumers should
follow state summaries or events rather than treating the HTTP response as
transfer completion.

## Pagination and summaries

`GET /api/v2/torrents` accepts `limit` and an opaque `cursor` and returns:

```json
{
  "items": [],
  "next_cursor": null
}
```

Callers must repeat the request with `next_cursor` until it is null. The current
CLI exposes only the first default page, so exhaustive automation should use
the API directly.

Torrent summaries combine durable fields with sampled engine data. Rates need
at least 250 ms between samples; the first sample is normally zero. A quiet rate
does not establish completion.

## Events

`GET /api/v2/events` upgrades to a WebSocket and emits schema-versioned events:

- `torrent_added`;
- `torrent_state_changed`;
- `torrent_removed`;
- `resync_required`.

Sequence numbers are monotonic only within one daemon process. They do not form
a durable replay log. If a subscriber falls behind the bounded broadcast
buffer, the server sends `resync_required` and closes the connection; retrieve
a fresh paginated snapshot before reconnecting. A stalled send is abandoned
after 10 seconds.

## Errors

API errors use problem JSON with a stable machine-oriented code. Current codes
include `unauthorized`, `invalid_request`, `not_found`, `conflict`,
`limit_reached`, and `internal_error`. Use the HTTP status and code for control
flow; do not parse the human-readable detail string.

Panics are caught at the HTTP boundary and become internal errors, but a panic
inside another asynchronous subsystem can still terminate that task or, under
release panic policy, the process. Panic containment is not a substitute for
supervision or restart policy.

## API description boundary

`GET /api/v2/openapi.json` is a hand-maintained discovery document. It describes
the principal routes but is not currently a complete generated schema for all
request and response bodies. Treat the [HTTP API reference](../reference/http-api.md)
and versioned Rust types as the more precise contract when generating a client.

## Related pages

- [HTTP API reference](../reference/http-api.md)
- [Command-line reference](../reference/command-line.md)
- [Observability](../operations/observability.md)
- [Storage and security](storage-security.md)
