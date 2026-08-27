[← Documentation home](../../README.md)

# Playbook: expose and use the remote API

Dendrite rejects a non-loopback API listener unless TLS certificate/key paths and
at least one allowed browser origin are configured. This playbook preserves that
contract and treats the administrator token as a service-wide secret.

## 1. Decide whether remote exposure is necessary

Prefer loopback plus local `dendritectl`, `docker exec`, or an authenticated host
administration channel when possible. Every authenticated API route can add,
start, stop, inspect, or remove torrents, rotate the token, consume events, and
read metrics.

Public without authentication:

```text
GET /healthz
GET /api/v2/openapi.json
```

Everything nested below `/api/v2` in the operational router requires a bearer
token or valid browser session.

## 2. Install TLS material

Use a certificate whose subject/SAN matches the hostname clients will use. Keep
the private key readable by the daemon and no broader than necessary. Example
paths:

```text
/etc/dendrite/tls/cert.pem
/etc/dendrite/tls/key.pem
```

The daemon serves TLS directly for a non-loopback bind. Certificate issuance and
renewal are external operational responsibilities.

## 3. Configure the listener and origins

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

List every browser origin that may send credentialed requests. Origins include
scheme, host, and non-default port. CORS is a browser policy, not authorization;
all operational requests still authenticate.

Validate before starting:

```sh
dendrite --config /etc/dendrite/dendrite.toml doctor
```

Stop the running daemon first so doctor can bind the configured sockets.

## 4. Use `dendritectl` remotely

Securely copy the current administrator token to the client host with mode 0600.
Then make both locations explicit:

```sh
dendritectl \
  --api https://dendrite.example.com:8412/api/v2 \
  --token-file ./admin.token \
  status
```

The client uses rustls-backed HTTPS through `reqwest`. A private CA must be
trusted by the client environment/build; do not disable certificate validation
in documentation or scripts.

The equivalent environment inputs are:

```sh
DENDRITE_API=https://dendrite.example.com:8412/api/v2 \
DENDRITE_TOKEN_FILE=./admin.token \
dendritectl status
```

## 5. Establish a browser session

A browser-oriented client first authenticates `POST /api/v2/auth/session` with
the bearer token. The response:

- sets `dendrite_session` as an HttpOnly, SameSite=Strict, Secure cookie for
  `/api/v2`;
- returns a CSRF token and 12-hour expiry in JSON;
- sets `Cache-Control: no-store`.

Subsequent `GET`, `HEAD`, and `OPTIONS` requests can rely on the cookie. Mutating
requests must also include the returned value as `X-CSRF-Token`. Log out with
`POST /api/v2/auth/session/logout` using that header.

The daemon holds sessions in memory. A restart, logout, expiry, or administrator
token rotation invalidates them.

## 6. Rotate the administrator token

Run rotation from a trusted client that can atomically replace its token file:

```sh
dendritectl \
  --api https://dendrite.example.com:8412/api/v2 \
  --token-file ./admin.token \
  rotate-token
```

The daemon persists the new token before switching its in-memory value. The
client then atomically replaces its local file. Rotation invalidates the old
bearer token and all browser sessions immediately.

If the client receives the new token but cannot persist it, recover it from the
successful HTTP response only if your transport tooling retained it securely;
otherwise access the daemon's `data_dir/admin.token` through a trusted host
channel. Do not repeatedly retry with the now-invalid old token.

## 7. Constrain network access

TLS and authentication are required but not sufficient operational policy:

- restrict port 8412 to intended client networks;
- rate-limit at the daemon and, if present, at an outer firewall/proxy;
- never publish `admin.token` in environment dumps, URLs, logs, or issue reports;
- monitor authentication failures and rejected requests;
- keep `/healthz` and the public OpenAPI document in the exposure assessment;
- remember that metrics are authenticated but reveal operational state.

## Failure branches

- **Startup says remote API requires TLS:** both TLS paths must be present.
- **Startup says allowed origin required:** configure at least one exact origin.
- **TLS hostname/CA error:** fix certificate identity or client trust; do not use
  plain HTTP.
- **Browser GET works but mutation is 401:** send the matching CSRF token header.
- **All sessions stop after rotation:** expected; establish new sessions with the
  new token.
- **HTTP 429:** API concurrency, requests-per-second, session, loaded-torrent, or
  active-torrent limit was reached; inspect the problem detail.

## Next steps

- Exact auth and routes: [HTTP API reference](../reference/http-api.md)
- Control-plane mechanics: [Control plane](../architecture/control-plane.md)
- Metrics and events: [Observability](../operations/observability.md)
