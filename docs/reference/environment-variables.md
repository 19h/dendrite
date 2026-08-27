[← Documentation home](../../README.md)

# Environment variables

The daemon has one CLI environment variable and a nested configuration
namespace. The client has two independent variables.

## Daemon file selection

| Variable | Equivalent | Purpose |
|---|---|---|
| `DENDRITE_CONFIG` | `dendrite --config <PATH>` | select the required TOML file |

An explicit `--config` value takes precedence through normal CLI parsing. This
variable is not part of the nested settings tree.

## Daemon setting overrides

Use `DENDRITE__`, then TOML path segments separated by double underscores.
Values override the selected file:

| Variable | TOML setting |
|---|---|
| `DENDRITE__DATA_DIR` | `data_dir` |
| `DENDRITE__DOWNLOAD_DIR` | `download_dir` |
| `DENDRITE__LISTEN__API` | `listen.api` |
| `DENDRITE__LISTEN__PEER` | `listen.peer` |
| `DENDRITE__LISTEN__DHT` | `listen.dht` |
| `DENDRITE__LISTEN__NAT_PMP_GATEWAY` | `listen.nat_pmp_gateway` |
| `DENDRITE__LISTEN__PEER_ENCRYPTION` | `listen.peer_encryption` |
| `DENDRITE__LISTEN__TLS_CERTIFICATE` | `listen.tls_certificate` |
| `DENDRITE__LISTEN__TLS_PRIVATE_KEY` | `listen.tls_private_key` |
| `DENDRITE__LIMITS__LOADED_TORRENTS` | `limits.loaded_torrents` |
| `DENDRITE__LIMITS__ACTIVE_TORRENTS` | `limits.active_torrents` |
| `DENDRITE__LIMITS__PEER_CONNECTIONS` | `limits.peer_connections` |
| `DENDRITE__LIMITS__METAINFO_BYTES` | `limits.metainfo_bytes` |
| `DENDRITE__LIMITS__TRACKER_RESPONSE_BYTES` | `limits.tracker_response_bytes` |
| `DENDRITE__LIMITS__WEBSOCKET_MESSAGE_BYTES` | `limits.websocket_message_bytes` |
| `DENDRITE__LIMITS__API_CONCURRENCY` | `limits.api_concurrency` |
| `DENDRITE__LIMITS__API_REQUESTS_PER_SECOND` | `limits.api_requests_per_second` |
| `DENDRITE__LIMITS__BROWSER_SESSIONS` | `limits.browser_sessions` |
| `DENDRITE__LIMITS__LIST_PAGE_SIZE` | `limits.list_page_size` |
| `DENDRITE__LOGGING__FILTER` | `logging.filter` |
| `DENDRITE__LOGGING__JSON` | `logging.json` |

Example:

```sh
DENDRITE_CONFIG=/etc/dendrite/dendrite.toml \
DENDRITE__LOGGING__JSON=true \
DENDRITE__LIMITS__ACTIVE_TORRENTS=64 \
dendrite
```

The current environment source does not enable list parsing. Keep
`listen.dht_bootstrap` and `listen.allowed_origins` in TOML instead of relying
on an environment encoding. Optional values are also clearer in TOML; use
environment overrides primarily for scalar changes.

All variables are process inputs. The daemon does not reload them after startup.

## Client variables

| Variable | Equivalent | Default |
|---|---|---|
| `DENDRITE_API` | `dendritectl --api` | `http://127.0.0.1:8412/api/v2` |
| `DENDRITE_TOKEN_FILE` | `dendritectl --token-file` | `./dendrite-data/admin.token` |

These affect only `dendritectl`; they do not reconfigure the daemon. Conversely,
`DENDRITE__DATA_DIR` does not automatically change the client's token path.

```sh
export DENDRITE_API=http://127.0.0.1:8412/api/v2
export DENDRITE_TOKEN_FILE=/var/lib/dendrite/admin.token
dendritectl status
```

Avoid putting the token value itself in an environment variable. The client
accepts a path so normal file permissions and secret mounts can protect it.

## systemd example

Use an environment file for non-secret scalar overrides if desired:

```ini
[Service]
EnvironmentFile=-/etc/dendrite/environment
```

```sh
# /etc/dendrite/environment
DENDRITE__LOGGING__JSON=true
DENDRITE__LIMITS__ACTIVE_TORRENTS=64
```

The packaged unit does not include this directive; add it as a drop-in and run
`systemctl daemon-reload`. Keep arrays, paths, listener security, and other
structural settings in the service TOML for auditability.

## Development-only variable

| Variable | Default | Purpose |
|---|---:|---|
| `DENDRITE_SOAK_CASES` | 100,000 | case count for the ignored simulator `extended_fault_soak` test |

Repository CI sets it to 100,000 in the scheduled/manual fault-soak job. It does
not affect `dendrite`, `dendritectl`, or normal simulator invocations. Other
`DENDRITE_*_CHILD` and crash-test variables in source are private subprocess
coordination for tests, not supported operator controls.

## Related pages

- [Configuration reference](configuration.md)
- [Command-line reference](command-line.md)
- [systemd playbook](../playbooks/systemd.md)
