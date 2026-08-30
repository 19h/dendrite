[← Documentation home](../../README.md)

# Command-line reference

Dendrite installs two operator commands. `dendrite` runs or diagnoses the
service; `dendritectl` is an authenticated HTTP client. A development-only
`dendrite-sim` binary models swarm behavior without running the daemon.

Examples below show the current `2.0.0-alpha.1` command surface.

## `dendrite`

```text
Usage: dendrite [OPTIONS] [COMMAND]

Commands:
  run
  doctor

Options:
      --config <CONFIG>  [env: DENDRITE_CONFIG=]
  -h, --help
  -V, --version
```

If no command is supplied, the daemon runs. These are equivalent:

```sh
dendrite --config /etc/dendrite/dendrite.toml
dendrite --config /etc/dendrite/dendrite.toml run
```

`--config` names a required TOML file when supplied. Without it, built-in
defaults plus nested environment overrides are used.

`doctor` loads and validates configuration, creates/probes the state and
download directories, creates or validates the token, opens the database and
storage backend, and binds all configured listeners. It prints a JSON report.
It has no additional flags and is not read-only. Run it as the service user
while the daemon is stopped.

## `dendritectl`

```text
Usage: dendritectl [OPTIONS] <COMMAND>

Options:
      --api <API>
          [env: DENDRITE_API=]
          [default: http://127.0.0.1:8412/api/v2]
      --token-file <TOKEN_FILE>
          [env: DENDRITE_TOKEN_FILE=]
          [default: ./dendrite-data/admin.token]
  -h, --help
  -V, --version
```

Global options must appear before the subcommand. `--api` is the versioned base
URL, not the host root. `--token-file` is read for every invocation; surrounding
whitespace is trimmed before constructing the bearer header.

Successful data commands print pretty JSON. Failures print one line to stderr
and exit unsuccessfully. The client prints the complete server problem body for
a non-success response.

### `status`

```sh
dendritectl status
```

Calls `GET /status` and prints daemon version, uptime, torrent/peer counts,
quarantine count, and storage backend.

### `list`

```sh
dendritectl list
```

Calls `GET /torrents` with server defaults. There are currently no client flags
for cursor or limit, so this command returns only the first page.

### `watch`

```text
Usage: dendritectl watch [OPTIONS] [ID]

Arguments:
  [ID]  Torrent ID. Omit it to watch every torrent

Options:
      --interval <INTERVAL>  Refresh interval in seconds [default: 1]
      --no-clear             Append snapshots instead of redrawing the terminal
```

`watch` is the human-facing live progress view. Without an ID it follows the
entire queue, traversing every API page; with an ID it shows a larger progress
bar and details for that torrent. Both views include verified bytes, total size,
live download/upload rates, connected peers, and estimated time remaining.

```sh
dendritectl watch
dendritectl watch 01a05054-eb7e-7da3-9978-a673389fad22
dendritectl watch --interval 5
```

Interactive terminals are redrawn in place until `Ctrl-C`. Redirected output
and `--no-clear` append timestamp-free snapshots, making them suitable for log
capture. The minimum interval is one second.

### `add`

```text
Usage: dendritectl add [OPTIONS] <SOURCE>

Arguments:
  <SOURCE>

Options:
      --start
```

If `SOURCE` starts exactly with `magnet:`, the client sends it to the magnet
JSON endpoint. Otherwise it reads `SOURCE` as a local metainfo file and sends a
multipart upload. Quote magnet URIs in a shell:

```sh
dendritectl add ./image.torrent
dendritectl add ./image.torrent --start
dendritectl add 'magnet:?xt=urn:btih:…&tr=…' --start
```

`--start` asks the daemon to schedule transfer after the record commits. Without
it, the new record remains `stopped`.

### `pause`, `resume`, and `recheck`

```sh
dendritectl pause <ID>
dendritectl resume <ID>
dendritectl recheck <ID>
```

`ID` is the torrent UUID printed by add/list. Each command calls the action
endpoint and prints the resulting summary. Recheck verifies payload data and
does not automatically reacquire invalid pieces; resume after an incomplete
result.

### `remove`

```sh
dendritectl remove <ID>
```

Removes the service record and identity indexes. It prints no output on success.
Payload files remain below `download_dir`.

### `rotate-token`

```sh
dendritectl rotate-token
```

The daemon rotates its credential and returns the new token. The client writes a
temporary file next to `--token-file` (mode `0600` on Unix), synchronizes it,
renames it over that path, synchronizes the parent directory, then prints
`administrator token rotated`.

Use a token path the invoking user can replace. If the remote rotation succeeds
but the local write fails, the old local token is already invalid; retrieve the
new `admin.token` through authorized host access.

## Explicit service targets

For automation, make the target unambiguous:

```sh
dendritectl \
  --api https://downloads.example.com:8412/api/v2 \
  --token-file /run/secrets/dendrite-admin-token \
  status
```

Environment equivalents:

```sh
export DENDRITE_API=https://downloads.example.com:8412/api/v2
export DENDRITE_TOKEN_FILE=/run/secrets/dendrite-admin-token
dendritectl list
```

## `dendrite-sim`

This workspace development tool runs a deterministic model and prints JSON:

```text
Usage: dendrite-sim [OPTIONS]

      --seed <SEED>                                  [default: 1]
      --pieces <PIECES>                              [default: 1024]
      --peers <PEERS>                                [default: 32]
      --maximum-steps <MAXIMUM_STEPS>                [default: 1000000]
      --corruption-per-mille <CORRUPTION_PER_MILLE>  [default: 10]
      --churn-per-mille <CHURN_PER_MILLE>            [default: 5]
  -h, --help
  -V, --version
```

It is not an administration client and does not connect to a running daemon.

## Related pages

- [First torrent playbook](../playbooks/first-torrent.md)
- [Torrent management](../operations/torrent-management.md)
- [HTTP API reference](http-api.md)
- [Environment variables](environment-variables.md)
