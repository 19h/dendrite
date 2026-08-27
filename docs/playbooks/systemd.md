[← Documentation home](../../README.md)

# Playbook: install Dendrite as a systemd service

This playbook installs the checked-in hardened unit with a dedicated `dendrite`
user, `/var/lib/dendrite` state, and `/srv/dendrite/downloads` payload storage.
Commands assume a Linux host with systemd and root access through `sudo`.

## 1. Build and install the matching binaries

```sh
cargo build --release --locked -p dendrite-daemon -p dendrite-cli
sudo install -m 0755 target/release/dendrite /usr/bin/dendrite
sudo install -m 0755 target/release/dendritectl /usr/bin/dendritectl
```

The unit's `ExecStart` uses `/usr/bin/dendrite`; installing only under
`/usr/local/bin` will not satisfy it.

## 2. Create the service identity and directories

```sh
sudo useradd \
  --system \
  --home-dir /var/lib/dendrite \
  --create-home \
  --shell /usr/sbin/nologin \
  dendrite

sudo install -d -o root -g root -m 0755 /etc/dendrite
sudo install -d -o dendrite -g dendrite -m 0750 /var/lib/dendrite
sudo install -d -o dendrite -g dendrite -m 0750 /srv/dendrite/downloads
```

If the user already exists, verify its UID, primary group, home, and ownership
instead of recreating it.

## 3. Write the service configuration

Create `/etc/dendrite/dendrite.toml`:

```toml
data_dir = "/var/lib/dendrite"
download_dir = "/srv/dendrite/downloads"

[listen]
api = "127.0.0.1:8412"
peer = "0.0.0.0:16493"
dht = "0.0.0.0:16309"
dht_bootstrap = []
peer_encryption = "disabled"
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

[logging]
filter = "dendrite=info"
json = false
```

Install it with non-secret, root-owned permissions:

```sh
sudo chown root:root /etc/dendrite/dendrite.toml
sudo chmod 0644 /etc/dendrite/dendrite.toml
```

The administrator secret is generated separately in `data_dir` with mode 0600;
do not put it in TOML.

## 4. Validate as the service user

The daemon must not be running while doctor tests listener availability:

```sh
sudo -u dendrite \
  /usr/bin/dendrite \
  --config /etc/dendrite/dendrite.toml \
  doctor
```

Require `"healthy": true` before installing or starting the unit. This command
also creates `/var/lib/dendrite/admin.token` and `state.redb` if absent.

## 5. Install and verify the unit

```sh
sudo install -m 0644 \
  packaging/dendrite.service \
  /etc/systemd/system/dendrite.service

sudo systemd-analyze verify /etc/systemd/system/dendrite.service
sudo systemctl daemon-reload
sudo systemctl enable --now dendrite.service
```

Checkpoint:

```sh
systemctl status dendrite.service
journalctl -u dendrite.service -n 100 --no-pager
```

The unit restricts filesystems, namespaces, devices, kernel interfaces, address
families, and executable memory. `ReadWritePaths` allows only the documented
state and payload directories. If you change those paths in TOML, update the unit
hardening boundary too or startup/storage will fail.

## 6. Administer the local service

The mode-0600 token is owned by `dendrite`. Run the client as that user or as
root; do not make the token world-readable:

```sh
sudo -u dendrite \
  /usr/bin/dendritectl \
  --token-file /var/lib/dendrite/admin.token \
  status

sudo -u dendrite \
  /usr/bin/dendritectl \
  --token-file /var/lib/dendrite/admin.token \
  add /path/readable/by/dendrite/example.torrent --start
```

The service account needs read permission on the metainfo path passed to the
client. The daemon receives uploaded bytes through the API; it does not open that
source path itself.

## 7. Configure network policy

For public peer connectivity, allow:

```text
16493/tcp  BitTorrent TCP peers
16493/udp  uTP peers
16309/udp  DHT listener
```

The API remains on host loopback and needs no firewall exposure. DHT still needs
explicit bootstrap nodes. NAT-PMP is opt-in and should name the actual local IPv4
gateway.

## 8. Stop and upgrade

```sh
sudo systemctl stop dendrite.service
sudo cp -a /var/lib/dendrite /var/lib/dendrite.backup-YYYYMMDD
sudo install -m 0755 target/release/dendrite /usr/bin/dendrite
sudo install -m 0755 target/release/dendritectl /usr/bin/dendritectl
sudo systemctl start dendrite.service
```

Replace `YYYYMMDD` with a unique label and ensure that backup destination does
not exist before copying; repeated `cp` into an existing directory can create a
nested layout that is easy to misread during recovery.

`SIGTERM` triggers graceful HTTP shutdown followed by a 30-second engine shutdown
grace. Keep daemon and client from the same revision. Because the project is
alpha, test restore with the new binary before treating the upgrade as complete;
do not rely on an older binary to open state changed by a newer schema.

## Failure branches

- **`status=203/EXEC`:** binary is missing from `/usr/bin` or is not executable.
- **Permission denied in state/downloads:** directory ownership conflicts with
  `User=dendrite`, `UMask=0077`, or `ReadWritePaths`.
- **Address already in use:** another process or daemon instance owns a listener.
- **Client token failure:** invoke as `dendrite`/root and use the absolute token.
- **Service starts manually but not under systemd:** compare TOML paths with unit
  hardening and inspect the journal.

## Next steps

- Exact settings: [Configuration reference](../reference/configuration.md)
- Monitoring and shutdown: [Observability](../operations/observability.md)
- Recovery: [Torrent recovery](recover-torrent.md)
