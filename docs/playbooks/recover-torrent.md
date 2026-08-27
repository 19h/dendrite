[← Documentation home](../../README.md)

# Playbook: diagnose and recover a torrent

Start with observation, then choose the smallest state-changing operation. Do not
delete `state.redb`, rewrite payload files, or remove the torrent merely because
an actor entered `error`.

## 1. Capture current service state

```sh
dendritectl status
dendritectl list
```

Record:

- daemon and API versions;
- storage backend;
- loaded/active torrent counts;
- quarantined record count;
- the affected torrent UUID, state, counters, rates, and peer count;
- the exact client error body, if any.

Rate fields are samples and can be zero without proving a stall. The `error`
state does not carry detail in `TorrentSummary`; use daemon logs or state events
for the failure text.

## 2. Inspect logs without restarting

Foreground process: inspect its stderr/stdout. systemd:

```sh
journalctl -u dendrite.service -n 200 --no-pager
journalctl -u dendrite.service --since '30 minutes ago'
```

Useful actor error classes include missing/invalid magnet metadata, no usable
tracker, no peers, peer-session failure, web-seed failure, conflicting payload
ownership, metainfo failure, storage I/O, and actor cancellation.

## 3. Separate service health from torrent health

The unauthenticated liveness route only proves the HTTP process is answering:

```sh
curl --fail --silent --show-error \
  http://127.0.0.1:8412/healthz \
  --output /dev/null
```

`dendritectl status` additionally proves authenticated API access and database
listing. Neither proves tracker reachability, peer interoperability, or payload
integrity for a particular torrent.

## 4. Use doctor only while stopped

Stop the daemon, then run doctor with the same configuration and user:

```sh
sudo systemctl stop dendrite.service
sudo -u dendrite dendrite --config /etc/dendrite/dendrite.toml doctor
```

Doctor checks configuration, directory writability, token, database, storage
backend, and all four configured listener binds. Running it alongside the daemon
produces expected address conflicts and is not a meaningful health result.

If `quarantined_records` is nonzero, the persistence layer found records it could
not decode and moved them out of the active torrent table. Preserve the database
for analysis. There is no public repair/import command for quarantined bytes.

## 5. Choose a torrent operation

### Resume an interrupted or stopped transfer

```sh
dendritectl resume <TORRENT_ID>
```

Use when metadata and existing completion state are trusted and the previous
failure was transient, such as network unavailability.

### Rebuild completion from payload bytes

```sh
dendritectl recheck <TORRENT_ID>
```

Use after a file was deleted, truncated, replaced, externally modified, restored
from backup, or written during an I/O failure. Recheck verifies pieces rather
than trusting sizes or timestamps.

Outcome:

```text
all pieces valid          checking -> seeding
missing/corrupt pieces    checking -> stopped
I/O or metadata failure   checking -> error
```

If recheck ends in `stopped`, resume to download missing pieces.

### Pause before filesystem work

```sh
dendritectl pause <TORRENT_ID>
```

Pause waits for the actor cancellation path before returning. It does not delete
or rewrite payload files.

### Remove only when forgetting state is intended

```sh
dendritectl remove <TORRENT_ID>
```

This is not a repair action. It deletes the durable torrent record and hash index
after actor cancellation. Payloads remain on disk, and re-adding the same torrent
creates a new UUID/completion record that should be rechecked.

## 6. Diagnose no-peer failures

Check in this order:

1. Does the torrent or magnet contain a usable `http`, `https`, or `udp` tracker?
2. Did the tracker return connectable compact peers?
3. If relying on DHT, is `dht_bootstrap` nonempty and reachable?
4. Is the torrent private? Private torrents intentionally suppress DHT, LSD, and
   PEX expansion.
5. Are peer TCP/uTP ports allowed through host firewall/NAT?
6. Is `peer_encryption = "required"` excluding plaintext-only peers?
7. Did peer-wire validation reject an invalid bitfield, block, proof, or frame?

LSD is LAN-local and not an internet bootstrap mechanism. NAT-PMP affects incoming
mapping/advertisement; it does not create outbound tracker or DHT results.

## 7. Diagnose storage failures

Confirm free space, inode availability, mount health, service-user permissions,
and unit hardening. Dendrite also rejects symlinked parents and multiply linked
payloads by design. Do not “fix” those errors by relaxing directory ownership or
following an external link; move intended data into a normal path below the
configured download root, then recheck.

On Linux, a `portable` backend is not itself a failure. Automatic startup falls
back when io_uring initialization is unavailable.

## 8. Restart safely

`Ctrl-C` and `SIGTERM` trigger graceful HTTP shutdown followed by a 30-second
engine shutdown grace. On restart, records persisted as `starting` or
`downloading` are resumed, and records persisted as `checking` are rechecked.
`stopped`, `seeding`, and `error` records remain loaded without a new actor.

A persisted `seeding` record does not respawn a download actor, but it remains
eligible for the shared incoming-seeding service. Resume only if you deliberately
want a new actor generation and normal start/download lifecycle.

## Escalation record

Before reporting a defect, capture:

```text
Dendrite commit/version:
host OS/kernel:
storage backend:
configuration with secrets/paths sanitized:
torrent version (v1/v2/hybrid/magnet):
torrent UUID and state:
exact command and problem response:
relevant logs:
doctor report while daemon stopped:
network prerequisites and peer-encryption mode:
filesystem type/free space:
```

Do not attach administrator tokens, private tracker URLs, private TLS keys, or
copyrighted payload data.

## Next steps

- Symptom index: [Troubleshooting](../troubleshooting.md)
- State transitions: [Torrent lifecycle](../architecture/torrent-lifecycle.md)
- Files and durability: [Data layout](../reference/data-layout.md)
