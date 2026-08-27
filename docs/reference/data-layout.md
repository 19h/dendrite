[← Documentation home](../../README.md)

# Data layout

Dendrite separates service state from downloaded payloads. Back up, mount, and
permission these roots according to their different contents.

## Logical layout

```text
data_dir/
├── admin.token    service-wide administrator credential
└── state.redb     transactional torrent records and indexes

download_dir/
└── <metainfo paths...>  verified and partial payload files
```

Temporary token files can briefly appear beside `admin.token` during atomic
rotation. redb can use its own internal file behavior; do not build
automation that edits database bytes or assumes table layout.

## Default source/local paths

When launched from a checkout or another working directory without path
overrides:

| Purpose | Path |
|---|---|
| state root | `./dendrite-data` |
| token | `./dendrite-data/admin.token` |
| database | `./dendrite-data/state.redb` |
| payload root | `./downloads` |

The default `dendritectl --token-file` is relative in the same way. If client
and daemon run from different working directories, make both paths explicit.

## Docker paths

The packaged image runs as UID 10001 with working directory `/home/dendrite`:

| Purpose | Container path |
|---|---|
| state volume | `/home/dendrite/dendrite-data` |
| token | `/home/dendrite/dendrite-data/admin.token` |
| database | `/home/dendrite/dendrite-data/state.redb` |
| payload volume | `/home/dendrite/downloads` |

Mount both volumes persistently. Recreating a container without its state
volume creates a new token/database and loses Dendrite's completion bookkeeping;
recreating without its payload volume loses downloaded bytes.

## systemd paths

The supplied service unit and playbook use:

| Purpose | Host path |
|---|---|
| configuration | `/etc/dendrite/dendrite.toml` |
| state root | `/var/lib/dendrite` |
| token | `/var/lib/dendrite/admin.token` |
| database | `/var/lib/dendrite/state.redb` |
| payload root | `/srv/dendrite/downloads` |
| daemon binary | `/usr/bin/dendrite` |

The unit grants write access only to the state and payload roots and runs with
`UMask=0077`.

## What the database owns

The current database schema stores:

- UUID, name, lifecycle state, v1/v2 identity;
- raw metainfo or original magnet URI;
- total length and piece-completion bitmap;
- downloaded/uploaded counters and added timestamp;
- unique identity indexes;
- quarantined undecodable record bytes.

It does not embed torrent payloads. It also does not provide a supported public
editing or export format.

## What remove does

`dendritectl remove <ID>` removes the database record and identity indexes after
cancelling active work. It does not unlink payload files. Delete retained files
only after independently resolving the intended paths and considering whether
another workflow uses them.

## Backups

For a coherent manual backup:

1. pause application-level writers if desired;
2. stop the daemon and confirm it exited;
3. copy `data_dir`, preserving private permissions;
4. copy or snapshot `download_dir` if payload recovery matters;
5. restart the daemon and verify status.

The token is a secret and must stay protected in backup storage. A state-only
backup retains intent/completion records but not data. A payload-only backup can
be imported/rechecked, but does not retain UUIDs, counters, or actor state.

Do not restore `state.redb` while the daemon has it open. Alpha database and
record versions can change; retain the matching Dendrite build/version with an
important backup.

## Moving payloads

Dendrite has one global payload root and no API move operation. To relocate the
entire root:

1. stop the daemon;
2. copy files while preserving their relative paths and regular-file nature;
3. update `download_dir`;
4. run doctor;
5. start the daemon;
6. recheck affected torrents before trusting completion.

Do not introduce symlinks or hard-linked payload files: confined storage rejects
symlink traversal and, on Unix, files with multiple hard links.

## Related pages

- [Storage and security](../architecture/storage-security.md)
- [Docker playbook](../playbooks/docker.md)
- [systemd playbook](../playbooks/systemd.md)
- [Recovery playbook](../playbooks/recover-torrent.md)
