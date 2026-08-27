[← Documentation home](../../README.md)

# Storage and security

Dendrite treats torrent metadata, peers, trackers, web seeds, API clients, and
pre-existing filesystem entries as untrusted inputs. Its principal defenses are
strict parsing, explicit bounds, capability-confined payload access,
transactional metadata, and authenticated administration.

This is an implementation description, not a formal security audit.

## Filesystem boundary

The storage crate opens one configured `download_dir` as a directory capability.
Torrent payload operations resolve normalized relative components beneath that
root rather than joining arbitrary operating-system paths.

`TorrentPath` rejects:

- empty, `.` and `..` components;
- `/`, `\`, NUL, and ASCII control characters;
- Windows-forbidden characters and reserved device names;
- components ending in a dot or space;
- unsafe syntax after components are normalized to Unicode NFC;
- a component longer than 255 UTF-8 bytes;
- a complete relative path longer than 4,096 UTF-8 bytes.

The cross-platform restrictions are deliberate even on Linux: a torrent should
not become safe or unsafe merely because it moved between supported storage
hosts.

## Links and pre-existing objects

Directories and files are opened without following symlinks. On Unix, an
existing regular file with more than one hard link is rejected before payload
writes. These checks reduce escape and confused-deputy attacks involving
objects prepared inside the download tree.

They do not protect a hostile process with permission to rename or replace
files concurrently, inspect payload content, or alter the daemon's directory
permissions. Run Dendrite as a dedicated account and keep other writers out of
both `data_dir` and `download_dir`.

## Exact ranges

The metainfo-derived file layout maps each torrent byte range to one or more
confined files. Storage reads and writes validate offsets and lengths against
that layout. A peer block cannot choose an arbitrary filename or address data
outside the torrent's declared payload.

Distinct loaded torrents also claim their normalized payload paths so two
actors cannot intentionally manage the same location under different UUIDs.

## I/O backends

The portable backend uses capability-aware blocking filesystem operations
behind asynchronous coordination. On Linux, startup attempts the `io_uring`
backend and falls back to portable I/O when the kernel or environment does not
support it. The selected value is exposed as `storage_backend` in status.

The backends share the same path and range invariants. Selection changes the
I/O mechanism, not the trust model or durable format.

## Verification before completion

Network bytes are written only in the ranges requested by the engine and are
verified against v1 piece hashes, v2 Merkle information, or both. On a valid
piece, affected payload files are synchronized before the completion bit is
committed to the torrent record.

This ordering favors recovery correctness after a crash. It does not replace
normal backups, storage hardware integrity, or a later recheck after external
file modification.

## State storage

`data_dir/state.redb` is a local transactional database. Its database schema is
currently version 2; individual torrent records have their own current encoding
version, currently 1. Hash indexes and record mutations commit together.

Records that cannot be decoded are moved to quarantine and counted in daemon
status. They are not silently treated as valid. Quarantine is a signal to stop
and investigate/restore, not a repair mechanism.

Database and record versions are internal migration facts, not a promise that
an alpha build can downgrade safely. Back up `data_dir` before changing builds.

## API secret

`data_dir/admin.token` is a 256-bit administrator credential represented as
unpadded base64url. On Unix it is created with mode `0600`. Bearer comparison is
constant-time; rotation invalidates browser sessions.

Protect the directory at the operating-system and backup layers. TLS protects a
remote token in transit, but anyone who can read the token file, process
arguments/environment used to copy it, daemon memory, or an unprotected backup
has administrator capability.

## Parser and allocation bounds

Untrusted structured input has several layers of limits:

- metainfo upload and bencode structural limits;
- tracker response limits;
- peer message and extension payload limits;
- WebSocket payload limits and send timeout;
- HTTP concurrency, request-rate, and body limits;
- loaded/active torrent, connection, session, and page-size limits.

Operators can reduce configured ceilings for a smaller deployment. Increasing
them expands worst-case memory, descriptor, CPU, and storage pressure; a valid
range is not proof that the maximum is appropriate for one host.

## Web and network boundaries

- A non-loopback API listener requires TLS and explicit allowed origins.
- Cookie-authenticated mutations require CSRF proof.
- Daemon web-seed requests reject private-address destinations.
- Private torrents suppress DHT, local discovery, and PEX.
- NAT-PMP and inbound peer access cross router/firewall boundaries only when
  explicitly configured and supported externally.

Payload privacy and traffic anonymity are outside this boundary. BitTorrent
peers and trackers necessarily learn network and content identifiers; peer
encryption is transport obfuscation/confidentiality between compatible peers,
not anonymity.

## Operator checklist

- Use a dedicated, unprivileged service account.
- Keep state and payload roots writable only where required.
- Never publish the API without the validated TLS/origin configuration and a
  host firewall.
- Treat the token and state backups as secrets.
- Keep the daemon stopped while copying `state.redb` for backup or recovery.
- Recheck after payloads are restored or modified outside Dendrite.
- Review logs and `quarantined_records` after an unclean shutdown or upgrade.

## Related pages

- [Data layout](../reference/data-layout.md)
- [Remote API playbook](../playbooks/remote-api.md)
- [systemd playbook](../playbooks/systemd.md)
- [Torrent recovery](../playbooks/recover-torrent.md)
