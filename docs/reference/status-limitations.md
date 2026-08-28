[← Documentation home](../../README.md)

# Status and limitations

Dendrite is currently `2.0.0-alpha.1`. The architecture is intentionally
bounded and security-conscious, but the alpha label matters: public shapes,
state migrations, packaging, and interoperability breadth can still change.

## Validated project path

| Area | Current status |
|---|---|
| language/toolchain | Rust 2024 edition, Rust 1.94.0 |
| automated host | Linux in repository CI |
| source builds | primary distribution path |
| Docker | Dockerfile present; builds daemon/client for an x86_64 Rust toolchain and Debian Bookworm runtime |
| systemd | hardened unit template present for Linux hosts |
| published binaries/images | not established by repository packaging alone |
| non-Linux | portable code paths exist, but CI support is not claimed |

## Implemented transfer scope

Implemented source paths cover v1, v2, and hybrid metainfo; magnets with peer
metadata acquisition; TCP and uTP peer transport; optional MSE; HTTP/UDP
trackers; DHT, local discovery, and PEX for public torrents; web-seed fallback;
NAT-PMP; compatible-peer hole punching; and incoming seeding.

This inventory is not a blanket interoperability claim. It says the behavior is
present and exercised by repository tests at the scopes described in
[Verification](../development/verification.md). Real peers, trackers, routers,
filesystems, and metadata producers add combinations the repository cannot
exhaust.

## Operator-interface limits

- There is no bundled UI.
- Authentication is one service-wide administrator token, not users/roles.
- The CLI list command retrieves only the first page; API callers can paginate.
- The CLI has no `announce` command although the API action value exists.
- API v2.0 rejects per-torrent destination and sequential scheduling.
- Remove retains payload data and has no delete-data option.
- There is no API to inspect or recover quarantine records.
- Defined `FilePriorityUpdate` and `Operation` JSON types have no routes.
- The OpenAPI document is hand-built and incomplete for client generation.
- Events are process-local and have no durable replay.
- Metrics are process-local, low-cardinality, and not per torrent.

## Lifecycle limits

- `stopping` is a public state value but current pause/shutdown paths do not
  persist it.
- Restart creates download actors for `starting`/`downloading` and recheck actors
  for `checking`, but not for `seeding`, `stopped`, or `error`.
- Persisted `seeding` records can still serve through the shared incoming peer
  service without a download actor.
- `announce` currently routes through engine start/resume behavior and can
  replace an active actor; it is not an isolated tracker refresh.
- Recheck identifies missing/corrupt data but does not repair it until resumed.

## Discovery and network limits

- DHT has no default bootstrap nodes.
- Initial discovery combines every tracker tier with a bounded DHT lookup when
  bootstrap nodes are configured; local discovery remains a no-peer fallback.
- Private torrents disable DHT, local discovery, and PEX.
- NAT-PMP gateway configuration is IPv4-only.
- NAT-PMP requires an explicit gateway and has no automatic discovery or UPnP
  fallback.
- Peer encryption does not provide anonymity or protect API/metadata/storage.
- Daemon web seeds reject private-address targets; there is no operator setting
  to relax that in the daemon configuration.

## Storage and deployment limits

- One service-wide `download_dir` owns every payload.
- Existing symlink paths and, on Unix, multiply hard-linked payload files are
  rejected by confined storage.
- Database formats are internal and alpha migrations/downgrades are not a stable
  external contract.
- Docker packaging pins an x86_64 Rust toolchain string; other image architectures
  are not claimed by that file.
- The systemd unit assumes the documented absolute directories and binary path.

## Observability caveats

- `/healthz` proves only that the HTTP router responds.
- Status currently turns database list/quarantine query errors into empty/zero
  values; check logs when counts unexpectedly disappear.
- Byte rates are in-process samples and begin at zero.
- Active torrent count is derived from durable state, not actor registry size.
- Debug logs can include peer addresses, URLs, UUIDs, and paths.

## Not claimed

The project does not currently claim:

- production readiness or an external security audit;
- all-client/all-tracker interoperability;
- anonymous or private traffic;
- zero-copy end-to-end I/O;
- published release artifacts for every supported host;
- live configuration reload;
- multi-tenant isolation;
- payload deletion safety on record removal;
- measured performance without named benchmark conditions.

## How to close a gap responsibly

Use the [documentation policy](../documentation-policy.md) claim vocabulary.
Add the narrowest executable test that demonstrates the behavior, update the
owning reference, and qualify what remains outside that test. For a protocol
claim, record direction, transport, identity form, policy gates, and counterpart.
For a performance claim, record host, commit, toolchain, profile, workload,
warm-up, samples, and distribution.

## Related pages

- [Verification](../development/verification.md)
- [Protocol scope](../architecture/protocols.md)
- [Performance](../operations/performance.md)
- [Troubleshooting](../troubleshooting.md)
