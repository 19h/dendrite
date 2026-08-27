[← Documentation home](../../README.md)

# Repository layout

Dendrite is one Rust workspace with a daemon composition root and small owning
crates. Make a public-behavior change in the lowest appropriate crate, then
update the integration boundary, tests, exact reference, and affected playbook.

```text
.
├── Cargo.toml                 workspace, shared versions/lints/profiles
├── Cargo.lock                 locked dependency graph
├── rust-toolchain.toml        Rust 1.94.0 toolchain
├── deny.toml                  dependency/license/source policy
├── example.toml               runnable local configuration
├── assets/                    README artwork
├── crates/
│   ├── api-types/             public JSON shapes
│   ├── config/                settings and validation
│   ├── core/                  pure domain types and piece picker
│   ├── metainfo/              bencode, magnets, v1/v2/hybrid parsing
│   ├── net/                   protocol codecs and network services
│   ├── persistence/           redb state and indexes
│   ├── storage/               confined payload I/O
│   ├── engine/                actors, discovery, transfer, verification
│   ├── daemon/                API/process composition and doctor
│   ├── cli/                   dendritectl
│   ├── simulator/             deterministic swarm fault model
│   └── benchmarks/            Criterion hot paths
├── docs/                      entrypoint-routed documentation
├── fuzz/                      cargo-fuzz package and targets
├── packaging/                 Dockerfile and systemd unit
└── .github/workflows/ci.yml   attempted automated gates
```

## Dependency direction

`dendrite-core` contains identities, paths, state values, and scheduling rules
without daemon or network ownership. Metainfo, networking, persistence, and
storage build on those domain types. The engine composes those subsystems. The
daemon composes the engine with configuration, authentication, API routes, and
process lifecycle.

`dendrite-api-types` is shared by daemon and client. Adding a Rust type there
does not create an API operation; a route and handler must actually consume or
produce it.

## Where a change belongs

| Change | Primary owner | Usually also update |
|---|---|---|
| hash, path, state, picker invariant | `core` | engine tests and concept docs |
| bencode/magnet/metainfo acceptance | `metainfo` | fuzz target, API errors, protocol docs |
| peer/tracker/DHT/extension codec | `net` | fuzz target and engine integration test |
| transactional record behavior | `persistence` | engine race/restart tests and data-layout docs |
| payload access/durability | `storage` | engine crash/recheck tests and security docs |
| actor/discovery/transfer policy | `engine` | architecture, operations, simulator as applicable |
| HTTP/auth/session/metrics behavior | `daemon/src/server.rs` | API types/reference and client if exposed |
| settings/defaults/ranges | `config` | example, configuration reference, playbooks |
| command or output | `daemon/src/main.rs` or `cli/src/main.rs` | CLI reference and workflow pages |
| operator packaging | `packaging` | corresponding self-contained playbook |

## Important process boundaries

- Persistence runs behind a bounded command channel so database work has one
  serialization point.
- Storage runs behind a bounded queue and selects its backend at initialization.
- The engine supervisor owns actor generations and shared incoming services.
- The API serializes mutations separately from read requests.
- `dendritectl` knows only HTTP types and token-file handling.

Keeping these boundaries intact makes failure ordering testable. For example,
the engine can synchronize payload storage before asking persistence to replace
the completion record, and API removal can cancel an actor before deleting its
record.

## Public contracts

Review these together when changing behavior:

- config fields and defaults;
- executable names, flags, environment variables, output and exit status;
- HTTP routes, authentication, JSON, status codes, and event kinds;
- torrent state semantics and restart behavior;
- path, database, and token locations;
- protocol policy such as private-torrent discovery or encryption mode;
- packaging paths and service hardening.

The [documentation policy](../documentation-policy.md) identifies the exact
source owner and required claim qualifiers for each class.

## Generated and local artifacts

`target/`, `fuzz/target/`, `dendrite-data/`, and `downloads/` are build/runtime
artifacts, not source inputs. Criterion writes under `target/criterion`. Do not
commit administrator tokens, databases, downloaded payloads, private tracker
URLs, or machine-specific benchmark results as general claims.

## Read next

- [Verification](verification.md)
- [Architecture overview](../architecture/overview.md)
- [Documentation policy](../documentation-policy.md)
