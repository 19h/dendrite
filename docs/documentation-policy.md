[← Documentation home](../README.md)

# Documentation policy

This page defines how Dendrite documentation is organized and how capability,
correctness, safety, and performance claims are qualified. It exists to keep the
root README useful without turning it into a duplicated implementation ledger.

## One entry point

The repository root [`README.md`](../README.md) is the only complete documentation
index.

- Do not add `docs/README.md` or another full table of contents.
- Every maintained page below `docs/` begins with a link to the root README.
- The README owns project identity, alpha status, the shortest runnable path,
  “I want to…” routing, the complete documentation map, and top-level boundaries.
- Workflow playbooks own self-contained end-to-end tasks and may repeat commands
  when that makes the procedure safer to follow.
- Concept and architecture pages explain why the system behaves as it does.
- Reference pages own exhaustive flags, fields, wire shapes, defaults, and limits.
- A detailed page links only to its immediate prerequisites and next steps; it
  does not reproduce the entire root map.

## Funnels and playbooks

An “I want to…” link should land on the smallest useful playbook. A playbook must:

1. state prerequisites and the environment it assumes;
2. provide copyable commands in execution order;
3. state the expected checkpoint after each important command;
4. give the first diagnostic branch for predictable failures;
5. explain destructive and non-destructive consequences;
6. end with focused links to concepts and exact reference.

Normative facts may be repeated in a playbook, but the corresponding reference
page remains the owner. When the fact changes, update both in the same change.

## Truth hierarchy

Use repository truth in this order:

1. The BitTorrent Enhancement Proposal or protocol specification defines the
   external protocol.
2. Cargo configuration and executable behavior define build and public interfaces.
3. Current source and executable tests define implemented behavior.
4. Current CI workflows define what automation actually attempts.
5. Maintained documentation explains those facts.
6. Historical prose, comments, names, and aspirations are supporting context only.

Repository-specific owners:

| Subject | Authoritative owner |
|---|---|
| Rust version, members, dependencies, profiles | `rust-toolchain.toml`, `Cargo.toml` |
| Daemon CLI | `crates/daemon/src/main.rs` and live `--help` |
| Client CLI | `crates/cli/src/main.rs` and live `--help` |
| Configuration | `crates/config/src/lib.rs` |
| HTTP routes and authentication | `crates/daemon/src/server.rs` |
| JSON types | `crates/api-types/src/lib.rs` plus route usage |
| Torrent lifecycle and protocol orchestration | `crates/engine/src/lib.rs` |
| Metadata, wire, persistence, storage contracts | their owning crates and tests |
| Attempted automation | `.github/workflows/ci.yml` |

If a shared JSON type exists but no route accepts or returns it, document it as a
defined type—not as an available API operation. If runtime OpenAPI prose and route
code differ, the route code wins and the discrepancy belongs in status/limitations.

## Claim vocabulary

| Label | Meaning |
|---|---|
| **Implemented** | Current source contains a reachable behavior. This alone does not establish interoperability or deployment support. |
| **Unit-tested** | A repository test directly exercises a local unit or invariant. |
| **Integration-tested** | A test runs multiple Dendrite components or a local protocol peer together. |
| **Simulator-tested** | The deterministic swarm simulator exercises the stated configuration and fault model. |
| **Fuzz-targeted** | A cargo-fuzz target accepts the input class. This does not state corpus duration or absence of defects. |
| **Benchmarked** | A measurement was collected with named host, toolchain, profile, input, and run conditions. |
| **Packaged** | A Dockerfile or service unit exists. This does not establish a published artifact or all-host support. |
| **Unsupported** | The behavior is absent or deliberately rejected. |
| **Unknown** | Current evidence is insufficient; state the probe needed to resolve it. |

Avoid unqualified `complete`, `full`, `proven`, `secure`, `every`, `zero-copy`, or
`production-ready`. Prefer scoped statements such as:

> The local integration test downloads a v2 fixture over TCP and verifies its
> SHA-256 Merkle piece before the actor reaches `seeding`.

That is not proof of interoperability with every v2 client, tracker, network, or
filesystem.

## Stable and volatile documentation

Stable pages explain mechanisms: process ownership, configuration precedence,
authentication, actor cancellation, verification-before-commit, and path
confinement. Volatile pages record version numbers, protocol inventory, platform
status, measured performance, current limits, and unsupported API fields.

Keep volatile details in reference/status or their owning subsystem page. Do not
copy an exhaustive protocol list into the README, every playbook, crate comments,
and the architecture overview.

## Required qualifiers

A protocol-support claim should identify:

- inbound, outbound, or both directions;
- discovery, codec, session, or complete transfer scope;
- TCP, uTP, HTTP, UDP, or encrypted transport as applicable;
- v1, v2, hybrid, or magnet identity scope;
- policy gates such as `private`, DHT bootstrap, web-seed SSRF restrictions, or
  peer-encryption mode;
- the named test or runtime route that supplies evidence.

A performance claim should identify commit, host CPU/OS, Rust version, Cargo
profile, workload and size, warm-up, repetitions, distribution, and whether the
measurement is a parser, picker, wire codec, simulation, or end-to-end transfer.

A security claim should state the protected boundary and the excluded threats.
Memory-safe Rust and bounded parsers are useful properties; neither makes the
daemon a hardened sandbox.

## Updating documentation

When public behavior changes:

1. update implementation and tests;
2. update the exact reference owner;
3. update affected playbooks and troubleshooting branches;
4. update architecture only if the mechanism changed;
5. update the README only for project-level routes, capabilities, or boundaries;
6. check every relative link and anchor;
7. compare CLI reference against current `--help` output;
8. search the repository for the old field, command, state, or claim;
9. record unresolved contradictions explicitly instead of smoothing them over.

## Style

- Lead with the outcome or operational decision.
- Use `sh` for executable shell commands, `toml` for configuration, `json` for
  bodies, and `text` for output or conceptual layouts.
- Quote magnet URIs in shell examples.
- Use bytes with IEC units when explaining limits and SI units for rates.
- Call `dendrite` the daemon and `dendritectl` the client; “CLI” alone is ambiguous.
- Distinguish a torrent's service record from its payload files.
- Do not publish real administrator tokens, private tracker URLs, or sensitive logs.

## Related pages

- [Status and limitations](reference/status-limitations.md)
- [Verification model](development/verification.md)
- [Repository layout](development/repository-layout.md)
