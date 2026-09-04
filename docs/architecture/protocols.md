[← Documentation home](../../README.md)

# Protocol scope

Dendrite implements the protocol pieces needed for daemon-driven v1, v2, and
hybrid torrent transfer. This page names implemented behavior; it does not
claim interoperability with every client, tracker, DHT implementation, proxy,
or unusual metainfo producer.

## Metadata and identities

| Area | Scope |
|---|---|
| BitTorrent v1 | bencoded metainfo, SHA-1 info hash, piece hashes, single/multi-file layout |
| BitTorrent v2 | SHA-256 truncated handshake identity, file tree, piece layers, Merkle verification |
| Hybrid | consistent v1 and v2 identities/layouts, with both identities indexed |
| Magnet | `btih` and `btmh` identities, display name, trackers, metadata acquisition |
| Peer metadata | extension protocol and metadata exchange for magnet imports |

Parsing is bounded by the configured metainfo size and internal structural
budgets. Malformed lengths, duplicate dictionary keys, inconsistent geometry,
unsafe paths, identity mismatches, and unsupported hash forms are rejected.

## Discovery

| Mechanism | Behavior |
|---|---|
| HTTP trackers | IPv6-preferred announces with IPv4 fallback and bounded response parsing |
| UDP trackers | connect and announce transactions with bounded parsing/timeouts |
| Tracker tiers | every valid tier is announced to and usable peers are deduplicated |
| DHT | UDP discovery for public torrents when bootstrap nodes are configured |
| Local service discovery | multicast discovery fallback for public torrents |
| PEX | peers learned from negotiated peer exchange during public swarm sessions |

Initial discovery announces to every tracker tier and combines those results
with a bounded DHT lookup when bootstrap nodes are configured. Local discovery
is tried when neither wide-area source returns a peer. Exhausted swarms repeat
discovery with bounded backoff.

The DHT lookup is iterative: eight `get_peers` queries stay in flight toward
the closest unqueried nodes, peers are collected from every responder, and the
walk stops once the sixteen closest responders have answered. Responsive nodes
are remembered for later lookups, and `announce_peer` is sent with the returned
tokens every fifteen minutes so other clients can discover this daemon. The
daemon does not answer DHT queries. Trackers are re-announced only after the
interval they returned has elapsed; DHT, local discovery, and PEX refresh every
minute.

HTTP announces prefer IPv6 on dual-stack hosts because trackers may return a
different peer population according to the request's address family. If IPv6
cannot connect, the same request falls back to the normal dual-stack client.
Announces request compact responses, omit redundant peer IDs, carry a stable
session key, and advertise encrypted-peer support when peer encryption is
enabled.

The default configuration has no DHT bootstrap nodes. Add trusted IP socket
addresses in TOML before expecting DHT to provide initial peers.

Private torrents use tracker discovery only. DHT, local discovery, and PEX are
suppressed to preserve the private flag's discovery boundary.

## Peer transport and extensions

Dendrite accepts and initiates BitTorrent peer sessions over TCP and uTP. The
same configured peer socket address supplies the TCP and UDP/uTP port.
Connections pass through global admission and per-session message bounds.

Peer protocol support includes:

- the base peer wire protocol and standard request/piece flow;
- protocol encryption negotiation (MSE/PE) when enabled;
- extension negotiation and peer metadata exchange;
- PEX on public torrents;
- v2 hash request/response messages;
- hole-punch coordination messages for IPv4 and IPv6 socket addresses, with the
  current local engine integration tests covering IPv4 sessions;
- incoming upload sessions for verified data.

The compatibility-oriented default is `peer_encryption = "preferred"`: Dendrite
tries MSE/PE first and falls back to plaintext. Peer transport encryption does
not imply that the HTTP API, trackers, DHT, payload files, or database are
encrypted.

## Web seeds

HTTP and HTTPS web seeds declared by metainfo can supply missing data when peer
transfer does not. Dendrite validates schemes, constructs range requests, and
bounds response sizes. The daemon rejects private-address web-seed targets to
reduce server-side request forgery risk.

Web seeds are data sources, not peer discovery, and do not make an unannounced
magnet self-sufficient: the actor still needs metadata before it can map and
verify payload ranges.

## NAT-PMP and hole punching

When `nat.gateway` is configured, Dendrite can request NAT-PMP mappings for its
peer listener. The gateway must be a nonzero IPv4 socket address. Mapping is
best-effort external network coordination; local configuration validation
cannot guarantee router support, public reachability, stable external ports, or
firewall policy.

Peer-assisted hole punching is implemented for compatible swarm participants.
It depends on another peer's support and topology and is not a replacement for
listener reachability.

## Standards map

The implementation is informed by these BitTorrent specifications:

- [BEP 3: base protocol and v1 metainfo](https://www.bittorrent.org/beps/bep_0003.html)
- [BEP 5: DHT](https://www.bittorrent.org/beps/bep_0005.html)
- [BEP 9: peer metadata exchange](https://www.bittorrent.org/beps/bep_0009.html)
- [BEP 10: extension protocol](https://www.bittorrent.org/beps/bep_0010.html)
- [BEP 11: peer exchange](https://www.bittorrent.org/beps/bep_0011.html)
- [BEP 14: local service discovery](https://www.bittorrent.org/beps/bep_0014.html)
- [BEP 15: UDP trackers](https://www.bittorrent.org/beps/bep_0015.html)
- [BEP 19: web seeds](https://www.bittorrent.org/beps/bep_0019.html)
- [BEP 29: uTorrent transport protocol](https://www.bittorrent.org/beps/bep_0029.html)
- [BEP 52: BitTorrent v2](https://www.bittorrent.org/beps/bep_0052.html)
- [BEP 55: hole punching](https://www.bittorrent.org/beps/bep_0055.html)

This is a scope map, not certification of complete support for every optional
clause. The repository's tests, deterministic vectors, simulations, and actual
peer/tracker trials are the evidence for specific behavior.

## Deliberate boundaries

- NAT-PMP configuration is explicitly IPv4-only; IP-version reachability for
  other mechanisms depends on the relevant listener, bootstrap, tracker, and
  peer addresses.
- There is no bundled proxy/Tor transport or traffic anonymity layer.
- There is no per-torrent sequential mode in API v2.0.
- There is no protocol-level content search or indexing service.
- Configuration enables mechanisms; external topology and counterpart behavior
  determine whether they succeed.

## Related pages

- [Torrent lifecycle](torrent-lifecycle.md)
- [Configuration reference](../reference/configuration.md)
- [Troubleshooting](../troubleshooting.md)
