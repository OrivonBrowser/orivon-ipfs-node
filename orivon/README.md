# Orivon IPFS node

The read-only IPFS node (`orivon-ipfs-gateway`) built as a WebAssembly component for
`wasm32-wasip2`, with a serve mode for apps that share content. Licensed under MIT OR Apache-2.0
(`LICENSE-MIT`, `LICENSE-APACHE`).

The node reads: libp2p over TCP and DNS, a Kademlia client, a Bitswap client, block verification, a
SQLite block cache with LRU eviction, IPNS record verification, DNSLink over DNS-over-HTTPS. By
default it provides nothing, listens for nothing but its own HTTP address, and pins nothing; serve
mode (below) can hold blocks, and with `--p2p-port` accept connections and announce them. It runs
single-threaded: the runtime is `current_thread`, and the pieces WASI has no equivalent for (QUIC,
WebSocket transport, TLS security upgrade, `reqwest`, interface enumeration) are replaced or removed.

## Build

```sh
export RUSTUP_HOME=...      # rustup install, stable toolchain with the wasm32-wasip2 target
export CARGO_HOME=...
export WASI_SDK_PATH=...    # wasi-sdk 34 or later (SQLite is C)
export ORIVON_JCO_DIR=...   # npm prefix with @bytecodealliance/jco-transpile@0.15.0
orivon/build.sh             # dist/ipfs-node.wasm, dist/ipfs-node.p2/, dist/LICENSE-MIT, dist/LICENSE-APACHE
```

`CARGO_TARGET_DIR`, `OUT_DIR` and `CARGO_JOBS` (default 2) are optional. Built with rustc 1.98.1,
wasi-sdk 34, jco-transpile 0.15.0. jco is not a dependency of this repository:
`orivon/transpile.mjs` drives the library installed under `ORIVON_JCO_DIR`, with the async import list
the Orivon WASI host expects. The first build downloads crates; later builds are incremental (a
incremental release build takes about a minute on two cores).

Run it outside Orivon:

```sh
wasmtime run -S inherit-network=y -S allow-ip-name-lookup=y -S tcp=y -S udp=y \
  --dir <data-dir>::/data dist/ipfs-node.wasm \
  --online --routing-mode auto --db /data/blocks.db --addr 127.0.0.1:<port> --trace-filter warn
```

`--routing-mode` is `auto` (the delegated lookup service and the light DHT at the same time),
`delegated` (service only), `light-dht` (network only) or `offline` (no lookups; the cache only).
`--delegated-router <url,url>` replaces the default service (`https://delegated-ipfs.dev/routing/v1`);
several routers are asked at once and their answers merged.

### Name resolution on WASI (what a host must allow)

The libp2p swarms (the light DHT, the Bitswap client and the serve swarm) resolve `/dns4`, `/dns6`
and `/dnsaddr` names themselves, with a resolver that talks to two name servers over TCP and never
over UDP: `1.1.1.1:53` and `1.0.0.1:53` (`orivon_ipfs_dns::tcp_resolver_config()`, one config shared
by the three swarms; builds for other targets are unchanged). A host that runs this
node therefore has to allow outgoing TCP connections to exactly those two addresses. It needs no
UDP grant: a datagram a host refuses is dropped without an error, which looks like a name that never
answers. A host that names the port, as Orivon does (port 53 is reserved there), names `1.1.1.1:53`
and `1.0.0.1:53`. Without the grant no `/dnsaddr` bootstrap name resolves; the node still dials the
one bootstrap peer given as a literal address, `/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ`
(the only literal-address peer in Kubo's default bootstrap list), which needs no lookup.

The HTTP client (DNS-over-HTTPS, delegated routing, an HTTP provider) is a separate path: it resolves
its host names through the host's own `ip-name-lookup`, and needs that and a TCP connect to the
server it names.

## Requests it answers

- `GET`/`HEAD /health`: `ok`.
- `GET`/`HEAD /ipfs/<cid>?format=raw`, or `Accept: application/vnd.ipld.raw`: the block's bytes,
  verified against the CID, `content-type: application/vnd.ipld.raw`, no UnixFS decoding; a path below
  the CID answers 400. `If-None-Match` of `"<cid>.raw"` answers 304 without fetching.
- `GET`/`HEAD /ipns/<key>?format=ipns-record`, or `Accept: application/vnd.ipfs.ipns-record`: the
  signed record as published, checked (signature, name, validity) before it is returned,
  `content-type: application/vnd.ipfs.ipns-record`, never from a cache. A DNS name answers 400.
- Both: 404 when nothing is found, 504 on a timeout, 502 for other failures, 400 for a bad CID or name.
- `GET /ipfs/<cid>[/path]` and `/ipns/<name>[/path]` without those parameters answer 404: the rendered
  pages read and write one block store shared by every caller, and this node does not offer them.

## Serve mode (`--serve`, off by default)

Without `--serve` the node behaves as described above. With it, a page can give the node blocks and
have them fetched from peers the node dials. `--serve` needs `--api-key <64 hex characters>`; run it
as `--online --routing-mode offline --serve --api-key <key> --addr 127.0.0.1:<port>`, no `--db`.
The node only dials out: no DHT, no delegated lookups, no libp2p listener, unless `--p2p-port`
is given (section "Listening and announcing").

Every request to the three endpoints must carry `x-orivon-key: <key>` (constant-time compare); a
missing or wrong key answers 401. `/health` and the reads above stay open. They ignore partitions.

- `GET /id`: `200 {"peer":"<peer id>"}`.
- `POST /blocks`, body a CARv1 file (64 MiB at most, 413 above): every block is checked against its CID
  (sha2-256; raw and dag-pb only) and kept in memory, never evicted. The table is charged each block's
  length plus 128 bytes, 256 MiB at most, and holds 200,000 blocks at most (507 above either).
  `200 {"roots":["<cid>",...],"blocks":<n>}`; a bad CAR or a wrong hash answers 400 and keeps nothing.
  One upload runs at a time (429 while another does); hashing yields to the runtime every 256 KiB.
- `POST /dial`, body `{"addr":"<multiaddr>"}` of the form `/ip4|ip6|dns4|dns6/<host>/tcp/<port>/p2p/<peer id>`:
  `200 {"peer":"<peer id>"}` once connected, 400 for another address form or a loopback, private,
  link-local, unspecified, multicast or `localhost` host, 502 `could not connect` on failure (never the
  transport's error text), 504 after 30 s, 429 beyond 16 peers. The peer is then kept connected: a
  dropped connection is dialed again after a backoff that starts at 1 s and doubles up to 60 s. The
  backoff resets only after a connection has lasted 30 s, and a peer with no such connection for an
  hour is forgotten. A hidden flag, `--allow-local-dial`, lifts the address check for tests.

Bitswap (`/ipfs/bitswap/1.2.0`, `1.1.0`, `1.0.0`) answers from the kept blocks only, never from the
read cache: want-block gets the block, want-have gets HAVE, a block not kept gets DONT_HAVE when asked,
cancels are ignored. Replies go on a new outbound stream to the asking peer; block bytes are shared with
the table, one reply holds 8 MiB of blocks at most (peers send the rest of their wants again), and a
block named several times in one wantlist is sent once. At most 64 inbound streams are served at once, 8
per peer; messages over 1 MiB are refused. A want that arrives before its block is kept is not remembered.

Known limits: no per-peer rate limit on wantlists; the API key is a command-line argument, so other
local users can read it from the process list; a `dns4`/`dns6` name that resolves to a private address
is not caught (with `--p2p-port` it is: that mode dials public IP addresses only).

`orivon/serve-check.sh` runs the whole path against a scratch Kubo (`MODE=native NODE_BIN=...` or
`MODE=wasm WASMTIME=... WASM=dist/ipfs-node.wasm`): POST a CAR, POST /dial to Kubo, then Kubo fetches
with `block get`, `ls`, `cat` and `dag stat`, and the node redials after a disconnect. The code is
the `orivon-ipfs-serve` crate and `crates/orivon-ipfs-gateway/src/serve_api.rs`.

### Listening and announcing (`--p2p-port <port>`, needs `--serve`)

Without the flag nothing in this section applies and the node behaves exactly as described above.

`--p2p-port <port>` (1 to 65535) makes the serve swarm listen on `/ip4/0.0.0.0/tcp/<port>` and
`/ip6/::/tcp/<port>` and accept connections from any peer. Bitswap answers an inbound peer from the
kept blocks exactly as it answers a peer the node dialed. IPv4 must listen, or the node exits with the
reason (the port is taken, say); IPv6 is tried too and may fail, as it does inside Orivon, whose
network listen binds IPv4 only. TCP is the only transport: a peer that can reach this node only
over QUIC, WebSocket or WebRTC cannot. A peer that reaches it through a circuit relay can (see
`--relay` below).

With `--online` and `--routing-mode auto` or `light-dht` the swarm also runs:

- a Kademlia client on the Amino DHT (`/ipfs/kad/1.0.0`, client mode: it answers no DHT queries, so no
  peer adds it to a routing table), started from the same bootstrap peers as the light DHT lookups
  (`orivon_ipfs_routing::DEFAULT_BOOTSTRAP_PEERS`) and refreshed every 10 minutes;
- AutoNAT **v1** (`/libp2p/autonat/1.0.0`) as a client. v1 is what the network serves: of 169 DHT
  servers a Kubo 0.43 client held connections to, 163 (96 percent) served v1 and 89 (53 percent) v2,
  and the five `bootstrap.libp2p.io` peers serve neither, so a v2 client would find a server for half the
  peers it meets. The node never acts as a server for others (every request is refused).

A node started with another routing mode, or without `--online`, only listens: no DHT, no AutoNAT,
`reachability` stays `unknown`, and `/provide` does not exist.

**Where the node says it can be reached.** Other peers tell the node, through identify, the address
they see it at. When two different peers report the same public IP address, the node asks AutoNAT
whether `<that IP>/tcp/<port>` accepts connections: a public peer dials it and answers yes or no.
Only an address AutoNAT confirmed becomes one of the node's external addresses (it is what a
provider record carries and what identify advertises); nothing else is ever announced. The node
asks first after 10 seconds. From `unknown` the first answer decides: `public`, or `private` when the
dial-back failed. After that a different answer has to come three times in a row to change it, and
the node drops its confirmed addresses when it turns `private`. It asks once a minute until three
answers agree and every 5 minutes after, and a newly learned address makes it ask again, so a
computer that cannot be reached keeps asking about once a minute. It asks peers
the DHT connected it to that serve AutoNAT, kept in a list of up to 16.

**Holding a place on a relay (`--relay <address>/p2p/<relay id>`, repeatable, needs `--p2p-port`).**
A computer behind a router that refuses incoming connections can ask a circuit relay v2 to hold a
reservation for it; a peer then connects to the relay, and the relay passes the connection on. The
address has the form `/ip4|ip6|dns4|dns6/<host>/tcp/<port>/p2p/<relay id>` and must be public (the
same rule as `POST /dial`; `--allow-local-dial` lifts it). Up to 8 relays, one reservation each; a
second address for a relay already named is ignored. A malformed address stops the node from starting.

- The swarm dials each relay, asks for a reservation, and renews it before it runs out. Once the relay
  accepts, `<relay address>/p2p/<relay id>/p2p-circuit/p2p/<this node>` is one of the node's
  external addresses and what a provider record carries next to the confirmed direct ones.
- When the connection to a relay drops, or the relay refuses, the node asks again after 2 seconds,
  doubling up to 2 minutes while it keeps failing. A reservation that lasted 30 seconds starts the
  count again. A relay that cannot be reached leaves everything else working.
- A node that holds a reservation announces its provided CIDs whatever AutoNAT says, at the moment the
  reservation is accepted, whenever one is accepted again after a loss, and on the usual schedule. Without
  a reservation the rule is the one above: only `public` announces. `reachability` is still the AutoNAT
  answer, and the circuit addresses are listed in `relays`, not in `external` or `listen`.
- A connection through a relay is an ordinary connection to the node: Bitswap, identify and ping run
  over it as they do over TCP, and the same bounds apply. The node asks the relay for no particular
  limit and treats a connection the relay marks as limited like any other; Orivon's relay sends no
  limit. A relay that does enforce a duration or byte limit ends the connection when it is reached.
  `POST /dial` takes direct addresses only.
- The same relay support is on the fetching side: a provider whose only address is
  `<relay address>/p2p/<relay id>/p2p-circuit/p2p/<peer id>` is dialed through the relay, ranked after
  every direct address.

**Requests** (the same `x-orivon-key` rule as above; the paths do not exist on a node without a
listener, and `/provide` not on one without a DHT):

- `GET /health` adds `x-orivon-node-features: serve,listen,provide` (`serve,listen` on a node
  without a DHT), with `,relay` added at the end when `--relay` was accepted. A node without
  `--p2p-port` sends no such header.
- `POST /provide`, body `{"cid":"<cid>"}` or `{"cid":"<cid>","tree":false}`: 404 unless a block of that
  CID (same multihash) is kept; else `202 {"cid":"<cid>","children":<n>,"truncated":<bool>}`. The node
  announces it, when it is reachable, as a provider record keyed by
  the CID's multihash, as Kubo does (so a CIDv0 and a CIDv1 of one block are one entry), carrying the
  confirmed external addresses. It announces when the node first turns `public` or a relay accepts a
  reservation, whenever it turns `public` again or a relay accepts one again, when a new external
  address is confirmed, and every 12 hours after an announcement reached a peer. An announcement that
  reached no peer is tried again after a minute. It never announces while the node is not `public`
  and holds no relay reservation. Four announcements run at once. Providing the same CID
  twice is one entry; a 1025th CID answers 429.
  Unless `"tree"` is `false`, the kept blocks below the CID are announced with it: a UnixFS folder's
  subfolders (and the shards of a large one) and the first block of each file in them, level by
  level, `children` of them (a file's other blocks are not: the peer that finds the first fetches
  them over the connection that answered). A peer that opens another file of the share later, on a
  connection of its own, starts from that file's CID and asks the network who has it; with the root
  alone announced the answer is nobody, and a fetcher that only offers wants to peers it found as
  providers never asks this node. Announcing costs one DHT store per CID, four at a time, repeated
  every 12 hours, so the table (1024 CIDs) bounds it: a share with more entries than fit is
  announced breadth first as far as it fits and `truncated` says so. The root is always announced
  first, and entries that do not fit are not fetched less reliably than before, only found by their
  own CID less often. Blocks the node does not hold are skipped.
- `DELETE /provide/<cid>`: stops announcing it and the entries it brought with it (`200
  {"cid":"<cid>"}`), 404 when it was not being provided. An entry another root, or a call of its own,
  also named stays. Records already published expire on their own (about 48 hours).
- `GET /status`: `{"peer":"<peer id>","listen":[<multiaddr>,...],"external":[<multiaddr>,...],
  "relays":[<multiaddr>,...],"reachability":"public"|"private"|"unknown",
  "providing":[{"cid":"<cid>","announced":<unix ms>|null}]}`.
  `listen` is what the swarm reports (one address per network interface; the wildcard addresses when
  the build cannot list interfaces, as the WebAssembly build cannot). Addresses carry no `/p2p/` suffix.
  `external` is the confirmed addresses. `relays` is the circuit addresses a relay holds a reservation
  for, `<relay address>/p2p/<relay id>/p2p-circuit/p2p/<this node>` (the only addresses here that
  end in a `/p2p/` part), and `[]` without `--relay`. `reachability` is `unknown` until a probe has an
  answer or while no address is confirmed, and says nothing of relays. `announced` is the time of the
  last announcement that reached a peer, directly or through a relay reservation.

**Bounds**, on top of every bound above: at most 64 established inbound connections, 4 per peer
(either direction), 32 inbound connections still being set up, 128 established connections the node
opened (peers it keeps, the DHT, AutoNAT servers), 1024 provided CIDs. A connection with no open
stream closes after 60 seconds, except those to the peers a page asked the node to keep. In this mode
the node also dials public IP addresses only (names are resolved first), whoever names the address, so
a DHT peer cannot send it to the local network; `--allow-local-dial` lifts that too.

An announcement walks the DHT through a hundred or so peers, most of which never answer, and holds a
connection to each that does for that idle minute; the provider record then goes to the twenty
nearest of them. The outbound limit is sized for that. A limit the walk reaches makes its later dials
fail, and the record lands on peers far from its key, where no lookup (the light DHT's or Kubo's)
looks. `--trace-filter orivon_kad_announce=debug` logs, for every announcement, how many peers the
walk found and how many it sent the record to, and `orivon_ipfs_serve=debug` logs each failed dial
with the reason, a refusing limit included.

**Known limits.** A computer behind a router that refuses incoming connections stays unreachable:
AutoNAT says `private`, nothing is announced unless `--relay` gives it a reservation, and the node
does not open the port itself (no UPnP, NAT-PMP or hole punching). It becomes `public` by itself when
the port is forwarded or the router starts accepting. Through a relay every byte of a fetch passes the
relay, so its bandwidth bounds the transfer. After reachability is lost the status can stay `public` for up to about 20
minutes, until enough probes fail. The announcement carries the node's addresses as of that moment;
an address that changes later is announced again once AutoNAT confirms the new one. The DHT
client is not a server, so the network learns the node only through the provider records.

`orivon/listen-check.sh` runs this against scratch Kubo nodes (`MODE=native NODE_BIN=...` or
`MODE=wasm WASMTIME=... WASM=dist/ipfs-node.wasm`). Part 1: a Kubo that has only the node's address
reads a file from it. Part 2, in private network namespaces (Linux, no root): two Kubo nodes with
public-looking addresses are the DHT and the AutoNAT servers, the node runs behind a virtual cable
bootstrapped from them (the hidden flag `--test-bootstrap <addr/p2p/id,...>` replaces the public
bootstrap peers; nothing else differs from use), Kubo's AutoNAT dials it back and `GET /status`
turns `public`, `POST /provide` is announced, and after every connection to the node was cut the
other Kubo finds it with `ipfs routing findprovs` and fetches the file. Part 3: the same with the
node's port closed by a firewall rule: AutoNAT says `private`, nothing is announced and no provider
is found. The script skips parts 2 and 3, and says so, where namespaces are not available. The
AutoNAT decisions are also tested in `crates/orivon-ipfs-serve/tests/provide.rs` against an
in-process AutoNAT server.

`orivon/later-connection-check.sh` checks the visitor who opens one file of a share, leaves, and
asks for another after 0, 20, 60 and 120 seconds, over Orivon's circuit relay and directly. The
fetcher is a scratch Kubo (with and without its Bitswap broadcast control, with and without the
DHT) or a second copy of the node (`FETCHER=node`, with a small routing service that names the node
as provider of what it reports as provided). Every step must find its file; a fetcher that looks files up in the public DHT now and then misses an
announced CID (about one in ten from a fresh Kubo), so a failed step there is run again before it is read
as a fault, and `TREE=false` shows what announcing the root alone costs. The Bitswap side is
also tested in process, over a relay and directly, in
`crates/orivon-ipfs-serve/tests/later_connection.rs`.

## Partitions

A caller that keeps a cache per site names the site in a request header, `x-orivon-partition`: an
opaque key of 1 to 128 characters of `[A-Za-z0-9_-]`; anything else, or two of the header, answers 400.

- With the header, blocks are read from and kept in that partition only. The block table and the
  in-memory hot cache are keyed by (partition, CID): a block kept for one partition is invisible to
  another, which fetches it from the network and keeps its own copy. A fetch in flight is shared only
  within its partition. The size cap is global (least recently used rows go first across all partitions).
- Without the header the block is fetched and served, and nothing is read from or kept in the store.
  Such requests never share an in-flight fetch either.
- Shared across partitions, on purpose and short-lived: the provider cache (5 minutes, 30 seconds
  for "none found"), the bad-provider list, the warm peers, and the connections. A caller that wants
  those unobservable has to account for them.
- `blocks.db` carries a schema version in its `metadata` table. A database with another version is
  dropped and recreated; it is only a cache.
- `--import-car` imports into the unpartitioned shared rows, which no request with or without the header
  reads. It is a debugging aid for the library API, not for this binary.

## Providers and routing

An HTTP provider is read with `GET <provider>/ipfs/<cid>?format=raw` and `Accept: application/vnd.ipld.raw`:
no redirects (a 3xx is a failure), a 2 MiB body limit, a 20 s timeout, the bytes checked against the CID
before they are stored, and a failing provider is marked bad for a while. `http://` providers (an IP and
port in the provider record) are read in plaintext; the CID check is what makes that safe.

The light DHT walks the Amino DHT from six bootstrap peers on a swarm of its own, built for the one
lookup and dropped after it. The walk asks ten peers at a time, holds up to 64 connections and 32
dials in progress, gives a dial 5 seconds and closes a connection 5 seconds after its last request:
most peers a walk meets never answer, and a limit it reaches fails the dials that follow, which keeps
the walk from the peers nearest the key. A provider that is itself a DHT client (a node behind a
relay, as `--serve` is) is in no routing table, so the only address of it is the one in the record the
server returned; the walk keeps those addresses, and ends 250 ms after the first provider it has an
address for. With `--trace-filter orivon_ipfs_routing=info` every walk logs one `dht_walk` line: its
requests, answers and failures, the dials a limit refused, and how it ended. The width, the limits and
the timeouts can be replaced for a measurement with `ORIVON_IPFS_DHT_PARALLELISM`,
`ORIVON_IPFS_DHT_CONNECTIONS`, `ORIVON_IPFS_DHT_PENDING_CONNECTIONS`,
`ORIVON_IPFS_DHT_CONNECT_TIMEOUT_MS` and `ORIVON_IPFS_DHT_IDLE_TIMEOUT_MS`.

`orivon/fresh-lookup-check.py` times what a user of Share meets: a serve node behind a relay announces
shares of fresh random bytes, and from the moment the root is announced a second node started like the
browser's (`node-auto`, `node-dht`) and a Kubo (`kubo-find`, `kubo-cat`) look it up. It needs the node,
Kubo, the address of a circuit relay and the network, and prints the seconds each took per share.

In `auto` the delegated service and the light DHT are asked at the same time, and the other lookup is
dropped as soon as one answers with providers. An empty answer from the service is retried once after
100 ms, with the DHT still running. IPNS records are asked of the service first and then the DHT.

## Known limits

- Providers that offer Bitswap over plain TCP, or a gateway-style HTTP(S) endpoint
  (`transport-ipfs-gateway-http`), or through a circuit relay (`/p2p-circuit` with the relay's address and
  id), can serve blocks. Providers reachable only over WSS, QUIC or WebRTC cannot. A block whose
  providers are all of those answers 502 or 504.
- The light DHT is a walk through a hundred or so peers on a swarm built for the lookup, so it takes
  seconds where the delegated service, when it knows the record, takes a fraction of one; both find
  only what the announcer's walk placed on the nearest peers.
- The HTTP client resolves names with the host's resolver (`ip-name-lookup`). On a network that
  answers the default delegated service's name with another address, that lookup fails with a
  certificate error and `auto` relies on the DHT.
- A provider lookup that finds nothing is remembered for 30 seconds.
