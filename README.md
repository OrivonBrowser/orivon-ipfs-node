# Orivon IPFS node

Orivon's IPFS node, written in Rust. It is compiled to WebAssembly (`wasm32-wasip2`) and run by
Orivon apps, such as Orivon Share, inside the Orivon browser. It can also be built and run as a
native program.

## What it does

- Fetches IPFS content and verifies every block against its CID before it is used: libp2p over TCP
  and DNS, a Kademlia client, a Bitswap client, delegated HTTP routing, IPNS record verification
  and DNSLink over DNS-over-HTTPS.
- Keeps blocks in a bounded SQLite cache.
- In serve mode, holds blocks it is given, lets peers fetch them over Bitswap, and with
  `--p2p-port` accepts connections and announces what it holds on the DHT, through a circuit relay
  when `--relay` names one.
- Answers a small HTTP API on a local address (`--addr`):

| Path | Purpose |
|---|---|
| `GET /health` | Liveness. With a listener it also sends the header `x-orivon-node-features`. |
| `GET /ipfs/<cid>?format=raw` | One verified block. |
| `GET /ipns/<key>?format=ipns-record` | A signed IPNS record, checked before it is returned. |
| `GET /id` | The node's peer ID (serve mode). |
| `POST /blocks`, `POST /dial` | Give the node blocks as a CAR file; connect it to a peer (serve mode). |
| `POST /provide`, `DELETE /provide/<cid>` | Announce, or stop announcing, a held block on the DHT. |
| `GET /status` | Peer ID, listen and external addresses, relay reservations, reachability, provided CIDs. |

`orivon/README.md` describes every request, flag and limit.

## Crates

| Crate | Role |
|---|---|
| `orivon-ipfs-core` | CIDs, block verification, CAR parsing |
| `orivon-ipfs-store` | SQLite block store with LRU eviction |
| `orivon-ipfs-unixfs` | UnixFS and dag-pb decoding |
| `orivon-ipfs-namesys` | IPNS records, DNSLink, HTTP client for WASI |
| `orivon-ipfs-routing` | Delegated routing and the light DHT |
| `orivon-ipfs-retrieval` | Bitswap and HTTP block retrieval |
| `orivon-ipfs-serve` | Serve mode: kept blocks, Bitswap server, listener, provider records |
| `orivon-ipfs-dns` | TCP-only DNS resolver configuration for hosts without UDP |
| `orivon-ipfs-gateway` | The `orivon-ipfs-gateway` binary and the HTTP API |

## Build

WebAssembly build, as Orivon runs it:

```sh
export RUSTUP_HOME=...      # rustup install whose stable toolchain has the wasm32-wasip2 target
export CARGO_HOME=...
export WASI_SDK_PATH=...    # wasi-sdk 34 or later
export ORIVON_JCO_DIR=...   # npm prefix with @bytecodealliance/jco-transpile@0.15.0
orivon/build.sh             # writes dist/ipfs-node.wasm and dist/ipfs-node.p2/
```

The workflow `.github/workflows/orivon-wasm.yml` runs the same script and uploads `dist/`.

Native build and tests:

```sh
cargo build --release -p orivon-ipfs-gateway
cargo test --workspace
```

Tests that need the public network or a Kubo binary are marked `#[ignore]`; the `Makefile` has a
target for each (`make live-smoke`, `make kubo-parity`, and so on).

## Licence

Licensed under MIT OR Apache-2.0, see LICENSE-MIT and LICENSE-APACHE.
