//! Serve mode: a node that is given blocks and offers them to peers.
//!
//! - [`KeptBlocks`]: the only blocks Bitswap answers from.
//! - [`parse_and_verify_car`]: how blocks come in.
//! - [`ServeNode`]: dials out, keeps the peers connected, answers their wantlists.
//! - [`P2pConfig`]: also listens for connections, finds out whether the computer can be reached
//!   from outside, and announces chosen CIDs on the DHT while it can. With circuit relays named,
//!   it holds a reservation on each and announces the circuit address too.
//!
//! Without a [`P2pConfig`] the node never listens for libp2p connections.

mod addr;
mod bitswap;
mod car;
mod keepalive;
mod kept;
mod node;
mod p2p;
mod provide;
mod relay;
mod tree;

pub use bitswap::{
    cid_prefix, reply_to_wantlist, BitswapMessage, BlockPayload, BlockPresence, Version, WantEntry,
    WantType, Wantlist,
};
pub use car::{parse_and_verify_car, parse_and_verify_car_yielding, CarError, VerifiedCar};
pub use kept::{KeptBlocks, KeptError, BLOCK_OVERHEAD, MAX_KEPT_BLOCKS, MAX_KEPT_BYTES};
pub use node::{parse_dial_addr, DialError, ServeNode, StartError, DIAL_TIMEOUT, MAX_PEERS};
pub use p2p::{
    DhtConfig, NodeStatus, P2pConfig, ProvideError, MAX_CONNECTIONS_PER_PEER, MAX_INBOUND_CONNECTIONS,
};
pub use provide::{AddOutcome, ProvidingEntry, Reachability, MAX_PROVIDED, REANNOUNCE_EVERY};
pub use relay::{parse_relay_addr, MAX_RELAYS};
pub use tree::announce_set;
