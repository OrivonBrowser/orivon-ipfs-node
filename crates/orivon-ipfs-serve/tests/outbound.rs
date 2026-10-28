//! An announcement walks the DHT through dozens of peers and the connections it opens count against
//! the node's outbound limit; a walk that is refused a dial hands its record to peers far from the
//! key, where no lookup looks. The node must hold as many as a walk keeps open.
#![cfg(not(target_os = "wasi"))]

mod support;

use libp2p::identify;
use libp2p::{Multiaddr, PeerId};
use orivon_ipfs_serve::{KeptBlocks, P2pConfig, ServeNode};
use support::{free_port, spawn_peer, tcp_swarm, with_peer, Seen};
use tokio::sync::mpsc;

/// More than the 32 the node once allowed, fewer than a walk through a full routing table opens.
const PEERS: usize = 48;

async fn listening_peer() -> (PeerId, Multiaddr, mpsc::UnboundedReceiver<Seen>) {
    let mut swarm = tcp_swarm(|key| {
        identify::Behaviour::new(identify::Config::new("test/1.0.0".to_string(), key.public()))
    });
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .unwrap();
    let (peer, mut events) = spawn_peer(swarm);
    loop {
        match events.recv().await {
            Some(Seen::Listening(addr)) => return (peer, addr, events),
            Some(_) => {}
            None => panic!("the peer stopped before it listened"),
        }
    }
}

#[tokio::test]
async fn the_node_holds_as_many_outbound_connections_as_a_dht_walk_keeps_open() {
    let node = ServeNode::start_with_p2p(KeptBlocks::default(), true, Some(P2pConfig::new(free_port())))
        .await
        .unwrap();
    // The peers' event channels stay open so that their tasks keep running.
    let mut peers = Vec::new();
    for _ in 0..PEERS {
        peers.push(listening_peer().await);
    }
    for (peer, addr, _) in &peers {
        let dialed = node.dial(&with_peer(addr, *peer)).await;
        assert_eq!(dialed.as_ref().ok(), Some(peer), "{dialed:?}");
    }
}
