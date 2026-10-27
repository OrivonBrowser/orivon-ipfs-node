//! Helpers shared by the integration tests: ports, polling, and a bare libp2p peer.
#![allow(dead_code)]

use futures::StreamExt;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{Multiaddr, PeerId, Swarm};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};

/// A TCP port nothing listens on right now.
pub fn free_port() -> u16 {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap().port()
}

/// Polls `check` until it returns `Some`, or panics after `limit`.
pub async fn eventually<T, F, Fut>(what: &str, limit: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let end = Instant::now() + limit;
    loop {
        if let Some(found) = check().await {
            return found;
        }
        if Instant::now() >= end {
            panic!("timed out waiting for: {what}");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Waits for the first connection a peer driven by [`spawn_peer`] reports.
pub async fn wait_connected(events: &mut mpsc::UnboundedReceiver<Seen>) -> PeerId {
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        match tokio::time::timeout_at(end, events.recv()).await {
            Ok(Some(Seen::Connected(peer))) => return peer,
            Ok(Some(_)) => {}
            _ => panic!("the peer never connected"),
        }
    }
}

pub fn loopback(port: u16) -> Multiaddr {
    format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap()
}

pub fn with_peer(addr: &Multiaddr, peer: PeerId) -> String {
    format!("{addr}/p2p/{peer}")
}

/// What a peer driven by [`spawn_peer`] reports.
#[derive(Debug)]
pub enum Seen {
    Listening(Multiaddr),
    Connected(PeerId),
    Closed(PeerId),
}

/// Runs `swarm` in a task. Its listen addresses and connection changes arrive on the channel.
pub fn spawn_peer<B>(mut swarm: Swarm<B>) -> (PeerId, mpsc::UnboundedReceiver<Seen>)
where
    B: NetworkBehaviour + Send + 'static,
    B::ToSwarm: std::fmt::Debug,
{
    let peer = *swarm.local_peer_id();
    let (seen, events) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let event = swarm.select_next_some().await;
            let report = match event {
                SwarmEvent::NewListenAddr { address, .. } => Some(Seen::Listening(address)),
                SwarmEvent::ConnectionEstablished { peer_id, .. } => Some(Seen::Connected(peer_id)),
                SwarmEvent::ConnectionClosed { peer_id, .. } => Some(Seen::Closed(peer_id)),
                _ => None,
            };
            if let Some(report) = report {
                if seen.send(report).is_err() {
                    return;
                }
            }
        }
    });
    (peer, events)
}

pub fn tcp_swarm<B, F>(behaviour: F) -> Swarm<B>
where
    B: NetworkBehaviour + Send + 'static,
    F: FnOnce(&libp2p::identity::Keypair) -> B,
{
    libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .unwrap()
        .with_behaviour(behaviour)
        .unwrap()
        .with_swarm_config(|config| config.with_idle_connection_timeout(Duration::from_secs(60)))
        .build()
}
