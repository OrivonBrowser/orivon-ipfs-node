//! Helpers shared by the integration tests: ports, polling, and a bare libp2p peer.
#![allow(dead_code)]

use futures::StreamExt;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{identify, relay, Multiaddr, PeerId, Swarm, SwarmBuilder};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct RelayServer {
    relay: relay::Behaviour,
    identify: identify::Behaviour,
}

/// A circuit relay v2 server on the loopback interface with limits no test comes near.
pub struct Relay {
    pub peer: PeerId,
    pub addr: Multiaddr,
    accepted: Arc<AtomicUsize>,
    cut: mpsc::UnboundedSender<()>,
}

impl Relay {
    /// `<address>/p2p/<relay id>`, the form `--relay` takes.
    pub fn bootstrap(&self) -> String {
        with_peer(&self.addr, self.peer)
    }

    /// Reservations accepted so far, renewals not counted.
    pub fn reservations(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    /// Closes every connection the relay has.
    pub fn cut_connections(&self) {
        let _ = self.cut.send(());
    }
}

pub async fn start_relay() -> Relay {
    let mut swarm = tcp_swarm(|key| RelayServer {
        relay: relay::Behaviour::new(
            key.public().to_peer_id(),
            relay::Config {
                max_reservations: 64,
                max_reservations_per_peer: 8,
                reservation_duration: Duration::from_secs(60 * 60),
                reservation_rate_limiters: Vec::new(),
                max_circuits: 64,
                max_circuits_per_peer: 8,
                max_circuit_duration: Duration::from_secs(60 * 60),
                max_circuit_bytes: 1 << 40,
                circuit_src_rate_limiters: Vec::new(),
            },
        ),
        identify: identify::Behaviour::new(identify::Config::new(
            "test/1.0.0".to_string(),
            key.public(),
        )),
    });
    let peer = *swarm.local_peer_id();
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .unwrap();
    let addr = loop {
        if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
            break address;
        }
    };
    swarm.add_external_address(addr.clone());
    let accepted = Arc::new(AtomicUsize::new(0));
    let (cut, mut cuts) = mpsc::unbounded_channel();
    let seen = accepted.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                event = swarm.select_next_some() => {
                    if let SwarmEvent::Behaviour(RelayServerEvent::Relay(
                        relay::Event::ReservationReqAccepted { renewed: false, .. },
                    )) = event
                    {
                        seen.fetch_add(1, Ordering::SeqCst);
                    }
                }
                Some(()) = cuts.recv() => {
                    let peers: Vec<PeerId> = swarm.connected_peers().copied().collect();
                    for peer in peers {
                        let _ = swarm.disconnect_peer_id(peer);
                    }
                }
            }
        }
    });
    Relay {
        peer,
        addr,
        accepted,
        cut,
    }
}

/// A peer that can dial a `/p2p-circuit` address and open streams.
#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
pub struct ThroughRelay {
    pub relay_client: relay::client::Behaviour,
    pub stream: libp2p_stream::Behaviour,
}

pub fn relay_client_swarm() -> Swarm<ThroughRelay> {
    SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .unwrap()
        .with_relay_client(libp2p::noise::Config::new, libp2p::yamux::Config::default)
        .unwrap()
        .with_behaviour(|_key, relay_client| ThroughRelay {
            relay_client,
            stream: libp2p_stream::Behaviour::new(),
        })
        .unwrap()
        .with_swarm_config(|config| config.with_idle_connection_timeout(Duration::from_secs(60)))
        .build()
}

/// Waits for the first connection to `peer` that a peer driven by [`spawn_peer`] reports.
pub async fn wait_connected_to(events: &mut mpsc::UnboundedReceiver<Seen>, peer: PeerId) {
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        match tokio::time::timeout_at(end, events.recv()).await {
            Ok(Some(Seen::Connected(connected))) if connected == peer => return,
            Ok(Some(_)) => {}
            _ => panic!("the peer never connected to {peer}"),
        }
    }
}
