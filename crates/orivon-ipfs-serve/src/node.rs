//! The libp2p side of serve mode: one swarm that dials the peers it was told to dial, keeps them
//! connected, and answers Bitswap wantlists from the kept table. With a [`P2pConfig`] it also
//! listens, so other peers can open connections to it, and announces what it holds on the DHT.

use crate::addr::{dns_name_is_local, ipv4_is_public, ipv6_is_public, PublicOnly};
use crate::bitswap::{read_message, reply_to_wantlist, write_message, BitswapMessage, Version};
use crate::keepalive;
use crate::kept::KeptBlocks;
use crate::p2p::{
    assumed_address, split_p2p, NodeStatus, P2p, P2pConfig, ProvideError, IDLE_CONNECTION_TIMEOUT as P2P_IDLE_TIMEOUT,
    KAD_QUERY_TIMEOUT, MAX_CONNECTIONS_PER_PEER, MAX_INBOUND_CONNECTIONS, MAX_OUTBOUND_CONNECTIONS, MAX_PENDING_INBOUND,
};
use crate::provide::AddOutcome;
use crate::relay::parse_relays;
use cid::Cid;
use futures::stream::select_all;
use futures::{AsyncWriteExt, StreamExt};
use libp2p::autonat;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::timeout::TransportTimeout;
use libp2p::core::upgrade::Version as UpgradeVersion;
use libp2p::identity::Keypair;
use libp2p::kad::{self, store::MemoryStore, store::MemoryStoreConfig};
use libp2p::multiaddr::Protocol;
use libp2p::relay;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{connection_limits, identify, noise, ping, yamux, Multiaddr, PeerId, Transport};
use libp2p_stream::Control;
#[cfg(target_os = "wasi")]
use libp2p_dns as dns;
#[cfg(target_os = "wasi")]
use libp2p_tcp as tcp;
#[cfg(not(target_os = "wasi"))]
use libp2p::{dns, tcp};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::time::{timeout, Instant};

/// How long `dial` waits for the connection.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(30);
/// Peers kept connected at once.
pub const MAX_PEERS: usize = 16;
const TRANSPORT_TIMEOUT: Duration = Duration::from_secs(15);
// Longer than any run of the node: the connections this node dials stay open until the peer or
// the network closes them, and then they are dialed again.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24 * 30);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
const REDIAL_TICK: Duration = Duration::from_secs(1);
/// Inbound Bitswap streams served at once, over all peers, and per peer. A stream beyond either
/// limit is dropped.
pub const MAX_STREAMS: usize = 64;
pub const MAX_STREAMS_PER_PEER: usize = 8;
/// A connection that lasts this long counts as healthy and clears the peer's failures.
const STABLE_AFTER: Duration = Duration::from_secs(30);
/// A kept peer that has not had a healthy connection for this long is forgotten.
const GIVE_UP_AFTER: Duration = Duration::from_secs(60 * 60);
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DialError {
    #[error("{0}")]
    BadAddress(String),
    #[error("timed out")]
    Timeout,
    #[error("{0}")]
    Failed(String),
    #[error("already keeping {MAX_PEERS} peers connected")]
    TooManyPeers,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct StartError(String);

/// `/ip4|ip6|dns4|dns6/<host>/tcp/<port>/p2p/<peer id>` and nothing else. Unless `allow_local`,
/// loopback, private, link-local, unspecified and similar addresses, and `localhost`, are refused,
/// so the page cannot use the node to probe the machine's network.
pub fn parse_dial_addr(text: &str, allow_local: bool) -> Result<(Multiaddr, PeerId), DialError> {
    let bad = |reason: &str| DialError::BadAddress(reason.to_string());
    let addr: Multiaddr = text.parse().map_err(|_| bad("not a multiaddr"))?;
    let parts: Vec<Protocol> = addr.iter().collect();
    let [host, Protocol::Tcp(_), Protocol::P2p(peer)] = parts.as_slice() else {
        return Err(bad(
            "expected /ip4|ip6|dns4|dns6/<host>/tcp/<port>/p2p/<peer id>",
        ));
    };
    let public = match host {
        Protocol::Ip4(ip) => ipv4_is_public(*ip),
        Protocol::Ip6(ip) => ipv6_is_public(*ip),
        Protocol::Dns4(name) | Protocol::Dns6(name) => !dns_name_is_local(name),
        _ => return Err(bad("host must be ip4, ip6, dns4 or dns6")),
    };
    if !public && !allow_local {
        return Err(bad("that address is not public"));
    }
    Ok((addr.clone(), *peer))
}

struct DialCommand {
    addr: Multiaddr,
    peer: PeerId,
    respond: oneshot::Sender<Result<(), DialError>>,
}

enum Command {
    Dial(DialCommand),
    Provide {
        cid: Cid,
        members: Vec<Cid>,
        respond: oneshot::Sender<Result<(AddOutcome, usize), ProvideError>>,
    },
    Unprovide {
        cid: Cid,
        respond: oneshot::Sender<Result<bool, ProvideError>>,
    },
    Status {
        respond: oneshot::Sender<Result<NodeStatus, ProvideError>>,
    },
}

#[derive(Clone)]
pub struct ServeNode {
    peer_id: PeerId,
    commands: mpsc::Sender<Command>,
    allow_local: bool,
    features: Option<&'static str>,
    provides: bool,
}

impl ServeNode {
    pub async fn start(kept: KeptBlocks) -> Result<Self, StartError> {
        Self::start_with(kept, false).await
    }

    /// `allow_local` lets `dial` reach loopback and private addresses; for tests and local setups.
    pub async fn start_with(kept: KeptBlocks, allow_local: bool) -> Result<Self, StartError> {
        Self::start_with_p2p(kept, allow_local, None).await
    }

    /// With `p2p`, the swarm also listens on the configured port and, when the configuration names
    /// a DHT, works out whether it can be reached and announces the CIDs it is asked to provide.
    pub async fn start_with_p2p(
        kept: KeptBlocks,
        allow_local: bool,
        p2p: Option<P2pConfig>,
    ) -> Result<Self, StartError> {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let relays = match &p2p {
            Some(config) => parse_relays(&config.relays, allow_local).map_err(StartError)?,
            None => Vec::new(),
        };
        let mut swarm = build_swarm(&keypair, allow_local, p2p.as_ref(), !relays.is_empty()).await?;
        let mut control = swarm.behaviour().stream.new_control();
        let mut incoming = Vec::new();
        for version in Version::all() {
            let streams = control
                .accept(version.protocol())
                .map_err(|err| StartError(err.to_string()))?;
            incoming.push(streams.map(move |(peer, stream)| (version, peer, stream)));
        }
        let features = p2p.as_ref().map(P2pConfig::features);
        let provides = p2p.as_ref().is_some_and(|config| config.dht.is_some());
        let p2p_state = match &p2p {
            Some(config) => Some(start_listening(&mut swarm, config, allow_local, relays)?),
            None => None,
        };
        let (commands, receiver) = mpsc::channel(32);
        tokio::spawn(async move {
            run(swarm, control, select_all(incoming), receiver, kept, p2p_state).await;
        });
        Ok(Self {
            peer_id,
            commands,
            allow_local,
            features,
            provides,
        })
    }

    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// The value of the `x-orivon-node-features` header, for a node started with a listener.
    pub fn features(&self) -> Option<&'static str> {
        self.features
    }

    /// True when the node can announce: it listens and has a DHT to announce on.
    pub fn can_provide(&self) -> bool {
        self.provides
    }

    /// Dials and returns the peer once connected, then keeps it connected.
    pub async fn dial(&self, addr: &str) -> Result<PeerId, DialError> {
        self.dial_within(addr, DIAL_TIMEOUT).await
    }

    pub async fn dial_within(&self, addr: &str, limit: Duration) -> Result<PeerId, DialError> {
        let (addr, peer) = parse_dial_addr(addr, self.allow_local)?;
        let (respond, response) = oneshot::channel();
        self.commands
            .send(Command::Dial(DialCommand {
                addr,
                peer,
                respond,
            }))
            .await
            .map_err(|_| DialError::Failed("the node stopped".into()))?;
        match timeout(limit, response).await {
            Err(_) => Err(DialError::Timeout),
            Ok(Err(_)) => Err(DialError::Failed("the node stopped".into())),
            Ok(Ok(result)) => result.map(|()| peer),
        }
    }

    /// Announces `cid` on the DHT whenever the node is reachable, and again every 12 hours.
    pub async fn provide(&self, cid: Cid) -> Result<AddOutcome, ProvideError> {
        self.provide_tree(cid, Vec::new())
            .await
            .map(|(outcome, _)| outcome)
    }

    /// Announces `cid` and, until `cid` is stopped, `members` with it (the entries of a folder, say).
    /// A member takes only a place that is free and is left out otherwise; the count that fit comes
    /// back. `cid` itself always gets a place unless the limit on announced CIDs is all named ones:
    /// a CID that is only listed under another root gives way.
    pub async fn provide_tree(
        &self,
        cid: Cid,
        members: Vec<Cid>,
    ) -> Result<(AddOutcome, usize), ProvideError> {
        self.ask(|respond| Command::Provide {
            cid,
            members,
            respond,
        })
        .await
    }

    /// Stops announcing `cid`; false when it was not being announced.
    pub async fn unprovide(&self, cid: Cid) -> Result<bool, ProvideError> {
        self.ask(|respond| Command::Unprovide { cid, respond }).await
    }

    pub async fn status(&self) -> Result<NodeStatus, ProvideError> {
        self.ask(|respond| Command::Status { respond }).await
    }

    async fn ask<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, ProvideError>>) -> Command,
    ) -> Result<T, ProvideError> {
        if self.features.is_none() {
            return Err(ProvideError::NotEnabled);
        }
        let (respond, response) = oneshot::channel();
        self.commands
            .send(command(respond))
            .await
            .map_err(|_| ProvideError::Stopped)?;
        response.await.map_err(|_| ProvideError::Stopped)?
    }
}

/// Starts the listeners. IPv4 must listen, or the node fails to start with the reason. IPv6 is
/// tried as well and may fail: a host can lack it, and Orivon's listen binds IPv4 only.
fn start_listening(
    swarm: &mut libp2p::Swarm<Behaviour>,
    config: &P2pConfig,
    allow_local: bool,
    relays: Vec<(PeerId, Multiaddr)>,
) -> Result<P2p, StartError> {
    let v4: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", config.port)
        .parse()
        .expect("a listen address");
    let v6: Multiaddr = format!("/ip6/::/tcp/{}", config.port)
        .parse()
        .expect("a listen address");
    let mut accepted = Vec::new();
    match swarm.listen_on(v4.clone()) {
        Ok(_) => accepted.push(v4),
        Err(err) => {
            return Err(StartError(format!(
                "cannot listen on port {}: {err}",
                config.port
            )))
        }
    }
    match swarm.listen_on(v6.clone()) {
        Ok(_) => accepted.push(v6),
        Err(err) => tracing::warn!(addr = %v6, %err, "not listening over IPv6"),
    }
    let behaviour = swarm.behaviour_mut();
    if let Some(dht) = &config.dht {
        for text in &dht.bootstrap {
            let Some((peer, addr)) = split_p2p(text) else {
                tracing::warn!(address = %text, "ignoring a bootstrap address without a peer id");
                continue;
            };
            if let Some(kad) = behaviour.kad.as_mut() {
                kad.add_address(&peer, addr.clone());
            }
        }
        if let Some(kad) = behaviour.kad.as_mut() {
            if kad.bootstrap().is_err() {
                tracing::warn!("the DHT has no bootstrap peer to start from");
            }
        }
    }
    if config.assume_public {
        swarm.add_external_address(assumed_address(config.port));
    }
    let now = Instant::now();
    let mut p2p = P2p::new(config, allow_local, accepted, relays, now);
    p2p.listen_relays(swarm, now);
    Ok(p2p)
}

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
pub(crate) struct Behaviour {
    pub(crate) stream: libp2p_stream::Behaviour,
    pub(crate) identify: identify::Behaviour,
    pub(crate) ping: ping::Behaviour,
    pub(crate) limits: connection_limits::Behaviour,
    pub(crate) keepalive: Toggle<keepalive::Behaviour>,
    pub(crate) kad: Toggle<kad::Behaviour<MemoryStore>>,
    pub(crate) autonat: Toggle<autonat::v1::Behaviour>,
    pub(crate) relay_client: Toggle<relay::client::Behaviour>,
}

async fn build_swarm(
    keypair: &Keypair,
    allow_local: bool,
    p2p: Option<&P2pConfig>,
    relays: bool,
) -> Result<libp2p::Swarm<Behaviour>, StartError> {
    let noise_config = noise::Config::new(keypair).map_err(|err| StartError(err.to_string()))?;
    let tcp_transport = tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(UpgradeVersion::V1Lazy)
        .authenticate(noise_config)
        .multiplex(yamux::Config::default())
        .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)));
    // A node that other peers can talk to dials public addresses only, whatever name or DHT
    // record points it elsewhere. Without a listener the page names every address itself.
    let tcp_transport = if p2p.is_some() && !allow_local {
        PublicOnly::new(tcp_transport.boxed()).boxed()
    } else {
        tcp_transport.boxed()
    };
    #[cfg(target_os = "wasi")]
    let transport = dns::tokio::Transport::custom(
        tcp_transport,
        orivon_ipfs_dns::tcp_resolver_config(),
        dns::ResolverOpts::default(),
    );
    #[cfg(not(target_os = "wasi"))]
    let transport = dns::tokio::Transport::system(tcp_transport)
        .map_err(|err| StartError(err.to_string()))?;
    let local_peer = keypair.public().to_peer_id();
    // A relay client turns `<relay>/p2p/<id>/p2p-circuit` into a place to listen, through a
    // reservation on that relay; its transport is tried before the direct one.
    let (transport, relay_client) = if relays {
        let (relay_transport, relay_client) = relay::client::new(local_peer);
        let relay_transport = relay_transport
            .upgrade(UpgradeVersion::V1Lazy)
            .authenticate(noise::Config::new(keypair).map_err(|err| StartError(err.to_string()))?)
            .multiplex(yamux::Config::default())
            .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)));
        let transport = relay_transport
            .or_transport(transport)
            .map(|either, _| either.into_inner())
            .boxed();
        (transport, Some(relay_client))
    } else {
        (transport.boxed(), None)
    };
    let transport = TransportTimeout::new(transport, TRANSPORT_TIMEOUT).boxed();

    let dht = p2p.is_some_and(|config| config.dht.is_some());
    let limits = match p2p {
        None => connection_limits::ConnectionLimits::default()
            .with_max_pending_outgoing(Some(32))
            .with_max_established(Some(2 * MAX_PEERS as u32)),
        Some(_) => connection_limits::ConnectionLimits::default()
            .with_max_pending_outgoing(Some(32))
            .with_max_pending_incoming(Some(MAX_PENDING_INBOUND))
            .with_max_established_incoming(Some(MAX_INBOUND_CONNECTIONS))
            .with_max_established_outgoing(Some(MAX_OUTBOUND_CONNECTIONS))
            .with_max_established(Some(MAX_INBOUND_CONNECTIONS + MAX_OUTBOUND_CONNECTIONS))
            .with_max_established_per_peer(Some(MAX_CONNECTIONS_PER_PEER)),
    };
    let behaviour = Behaviour {
        stream: libp2p_stream::Behaviour::new(),
        identify: identify::Behaviour::new(
            identify::Config::new("ipfs/0.1.0".to_string(), keypair.public())
                .with_agent_version(format!("orivon-serve/{}", env!("CARGO_PKG_VERSION"))),
        ),
        ping: ping::Behaviour::new(ping::Config::new()),
        limits: connection_limits::Behaviour::new(limits),
        keepalive: p2p.map(|_| keepalive::Behaviour::default()).into(),
        kad: dht.then(|| kad_behaviour(local_peer)).into(),
        autonat: p2p
            .filter(|config| config.dht.is_some() && !config.assume_public)
            .map(|config| {
                let mut autonat_config = config.autonat.clone();
                autonat_config.only_global_ips = !allow_local;
                autonat::v1::Behaviour::new(local_peer, autonat_config)
            })
            .into(),
        relay_client: relay_client.into(),
    };
    let config = libp2p::swarm::Config::with_executor(|future| {
        tokio::spawn(future);
    })
    .with_idle_connection_timeout(if p2p.is_some() {
        P2P_IDLE_TIMEOUT
    } else {
        IDLE_CONNECTION_TIMEOUT
    });
    Ok(libp2p::Swarm::new(transport, behaviour, local_peer, config))
}

/// A DHT client: it asks the Amino DHT and announces on it, and does not answer its queries.
fn kad_behaviour(local_peer: PeerId) -> kad::Behaviour<MemoryStore> {
    let mut config = kad::Config::new(kad::PROTOCOL_NAME);
    config.set_query_timeout(KAD_QUERY_TIMEOUT);
    // The node decides when to announce: only while it is reachable.
    config.set_provider_publication_interval(None);
    config.set_periodic_bootstrap_interval(None);
    let store = MemoryStore::with_config(
        local_peer,
        MemoryStoreConfig {
            max_provided_keys: crate::provide::MAX_PROVIDED,
            ..MemoryStoreConfig::default()
        },
    );
    let mut behaviour = kad::Behaviour::with_config(local_peer, store, config);
    behaviour.set_mode(Some(kad::Mode::Client));
    behaviour
}

/// What the node remembers about a peer it keeps connected.
struct Keep {
    addr: Multiaddr,
    failures: u32,
    retry_at: Option<Instant>,
    connected_at: Option<Instant>,
    failing_since: Option<Instant>,
}

fn backoff(failures: u32) -> Duration {
    Duration::from_secs(1u64 << failures.min(6)).min(MAX_BACKOFF)
}

impl Keep {
    fn new(addr: Multiaddr) -> Self {
        Self {
            addr,
            failures: 0,
            retry_at: None,
            connected_at: None,
            failing_since: None,
        }
    }

    fn established(&mut self, now: Instant) {
        self.connected_at = Some(now);
        self.retry_at = None;
    }

    /// A connection that has lasted [`STABLE_AFTER`] clears the failures. Only that does: a peer
    /// that accepts and drops at once keeps counting up.
    fn settle(&mut self, now: Instant) {
        if self
            .connected_at
            .is_some_and(|since| now.duration_since(since) >= STABLE_AFTER)
        {
            self.failures = 0;
            self.failing_since = None;
        }
    }

    fn failed(&mut self, now: Instant) {
        self.failures += 1;
        self.failing_since.get_or_insert(now);
        self.retry_at = Some(now + backoff(self.failures));
    }

    fn closed(&mut self, now: Instant) {
        self.settle(now);
        let lasted = self.connected_at.take();
        if lasted.is_some_and(|since| now.duration_since(since) >= STABLE_AFTER) {
            self.retry_at = Some(now + backoff(0));
        } else {
            self.failed(now);
        }
    }

    fn due(&self, now: Instant) -> bool {
        self.connected_at.is_none() && self.retry_at.is_some_and(|at| at <= now)
    }

    fn expired(&self, now: Instant) -> bool {
        self.connected_at.is_none()
            && self
                .failing_since
                .is_some_and(|since| now.duration_since(since) >= GIVE_UP_AFTER)
    }
}

/// Counts open inbound streams, in all and per peer.
#[derive(Clone)]
struct StreamLimits {
    all: Arc<Semaphore>,
    per_peer: Arc<Mutex<HashMap<PeerId, usize>>>,
    max_per_peer: usize,
}

struct StreamPermit {
    _all: OwnedSemaphorePermit,
    peer: PeerId,
    per_peer: Arc<Mutex<HashMap<PeerId, usize>>>,
}

impl Drop for StreamPermit {
    fn drop(&mut self) {
        let mut counts = self.per_peer.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = counts.get_mut(&self.peer) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.peer);
            }
        }
    }
}

impl StreamLimits {
    fn new(max_all: usize, max_per_peer: usize) -> Self {
        Self {
            all: Arc::new(Semaphore::new(max_all)),
            per_peer: Arc::default(),
            max_per_peer,
        }
    }

    fn try_acquire(&self, peer: PeerId) -> Option<StreamPermit> {
        let all = self.all.clone().try_acquire_owned().ok()?;
        let mut counts = self.per_peer.lock().unwrap_or_else(|e| e.into_inner());
        let count = counts.entry(peer).or_default();
        if *count >= self.max_per_peer {
            return None;
        }
        *count += 1;
        Some(StreamPermit {
            _all: all,
            peer,
            per_peer: self.per_peer.clone(),
        })
    }
}

async fn run<S>(
    mut swarm: libp2p::Swarm<Behaviour>,
    control: Control,
    mut incoming: S,
    mut commands: mpsc::Receiver<Command>,
    kept: KeptBlocks,
    mut p2p: Option<P2p>,
) where
    S: futures::Stream<Item = (Version, PeerId, libp2p::swarm::Stream)> + Unpin,
{
    let mut connected = HashSet::<PeerId>::new();
    let mut dialing = HashSet::<PeerId>::new();
    let mut waiting = HashMap::<PeerId, Vec<oneshot::Sender<Result<(), DialError>>>>::new();
    let mut first_addr = HashMap::<PeerId, Multiaddr>::new();
    let mut keep = HashMap::<PeerId, Keep>::new();
    let limits = StreamLimits::new(MAX_STREAMS, MAX_STREAMS_PER_PEER);
    let mut tick = tokio::time::interval(REDIAL_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { return };
                let DialCommand { addr, peer, respond } = match command {
                    Command::Dial(dial) => dial,
                    Command::Provide { cid, members, respond } => {
                        let _ = respond.send(match p2p.as_mut() {
                            Some(p2p) => p2p.provide(&mut swarm, cid, &members, Instant::now()),
                            None => Err(ProvideError::NotEnabled),
                        });
                        continue;
                    }
                    Command::Unprovide { cid, respond } => {
                        let _ = respond.send(match p2p.as_mut() {
                            Some(p2p) => Ok(p2p.unprovide(&mut swarm, &cid, Instant::now())),
                            None => Err(ProvideError::NotEnabled),
                        });
                        continue;
                    }
                    Command::Status { respond } => {
                        let _ = respond.send(match p2p.as_ref() {
                            Some(p2p) => Ok(p2p.status(*swarm.local_peer_id())),
                            None => Err(ProvideError::NotEnabled),
                        });
                        continue;
                    }
                };
                if connected.contains(&peer) {
                    if !keep.contains_key(&peer) {
                        keep.insert(peer, Keep::new(addr));
                        keep_open(&mut swarm, peer, true);
                    }
                    let _ = respond.send(Ok(()));
                    continue;
                }
                if !keep.contains_key(&peer)
                    && !first_addr.contains_key(&peer)
                    && keep.len() + first_addr.len() >= MAX_PEERS
                {
                    let _ = respond.send(Err(DialError::TooManyPeers));
                    continue;
                }
                waiting.entry(peer).or_default().push(respond);
                if dialing.contains(&peer) {
                    continue;
                }
                if let Some(entry) = keep.get_mut(&peer) {
                    entry.addr = addr.clone();
                    entry.retry_at = None;
                }
                match swarm.dial(addr.clone()) {
                    Ok(()) => {
                        dialing.insert(peer);
                        first_addr.insert(peer, addr);
                    }
                    Err(err) => {
                        tracing::debug!(%peer, %err, "dial refused");
                        for waiter in waiting.remove(&peer).unwrap_or_default() {
                            let _ = waiter.send(Err(DialError::Failed("could not connect".into())));
                        }
                    }
                }
            }
            event = swarm.select_next_some() => {
                if let Some(p2p) = p2p.as_mut() {
                    p2p.on_event(&mut swarm, &event, Instant::now());
                }
                log_connection_event(&event);
                match event {
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    let now = Instant::now();
                    connected.insert(peer_id);
                    dialing.remove(&peer_id);
                    if let Some(addr) = first_addr.remove(&peer_id) {
                        if !keep.contains_key(&peer_id) {
                            keep.insert(peer_id, Keep::new(addr));
                            keep_open(&mut swarm, peer_id, true);
                        }
                    }
                    if let Some(entry) = keep.get_mut(&peer_id) {
                        entry.established(now);
                    }
                    for waiter in waiting.remove(&peer_id).unwrap_or_default() {
                        let _ = waiter.send(Ok(()));
                    }
                }
                SwarmEvent::ConnectionClosed { peer_id, num_established: 0, .. } => {
                    connected.remove(&peer_id);
                    if let Some(entry) = keep.get_mut(&peer_id) {
                        entry.closed(Instant::now());
                    }
                }
                SwarmEvent::OutgoingConnectionError { peer_id: Some(peer_id), error, .. } => {
                    // Only a dial this task started counts; anything else is not ours to act on.
                    if dialing.remove(&peer_id) {
                        tracing::debug!(%peer_id, %error, "dial failed");
                        first_addr.remove(&peer_id);
                        for waiter in waiting.remove(&peer_id).unwrap_or_default() {
                            let _ = waiter.send(Err(DialError::Failed("could not connect".into())));
                        }
                        if let Some(entry) = keep.get_mut(&peer_id) {
                            entry.failed(Instant::now());
                        }
                    }
                }
                _ => {}
                }
            }
            Some((version, peer, stream)) = incoming.next() => {
                tracing::debug!(%peer, protocol = %version.protocol(), "inbound bitswap stream");
                match limits.try_acquire(peer) {
                    Some(permit) => {
                        tokio::spawn(serve_stream(
                            peer, stream, version, kept.clone(), control.clone(), permit,
                        ));
                    }
                    None => tracing::debug!(%peer, "inbound bitswap stream dropped: too many open"),
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                let mut forgotten = Vec::new();
                keep.retain(|peer, entry| {
                    let gone = entry.expired(now);
                    if gone {
                        tracing::info!(%peer, "no longer keeping the peer connected");
                        forgotten.push(*peer);
                    }
                    !gone
                });
                for peer in forgotten {
                    keep_open(&mut swarm, peer, false);
                }
                if let Some(p2p) = p2p.as_mut() {
                    p2p.tick(&mut swarm, now);
                }
                for (peer, entry) in keep.iter_mut() {
                    if connected.contains(peer) {
                        entry.settle(now);
                        continue;
                    }
                    if dialing.contains(peer) || !entry.due(now) {
                        continue;
                    }
                    entry.retry_at = None;
                    match swarm.dial(entry.addr.clone()) {
                        Ok(()) => {
                            dialing.insert(*peer);
                        }
                        Err(err) => {
                            tracing::warn!(%peer, %err, "redial refused");
                            entry.failed(now);
                        }
                    }
                }
            }
        }
    }
}

/// What a peer's connections did, for a debug log: which connection, over which address, and why
/// it closed.
fn log_connection_event(event: &SwarmEvent<BehaviourEvent>) {
    match event {
        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            num_established,
            ..
        } => tracing::debug!(
            %peer_id,
            connection = ?connection_id,
            address = %endpoint.get_remote_address(),
            dialer = endpoint.is_dialer(),
            established = num_established.get(),
            "connection established"
        ),
        SwarmEvent::ConnectionClosed {
            peer_id,
            connection_id,
            endpoint,
            num_established,
            cause,
            ..
        } => tracing::debug!(
            %peer_id,
            connection = ?connection_id,
            address = %endpoint.get_remote_address(),
            remaining = *num_established,
            cause = ?cause,
            "connection closed"
        ),
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => tracing::debug!(
            peer = ?peer_id,
            error = %error_chain(error),
            "outgoing connection failed"
        ),
        SwarmEvent::IncomingConnectionError { send_back_addr, error, .. } => tracing::debug!(
            address = %send_back_addr,
            error = %error_chain(error),
            "incoming connection refused"
        ),
        _ => {}
    }
}

/// An error and the causes under it: a refused dial says only that it was refused until the limit
/// that refused it is read.
fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(inner) = cause {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        cause = inner.source();
    }
    text
}

/// Asks the swarm to hold the connections to `peer` open (or to let them close when idle).
fn keep_open(swarm: &mut libp2p::Swarm<Behaviour>, peer: PeerId, keep: bool) {
    if let Some(keepalive) = swarm.behaviour_mut().keepalive.as_mut() {
        keepalive.set(peer, keep);
    }
}

async fn serve_stream(
    peer: PeerId,
    mut stream: libp2p::swarm::Stream,
    version: Version,
    kept: KeptBlocks,
    control: Control,
    _permit: StreamPermit,
) {
    loop {
        let message = match timeout(STREAM_IDLE_TIMEOUT, read_message(&mut stream)).await {
            Ok(Ok(Some(message))) => message,
            Ok(Ok(None)) => {
                tracing::debug!(%peer, "bitswap stream ended");
                break;
            }
            Ok(Err(err)) => {
                tracing::debug!(%peer, %err, "bitswap stream failed");
                break;
            }
            Err(_) => {
                tracing::debug!(%peer, "bitswap stream idle");
                break;
            }
        };
        let replies = reply_to_wantlist(&message, &kept, version);
        if tracing::enabled!(tracing::Level::DEBUG) {
            let entries = message.wantlist.as_ref().map_or(0, |list| list.entries.len());
            let blocks: usize = replies.iter().map(|reply| reply.payload.len() + reply.blocks.len()).sum();
            let presences: usize = replies.iter().map(|reply| reply.block_presences.len()).sum();
            tracing::debug!(%peer, entries, blocks, presences, "bitswap wantlist");
        }
        if !replies.is_empty() {
            match send_replies(&control, peer, version, &replies).await {
                Ok(()) => tracing::debug!(%peer, messages = replies.len(), "bitswap reply sent"),
                Err(err) => tracing::debug!(%peer, %err, "bitswap reply not sent"),
            }
        }
    }
}

async fn send_replies(
    control: &Control,
    peer: PeerId,
    version: Version,
    replies: &[BitswapMessage],
) -> Result<(), String> {
    let mut control = control.clone();
    let mut stream = timeout(OPEN_STREAM_TIMEOUT, control.open_stream(peer, version.protocol()))
        .await
        .map_err(|_| "opening a stream timed out".to_string())?
        .map_err(|err| err.to_string())?;
    for reply in replies {
        timeout(WRITE_TIMEOUT, write_message(&mut stream, reply))
            .await
            .map_err(|_| "writing timed out".to_string())?
            .map_err(|err| err.to_string())?;
    }
    stream.close().await.map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PEER: &str = "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP";

    fn parse(host_and_port: &str, allow_local: bool) -> Result<(Multiaddr, PeerId), DialError> {
        parse_dial_addr(&format!("/{host_and_port}/p2p/{PEER}"), allow_local)
    }

    #[test]
    fn accepts_public_tcp_addresses_with_a_peer_id() {
        for host in [
            "ip4/93.184.216.34/tcp/4001",
            "ip6/2606:2800:220:1:248:1893:25c8:1946/tcp/4001",
            "dns4/example.com/tcp/4001",
            "dns6/example.com/tcp/4001",
        ] {
            let text = format!("/{host}/p2p/{PEER}");
            let (addr, peer) = parse_dial_addr(&text, false).unwrap();
            assert_eq!(addr.to_string(), text);
            assert_eq!(peer.to_string(), PEER);
        }
    }

    #[test]
    fn refuses_other_address_forms() {
        for text in [
            "",
            "garbage",
            "/ip4/93.184.216.34/tcp/4001",
            &format!("/ip4/93.184.216.34/udp/4001/quic-v1/p2p/{PEER}"),
            &format!("/ip4/93.184.216.34/tcp/4001/ws/p2p/{PEER}"),
            &format!("/dnsaddr/bootstrap.libp2p.io/p2p/{PEER}"),
            &format!("/dns/example.com/tcp/4001/p2p/{PEER}"),
            &format!("/ip4/93.184.216.34/tcp/4001/p2p/{PEER}/p2p-circuit"),
        ] {
            assert!(
                matches!(parse_dial_addr(text, true), Err(DialError::BadAddress(_))),
                "{text}"
            );
        }
    }

    #[test]
    fn refuses_local_and_private_hosts_unless_allowed() {
        for host in [
            "ip4/127.0.0.1/tcp/4001",
            "ip4/10.1.2.3/tcp/4001",
            "ip4/172.16.0.1/tcp/4001",
            "ip4/192.168.1.1/tcp/4001",
            "ip4/169.254.169.254/tcp/80",
            "ip4/100.64.0.1/tcp/80",
            "ip4/0.0.0.0/tcp/4001",
            "ip4/224.0.0.1/tcp/4001",
            "ip6/::1/tcp/4001",
            "ip6/::/tcp/4001",
            "ip6/fe80::1/tcp/4001",
            "ip6/fd00::1/tcp/4001",
            "ip6/::ffff:127.0.0.1/tcp/4001",
            "ip6/::ffff:10.0.0.1/tcp/4001",
            "dns4/localhost/tcp/4001",
            "dns4/LOCALHOST./tcp/4001",
            "dns6/app.localhost/tcp/4001",
        ] {
            assert!(
                matches!(parse(host, false), Err(DialError::BadAddress(_))),
                "{host} should be refused"
            );
            assert!(parse(host, true).is_ok(), "{host} should pass when allowed");
        }
    }

    #[test]
    fn backoff_doubles_and_stops_at_a_minute() {
        assert_eq!(backoff(0), Duration::from_secs(1));
        assert_eq!(backoff(1), Duration::from_secs(2));
        assert_eq!(backoff(5), Duration::from_secs(32));
        assert_eq!(backoff(6), Duration::from_secs(60));
        assert_eq!(backoff(40), Duration::from_secs(60));
    }

    fn keep() -> Keep {
        Keep::new("/ip4/93.184.216.34/tcp/4001".parse().unwrap())
    }

    #[test]
    fn a_peer_that_connects_and_drops_at_once_backs_off_further_each_time() {
        let mut entry = keep();
        let mut now = Instant::now();
        let mut waits = Vec::new();
        for _ in 0..5 {
            entry.established(now);
            now += Duration::from_millis(100);
            entry.closed(now);
            waits.push(entry.retry_at.unwrap() - now);
            now = entry.retry_at.unwrap();
            assert!(entry.due(now));
        }
        assert_eq!(
            waits,
            [2, 4, 8, 16, 32].map(Duration::from_secs).to_vec()
        );
    }

    #[test]
    fn a_connection_that_lasts_clears_the_failures() {
        let mut entry = keep();
        let mut now = Instant::now();
        entry.established(now);
        now += Duration::from_secs(1);
        entry.closed(now);
        assert_eq!(entry.failures, 1);
        entry.established(now);
        now += STABLE_AFTER;
        entry.settle(now);
        assert_eq!(entry.failures, 0);
        assert!(entry.failing_since.is_none());
        entry.closed(now);
        assert_eq!(entry.retry_at, Some(now + Duration::from_secs(1)));
    }

    #[test]
    fn a_peer_that_never_comes_back_is_dropped_after_an_hour() {
        let mut entry = keep();
        let start = Instant::now();
        entry.established(start);
        entry.closed(start + Duration::from_secs(1));
        assert!(!entry.expired(start + Duration::from_secs(60)));
        let mut now = start + Duration::from_secs(1);
        while now < start + GIVE_UP_AFTER {
            now += Duration::from_secs(60);
            entry.failed(now);
        }
        assert!(entry.expired(now));
        // A connected peer is never dropped.
        entry.established(now);
        assert!(!entry.expired(now + GIVE_UP_AFTER));
    }

    #[test]
    fn stream_limits_apply_in_all_and_per_peer_and_free_on_drop() {
        let a = PeerId::random();
        let b = PeerId::random();
        let limits = StreamLimits::new(3, 2);
        let a1 = limits.try_acquire(a).unwrap();
        let _a2 = limits.try_acquire(a).unwrap();
        assert!(limits.try_acquire(a).is_none(), "per peer");
        let _b1 = limits.try_acquire(b).unwrap();
        assert!(limits.try_acquire(b).is_none(), "in all");
        drop(a1);
        let _b2 = limits.try_acquire(b).expect("a slot freed by the drop");
        assert!(limits.try_acquire(a).is_none(), "in all again");
    }
}
