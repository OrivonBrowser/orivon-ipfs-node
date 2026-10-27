//! Serve mode with a libp2p listener: accepts connections, finds out whether the computer can be
//! reached from outside, and announces the CIDs it was asked to provide on the DHT.
//!
//! [`P2pConfig`] is what the node is started with. `P2p` is the state the swarm loop keeps:
//! the addresses the swarm listens on and has confirmed, the reachability AutoNAT reports, and
//! the announcements ([`Provided`]).

use crate::addr::{candidate_from_observed, ipv4_is_public, ipv6_is_public, Observations};
use crate::node::{Behaviour, BehaviourEvent};
use crate::provide::{
    provider_key, AddOutcome, Provided, ProvidingEntry, Reachability, TooMany, MAX_ANNOUNCING,
    REANNOUNCE_EVERY,
};
use cid::Cid;
use libp2p::autonat;
use libp2p::kad::{self, QueryId, QueryResult, RecordKey};
use libp2p::multiaddr::Protocol;
use libp2p::swarm::SwarmEvent;
use libp2p::{identify, Multiaddr, PeerId, Swarm};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

/// Established inbound connections, over all peers.
pub const MAX_INBOUND_CONNECTIONS: u32 = 64;
/// Established connections to one peer, in either direction.
pub const MAX_CONNECTIONS_PER_PEER: u32 = 4;
/// Inbound connections still being set up.
pub const MAX_PENDING_INBOUND: u32 = 32;
/// Established connections this node opened: the peers it keeps, the DHT, the AutoNAT servers.
pub const MAX_OUTBOUND_CONNECTIONS: u32 = 32;
/// A connection with no open stream closes after this, unless its peer is one the node keeps.
pub(crate) const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const KAD_QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const KAD_BOOTSTRAP_EVERY: Duration = Duration::from_secs(10 * 60);
/// Peers that serve AutoNAT v1, remembered so a probe has someone to ask after the connections
/// the DHT opened have closed.
const MAX_AUTONAT_SERVERS: usize = 16;
const AUTONAT_V1_PROTOCOL: &str = "/libp2p/autonat/1.0.0";
/// Different peers that must report the same address before AutoNAT is asked about it.
const MIN_OBSERVATIONS: usize = 2;

/// The DHT the node announces on.
#[derive(Debug, Clone)]
pub struct DhtConfig {
    /// `<address>/p2p/<peer id>` of the peers the DHT starts from.
    pub bootstrap: Vec<String>,
}

/// What a serve node needs beyond dialing out, to be reached and to announce.
#[derive(Debug, Clone)]
pub struct P2pConfig {
    /// TCP port the swarm listens on, on every IPv4 and IPv6 interface.
    pub port: u16,
    /// A DHT client and reachability detection; without it the node accepts connections and
    /// announces nothing.
    pub dht: Option<DhtConfig>,
    pub reannounce_every: Duration,
    /// Different peers that must report an address before AutoNAT tests it.
    pub min_observations: usize,
    pub autonat: autonat::v1::Config,
    /// Treats the loopback address as confirmed and the node as public, with no AutoNAT. For tests.
    pub assume_public: bool,
}

impl P2pConfig {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            dht: None,
            reannounce_every: REANNOUNCE_EVERY,
            min_observations: MIN_OBSERVATIONS,
            autonat: autonat::v1::Config {
                boot_delay: Duration::from_secs(10),
                retry_interval: Duration::from_secs(60),
                refresh_interval: Duration::from_secs(5 * 60),
                // This node asks others to test it and never tests anyone.
                throttle_clients_global_max: 0,
                ..autonat::v1::Config::default()
            },
            assume_public: false,
        }
    }

    pub fn with_dht(mut self, bootstrap: Vec<String>) -> Self {
        self.dht = Some(DhtConfig { bootstrap });
        self
    }

    pub fn with_assume_public(mut self) -> Self {
        self.assume_public = true;
        self
    }

    /// The value of the `x-orivon-node-features` header.
    pub fn features(&self) -> &'static str {
        if self.dht.is_some() {
            "serve,listen,provide"
        } else {
            "serve,listen"
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProvideError {
    #[error("the node was not started with a listener")]
    NotEnabled,
    #[error("the node has no DHT to announce on")]
    NoDht,
    #[error("already announcing the most CIDs allowed")]
    TooMany,
    #[error("the node stopped")]
    Stopped,
}

impl From<TooMany> for ProvideError {
    fn from(_: TooMany) -> Self {
        ProvideError::TooMany
    }
}

/// What `GET /status` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStatus {
    pub peer: PeerId,
    pub listen: Vec<Multiaddr>,
    pub external: Vec<Multiaddr>,
    pub reachability: Reachability,
    pub providing: Vec<ProvidingEntry>,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

/// `<address>/p2p/<peer id>` as the pair a routing table is given.
pub(crate) fn split_p2p(text: &str) -> Option<(PeerId, Multiaddr)> {
    let mut addr: Multiaddr = text.parse().ok()?;
    match addr.pop()? {
        Protocol::P2p(peer) => Some((peer, addr)),
        _ => None,
    }
}

/// The address a node told to assume it is reachable gives out: its loopback address.
pub(crate) fn assumed_address(port: u16) -> Multiaddr {
    format!("/ip4/127.0.0.1/tcp/{port}")
        .parse()
        .expect("an external address")
}

/// `addr` without a trailing `/p2p/<peer id>`, which AutoNAT adds to the address it confirms.
fn without_peer(addr: &Multiaddr) -> Multiaddr {
    let mut addr = addr.clone();
    if matches!(addr.iter().last(), Some(Protocol::P2p(_))) {
        addr.pop();
    }
    addr
}

/// What AutoNAT's status means for announcing. Public needs an address AutoNAT confirmed, since
/// that is the address the provider record carries.
fn verdict(
    assume_public: bool,
    nat: Option<&autonat::v1::NatStatus>,
    has_confirmed_address: bool,
) -> Reachability {
    if assume_public {
        return Reachability::Public;
    }
    match nat {
        Some(autonat::v1::NatStatus::Public(_)) if has_confirmed_address => Reachability::Public,
        Some(autonat::v1::NatStatus::Private) => Reachability::Private,
        _ => Reachability::Unknown,
    }
}

/// An address worth giving the routing table: a bare TCP address on a public IP (or any IP when
/// local addresses are allowed). Names and other transports are left to the peers that use them.
fn routable(addr: &Multiaddr, allow_local: bool) -> bool {
    let parts: Vec<Protocol> = addr.iter().collect();
    let [ip, Protocol::Tcp(_)] = parts.as_slice() else {
        return false;
    };
    match ip {
        Protocol::Ip4(ip) => allow_local || ipv4_is_public(*ip),
        Protocol::Ip6(ip) => allow_local || ipv6_is_public(*ip),
        _ => false,
    }
}

pub(crate) struct P2p {
    port: u16,
    allow_local: bool,
    assume_public: bool,
    dht: bool,
    /// The addresses given to `listen_on`, reported while the swarm has named no interface.
    wildcard: Vec<Multiaddr>,
    listen: BTreeSet<Multiaddr>,
    external: BTreeSet<Multiaddr>,
    reachability: Reachability,
    provided: Provided,
    announcing: HashMap<QueryId, Cid>,
    observations: Observations,
    autonat_servers: HashSet<PeerId>,
    next_bootstrap: Instant,
}

impl P2p {
    pub(crate) fn new(
        config: &P2pConfig,
        allow_local: bool,
        wildcard: Vec<Multiaddr>,
        now: Instant,
    ) -> Self {
        let mut provided = Provided::new(config.reannounce_every);
        let reachability = if config.assume_public {
            Reachability::Public
        } else {
            Reachability::Unknown
        };
        provided.set_reachability(reachability, now);
        Self {
            port: config.port,
            allow_local,
            assume_public: config.assume_public,
            dht: config.dht.is_some(),
            wildcard,
            listen: BTreeSet::new(),
            external: if config.assume_public {
                BTreeSet::from([assumed_address(config.port)])
            } else {
                BTreeSet::new()
            },
            reachability,
            provided,
            announcing: HashMap::new(),
            observations: Observations::new(config.min_observations),
            autonat_servers: HashSet::new(),
            next_bootstrap: now + KAD_BOOTSTRAP_EVERY,
        }
    }

    pub(crate) fn provide(&mut self, cid: Cid, now: Instant) -> Result<AddOutcome, ProvideError> {
        if !self.dht {
            return Err(ProvideError::NoDht);
        }
        Ok(self.provided.add(cid, now)?)
    }

    pub(crate) fn unprovide(&mut self, swarm: &mut Swarm<Behaviour>, cid: &Cid) -> bool {
        let removed = self.provided.remove(cid);
        if removed {
            if let Some(kad) = swarm.behaviour_mut().kad.as_mut() {
                kad.stop_providing(&RecordKey::new(&provider_key(cid)));
            }
        }
        removed
    }

    pub(crate) fn status(&self, peer: PeerId) -> NodeStatus {
        let listen = if self.listen.is_empty() {
            self.wildcard.clone()
        } else {
            self.listen.iter().cloned().collect()
        };
        NodeStatus {
            peer,
            listen,
            external: self
                .external
                .iter()
                .map(without_peer)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            reachability: self.reachability,
            providing: self.provided.snapshot(),
        }
    }

    pub(crate) fn on_event(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        event: &SwarmEvent<BehaviourEvent>,
        now: Instant,
    ) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::debug!(%address, "listening");
                self.listen.insert(address.clone());
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                self.listen.remove(address);
            }
            SwarmEvent::ListenerError { error, .. } => {
                tracing::warn!(%error, "listener error");
            }
            SwarmEvent::ListenerClosed {
                addresses, reason, ..
            } => {
                tracing::warn!(?addresses, ?reason, "listener closed");
                for address in addresses {
                    self.listen.remove(address);
                }
            }
            SwarmEvent::ExternalAddrConfirmed { address } => {
                tracing::info!(%address, "external address confirmed");
                if self.external.insert(address.clone()) {
                    self.provided.address_added(now);
                }
                self.sync_reachability(swarm, now);
            }
            SwarmEvent::ExternalAddrExpired { address } => {
                self.external.remove(address);
            }
            SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => self.on_identify(swarm, *peer_id, info),
            SwarmEvent::Behaviour(BehaviourEvent::Kad(event)) => self.on_kad(event, now),
            SwarmEvent::Behaviour(BehaviourEvent::Autonat(event)) => {
                tracing::debug!(?event, "autonat");
                self.sync_reachability(swarm, now);
            }
            _ => {}
        }
    }

    fn on_identify(&mut self, swarm: &mut Swarm<Behaviour>, peer: PeerId, info: &identify::Info) {
        let behaviour = swarm.behaviour_mut();
        if let Some(autonat) = behaviour.autonat.as_mut() {
            if self.autonat_servers.len() < MAX_AUTONAT_SERVERS
                && info
                    .protocols
                    .iter()
                    .any(|protocol| protocol.as_ref() == AUTONAT_V1_PROTOCOL)
                && !self.autonat_servers.contains(&peer)
            {
                if let Some(addr) = info
                    .listen_addrs
                    .iter()
                    .find(|addr| routable(addr, self.allow_local))
                {
                    self.autonat_servers.insert(peer);
                    autonat.add_server(peer, Some(addr.clone()));
                }
            }
            if let Some(candidate) =
                candidate_from_observed(&info.observed_addr, self.port, self.allow_local)
            {
                if self.observations.report(peer, candidate.clone()) {
                    tracing::debug!(%candidate, "asking AutoNAT about an address");
                    autonat.probe_address(candidate);
                }
            }
        }
        if let Some(kad) = behaviour.kad.as_mut() {
            if info.protocols.contains(&kad::PROTOCOL_NAME) {
                for addr in &info.listen_addrs {
                    if routable(addr, self.allow_local) {
                        kad.add_address(&peer, addr.clone());
                    }
                }
            }
        }
    }

    fn on_kad(&mut self, event: &kad::Event, now: Instant) {
        let kad::Event::OutboundQueryProgressed {
            id,
            result: QueryResult::StartProviding(result),
            stats,
            ..
        } = event
        else {
            return;
        };
        let Some(cid) = self.announcing.remove(id) else {
            return;
        };
        match result {
            Ok(_) if stats.num_successes() > 0 => {
                tracing::info!(%cid, peers = stats.num_successes(), "announced");
                self.provided.announced(&cid, now, unix_ms());
            }
            other => {
                tracing::info!(%cid, result = ?other, "announcement reached no peer");
                self.provided.failed(&cid, now);
            }
        }
    }

    /// Takes AutoNAT's verdict as the node's reachability. A private verdict drops the addresses it
    /// had confirmed, so nothing is announced that no one can reach.
    fn sync_reachability(&mut self, swarm: &mut Swarm<Behaviour>, now: Instant) {
        let nat = swarm
            .behaviour()
            .autonat
            .as_ref()
            .map(|autonat| autonat.nat_status());
        let verdict = verdict(self.assume_public, nat.as_ref(), !self.external.is_empty());
        if verdict == self.reachability {
            return;
        }
        tracing::info!(
            from = self.reachability.as_str(),
            to = verdict.as_str(),
            "reachability"
        );
        self.reachability = verdict;
        if verdict == Reachability::Private {
            for address in std::mem::take(&mut self.external) {
                swarm.remove_external_address(&address);
            }
        }
        self.provided.set_reachability(verdict, now);
    }

    /// Once a second: starts the announcements that are due and refreshes the routing table.
    pub(crate) fn tick(&mut self, swarm: &mut Swarm<Behaviour>, now: Instant) {
        self.sync_reachability(swarm, now);
        if !self.dht {
            return;
        }
        let room = MAX_ANNOUNCING.saturating_sub(self.provided.announcing());
        for cid in self.provided.take_due(now, room) {
            let started = swarm
                .behaviour_mut()
                .kad
                .as_mut()
                .map(|kad| kad.start_providing(RecordKey::new(&provider_key(&cid))));
            match started {
                Some(Ok(query)) => {
                    self.announcing.insert(query, cid);
                }
                other => {
                    tracing::warn!(%cid, result = ?other.map(|r| r.map(|_| ())), "announcement not started");
                    self.provided.failed(&cid, now);
                }
            }
        }
        if now >= self.next_bootstrap {
            self.next_bootstrap = now + KAD_BOOTSTRAP_EVERY;
            if let Some(kad) = swarm.behaviour_mut().kad.as_mut() {
                let _ = kad.bootstrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bootstrap_address_splits_into_peer_and_address() {
        let (peer, addr) = split_p2p(
            "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
        )
        .unwrap();
        assert_eq!(addr.to_string(), "/dnsaddr/bootstrap.libp2p.io");
        assert_eq!(
            peer.to_string(),
            "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
        );
        assert!(split_p2p("/ip4/1.2.3.4/tcp/4001").is_none());
        assert!(split_p2p("garbage").is_none());
    }

    #[test]
    fn only_plain_public_tcp_addresses_reach_the_routing_table() {
        let addr = |text: &str| -> Multiaddr { text.parse().unwrap() };
        assert!(routable(&addr("/ip4/93.184.216.34/tcp/4001"), false));
        assert!(routable(
            &addr("/ip6/2606:2800:220:1:248:1893:25c8:1946/tcp/4001"),
            false
        ));
        for text in [
            "/ip4/10.0.0.1/tcp/4001",
            "/ip4/127.0.0.1/tcp/4001",
            "/ip4/93.184.216.34/udp/4001/quic-v1",
            "/ip4/93.184.216.34/tcp/4001/tls/ws",
            "/dns4/example.com/tcp/4001",
        ] {
            assert!(!routable(&addr(text), false), "{text}");
        }
        assert!(routable(&addr("/ip4/127.0.0.1/tcp/4001"), true));
    }

    #[test]
    fn reachability_follows_autonat_and_needs_a_confirmed_address_to_be_public() {
        use autonat::v1::NatStatus;
        let public = NatStatus::Public("/ip4/93.184.216.34/tcp/4801".parse().unwrap());
        assert_eq!(verdict(false, Some(&public), true), Reachability::Public);
        assert_eq!(verdict(false, Some(&public), false), Reachability::Unknown);
        assert_eq!(
            verdict(false, Some(&NatStatus::Private), true),
            Reachability::Private
        );
        assert_eq!(
            verdict(false, Some(&NatStatus::Private), false),
            Reachability::Private
        );
        assert_eq!(
            verdict(false, Some(&NatStatus::Unknown), true),
            Reachability::Unknown
        );
        assert_eq!(verdict(false, None, true), Reachability::Unknown);
        assert_eq!(verdict(true, None, false), Reachability::Public);
        assert_eq!(
            verdict(true, Some(&NatStatus::Private), false),
            Reachability::Public
        );
    }

    #[test]
    fn a_confirmed_address_is_reported_without_the_peer_id() {
        let peer = "/p2p/12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP";
        let with: Multiaddr = format!("/ip4/93.184.216.34/tcp/4801{peer}")
            .parse()
            .unwrap();
        let bare: Multiaddr = "/ip4/93.184.216.34/tcp/4801".parse().unwrap();
        assert_eq!(without_peer(&with), bare);
        assert_eq!(without_peer(&bare), bare);
    }

    #[test]
    fn the_features_name_what_the_node_can_do() {
        assert_eq!(P2pConfig::new(4801).features(), "serve,listen");
        assert_eq!(
            P2pConfig::new(4801).with_dht(vec![]).features(),
            "serve,listen,provide"
        );
    }

    #[test]
    fn this_node_never_tests_other_peers() {
        assert_eq!(P2pConfig::new(4801).autonat.throttle_clients_global_max, 0);
    }
}
