//! Reachability and announcements, against in-process peers that play the public network's
//! parts: a DHT server and an AutoNAT server.
#![cfg(not(target_os = "wasi"))]

mod support;

use orivon_ipfs_core::{cid_from_data, CODEC_RAW};
use orivon_ipfs_serve::{
    AddOutcome, KeptBlocks, NodeStatus, P2pConfig, ProvideError, Reachability, ServeNode,
};
use futures::StreamExt;
use libp2p::kad::store::{MemoryStore, RecordStore};
use libp2p::kad::{self, ProviderRecord, RecordKey};
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{autonat, identify, Multiaddr, PeerId};
use std::time::Duration;
use support::{eventually, free_port, loopback, start_relay, tcp_swarm, with_peer};
use tokio::sync::{mpsc, oneshot};

const LIMIT: Duration = Duration::from_secs(40);

#[derive(NetworkBehaviour)]
#[behaviour(prelude = "libp2p::swarm::derive_prelude")]
struct ServerBehaviour {
    identify: identify::Behaviour,
    kad: kad::Behaviour<MemoryStore>,
    autonat: Toggle<autonat::v1::Behaviour>,
}

enum Ask {
    Providers(Vec<u8>, oneshot::Sender<Vec<ProviderRecord>>),
    Announcements(oneshot::Sender<usize>),
}

/// A DHT server (and optionally an AutoNAT server) on the loopback interface.
struct Server {
    peer: PeerId,
    addr: Multiaddr,
    asks: mpsc::Sender<Ask>,
}

impl Server {
    async fn start(with_autonat: bool) -> Self {
        let mut swarm = tcp_swarm(|key| {
            let peer = key.public().to_peer_id();
            let mut config = kad::Config::new(kad::PROTOCOL_NAME);
            config.set_query_timeout(Duration::from_secs(20));
            let mut kad = kad::Behaviour::with_config(peer, MemoryStore::new(peer), config);
            kad.set_mode(Some(kad::Mode::Server));
            ServerBehaviour {
                identify: identify::Behaviour::new(identify::Config::new(
                    "test/1.0.0".to_string(),
                    key.public(),
                )),
                kad,
                autonat: with_autonat
                    .then(|| {
                        autonat::v1::Behaviour::new(
                            peer,
                            autonat::v1::Config {
                                only_global_ips: false,
                                throttle_clients_peer_max: 1000,
                                throttle_clients_global_max: 1000,
                                throttle_clients_period: Duration::from_millis(1),
                                ..autonat::v1::Config::default()
                            },
                        )
                    })
                    .into(),
            }
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
        let (asks, mut requests) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut announcements = 0usize;
            loop {
                tokio::select! {
                    event = swarm.select_next_some() => {
                        if let SwarmEvent::Behaviour(ServerBehaviourEvent::Kad(
                            kad::Event::InboundRequest { request: kad::InboundRequest::AddProvider { .. } },
                        )) = event {
                            announcements += 1;
                        }
                    }
                    Some(ask) = requests.recv() => match ask {
                        Ask::Providers(key, reply) => {
                            let found = swarm.behaviour_mut().kad.store_mut().providers(&RecordKey::new(&key));
                            let _ = reply.send(found);
                        }
                        Ask::Announcements(reply) => { let _ = reply.send(announcements); }
                    }
                }
            }
        });
        Self { peer, addr, asks }
    }

    fn bootstrap(&self) -> String {
        with_peer(&self.addr, self.peer)
    }

    async fn providers(&self, key: Vec<u8>) -> Vec<ProviderRecord> {
        let (reply, answer) = oneshot::channel();
        self.asks.send(Ask::Providers(key, reply)).await.unwrap();
        answer.await.unwrap()
    }

    async fn announcements(&self) -> usize {
        let (reply, answer) = oneshot::channel();
        self.asks.send(Ask::Announcements(reply)).await.unwrap();
        answer.await.unwrap()
    }
}

/// True when the provider record carries the loopback address of `port` (kad adds the peer id).
fn names_address(record: &ProviderRecord, port: u16) -> bool {
    let wanted = loopback(port).to_string();
    record
        .addresses
        .iter()
        .any(|addr| addr.to_string().starts_with(&wanted))
}

fn kept_block(data: &[u8]) -> (KeptBlocks, cid::Cid) {
    let kept = KeptBlocks::default();
    let cid = cid_from_data(CODEC_RAW, data);
    kept.insert_all(vec![(cid, data.to_vec())]).unwrap();
    (kept, cid)
}

async fn status_where(
    node: &ServeNode,
    what: &str,
    ok: impl Fn(&NodeStatus) -> bool,
) -> NodeStatus {
    let ok = &ok;
    eventually(what, LIMIT, || async move {
        let status = node.status().await.unwrap();
        ok(&status).then_some(status)
    })
    .await
}

#[tokio::test]
async fn a_provided_cid_reaches_the_dht_with_the_confirmed_address_and_is_renewed() {
    let server = Server::start(false).await;
    let (kept, cid) = kept_block(b"announce me");
    let port = free_port();
    let mut config = P2pConfig::new(port)
        .with_dht(vec![server.bootstrap()])
        .with_assume_public();
    config.reannounce_every = Duration::from_secs(3);
    let node = ServeNode::start_with_p2p(kept, true, Some(config))
        .await
        .unwrap();

    assert_eq!(node.provide(cid).await, Ok(AddOutcome::Added));
    assert_eq!(node.provide(cid).await, Ok(AddOutcome::AlreadyProvided));

    let first = status_where(&node, "the first announcement", |s| {
        s.providing
            .first()
            .is_some_and(|entry| entry.announced.is_some())
    })
    .await;
    assert_eq!(first.reachability, Reachability::Public);
    assert_eq!(first.external, vec![loopback(port)]);
    assert_eq!(first.providing[0].cid, cid.to_string());
    let key = cid.hash().to_bytes();
    let records = server.providers(key.clone()).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].provider, node.peer_id());
    assert!(
        names_address(&records[0], port),
        "{:?}",
        records[0].addresses
    );

    let first_time = first.providing[0].announced.unwrap();
    let renewed = status_where(&node, "a renewal", |s| {
        s.providing[0].announced.is_some_and(|at| at > first_time)
    })
    .await;
    assert!(renewed.providing[0].announced.unwrap() >= first_time + 2_000);
    assert!(server.announcements().await >= 2);
}

#[tokio::test]
async fn nothing_is_announced_while_the_node_is_not_known_to_be_reachable() {
    let server = Server::start(false).await;
    let (kept, cid) = kept_block(b"not yet");
    let mut config = P2pConfig::new(free_port()).with_dht(vec![server.bootstrap()]);
    config.autonat.boot_delay = Duration::from_millis(200);
    let node = ServeNode::start_with_p2p(kept, true, Some(config))
        .await
        .unwrap();
    node.provide(cid).await.unwrap();

    tokio::time::sleep(Duration::from_secs(6)).await;
    let status = node.status().await.unwrap();
    assert_eq!(status.reachability, Reachability::Unknown);
    assert!(status.external.is_empty());
    assert_eq!(status.providing.len(), 1);
    assert_eq!(status.providing[0].announced, None);
    assert_eq!(server.announcements().await, 0);
    assert!(server.providers(cid.hash().to_bytes()).await.is_empty());
}

#[tokio::test]
async fn a_node_an_autonat_server_can_dial_turns_public_and_then_announces() {
    let server = Server::start(true).await;
    let (kept, cid) = kept_block(b"announce when confirmed");
    let port = free_port();
    let mut config = P2pConfig::new(port).with_dht(vec![server.bootstrap()]);
    config.min_observations = 1;
    config.autonat.boot_delay = Duration::from_millis(300);
    config.autonat.retry_interval = Duration::from_secs(1);
    let node = ServeNode::start_with_p2p(kept, true, Some(config))
        .await
        .unwrap();
    node.provide(cid).await.unwrap();

    let status = status_where(
        &node,
        "the announcement after AutoNAT confirmed the address",
        |s| {
            s.providing
                .first()
                .is_some_and(|entry| entry.announced.is_some())
        },
    )
    .await;
    assert_eq!(status.reachability, Reachability::Public);
    assert!(
        status.external.contains(&loopback(port)),
        "{:?}",
        status.external
    );
    let records = server.providers(cid.hash().to_bytes()).await;
    assert_eq!(records.len(), 1);
    assert!(
        names_address(&records[0], port),
        "{:?}",
        records[0].addresses
    );
}

#[tokio::test]
async fn unprovide_stops_the_renewals() {
    let server = Server::start(false).await;
    let (kept, cid) = kept_block(b"announce then stop");
    let mut config = P2pConfig::new(free_port())
        .with_dht(vec![server.bootstrap()])
        .with_assume_public();
    config.reannounce_every = Duration::from_secs(2);
    let node = ServeNode::start_with_p2p(kept, true, Some(config))
        .await
        .unwrap();
    node.provide(cid).await.unwrap();
    status_where(&node, "an announcement", |s| {
        s.providing
            .first()
            .is_some_and(|entry| entry.announced.is_some())
    })
    .await;

    assert_eq!(node.unprovide(cid).await, Ok(true));
    assert_eq!(node.unprovide(cid).await, Ok(false));
    assert!(node.status().await.unwrap().providing.is_empty());
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = server.announcements().await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(server.announcements().await, before);
}

#[tokio::test]
async fn too_many_cids_are_refused() {
    let server = Server::start(false).await;
    let kept = KeptBlocks::default();
    let node = ServeNode::start_with_p2p(
        kept,
        true,
        Some(P2pConfig::new(free_port()).with_dht(vec![server.bootstrap()])),
    )
    .await
    .unwrap();
    for index in 0..orivon_ipfs_serve::MAX_PROVIDED as u32 {
        let cid = cid_from_data(CODEC_RAW, &index.to_be_bytes());
        assert_eq!(node.provide(cid).await, Ok(AddOutcome::Added));
    }
    let one_more = cid_from_data(CODEC_RAW, b"one more");
    assert_eq!(node.provide(one_more).await, Err(ProvideError::TooMany));
    let first = cid_from_data(CODEC_RAW, &0u32.to_be_bytes());
    assert_eq!(node.provide(first).await, Ok(AddOutcome::AlreadyProvided));
}

#[tokio::test]
async fn a_node_behind_a_relay_announces_the_circuit_address_whatever_autonat_says() {
    let server = Server::start(false).await;
    let relay = start_relay().await;
    let (kept, cid) = kept_block(b"announce through a relay");
    // No AutoNAT server answers, so the node never turns public: the reservation is all it has.
    let config = P2pConfig::new(free_port())
        .with_dht(vec![server.bootstrap()])
        .with_relays(vec![relay.bootstrap()]);
    let node = ServeNode::start_with_p2p(kept, true, Some(config))
        .await
        .unwrap();
    assert_eq!(node.features(), Some("serve,listen,provide,relay"));
    node.provide(cid).await.unwrap();

    let status = status_where(&node, "an announcement through the relay", |s| {
        s.providing
            .first()
            .is_some_and(|entry| entry.announced.is_some())
    })
    .await;
    assert_ne!(status.reachability, Reachability::Public);
    assert!(status.external.is_empty(), "{:?}", status.external);
    assert_eq!(status.relays.len(), 1);
    let circuit = status.relays[0].to_string();
    let records = server.providers(cid.hash().to_bytes()).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].provider, node.peer_id());
    assert!(
        records[0]
            .addresses
            .iter()
            .any(|addr| addr.to_string().starts_with(&circuit)),
        "{circuit} not in {:?}",
        records[0].addresses
    );
}

#[tokio::test]
async fn a_node_that_cannot_reach_its_relay_announces_nothing() {
    let server = Server::start(false).await;
    let (kept, cid) = kept_block(b"no relay, no announcement");
    let nobody = format!("/ip4/127.0.0.1/tcp/{}/p2p/{}", free_port(), PeerId::random());
    let config = P2pConfig::new(free_port())
        .with_dht(vec![server.bootstrap()])
        .with_relays(vec![nobody]);
    let node = ServeNode::start_with_p2p(kept, true, Some(config))
        .await
        .unwrap();
    node.provide(cid).await.unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
    let status = node.status().await.unwrap();
    assert!(status.relays.is_empty());
    assert_eq!(status.providing[0].announced, None);
    assert_eq!(server.announcements().await, 0);
}
