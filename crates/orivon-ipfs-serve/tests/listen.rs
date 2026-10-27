//! A serve node that listens: what an inbound peer gets, and what bounds it.
#![cfg(not(target_os = "wasi"))]

mod support;

use orivon_ipfs_core::{cid_from_data, CODEC_RAW};
use orivon_ipfs_serve::{
    BitswapMessage, KeptBlocks, P2pConfig, ServeNode, Version, WantEntry, WantType, Wantlist,
    MAX_CONNECTIONS_PER_PEER, MAX_INBOUND_CONNECTIONS,
};
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use prost::Message;
use std::time::Duration;
use support::{eventually, free_port, loopback, spawn_peer, tcp_swarm, wait_connected, Seen};
use tokio::time::timeout;

const LIMIT: Duration = Duration::from_secs(20);

async fn listening_node(kept: &KeptBlocks) -> (ServeNode, u16) {
    let port = free_port();
    let node = ServeNode::start_with_p2p(kept.clone(), true, Some(P2pConfig::new(port)))
        .await
        .expect("the node starts");
    (node, port)
}

fn want(cid: &cid::Cid, want_type: WantType) -> BitswapMessage {
    BitswapMessage {
        wantlist: Some(Wantlist {
            entries: vec![WantEntry {
                block: cid.to_bytes(),
                priority: 1,
                cancel: false,
                want_type: want_type as i32,
                send_dont_have: true,
                tokens: Vec::new(),
            }],
            full: false,
        }),
        ..BitswapMessage::default()
    }
}

#[tokio::test]
async fn an_inbound_peer_is_answered_from_the_kept_blocks_like_a_dialed_one() {
    let kept = KeptBlocks::default();
    let data = b"served to whoever connects".to_vec();
    let cid = cid_from_data(CODEC_RAW, &data);
    kept.insert_all(vec![(cid, data.clone())]).unwrap();
    let missing = cid_from_data(CODEC_RAW, b"nobody gave the node this");
    let (node, port) = listening_node(&kept).await;

    let mut swarm = tcp_swarm(|_| libp2p_stream::Behaviour::new());
    let mut control = swarm.behaviour().new_control();
    let mut replies = control.accept(Version::V1_2.protocol()).unwrap();
    swarm
        .dial(
            DialOpts::peer_id(node.peer_id())
                .addresses(vec![loopback(port)])
                .build(),
        )
        .unwrap();
    let (_, mut events) = spawn_peer(swarm);
    wait_connected(&mut events).await;

    for (wanted, want_type, expect_block, expect_have) in [
        (&cid, WantType::Block, true, false),
        (&cid, WantType::Have, false, true),
        (&missing, WantType::Have, false, false),
    ] {
        let mut stream = control
            .open_stream(node.peer_id(), Version::V1_2.protocol())
            .await
            .unwrap();
        stream
            .write_all(&want(wanted, want_type).encode_length_delimited_to_vec())
            .await
            .unwrap();
        stream.flush().await.unwrap();
        let (_, mut reply_stream) = timeout(LIMIT, replies.next()).await.unwrap().unwrap();
        let mut bytes = Vec::new();
        reply_stream.read_to_end(&mut bytes).await.unwrap();
        let reply = BitswapMessage::decode_length_delimited(bytes.as_slice()).unwrap();
        if expect_block {
            assert_eq!(&reply.payload[0].data[..], &data[..]);
        } else {
            assert!(reply.payload.is_empty());
            assert_eq!(reply.block_presences.len(), 1);
            assert_eq!(reply.block_presences[0].is_have(), expect_have);
        }
    }
}

#[tokio::test]
async fn one_peer_holds_at_most_four_connections() {
    let (node, port) = listening_node(&KeptBlocks::default()).await;
    let mut swarm = tcp_swarm(|_| libp2p_stream::Behaviour::new());
    for _ in 0..(MAX_CONNECTIONS_PER_PEER + 2) {
        swarm
            .dial(
                DialOpts::peer_id(node.peer_id())
                    .addresses(vec![loopback(port)])
                    .condition(PeerCondition::Always)
                    .build(),
            )
            .unwrap();
    }
    let (_, mut events) = spawn_peer(swarm);
    let mut open = 0i32;
    let settle = tokio::time::Instant::now() + Duration::from_secs(4);
    while tokio::time::Instant::now() < settle {
        match timeout(Duration::from_millis(200), events.recv()).await {
            Ok(Some(Seen::Connected(_))) => open += 1,
            Ok(Some(Seen::Closed(_))) => open -= 1,
            _ => {}
        }
    }
    assert_eq!(open, MAX_CONNECTIONS_PER_PEER as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sixty_four_inbound_connections_are_held_and_no_more() {
    let (node, port) = listening_node(&KeptBlocks::default()).await;
    let clients = MAX_INBOUND_CONNECTIONS as usize + 8;
    let mut receivers = Vec::new();
    // One at a time: the bound on connections still being set up is a different one, and a
    // handshake that would be refused for it must not be mistaken for the bound under test.
    for _ in 0..clients {
        let mut swarm = tcp_swarm(|_| libp2p_stream::Behaviour::new());
        swarm
            .dial(
                DialOpts::peer_id(node.peer_id())
                    .addresses(vec![loopback(port)])
                    .build(),
            )
            .unwrap();
        let (_, mut events) = spawn_peer(swarm);
        wait_connected(&mut events).await;
        receivers.push(events);
    }
    // Every client saw its connection come up; the node closes the ones over the bound.
    let mut open = clients as i32;
    let settle = tokio::time::Instant::now() + Duration::from_secs(4);
    while tokio::time::Instant::now() < settle {
        for events in receivers.iter_mut() {
            while let Ok(event) = events.try_recv() {
                if let Seen::Closed(_) = event {
                    open -= 1;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(open, MAX_INBOUND_CONNECTIONS as i32);
    drop(node);
}

#[tokio::test]
async fn the_node_reports_where_it_listens() {
    let (node, port) = listening_node(&KeptBlocks::default()).await;
    let node_ref = &node;
    let status = eventually("a listen address", LIMIT, || async move {
        let status = node_ref.status().await.unwrap();
        status
            .listen
            .iter()
            .any(|addr| addr == &loopback(port))
            .then_some(status)
    })
    .await;
    assert_eq!(status.peer, node.peer_id());
    assert!(status.external.is_empty());
    assert_eq!(status.reachability.as_str(), "unknown");
    assert!(status.providing.is_empty());
    assert_eq!(node.features(), Some("serve,listen"));
    assert!(!node.can_provide());
    assert_eq!(
        node.provide(cid_from_data(CODEC_RAW, b"x")).await,
        Err(orivon_ipfs_serve::ProvideError::NoDht)
    );
}

#[tokio::test]
async fn a_node_without_a_listener_has_no_status_and_does_not_listen() {
    let node = ServeNode::start_with(KeptBlocks::default(), true)
        .await
        .unwrap();
    assert!(node.features().is_none());
    assert!(node.status().await.is_err());
    assert!(node.provide(cid_from_data(CODEC_RAW, b"x")).await.is_err());
}

#[tokio::test]
async fn a_port_in_use_stops_the_node_from_starting() {
    let taken = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let started =
        ServeNode::start_with_p2p(KeptBlocks::default(), true, Some(P2pConfig::new(port))).await;
    let err = started.err().expect("the port is taken");
    assert!(err.to_string().contains(&format!("port {port}")), "{err}");
}
