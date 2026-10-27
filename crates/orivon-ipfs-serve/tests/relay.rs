//! A serve node that holds a circuit relay reservation, against an in-process relay server: the
//! reservation, what the node reports, how it recovers, and a peer that fetches through it.
#![cfg(not(target_os = "wasi"))]

mod support;

use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::{Multiaddr, PeerId};
use orivon_ipfs_core::{cid_from_data, CODEC_RAW};
use orivon_ipfs_serve::{
    BitswapMessage, KeptBlocks, NodeStatus, P2pConfig, Reachability, ServeNode, Version, WantEntry,
    WantType, Wantlist,
};
use prost::Message;
use std::time::Duration;
use support::{
    eventually, free_port, relay_client_swarm, spawn_peer, start_relay, wait_connected_to,
};
use tokio::time::timeout;

const LIMIT: Duration = Duration::from_secs(40);

async fn node_behind(relay: &support::Relay, kept: KeptBlocks) -> ServeNode {
    ServeNode::start_with_p2p(
        kept,
        true,
        Some(P2pConfig::new(free_port()).with_relays(vec![relay.bootstrap()])),
    )
    .await
    .expect("the node starts")
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

fn want(cid: &cid::Cid) -> BitswapMessage {
    BitswapMessage {
        wantlist: Some(Wantlist {
            entries: vec![WantEntry {
                block: cid.to_bytes(),
                priority: 1,
                cancel: false,
                want_type: WantType::Block as i32,
                send_dont_have: true,
                tokens: Vec::new(),
            }],
            full: false,
        }),
        ..BitswapMessage::default()
    }
}

#[tokio::test]
async fn a_node_holds_a_reservation_and_reports_the_circuit_address() {
    let relay = start_relay().await;
    let node = node_behind(&relay, KeptBlocks::default()).await;
    assert_eq!(node.features(), Some("serve,listen,relay"));

    let status = status_where(&node, "a reservation", |s| !s.relays.is_empty()).await;
    let expected: Multiaddr = format!("{}/p2p-circuit/p2p/{}", relay.bootstrap(), node.peer_id())
        .parse()
        .unwrap();
    assert_eq!(status.relays, vec![expected]);
    let counted = &relay;
    eventually("the relay counting the reservation", LIMIT, || async move {
        (counted.reservations() == 1).then_some(())
    })
    .await;
    // AutoNAT's answer and the addresses it confirmed are not touched by the reservation.
    assert_eq!(status.reachability, Reachability::Unknown);
    assert!(status.external.is_empty(), "{:?}", status.external);
    assert!(
        status
            .listen
            .iter()
            .all(|addr| !addr.to_string().contains("p2p-circuit")),
        "{:?}",
        status.listen
    );
}

#[tokio::test]
async fn the_reservation_is_asked_for_again_after_the_connection_drops() {
    let relay = start_relay().await;
    let node = node_behind(&relay, KeptBlocks::default()).await;
    status_where(&node, "a reservation", |s| !s.relays.is_empty()).await;

    relay.cut_connections();
    let node = &node;
    let relay = &relay;
    eventually("a second reservation", LIMIT, || async move {
        let held = !node.status().await.unwrap().relays.is_empty();
        (relay.reservations() >= 2 && held).then_some(())
    })
    .await;
}

#[tokio::test]
async fn a_relay_that_cannot_be_reached_leaves_the_node_working() {
    let nobody = format!("/ip4/127.0.0.1/tcp/{}/p2p/{}", free_port(), PeerId::random());
    let node = ServeNode::start_with_p2p(
        KeptBlocks::default(),
        true,
        Some(P2pConfig::new(free_port()).with_relays(vec![nobody])),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let status = node.status().await.unwrap();
    assert!(status.relays.is_empty());
    assert_eq!(status.peer, node.peer_id());
}

#[tokio::test]
async fn a_relay_address_that_is_not_public_stops_the_node_from_starting() {
    let local = format!("/ip4/127.0.0.1/tcp/4001/p2p/{}", PeerId::random());
    let started = ServeNode::start_with_p2p(
        KeptBlocks::default(),
        false,
        Some(P2pConfig::new(free_port()).with_relays(vec![local])),
    )
    .await;
    let err = started.err().expect("a loopback relay is refused");
    assert!(err.to_string().contains("relay"), "{err}");
}

#[tokio::test]
async fn a_peer_fetches_a_held_block_through_the_circuit_address() {
    let data = b"served through a circuit relay".to_vec();
    let cid = cid_from_data(CODEC_RAW, &data);
    let kept = KeptBlocks::default();
    kept.insert_all(vec![(cid, data.clone())]).unwrap();
    let relay = start_relay().await;
    let node = node_behind(&relay, kept).await;
    let status = status_where(&node, "a reservation", |s| !s.relays.is_empty()).await;

    let mut swarm = relay_client_swarm();
    let mut control = swarm.behaviour().stream.new_control();
    let mut replies = control.accept(Version::V1_2.protocol()).unwrap();
    swarm.dial(status.relays[0].clone()).unwrap();
    let (_, mut events) = spawn_peer(swarm);
    wait_connected_to(&mut events, node.peer_id()).await;

    let mut stream = control
        .open_stream(node.peer_id(), Version::V1_2.protocol())
        .await
        .unwrap();
    stream
        .write_all(&want(&cid).encode_length_delimited_to_vec())
        .await
        .unwrap();
    stream.flush().await.unwrap();
    let (_, mut reply_stream) = timeout(LIMIT, replies.next()).await.unwrap().unwrap();
    let mut bytes = Vec::new();
    reply_stream.read_to_end(&mut bytes).await.unwrap();
    let reply = BitswapMessage::decode_length_delimited(bytes.as_slice()).unwrap();
    assert_eq!(&reply.payload[0].data[..], &data[..]);
}
