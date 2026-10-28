//! A peer whose first connection to a serve node has ended asks again on a new one: the node answers
//! that connection like the first, over a circuit relay and directly, after no wait and after a long one.
#![cfg(not(target_os = "wasi"))]

mod support;

use cid::Cid;
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::swarm::{NetworkBehaviour, SwarmEvent};
use libp2p::{Multiaddr, PeerId, Swarm};
use libp2p_stream::{Control, IncomingStreams};
use orivon_ipfs_core::{cid_from_data, CODEC_RAW};
use orivon_ipfs_serve::{
    BitswapMessage, KeptBlocks, NodeStatus, P2pConfig, ServeNode, Version, WantEntry, WantType,
    Wantlist,
};
use prost::Message;
use std::time::Duration;
use support::{
    eventually, free_port, loopback, relay_client_swarm, start_relay, tcp_swarm, with_peer, Seen,
};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};

const LIMIT: Duration = Duration::from_secs(40);
/// Longer than the node's ping timeout and the fetching side's own idle timeout.
const LONG_IDLE: Duration = Duration::from_secs(25);

enum Command {
    Dial(Multiaddr),
    Disconnect(PeerId),
}

/// Runs `swarm` in a task that dials and disconnects on command and reports its connections.
fn drive<B>(
    mut swarm: Swarm<B>,
) -> (
    mpsc::UnboundedSender<Command>,
    mpsc::UnboundedReceiver<Seen>,
)
where
    B: NetworkBehaviour + Send + 'static,
    B::ToSwarm: std::fmt::Debug,
{
    let (commands, mut inbox) = mpsc::unbounded_channel();
    let (seen, events) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                event = swarm.select_next_some() => {
                    let report = match event {
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => Some(Seen::Connected(peer_id)),
                        SwarmEvent::ConnectionClosed { peer_id, .. } => Some(Seen::Closed(peer_id)),
                        _ => None,
                    };
                    if let Some(report) = report {
                        let _ = seen.send(report);
                    }
                }
                command = inbox.recv() => match command {
                    Some(Command::Dial(addr)) => {
                        let _ = swarm.dial(addr);
                    }
                    Some(Command::Disconnect(peer)) => {
                        let _ = swarm.disconnect_peer_id(peer);
                    }
                    None => return,
                }
            }
        }
    });
    (commands, events)
}

async fn next_for(events: &mut mpsc::UnboundedReceiver<Seen>, what: &str, wanted: impl Fn(&Seen) -> bool) {
    let end = Instant::now() + LIMIT;
    loop {
        match tokio::time::timeout_at(end, events.recv()).await {
            Ok(Some(event)) if wanted(&event) => return,
            Ok(Some(_)) => {}
            _ => panic!("timed out waiting for: {what}"),
        }
    }
}

fn want(cid: &Cid) -> BitswapMessage {
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

/// Asks `node` for `cid` the way a Bitswap client does: a want on a stream of ours, the block on a
/// stream the node opens. `None` when no block arrives within `LIMIT`.
async fn fetch(
    control: &mut Control,
    replies: &mut IncomingStreams,
    node: PeerId,
    cid: &Cid,
) -> Option<Vec<u8>> {
    let ask = async {
        let mut stream = control.open_stream(node, Version::V1_2.protocol()).await.ok()?;
        stream
            .write_all(&want(cid).encode_length_delimited_to_vec())
            .await
            .ok()?;
        stream.flush().await.ok()?;
        let (_, mut reply) = replies.next().await?;
        let mut bytes = Vec::new();
        reply.read_to_end(&mut bytes).await.ok()?;
        let reply = BitswapMessage::decode_length_delimited(bytes.as_slice()).ok()?;
        reply.payload.first().map(|payload| payload.data.to_vec())
    };
    timeout(LIMIT, ask).await.ok().flatten()
}

fn files(count: usize) -> Vec<(Cid, Vec<u8>)> {
    (0..count)
        .map(|index| {
            let data = format!("file number {index} of the share, ").repeat(1500).into_bytes();
            (cid_from_data(CODEC_RAW, &data), data)
        })
        .collect()
}

fn kept_with(files: &[(Cid, Vec<u8>)]) -> KeptBlocks {
    let kept = KeptBlocks::default();
    kept.insert_all(files.to_vec()).unwrap();
    kept
}

async fn status_where(node: &ServeNode, what: &str, ok: impl Fn(&NodeStatus) -> bool) -> NodeStatus {
    let ok = &ok;
    eventually(what, LIMIT, || async move {
        let status = node.status().await.unwrap();
        ok(&status).then_some(status)
    })
    .await
}

struct Visitor {
    commands: mpsc::UnboundedSender<Command>,
    events: mpsc::UnboundedReceiver<Seen>,
    control: Control,
    replies: IncomingStreams,
    node: PeerId,
    address: Multiaddr,
}

impl Visitor {
    /// Connects to the node, asks for `file`, and leaves. Every call after the first is a
    /// connection the node has not seen from this peer before.
    async fn visit(&mut self, file: &(Cid, Vec<u8>), note: &str) {
        let node = self.node;
        self.commands
            .send(Command::Dial(self.address.clone()))
            .unwrap();
        next_for(&mut self.events, "a connection to the node", |e| {
            matches!(e, Seen::Connected(p) if *p == node)
        })
        .await;
        let got = fetch(&mut self.control, &mut self.replies, node, &file.0).await;
        assert_eq!(
            got.as_deref(),
            Some(&file.1[..]),
            "{note}: the node did not send the block"
        );
        self.commands.send(Command::Disconnect(node)).unwrap();
        next_for(&mut self.events, "the connection closing", |e| {
            matches!(e, Seen::Closed(p) if *p == node)
        })
        .await;
    }

    /// One visit per file, the first at once, the second right after it and the third after
    /// [`LONG_IDLE`].
    async fn come_back(&mut self, files: &[(Cid, Vec<u8>)]) {
        for (index, wait) in [None, Some(Duration::ZERO), Some(LONG_IDLE)]
            .into_iter()
            .enumerate()
        {
            if let Some(wait) = wait {
                sleep(wait).await;
            }
            self.visit(&files[index], &format!("visit {index} after {wait:?}"))
                .await;
        }
    }
}

#[tokio::test]
async fn a_peer_that_comes_back_through_the_relay_is_served_again() {
    let files = files(3);
    let relay = start_relay().await;
    let node = ServeNode::start_with_p2p(
        kept_with(&files),
        true,
        Some(P2pConfig::new(free_port()).with_relays(vec![relay.bootstrap()])),
    )
    .await
    .expect("the node starts");
    let status = status_where(&node, "a reservation", |s| !s.relays.is_empty()).await;

    let swarm = relay_client_swarm();
    let mut control = swarm.behaviour().stream.new_control();
    let replies = control.accept(Version::V1_2.protocol()).unwrap();
    let (commands, events) = drive(swarm);
    let mut visitor = Visitor {
        commands,
        events,
        control,
        replies,
        node: node.peer_id(),
        address: status.relays[0].clone(),
    };
    visitor.come_back(&files).await;
}

#[tokio::test]
async fn a_peer_that_comes_back_directly_is_served_again() {
    let files = files(3);
    let port = free_port();
    let node = ServeNode::start_with_p2p(kept_with(&files), true, Some(P2pConfig::new(port)))
        .await
        .expect("the node starts");

    let swarm = tcp_swarm(|_| libp2p_stream::Behaviour::new());
    let mut control = swarm.behaviour().new_control();
    let replies = control.accept(Version::V1_2.protocol()).unwrap();
    let (commands, events) = drive(swarm);
    let mut visitor = Visitor {
        commands,
        events,
        control,
        replies,
        node: node.peer_id(),
        address: with_peer(&loopback(port), node.peer_id()).parse().unwrap(),
    };
    visitor.come_back(&files).await;
}
