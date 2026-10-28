//! Keeps the connections to chosen peers open while every other connection closes when idle.
//!
//! A node that talks to the DHT opens many short-lived connections and must let them close; the
//! peers a page asked it to stay connected to must not close with them. The swarm closes a
//! connection when no handler wants it kept, so this behaviour gives each connection a handler
//! that wants it kept exactly while its peer is chosen.

use libp2p::core::transport::PortUse;
use libp2p::core::upgrade::DeniedUpgrade;
use libp2p::core::Endpoint;
use libp2p::swarm::behaviour::FromSwarm;
use libp2p::swarm::handler::{ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound};
use libp2p::swarm::{
    ConnectionDenied, ConnectionHandler, ConnectionHandlerEvent, ConnectionId, NetworkBehaviour,
    NotifyHandler, SubstreamProtocol, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::task::{Context, Poll};

#[derive(Default)]
pub(crate) struct Behaviour {
    chosen: HashSet<PeerId>,
    connections: HashMap<PeerId, HashSet<ConnectionId>>,
    notify: VecDeque<ToSwarm<Infallible, bool>>,
}

impl Behaviour {
    /// True while the connections to `peer` are kept open.
    pub(crate) fn keeps(&self, peer: &PeerId) -> bool {
        self.chosen.contains(peer)
    }

    /// Keeps (or stops keeping) every connection to `peer`, now and in the future.
    pub(crate) fn set(&mut self, peer: PeerId, keep: bool) {
        let changed = if keep {
            self.chosen.insert(peer)
        } else {
            self.chosen.remove(&peer)
        };
        if !changed {
            return;
        }
        for connection in self.connections.get(&peer).into_iter().flatten() {
            self.notify.push_back(ToSwarm::NotifyHandler {
                peer_id: peer,
                handler: NotifyHandler::One(*connection),
                event: keep,
            });
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;
    type ToSwarm = Infallible;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        peer: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler {
            keep: self.chosen.contains(&peer),
        })
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        peer: PeerId,
        _: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler {
            keep: self.chosen.contains(&peer),
        })
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(established) => {
                self.connections
                    .entry(established.peer_id)
                    .or_default()
                    .insert(established.connection_id);
            }
            FromSwarm::ConnectionClosed(closed) => {
                if let Some(connections) = self.connections.get_mut(&closed.peer_id) {
                    connections.remove(&closed.connection_id);
                    if connections.is_empty() {
                        self.connections.remove(&closed.peer_id);
                    }
                }
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {}
    }

    fn poll(&mut self, _: &mut Context<'_>) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        match self.notify.pop_front() {
            Some(event) => Poll::Ready(event),
            None => Poll::Pending,
        }
    }
}

pub(crate) struct Handler {
    keep: bool,
}

impl ConnectionHandler for Handler {
    type FromBehaviour = bool;
    type ToBehaviour = Infallible;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn connection_keep_alive(&self) -> bool {
        self.keep
    }

    fn on_behaviour_event(&mut self, keep: bool) {
        self.keep = keep;
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<Self::InboundProtocol, Self::OutboundProtocol>,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol, ..
            }) => match protocol {},
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol, ..
            }) => match protocol {},
            _ => {}
        }
    }
}
