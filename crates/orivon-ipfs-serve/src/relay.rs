//! Circuit relay v2 reservations: the node asks each relay it was given to hold a place for it, so
//! a peer that cannot reach the computer directly connects through the relay instead.
//!
//! The libp2p relay client does the protocol. Listening on `<relay>/p2p/<id>/p2p-circuit` dials
//! the relay, asks for a reservation, renews it before it runs out, and confirms the circuit
//! address as an external address once the relay accepts. When a reservation fails or the
//! connection to the relay drops, the listener just closes; [`Relays`] listens again after a
//! backoff.

use crate::node::{parse_dial_addr, Behaviour};
use crate::p2p::without_peer;
use libp2p::core::transport::ListenerId;
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId, Swarm};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::Instant;

/// Relays one node holds reservations on at once.
pub const MAX_RELAYS: usize = 8;
const MAX_BACKOFF: Duration = Duration::from_secs(2 * 60);
/// A reservation that lasts this long counts as healthy and clears the relay's failures.
const STABLE_AFTER: Duration = Duration::from_secs(30);

/// `<address>/p2p/<relay id>` as the relay's peer and the address the swarm listens on to hold a
/// reservation (`<address>/p2p/<relay id>/p2p-circuit`). The address has the same form as one
/// given to `POST /dial`: `/ip4|ip6|dns4|dns6/<host>/tcp/<port>/p2p/<peer id>`.
pub fn parse_relay_addr(text: &str, allow_local: bool) -> Result<(PeerId, Multiaddr), String> {
    let (addr, peer) = parse_dial_addr(text, allow_local).map_err(|err| err.to_string())?;
    Ok((peer, addr.with(Protocol::P2pCircuit)))
}

/// Every relay address, once per relay: a second address for a relay already named is dropped,
/// because the client keeps one reservation per relay connection.
pub(crate) fn parse_relays(
    texts: &[String],
    allow_local: bool,
) -> Result<Vec<(PeerId, Multiaddr)>, String> {
    let mut relays: Vec<(PeerId, Multiaddr)> = Vec::new();
    for text in texts {
        let (peer, listen) =
            parse_relay_addr(text, allow_local).map_err(|err| format!("relay {text}: {err}"))?;
        if !relays.iter().any(|(known, _)| *known == peer) {
            relays.push((peer, listen));
        }
    }
    if relays.len() > MAX_RELAYS {
        return Err(format!("at most {MAX_RELAYS} relays"));
    }
    Ok(relays)
}

fn is_circuit(addr: &Multiaddr) -> bool {
    addr.iter().any(|part| matches!(part, Protocol::P2pCircuit))
}

fn backoff(failures: u32) -> Duration {
    Duration::from_secs(1u64 << failures.min(7)).min(MAX_BACKOFF)
}

/// What the node knows about one relay.
struct Relay {
    /// `<address>/p2p/<relay id>/p2p-circuit`
    listen: Multiaddr,
    listener: Option<ListenerId>,
    /// The circuit addresses the relay accepted a reservation for.
    held: BTreeSet<Multiaddr>,
    held_since: Option<Instant>,
    failures: u32,
    retry_at: Option<Instant>,
}

impl Relay {
    fn new(listen: Multiaddr, now: Instant) -> Self {
        Self {
            listen,
            listener: None,
            held: BTreeSet::new(),
            held_since: None,
            failures: 0,
            retry_at: Some(now),
        }
    }

    /// A circuit address the client confirmed is `<listen>/p2p/<this node>`.
    fn names(&self, confirmed: &Multiaddr) -> bool {
        without_peer(confirmed) == self.listen
    }

    fn due(&self, now: Instant) -> bool {
        self.listener.is_none() && self.retry_at.is_some_and(|at| at <= now)
    }

    fn failed(&mut self, now: Instant) {
        self.failures += 1;
        self.retry_at = Some(now + backoff(self.failures));
    }

    /// The listener closed: a reservation that lasted [`STABLE_AFTER`] clears the failures first,
    /// so a relay that held for a while is asked again soon and one that refuses is asked less
    /// and less often.
    fn closed(&mut self, now: Instant) {
        if self
            .held_since
            .is_some_and(|since| now.duration_since(since) >= STABLE_AFTER)
        {
            self.failures = 0;
        }
        self.listener = None;
        self.held_since = None;
        self.held.clear();
        self.failed(now);
    }
}

/// The reservations the node keeps.
pub(crate) struct Relays {
    entries: Vec<Relay>,
}

impl Relays {
    pub(crate) fn new(targets: Vec<(PeerId, Multiaddr)>, now: Instant) -> Self {
        Self {
            entries: targets
                .into_iter()
                .map(|(_, listen)| Relay::new(listen, now))
                .collect(),
        }
    }

    /// True while at least one relay holds a reservation.
    pub(crate) fn active(&self) -> bool {
        self.entries.iter().any(|relay| !relay.held.is_empty())
    }

    /// The circuit addresses held, `<address>/p2p/<relay id>/p2p-circuit/p2p/<this node>`.
    pub(crate) fn held(&self) -> Vec<Multiaddr> {
        self.entries
            .iter()
            .flat_map(|relay| relay.held.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(crate) fn owns(&self, listener: ListenerId) -> bool {
        self.entries
            .iter()
            .any(|relay| relay.listener == Some(listener))
    }

    /// Starts listening on every relay that has no listener and whose backoff has passed.
    pub(crate) fn listen(&mut self, swarm: &mut Swarm<Behaviour>, now: Instant) {
        for relay in self.entries.iter_mut().filter(|relay| relay.due(now)) {
            match swarm.listen_on(relay.listen.clone()) {
                Ok(listener) => {
                    tracing::debug!(address = %relay.listen, "asking a relay for a reservation");
                    relay.listener = Some(listener);
                    relay.retry_at = None;
                }
                Err(err) => {
                    tracing::warn!(address = %relay.listen, %err, "cannot listen through the relay");
                    relay.failed(now);
                }
            }
        }
    }

    /// The client confirmed a circuit address: the relay accepted the reservation. False when the
    /// address is none of ours.
    pub(crate) fn confirmed(&mut self, address: &Multiaddr, now: Instant) -> bool {
        let Some(relay) = self.entries.iter_mut().find(|relay| relay.names(address)) else {
            return false;
        };
        relay.held.insert(address.clone());
        relay.held_since.get_or_insert(now);
        true
    }

    /// The client dropped a circuit address because the connection to its relay closed.
    pub(crate) fn expired(&mut self, address: &Multiaddr) -> bool {
        let Some(relay) = self.entries.iter_mut().find(|relay| relay.names(address)) else {
            return false;
        };
        relay.held.remove(address)
    }

    /// A relay's listener closed (the reservation failed, or the connection dropped). The
    /// addresses it held are no longer ours to give out. False when the listener is none of ours.
    pub(crate) fn closed(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        listener: ListenerId,
        reason: &Result<(), std::io::Error>,
        now: Instant,
    ) -> bool {
        let Some(relay) = self
            .entries
            .iter_mut()
            .find(|relay| relay.listener == Some(listener))
        else {
            return false;
        };
        for address in std::mem::take(&mut relay.held) {
            swarm.remove_external_address(&address);
        }
        relay.closed(now);
        match reason {
            Ok(()) => tracing::info!(address = %relay.listen, "relay connection lost"),
            Err(err) => {
                tracing::warn!(address = %relay.listen, %err, "relay reservation failed");
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RELAY: &str = "12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP";
    const OTHER: &str = "12D3KooWJdw4Tux8MAkbsVY6nLNtLnM25jS2EGNC7AFiJRQ6H7Cu";

    #[test]
    fn a_relay_address_becomes_the_address_to_listen_on() {
        let (peer, listen) =
            parse_relay_addr(&format!("/dns4/relay.example/tcp/4001/p2p/{RELAY}"), false).unwrap();
        assert_eq!(peer.to_string(), RELAY);
        assert_eq!(
            listen.to_string(),
            format!("/dns4/relay.example/tcp/4001/p2p/{RELAY}/p2p-circuit")
        );
    }

    #[test]
    fn only_a_plain_tcp_address_with_the_relay_id_is_a_relay() {
        for text in [
            "",
            "garbage",
            "/ip4/93.184.216.34/tcp/4001",
            &format!("/ip4/93.184.216.34/udp/4001/quic-v1/p2p/{RELAY}"),
            &format!("/ip4/93.184.216.34/tcp/4001/ws/p2p/{RELAY}"),
            &format!("/dnsaddr/relay.example/p2p/{RELAY}"),
            &format!("/ip4/93.184.216.34/tcp/4001/p2p/{RELAY}/p2p-circuit"),
        ] {
            assert!(parse_relay_addr(text, true).is_err(), "{text}");
        }
    }

    #[test]
    fn a_relay_on_a_local_address_needs_local_dials_allowed() {
        let text = format!("/ip4/127.0.0.1/tcp/4001/p2p/{RELAY}");
        assert!(parse_relay_addr(&text, false).is_err());
        assert!(parse_relay_addr(&text, true).is_ok());
    }

    #[test]
    fn a_relay_named_twice_is_one_reservation_and_the_count_is_bounded() {
        let first = format!("/ip4/93.184.216.34/tcp/4001/p2p/{RELAY}");
        let again = format!("/ip4/93.184.216.35/tcp/4002/p2p/{RELAY}");
        let other = format!("/ip4/93.184.216.36/tcp/4001/p2p/{OTHER}");
        let relays = parse_relays(&[first.clone(), again, other], false).unwrap();
        assert_eq!(relays.len(), 2);
        assert!(relays[0].1.to_string().starts_with("/ip4/93.184.216.34/"));
        let many = vec![first; 3];
        assert_eq!(parse_relays(&many, false).unwrap().len(), 1);
        let too_many: Vec<String> = (1..=MAX_RELAYS as u8 + 1)
            .map(|index| format!("/ip4/93.184.216.{index}/tcp/4001/p2p/{}", PeerId::random()))
            .collect();
        assert!(parse_relays(&too_many, false).is_err());
        assert!(parse_relays(&["nonsense".to_string()], false).is_err());
    }

    fn relay() -> (Relay, Multiaddr) {
        let (_, listen) =
            parse_relay_addr(&format!("/ip4/93.184.216.34/tcp/4001/p2p/{RELAY}"), false).unwrap();
        let confirmed = listen.clone().with(Protocol::P2p(PeerId::random()));
        (Relay::new(listen, Instant::now()), confirmed)
    }

    #[test]
    fn the_confirmed_address_is_the_listen_address_with_this_nodes_id() {
        let (relay, confirmed) = relay();
        assert!(relay.names(&confirmed));
        assert!(!relay.names(&"/ip4/93.184.216.34/tcp/4001".parse().unwrap()));
        assert!(is_circuit(&confirmed));
        assert!(!is_circuit(&"/ip4/93.184.216.34/tcp/4001".parse().unwrap()));
    }

    #[test]
    fn a_relay_is_asked_at_once_and_then_after_a_growing_pause() {
        let (mut relay, _) = relay();
        let start = Instant::now();
        assert!(relay.due(start));
        relay.listener = Some(ListenerId::next());
        assert!(!relay.due(start), "listening");
        let mut now = start;
        let mut pauses = Vec::new();
        for _ in 0..9 {
            relay.closed(now);
            pauses.push(relay.retry_at.unwrap() - now);
            now = relay.retry_at.unwrap();
            assert!(relay.due(now));
            relay.listener = Some(ListenerId::next());
        }
        assert_eq!(
            pauses,
            [2, 4, 8, 16, 32, 64, 120, 120, 120].map(Duration::from_secs)
        );
    }

    #[test]
    fn a_reservation_that_lasted_clears_the_failures() {
        let (mut relay, confirmed) = relay();
        let mut now = Instant::now();
        relay.closed(now);
        relay.closed(now);
        assert_eq!(relay.failures, 2);
        relay.listener = Some(ListenerId::next());
        relay.held.insert(confirmed);
        relay.held_since = Some(now);
        now += STABLE_AFTER;
        relay.closed(now);
        assert_eq!(relay.failures, 1, "cleared, then this close counted");
        assert!(relay.held.is_empty());
        assert_eq!(relay.retry_at, Some(now + Duration::from_secs(2)));
    }

    #[test]
    fn the_held_addresses_are_listed_while_a_relay_holds_them() {
        let (relay, confirmed) = relay();
        let mut relays = Relays {
            entries: vec![relay],
        };
        assert!(!relays.active());
        assert!(relays.held().is_empty());
        assert!(relays.confirmed(&confirmed, Instant::now()));
        assert!(relays.active());
        assert_eq!(relays.held(), vec![confirmed.clone()]);
        assert!(!relays.confirmed(&"/ip4/1.2.3.4/tcp/1/p2p-circuit".parse().unwrap(), Instant::now()));
        assert!(relays.expired(&confirmed));
        assert!(!relays.active());
    }
}
