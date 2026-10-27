//! Which addresses the serve node will dial, and which it will ask AutoNAT about.

use libp2p::core::transport::{DialOpts, ListenerId, TransportError, TransportEvent};
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId, Transport};
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::pin::Pin;
use std::task::{Context, Poll};

pub(crate) fn ipv4_is_public(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    !(ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        || a == 0
        || (a == 100 && b & 0xc0 == 64))
}

pub(crate) fn ipv6_is_public(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return ipv4_is_public(v4);
    }
    let first = ip.segments()[0];
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || first & 0xfe00 == 0xfc00
        || first & 0xffc0 == 0xfe80)
}

pub(crate) fn dns_name_is_local(name: &str) -> bool {
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    name == "localhost" || name.ends_with(".localhost")
}

/// The transport under the DNS resolver, in a node that is reachable by other peers: it dials
/// public IP addresses only. The DHT and the peers it talks to name addresses of their own choosing,
/// and a name that resolves to a private address arrives here as that address.
pub(crate) struct PublicOnly<T> {
    inner: T,
}

impl<T> PublicOnly<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self { inner }
    }
}

/// True for a first component that is a public IP address.
pub(crate) fn is_public_ip_addr(addr: &Multiaddr) -> bool {
    match addr.iter().next() {
        Some(Protocol::Ip4(ip)) => ipv4_is_public(ip),
        Some(Protocol::Ip6(ip)) => ipv6_is_public(ip),
        _ => false,
    }
}

impl<T> Transport for PublicOnly<T>
where
    T: Transport + Unpin,
{
    type Output = T::Output;
    type Error = T::Error;
    type ListenerUpgrade = T::ListenerUpgrade;
    type Dial = T::Dial;

    fn listen_on(
        &mut self,
        id: ListenerId,
        addr: Multiaddr,
    ) -> Result<(), TransportError<Self::Error>> {
        self.inner.listen_on(id, addr)
    }

    fn remove_listener(&mut self, id: ListenerId) -> bool {
        self.inner.remove_listener(id)
    }

    fn dial(
        &mut self,
        addr: Multiaddr,
        opts: DialOpts,
    ) -> Result<Self::Dial, TransportError<Self::Error>> {
        if !is_public_ip_addr(&addr) {
            return Err(TransportError::MultiaddrNotSupported(addr));
        }
        self.inner.dial(addr, opts)
    }

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

/// The address a remote saw us at, with our listen port in place of the port it saw. A TCP
/// connection we dialed leaves from a port of the system's choosing, so the port is not worth
/// asking about; the IP address is the part only the remote can tell us. `None` for a relayed
/// address, anything but TCP over IP, and a private IP unless `allow_local`.
pub(crate) fn candidate_from_observed(
    observed: &Multiaddr,
    port: u16,
    allow_local: bool,
) -> Option<Multiaddr> {
    let mut parts = observed.iter();
    let ip = match parts.next()? {
        Protocol::Ip4(ip) if allow_local || ipv4_is_public(ip) => Protocol::Ip4(ip),
        Protocol::Ip6(ip) if allow_local || ipv6_is_public(ip) => Protocol::Ip6(ip),
        _ => return None,
    };
    if !matches!(parts.next()?, Protocol::Tcp(_)) {
        return None;
    }
    if parts.any(|part| !matches!(part, Protocol::P2p(_))) {
        return None;
    }
    Some(Multiaddr::empty().with(ip).with(Protocol::Tcp(port)))
}

/// Candidate addresses and the peers that reported each. Remote peers can say anything about where
/// they see us, so a candidate is passed on to AutoNAT only once enough different peers agree.
pub(crate) struct Observations {
    reporters: HashMap<Multiaddr, HashSet<PeerId>>,
    passed_on: HashSet<Multiaddr>,
    needed: usize,
}

/// Candidates kept while they wait for agreement.
const MAX_WAITING_CANDIDATES: usize = 32;
/// Candidates ever passed on to AutoNAT, which cannot forget one.
const MAX_PASSED_ON: usize = 8;

impl Observations {
    pub(crate) fn new(needed: usize) -> Self {
        Self {
            reporters: HashMap::new(),
            passed_on: HashSet::new(),
            needed: needed.max(1),
        }
    }

    /// True when this report is the one that makes `candidate` worth asking AutoNAT about.
    pub(crate) fn report(&mut self, peer: PeerId, candidate: Multiaddr) -> bool {
        if self.passed_on.contains(&candidate) {
            return false;
        }
        if !self.reporters.contains_key(&candidate)
            && self.reporters.len() >= MAX_WAITING_CANDIDATES
        {
            return false;
        }
        let seen = self.reporters.entry(candidate.clone()).or_default();
        seen.insert(peer);
        if seen.len() < self.needed || self.passed_on.len() >= MAX_PASSED_ON {
            return false;
        }
        self.reporters.remove(&candidate);
        self.passed_on.insert(candidate);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(text: &str) -> Multiaddr {
        text.parse().unwrap()
    }

    #[test]
    fn the_candidate_is_the_observed_ip_with_our_port() {
        assert_eq!(
            candidate_from_observed(&addr("/ip4/93.184.216.34/tcp/51234"), 4801, false),
            Some(addr("/ip4/93.184.216.34/tcp/4801"))
        );
        assert_eq!(
            candidate_from_observed(
                &addr("/ip6/2606:2800:220:1:248:1893:25c8:1946/tcp/51234/p2p/12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP"),
                4801,
                false
            ),
            Some(addr("/ip6/2606:2800:220:1:248:1893:25c8:1946/tcp/4801"))
        );
    }

    #[test]
    fn private_relayed_and_non_tcp_observations_give_no_candidate() {
        for text in [
            "/ip4/127.0.0.1/tcp/1",
            "/ip4/192.168.1.5/tcp/1",
            "/ip4/10.0.0.1/tcp/1",
            "/ip6/::1/tcp/1",
            "/ip6/fe80::1/tcp/1",
            "/ip4/93.184.216.34/udp/1/quic-v1",
            "/dns4/example.com/tcp/1",
            "/ip4/93.184.216.34/tcp/1/p2p/12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP/p2p-circuit",
        ] {
            assert_eq!(candidate_from_observed(&addr(text), 4801, false), None, "{text}");
        }
        assert_eq!(
            candidate_from_observed(&addr("/ip4/127.0.0.1/tcp/1"), 4801, true),
            Some(addr("/ip4/127.0.0.1/tcp/4801"))
        );
    }

    #[test]
    fn a_candidate_is_passed_on_once_enough_different_peers_report_it() {
        let mut seen = Observations::new(2);
        let a = PeerId::random();
        let b = PeerId::random();
        let candidate = addr("/ip4/93.184.216.34/tcp/4801");
        assert!(!seen.report(a, candidate.clone()));
        assert!(
            !seen.report(a, candidate.clone()),
            "the same peer twice is one report"
        );
        assert!(seen.report(b, candidate.clone()));
        assert!(!seen.report(PeerId::random(), candidate), "passed on once");
    }

    #[test]
    fn at_most_eight_candidates_are_ever_passed_on() {
        let mut seen = Observations::new(1);
        let mut passed = 0;
        for index in 0..20u8 {
            let candidate = addr(&format!("/ip4/93.184.216.{index}/tcp/4801"));
            if seen.report(PeerId::random(), candidate) {
                passed += 1;
            }
        }
        assert_eq!(passed, 8);
    }

    #[test]
    fn waiting_candidates_are_bounded() {
        let mut seen = Observations::new(2);
        for index in 0..100u8 {
            seen.report(
                PeerId::random(),
                addr(&format!("/ip4/93.184.217.{index}/tcp/4801")),
            );
        }
        assert!(seen.reporters.len() <= MAX_WAITING_CANDIDATES);
    }

    #[test]
    fn only_public_ip_addresses_are_dialed() {
        assert!(is_public_ip_addr(&addr("/ip4/93.184.216.34/tcp/4001")));
        assert!(is_public_ip_addr(&addr(
            "/ip6/2606:2800:220:1:248:1893:25c8:1946/tcp/4001"
        )));
        for text in [
            "/ip4/127.0.0.1/tcp/4001",
            "/ip4/10.1.2.3/tcp/4001",
            "/ip4/192.168.0.1/tcp/4001",
            "/ip4/169.254.169.254/tcp/80",
            "/ip6/::1/tcp/4001",
            "/ip6/fd00::1/tcp/4001",
            "/dns4/example.com/tcp/4001",
        ] {
            assert!(!is_public_ip_addr(&addr(text)), "{text}");
        }
    }
}
