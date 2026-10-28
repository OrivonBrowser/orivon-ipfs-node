//! Keeps the connections this node opened within bounds without refusing a new one.
//!
//! An announcement walks the DHT through dozens of peers, each over a connection of its own, and
//! the swarm closes an idle connection only after its idle timeout. A hard
//! limit on outbound connections then makes the later dials of a walk fail, and a walk that
//! cannot dial the peers nearest the key hands its record to peers far from it, where no lookup
//! looks. Past [`HIGH_WATER`] the oldest connections close instead, down to [`LOW_WATER`].

use libp2p::swarm::ConnectionId;
use libp2p::PeerId;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

/// More outbound connections than this and the oldest ones are closed.
pub(crate) const HIGH_WATER: usize = 48;
/// The count the closing stops at.
pub(crate) const LOW_WATER: usize = 32;
/// A connection younger than this stays: its request may not have been answered yet.
pub(crate) const MIN_AGE: Duration = Duration::from_secs(3);

/// The connections this node dialed, and when each was established.
#[derive(Default)]
pub(crate) struct Trimmer {
    opened: HashMap<ConnectionId, (PeerId, Instant)>,
}

impl Trimmer {
    pub(crate) fn opened(&mut self, connection: ConnectionId, peer: PeerId, now: Instant) {
        self.opened.insert(connection, (peer, now));
    }

    pub(crate) fn closed(&mut self, connection: &ConnectionId) {
        self.opened.remove(connection);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.opened.len()
    }

    /// The connections to close now, oldest first: none until the count passes [`HIGH_WATER`],
    /// then enough to bring it to [`LOW_WATER`]. A connection younger than [`MIN_AGE`], or to a
    /// peer `protected` names, is never one of them. The ones returned are forgotten here.
    pub(crate) fn excess(
        &mut self,
        now: Instant,
        protected: impl Fn(&PeerId) -> bool,
    ) -> Vec<ConnectionId> {
        if self.opened.len() <= HIGH_WATER {
            return Vec::new();
        }
        let mut candidates: Vec<(Instant, ConnectionId)> = self
            .opened
            .iter()
            .filter(|(_, (peer, since))| now.duration_since(*since) >= MIN_AGE && !protected(peer))
            .map(|(connection, (_, since))| (*since, *connection))
            .collect();
        candidates.sort_by_key(|(since, _)| *since);
        let surplus = self.opened.len() - LOW_WATER;
        let closing: Vec<ConnectionId> = candidates
            .into_iter()
            .take(surplus)
            .map(|(_, connection)| connection)
            .collect();
        for connection in &closing {
            self.opened.remove(connection);
        }
        closing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(trimmer: &mut Trimmer, count: usize, start: Instant) -> Vec<(ConnectionId, PeerId)> {
        (0..count)
            .map(|n| {
                let connection = ConnectionId::new_unchecked(n);
                let peer = PeerId::random();
                trimmer.opened(connection, peer, start + Duration::from_millis(n as u64));
                (connection, peer)
            })
            .collect()
    }

    #[test]
    fn nothing_closes_up_to_the_high_water_mark() {
        let mut trimmer = Trimmer::default();
        let start = Instant::now();
        fill(&mut trimmer, HIGH_WATER, start);
        assert!(trimmer.excess(start + Duration::from_secs(60), |_| false).is_empty());
        assert_eq!(trimmer.len(), HIGH_WATER);
    }

    #[test]
    fn past_the_high_water_mark_the_oldest_close_down_to_the_low_water_mark() {
        let mut trimmer = Trimmer::default();
        let start = Instant::now();
        let all = fill(&mut trimmer, HIGH_WATER + 1, start);
        let closing = trimmer.excess(start + Duration::from_secs(60), |_| false);
        assert_eq!(closing.len(), HIGH_WATER + 1 - LOW_WATER);
        assert_eq!(trimmer.len(), LOW_WATER);
        let oldest: Vec<ConnectionId> = all.iter().take(closing.len()).map(|(c, _)| *c).collect();
        assert_eq!(closing, oldest);
    }

    #[test]
    fn a_young_connection_and_a_protected_peer_stay() {
        let mut trimmer = Trimmer::default();
        let start = Instant::now();
        let all = fill(&mut trimmer, HIGH_WATER + 10, start);
        let protected = all[0].1;
        // Everything but the last ten is old enough; the first one's peer is protected.
        let now = start + MIN_AGE + Duration::from_millis((HIGH_WATER - 1) as u64);
        let closing = trimmer.excess(now, |peer| *peer == protected);
        assert!(!closing.contains(&all[0].0));
        for (connection, _) in all.iter().skip(HIGH_WATER) {
            assert!(!closing.contains(connection));
        }
        assert!(!closing.is_empty());
    }

    #[test]
    fn a_closed_connection_is_forgotten() {
        let mut trimmer = Trimmer::default();
        let start = Instant::now();
        let all = fill(&mut trimmer, 3, start);
        trimmer.closed(&all[1].0);
        assert_eq!(trimmer.len(), 2);
    }
}
