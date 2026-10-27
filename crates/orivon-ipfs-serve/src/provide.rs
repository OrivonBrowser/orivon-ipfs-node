//! What the node was asked to announce on the DHT, and when each announcement is due.
//!
//! Nothing here touches the network: [`Provided`] says which CIDs to announce now, and the node
//! reports back how each announcement went.

use cid::Cid;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

/// CIDs the node announces at once; one more is refused.
pub const MAX_PROVIDED: usize = 1024;
/// How often a provider record is renewed, as Kubo does.
pub const REANNOUNCE_EVERY: Duration = Duration::from_secs(12 * 60 * 60);
/// How long an announcement that did not reach anyone waits before it is tried again.
pub const RETRY_AFTER: Duration = Duration::from_secs(60);
/// Announcements running at once.
pub const MAX_ANNOUNCING: usize = 4;

/// Whether other peers can open connections to this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reachability {
    Unknown,
    Public,
    Private,
}

impl Reachability {
    pub fn as_str(self) -> &'static str {
        match self {
            Reachability::Unknown => "unknown",
            Reachability::Public => "public",
            Reachability::Private => "private",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddOutcome {
    Added,
    AlreadyProvided,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("already announcing {MAX_PROVIDED} CIDs")]
pub struct TooMany;

/// One CID in the state: its text as given, when it last reached the DHT, when it is due next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvidingEntry {
    pub cid: String,
    /// Unix time in milliseconds of the last announcement that reached the DHT.
    pub announced: Option<u64>,
}

struct Entry {
    cid: Cid,
    announced: Option<u64>,
    due: Instant,
    announcing: bool,
}

/// The key a provider record is stored under: the CID's multihash, as Kubo does, so a CIDv0 and a
/// CIDv1 of the same block are one record.
pub fn provider_key(cid: &Cid) -> Vec<u8> {
    cid.hash().to_bytes()
}

pub struct Provided {
    entries: HashMap<Vec<u8>, Entry>,
    reachability: Reachability,
    reannounce_every: Duration,
    max: usize,
}

impl Provided {
    pub fn new(reannounce_every: Duration) -> Self {
        Self::with_max(reannounce_every, MAX_PROVIDED)
    }

    pub fn with_max(reannounce_every: Duration, max: usize) -> Self {
        Self {
            entries: HashMap::new(),
            reachability: Reachability::Unknown,
            reannounce_every,
            max,
        }
    }

    pub fn add(&mut self, cid: Cid, now: Instant) -> Result<AddOutcome, TooMany> {
        let key = provider_key(&cid);
        if self.entries.contains_key(&key) {
            return Ok(AddOutcome::AlreadyProvided);
        }
        if self.entries.len() >= self.max {
            return Err(TooMany);
        }
        self.entries.insert(
            key,
            Entry {
                cid,
                announced: None,
                due: now,
                announcing: false,
            },
        );
        Ok(AddOutcome::Added)
    }

    pub fn remove(&mut self, cid: &Cid) -> bool {
        self.entries.remove(&provider_key(cid)).is_some()
    }

    /// Records the node's reachability. Becoming public makes every CID due at once, since the
    /// records it published earlier (or none) say nothing about the address that now works.
    pub fn set_reachability(&mut self, reachability: Reachability, now: Instant) {
        let turned_public =
            reachability == Reachability::Public && self.reachability != Reachability::Public;
        self.reachability = reachability;
        if turned_public {
            self.make_all_due(now);
        }
    }

    /// A new confirmed address while public: the records name the old ones, so announce again.
    pub fn address_added(&mut self, now: Instant) {
        if self.reachability == Reachability::Public {
            self.make_all_due(now);
        }
    }

    fn make_all_due(&mut self, now: Instant) {
        for entry in self.entries.values_mut() {
            entry.due = now;
        }
    }

    /// The CIDs to announce now, at most `room` of them, none unless the node is public. Each is
    /// marked as running until [`Provided::announced`] or [`Provided::failed`] says how it went.
    pub fn take_due(&mut self, now: Instant, room: usize) -> Vec<Cid> {
        if self.reachability != Reachability::Public || room == 0 {
            return Vec::new();
        }
        let mut due: Vec<&mut Entry> = self
            .entries
            .values_mut()
            .filter(|entry| !entry.announcing && entry.due <= now)
            .collect();
        due.sort_by_key(|entry| entry.due);
        due.into_iter()
            .take(room)
            .map(|entry| {
                entry.announcing = true;
                entry.cid
            })
            .collect()
    }

    pub fn announcing(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.announcing)
            .count()
    }

    pub fn announced(&mut self, cid: &Cid, now: Instant, unix_ms: u64) {
        if let Some(entry) = self.entries.get_mut(&provider_key(cid)) {
            entry.announcing = false;
            entry.announced = Some(unix_ms);
            entry.due = now + self.reannounce_every;
        }
    }

    pub fn failed(&mut self, cid: &Cid, now: Instant) {
        if let Some(entry) = self.entries.get_mut(&provider_key(cid)) {
            entry.announcing = false;
            entry.due = now + RETRY_AFTER;
        }
    }

    /// The CIDs in a stable order.
    pub fn snapshot(&self) -> Vec<ProvidingEntry> {
        let mut all: Vec<ProvidingEntry> = self
            .entries
            .values()
            .map(|entry| ProvidingEntry {
                cid: entry.cid.to_string(),
                announced: entry.announced,
            })
            .collect();
        all.sort_by(|a, b| a.cid.cmp(&b.cid));
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orivon_ipfs_core::{cid_from_data, CODEC_DAG_PB, CODEC_RAW};

    fn cid(data: &[u8]) -> Cid {
        cid_from_data(CODEC_RAW, data)
    }

    fn state() -> (Provided, Instant) {
        (Provided::new(REANNOUNCE_EVERY), Instant::now())
    }

    #[test]
    fn nothing_is_due_until_the_node_is_public() {
        let (mut provided, now) = state();
        provided.add(cid(b"a"), now).unwrap();
        assert!(provided.take_due(now, 4).is_empty(), "unknown");
        provided.set_reachability(Reachability::Private, now);
        assert!(provided.take_due(now, 4).is_empty(), "private");
        provided.set_reachability(Reachability::Public, now);
        assert_eq!(provided.take_due(now, 4), vec![cid(b"a")]);
    }

    #[test]
    fn a_cid_is_taken_once_until_it_reports_back() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        provided.add(cid(b"a"), now).unwrap();
        assert_eq!(provided.take_due(now, 4).len(), 1);
        assert!(provided.take_due(now, 4).is_empty(), "already running");
        assert_eq!(provided.announcing(), 1);
    }

    #[test]
    fn an_announced_cid_is_due_again_after_twelve_hours() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.take_due(now, 4);
        provided.announced(&a, now, 1_700_000_000_000);
        assert_eq!(provided.snapshot()[0].announced, Some(1_700_000_000_000));
        assert!(provided
            .take_due(now + Duration::from_secs(12 * 3600 - 1), 4)
            .is_empty());
        assert_eq!(
            provided.take_due(now + Duration::from_secs(12 * 3600), 4),
            vec![a]
        );
    }

    #[test]
    fn a_failed_announcement_is_retried_after_a_minute() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.take_due(now, 4);
        provided.failed(&a, now);
        assert!(provided
            .take_due(now + RETRY_AFTER - Duration::from_secs(1), 4)
            .is_empty());
        assert_eq!(provided.take_due(now + RETRY_AFTER, 4), vec![a]);
        assert_eq!(provided.snapshot()[0].announced, None);
    }

    #[test]
    fn turning_public_again_announces_everything_at_once() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.take_due(now, 4);
        provided.announced(&a, now, 1);
        provided.set_reachability(Reachability::Private, now);
        provided.set_reachability(Reachability::Public, now + Duration::from_secs(60));
        assert_eq!(provided.take_due(now + Duration::from_secs(60), 4), vec![a]);
    }

    #[test]
    fn staying_public_does_not_announce_again() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.take_due(now, 4);
        provided.announced(&a, now, 1);
        provided.set_reachability(Reachability::Public, now + Duration::from_secs(60));
        assert!(provided
            .take_due(now + Duration::from_secs(60), 4)
            .is_empty());
    }

    #[test]
    fn a_new_address_while_public_announces_everything_again() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.take_due(now, 4);
        provided.announced(&a, now, 1);
        provided.address_added(now + Duration::from_secs(5));
        assert_eq!(provided.take_due(now + Duration::from_secs(5), 4), vec![a]);
        let (mut private, now) = state();
        private.add(cid(b"b"), now).unwrap();
        private.address_added(now);
        assert!(private.take_due(now, 4).is_empty());
    }

    #[test]
    fn at_most_the_room_given_is_taken() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        for index in 0..10u8 {
            provided.add(cid(&[index]), now).unwrap();
        }
        assert_eq!(provided.take_due(now, 3).len(), 3);
        assert_eq!(provided.take_due(now, 0).len(), 0);
        assert_eq!(provided.take_due(now, 100).len(), 7);
    }

    #[test]
    fn the_table_is_bounded_and_a_repeat_costs_nothing() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 2);
        let now = Instant::now();
        assert_eq!(provided.add(cid(b"a"), now), Ok(AddOutcome::Added));
        assert_eq!(provided.add(cid(b"b"), now), Ok(AddOutcome::Added));
        assert_eq!(
            provided.add(cid(b"a"), now),
            Ok(AddOutcome::AlreadyProvided)
        );
        assert_eq!(provided.add(cid(b"c"), now), Err(TooMany));
        assert!(provided.remove(&cid(b"a")));
        assert!(!provided.remove(&cid(b"a")));
        assert_eq!(provided.add(cid(b"c"), now), Ok(AddOutcome::Added));
        assert_eq!(MAX_PROVIDED, 1024);
    }

    #[test]
    fn a_v0_and_a_v1_cid_of_one_block_are_one_entry() {
        let (mut provided, now) = state();
        let v1 = cid_from_data(CODEC_DAG_PB, b"node");
        let v0 = Cid::new_v0(*v1.hash()).unwrap();
        assert_eq!(provider_key(&v0), provider_key(&v1));
        provided.add(v1, now).unwrap();
        assert_eq!(provided.add(v0, now), Ok(AddOutcome::AlreadyProvided));
        assert!(provided.remove(&v0));
        assert!(provided.snapshot().is_empty());
    }

    #[test]
    fn a_removed_cid_that_reports_back_is_ignored() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.take_due(now, 4);
        provided.remove(&a);
        provided.announced(&a, now, 1);
        provided.failed(&a, now);
        assert!(provided.snapshot().is_empty());
    }

    #[test]
    fn the_snapshot_lists_every_cid_with_its_announcement_time() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let (a, b) = (cid(b"a"), cid(b"b"));
        provided.add(a, now).unwrap();
        provided.add(b, now).unwrap();
        provided.take_due(now, 4);
        provided.announced(&a, now, 42);
        let snapshot = provided.snapshot();
        assert_eq!(snapshot.len(), 2);
        let by = |c: &Cid| {
            snapshot
                .iter()
                .find(|e| e.cid == c.to_string())
                .unwrap()
                .announced
        };
        assert_eq!(by(&a), Some(42));
        assert_eq!(by(&b), None);
    }
}
