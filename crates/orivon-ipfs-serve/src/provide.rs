//! What the node was asked to announce on the DHT, and when each announcement is due.
//!
//! Nothing here touches the network: [`Provided`] says which CIDs to announce now, and the node
//! reports back how each announcement went.

use cid::Cid;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::Instant;

/// CIDs the node announces at once. That many roots, or CIDs asked for by name, always fit: a CID
/// that is only listed under a root gives way to them, and one more named CID is refused.
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
    /// Unix time in milliseconds of the last announcement that reached the DHT, whether the
    /// records it published carry a direct address or a circuit address.
    pub announced: Option<u64>,
}

struct Entry {
    cid: Cid,
    announced: Option<u64>,
    due: Instant,
    announcing: bool,
    /// Asked for by its own name, which is what a root of [`Provided::add_tree`] is.
    named: bool,
    /// How many trees list it. It stays announced while it is named or listed.
    listed: u32,
    /// When it was given, so the first CID given is announced first among those due together.
    order: u64,
}

/// What one root asked to have announced with it, and how much of that the table holds.
struct Tree {
    /// The members the last call listed, breadth first, without the root or repeats.
    wanted: Vec<Cid>,
    /// The keys of the members of `wanted` the table lists under this root now.
    listed: HashSet<Vec<u8>>,
}

impl Tree {
    /// Some member it asked for is not in the table.
    fn is_cut(&self) -> bool {
        self.listed.len() < self.wanted.len()
    }
}

/// What [`Provided::add_tree`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeAdded {
    pub outcome: AddOutcome,
    /// Members now listed under the root.
    pub listed: usize,
    /// CIDs that are no longer announced at all, so the DHT should stop providing them: what the
    /// root listed before and nothing lists now, and what was pushed out to make room for the root.
    pub dropped: Vec<Cid>,
}

/// What [`Provided::remove`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removed {
    /// The CID had been asked for by its own name.
    pub was_named: bool,
    /// The CIDs that are no longer announced at all, which the DHT should stop providing.
    pub dropped: Vec<Cid>,
}

/// The key a provider record is stored under: the CID's multihash, as Kubo does, so a CIDv0 and a
/// CIDv1 of the same block are one record.
pub fn provider_key(cid: &Cid) -> Vec<u8> {
    cid.hash().to_bytes()
}

pub struct Provided {
    entries: HashMap<Vec<u8>, Entry>,
    /// For each root given to [`Provided::add_tree`], what it asked for and what the table lists.
    trees: HashMap<Vec<u8>, Tree>,
    next_order: u64,
    reachability: Reachability,
    /// A relay holds a reservation for the node, so peers can reach it through the relay whatever
    /// AutoNAT says about a direct connection.
    relayed: bool,
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
            trees: HashMap::new(),
            next_order: 0,
            reachability: Reachability::Unknown,
            relayed: false,
            reannounce_every,
            max,
        }
    }

    /// `name` without the list of what it pushed out, for the table's own tests: the node names a
    /// CID through [`Provided::add_tree`].
    #[cfg(test)]
    pub fn add(&mut self, cid: Cid, now: Instant) -> Result<AddOutcome, TooMany> {
        self.name(cid, now, &mut Vec::new())
    }

    /// Asks for `cid` by its own name. A CID named always fits while fewer than `max` CIDs are
    /// named: when the table is full, one that is only listed under a root is pushed out, and
    /// goes into `pushed_out`. Refused only when every CID held is named.
    fn name(
        &mut self,
        cid: Cid,
        now: Instant,
        pushed_out: &mut Vec<Cid>,
    ) -> Result<AddOutcome, TooMany> {
        let key = provider_key(&cid);
        if let Some(entry) = self.entries.get_mut(&key) {
            return Ok(if std::mem::replace(&mut entry.named, true) {
                AddOutcome::AlreadyProvided
            } else {
                AddOutcome::Added
            });
        }
        if self.entries.len() >= self.max {
            pushed_out.push(self.push_out().ok_or(TooMany)?);
        }
        self.insert(cid, now, true);
        Ok(AddOutcome::Added)
    }

    /// Names `root` like `name` and lists `members` under it: they are announced for as
    /// long as the root is, however many roots list the same one. A member takes only room that is
    /// free: one that does not fit is left out, never the root, which pushes a listed CID of
    /// another root out when the table is full. Giving the same root again replaces its list.
    pub fn add_tree(
        &mut self,
        root: Cid,
        members: &[Cid],
        now: Instant,
    ) -> Result<TreeAdded, TooMany> {
        let mut dropped = Vec::new();
        let outcome = self.name(root, now, &mut dropped)?;
        let root_key = provider_key(&root);
        let previous = self.trees.remove(&root_key);
        let mut seen = HashSet::from([root_key.clone()]);
        let mut wanted = Vec::new();
        let mut listed = HashSet::new();
        for member in members {
            let key = provider_key(member);
            if !seen.insert(key.clone()) {
                continue;
            }
            wanted.push(*member);
            if let Some(entry) = self.entries.get_mut(&key) {
                entry.listed += 1;
            } else if self.entries.len() < self.max {
                self.insert(*member, now, false).listed = 1;
            } else {
                continue;
            }
            listed.insert(key);
        }
        for key in previous.iter().flat_map(|tree| tree.listed.iter()) {
            dropped.extend(self.unlist(key));
        }
        self.trees.insert(root_key.clone(), Tree { wanted, listed });
        self.top_up(now);
        // A CID pushed out and listed again by the same call is still announced.
        dropped.retain(|cid| !self.entries.contains_key(&provider_key(cid)));
        let listed = self.trees.get(&root_key).map_or(0, |tree| tree.listed.len());
        Ok(TreeAdded {
            outcome,
            listed,
            dropped,
        })
    }

    /// Removes one CID that is only listed, never a named one. It comes from the tree that lists
    /// the most members (the newer root when trees are level), and is that tree's newest entry.
    /// None when every CID held is named.
    fn push_out(&mut self) -> Option<Cid> {
        let root = self
            .trees
            .iter()
            .filter(|(_, tree)| tree.listed.iter().any(|key| self.is_only_listed(key)))
            .max_by_key(|(root, tree)| (tree.listed.len(), self.order_of(root)))
            .map(|(root, _)| root.clone())?;
        let key = self.trees[&root]
            .listed
            .iter()
            .filter(|key| self.is_only_listed(key))
            .max_by_key(|key| self.order_of(key))?
            .clone();
        let entry = self.entries.remove(&key)?;
        for tree in self.trees.values_mut() {
            tree.listed.remove(&key);
        }
        Some(entry.cid)
    }

    /// The CID under `key` is held, and no call asked for it by its own name.
    fn is_only_listed(&self, key: &[u8]) -> bool {
        self.entries.get(key).is_some_and(|entry| !entry.named)
    }

    fn order_of(&self, key: &[u8]) -> u64 {
        self.entries.get(key).map_or(0, |entry| entry.order)
    }

    /// While the table has room, lists what the trees that were cut asked for: the tree that holds
    /// the fewest members first (the older root when trees are level), one member at a time, in the
    /// order the root listed them.
    fn top_up(&mut self, now: Instant) {
        while self.entries.len() < self.max {
            let Some(root) = self
                .trees
                .iter()
                .filter(|(_, tree)| tree.is_cut())
                .min_by_key(|(root, tree)| (tree.listed.len(), self.order_of(root)))
                .map(|(root, _)| root.clone())
            else {
                return;
            };
            self.list_next(&root, now);
        }
    }

    /// Lists the first member `root` asked for that the table does not list under it yet.
    fn list_next(&mut self, root: &[u8], now: Instant) {
        let next = {
            let Some(tree) = self.trees.get_mut(root) else {
                return;
            };
            let next = tree
                .wanted
                .iter()
                .map(|member| (*member, provider_key(member)))
                .find(|(_, key)| !tree.listed.contains(key));
            if let Some((_, key)) = &next {
                tree.listed.insert(key.clone());
            }
            next
        };
        let Some((member, key)) = next else {
            return;
        };
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.listed += 1;
        } else {
            self.insert(member, now, false).listed = 1;
        }
    }

    fn insert(&mut self, cid: Cid, now: Instant, named: bool) -> &mut Entry {
        let order = self.next_order;
        self.next_order += 1;
        self.entries.entry(provider_key(&cid)).or_insert(Entry {
            cid,
            announced: None,
            due: now,
            announcing: false,
            named,
            listed: 0,
            order,
        })
    }

    /// One tree stops listing the CID under `key`. Returns it when nothing names or lists it any
    /// more, and it is gone.
    fn unlist(&mut self, key: &[u8]) -> Option<Cid> {
        let entry = self.entries.get_mut(key)?;
        entry.listed = entry.listed.saturating_sub(1);
        if entry.listed > 0 || entry.named {
            return None;
        }
        let cid = entry.cid;
        self.entries.remove(key);
        Some(cid)
    }

    /// Stops announcing `cid` by name, and what it listed unless another root lists it too. The
    /// room this frees goes to the trees that were cut.
    pub fn remove(&mut self, cid: &Cid, now: Instant) -> Removed {
        let key = provider_key(cid);
        let was_named = self
            .entries
            .get_mut(&key)
            .is_some_and(|entry| std::mem::replace(&mut entry.named, false));
        if !was_named {
            return Removed {
                was_named,
                dropped: Vec::new(),
            };
        }
        let listed = self.trees.remove(&key).map(|tree| tree.listed).unwrap_or_default();
        let mut dropped: Vec<Cid> = listed
            .iter()
            .filter_map(|member| self.unlist(member))
            .collect();
        if self.entries.get(&key).is_some_and(|entry| entry.listed == 0) {
            self.entries.remove(&key);
            dropped.insert(0, *cid);
        }
        self.top_up(now);
        Removed {
            was_named,
            dropped,
        }
    }

    /// True when a provider record would point at an address that works: the node is public, or a
    /// relay holds a reservation for it.
    fn announceable(&self) -> bool {
        self.reachability == Reachability::Public || self.relayed
    }

    /// Records the node's reachability. Becoming announceable makes every CID due at once, since
    /// the records it published earlier (or none) say nothing about the address that now works.
    pub fn set_reachability(&mut self, reachability: Reachability, now: Instant) {
        let was = self.announceable();
        self.reachability = reachability;
        if !was && self.announceable() {
            self.make_all_due(now);
        }
    }

    /// Records whether a relay holds a reservation for the node. It makes the node announceable
    /// like a public one, since the circuit address is one a peer can dial.
    pub fn set_relayed(&mut self, relayed: bool, now: Instant) {
        let was = self.announceable();
        self.relayed = relayed;
        if !was && self.announceable() {
            self.make_all_due(now);
        }
    }

    /// A new confirmed address while announceable: the records name the old ones, so announce again.
    pub fn address_added(&mut self, now: Instant) {
        if self.announceable() {
            self.make_all_due(now);
        }
    }

    fn make_all_due(&mut self, now: Instant) {
        for entry in self.entries.values_mut() {
            entry.due = now;
        }
    }

    /// The CIDs to announce now, at most `room` of them, none unless the node is public or a relay
    /// holds a reservation for it. Each is marked as running until [`Provided::announced`] or
    /// [`Provided::failed`] says how it went.
    pub fn take_due(&mut self, now: Instant, room: usize) -> Vec<Cid> {
        if !self.announceable() || room == 0 {
            return Vec::new();
        }
        let mut due: Vec<&mut Entry> = self
            .entries
            .values_mut()
            .filter(|entry| !entry.announcing && entry.due <= now)
            .collect();
        due.sort_by_key(|entry| (entry.due, entry.order));
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
    fn a_relay_reservation_makes_a_private_node_announce() {
        let (mut provided, now) = state();
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.set_reachability(Reachability::Private, now);
        assert!(provided.take_due(now, 4).is_empty(), "private, no relay");
        provided.set_relayed(true, now);
        assert_eq!(provided.take_due(now, 4), vec![a]);
        provided.announced(&a, now, 7);
        assert_eq!(provided.snapshot()[0].announced, Some(7));
    }

    #[test]
    fn losing_the_relay_stops_announcing_unless_the_node_is_public() {
        let (mut provided, now) = state();
        provided.add(cid(b"a"), now).unwrap();
        provided.set_relayed(true, now);
        provided.set_relayed(false, now);
        assert!(provided.take_due(now, 4).is_empty());
        provided.set_reachability(Reachability::Public, now);
        provided.set_relayed(true, now);
        provided.set_relayed(false, now);
        assert_eq!(provided.take_due(now, 4).len(), 1, "still public");
    }

    #[test]
    fn a_relay_reservation_while_public_announces_again_only_with_its_address() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.take_due(now, 4);
        provided.announced(&a, now, 1);
        provided.set_relayed(true, now + Duration::from_secs(5));
        assert!(
            provided.take_due(now + Duration::from_secs(5), 4).is_empty(),
            "the reservation alone changes nothing the records say"
        );
        provided.address_added(now + Duration::from_secs(5));
        assert_eq!(provided.take_due(now + Duration::from_secs(5), 4), vec![a]);
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
        assert!(provided.remove(&cid(b"a"), now).was_named);
        assert!(!provided.remove(&cid(b"a"), now).was_named);
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
        assert!(provided.remove(&v0, now).was_named);
        assert!(provided.snapshot().is_empty());
    }

    #[test]
    fn a_removed_cid_that_reports_back_is_ignored() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let a = cid(b"a");
        provided.add(a, now).unwrap();
        provided.take_due(now, 4);
        provided.remove(&a, now);
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

    fn ids(names: &[&[u8]]) -> Vec<Cid> {
        names.iter().map(|name| cid(name)).collect()
    }

    #[test]
    fn a_tree_announces_its_root_before_its_members() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let members = ids(&[b"one", b"two", b"three", b"four", b"five"]);
        let root = cid(b"root");
        let added = provided.add_tree(root, &members, now).unwrap();
        assert_eq!(added.listed, 5);
        assert_eq!(provided.snapshot().len(), 6);
        assert_eq!(provided.take_due(now, 1), vec![root]);
        assert_eq!(provided.take_due(now, 2), members[..2].to_vec());
    }

    #[test]
    fn removing_the_root_removes_what_only_it_listed() {
        let (mut provided, now) = state();
        let members = ids(&[b"one", b"two"]);
        let root = cid(b"root");
        provided.add_tree(root, &members, now).unwrap();
        let removed = provided.remove(&root, now);
        assert!(removed.was_named);
        assert_eq!(removed.dropped.len(), 3);
        assert!(removed.dropped.contains(&root));
        assert!(provided.snapshot().is_empty());
        assert!(!provided.remove(&root, now).was_named);
    }

    #[test]
    fn a_member_two_roots_list_stays_until_both_are_gone() {
        let (mut provided, now) = state();
        let shared = cid(b"shared file");
        let (first, second) = (cid(b"first"), cid(b"second"));
        provided.add_tree(first, &[shared], now).unwrap();
        provided.add_tree(second, &[shared], now).unwrap();
        let removed = provided.remove(&first, now);
        assert_eq!(removed.dropped, vec![first]);
        assert_eq!(provided.snapshot().len(), 2);
        let removed = provided.remove(&second, now);
        assert_eq!(removed.dropped.len(), 2);
        assert!(provided.snapshot().is_empty());
    }

    #[test]
    fn a_member_asked_for_by_name_survives_its_root() {
        let (mut provided, now) = state();
        let member = cid(b"member");
        let root = cid(b"root");
        provided.add_tree(root, &[member], now).unwrap();
        assert_eq!(provided.add(member, now), Ok(AddOutcome::Added));
        let removed = provided.remove(&root, now);
        assert_eq!(removed.dropped, vec![root]);
        assert_eq!(provided.snapshot().len(), 1);
        assert!(provided.remove(&member, now).was_named);
        assert!(provided.snapshot().is_empty());
    }

    #[test]
    fn a_member_is_not_a_cid_that_can_be_removed_by_itself() {
        let (mut provided, now) = state();
        let member = cid(b"member");
        provided.add_tree(cid(b"root"), &[member], now).unwrap();
        assert!(!provided.remove(&member, now).was_named);
        assert_eq!(provided.snapshot().len(), 2);
    }

    #[test]
    fn giving_a_root_again_keeps_what_was_announced_and_drops_what_left() {
        let (mut provided, now) = state();
        provided.set_reachability(Reachability::Public, now);
        let (kept, gone, new) = (cid(b"kept"), cid(b"gone"), cid(b"new"));
        let root = cid(b"root");
        provided.add_tree(root, &[kept, gone], now).unwrap();
        provided.take_due(now, 10);
        provided.announced(&kept, now, 5);
        let again = provided.add_tree(root, &[kept, new], now).unwrap();
        assert_eq!(again.outcome, AddOutcome::AlreadyProvided);
        assert_eq!(again.listed, 2);
        assert_eq!(again.dropped, vec![gone]);
        let by = |c: &Cid| {
            provided
                .snapshot()
                .into_iter()
                .find(|e| e.cid == c.to_string())
                .map(|e| e.announced)
        };
        assert_eq!(by(&kept), Some(Some(5)));
        assert_eq!(by(&new), Some(None));
        assert_eq!(by(&gone), None);
    }

    fn holds(provided: &Provided, cid: &Cid) -> bool {
        provided
            .snapshot()
            .iter()
            .any(|entry| entry.cid == cid.to_string())
    }

    #[test]
    fn a_tree_fills_the_table_and_a_new_root_still_fits() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 4);
        let now = Instant::now();
        let members = ids(&[b"1", b"2", b"3", b"4", b"5"]);
        let added = provided.add_tree(cid(b"root"), &members, now).unwrap();
        assert_eq!(added.listed, 3);
        assert_eq!(provided.snapshot().len(), 4);
        let other = provided.add_tree(cid(b"other"), &members, now).unwrap();
        assert_eq!(other.outcome, AddOutcome::Added);
        assert_eq!(other.dropped, vec![members[2]]);
        assert_eq!(other.listed, 2, "it lists what is held and takes no room for the rest");
        assert_eq!(provided.snapshot().len(), 4);
    }

    #[test]
    fn a_new_root_pushes_out_the_newest_member_when_the_table_is_full() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 6);
        let now = Instant::now();
        let members = ids(&[b"m1", b"m2", b"m3", b"m4", b"m5"]);
        let first = cid(b"first");
        assert_eq!(provided.add_tree(first, &members, now).unwrap().listed, 5);
        assert_eq!(provided.snapshot().len(), 6);

        let second = cid(b"second");
        let added = provided.add_tree(second, &[], now).unwrap();
        assert_eq!(added.outcome, AddOutcome::Added);
        assert_eq!(added.dropped, vec![members[4]]);
        assert_eq!(provided.snapshot().len(), 6);
        assert!(holds(&provided, &second));
        assert!(holds(&provided, &first));
        assert!(holds(&provided, &members[3]));
        assert!(!holds(&provided, &members[4]));

        let again = provided.add_tree(first, &members, now).unwrap();
        assert_eq!(again.outcome, AddOutcome::AlreadyProvided);
        assert_eq!(again.listed, 4, "the tree that gave way reports one member fewer");
        assert!(again.dropped.is_empty());
        assert!(!holds(&provided, &members[4]), "giving the root again takes no one's place");
    }

    #[test]
    fn the_tree_that_lists_the_most_gives_way_first() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 7);
        let now = Instant::now();
        let big = ids(&[b"b1", b"b2", b"b3", b"b4"]);
        let small = ids(&[b"s1"]);
        provided.add_tree(cid(b"big"), &big, now).unwrap();
        provided.add_tree(cid(b"small"), &small, now).unwrap();
        assert_eq!(provided.snapshot().len(), 7);
        let third = provided.add_tree(cid(b"third"), &[], now).unwrap();
        assert_eq!(third.dropped, vec![big[3]]);
        let fourth = provided.add_tree(cid(b"fourth"), &[], now).unwrap();
        assert_eq!(fourth.dropped, vec![big[2]]);
        assert!(holds(&provided, &small[0]));
    }

    #[test]
    fn of_two_level_trees_the_newer_gives_way() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 4);
        let now = Instant::now();
        let (x, y) = (cid(b"x"), cid(b"y"));
        provided.add_tree(cid(b"older"), &[x], now).unwrap();
        provided.add_tree(cid(b"newer"), &[y], now).unwrap();
        let added = provided.add_tree(cid(b"third"), &[], now).unwrap();
        assert_eq!(added.dropped, vec![y]);
    }

    #[test]
    fn a_member_two_roots_list_leaves_both_when_it_is_pushed_out() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 4);
        let now = Instant::now();
        let (p, shared) = (cid(b"p"), cid(b"shared"));
        let (x, y) = (cid(b"x"), cid(b"y"));
        provided.add_tree(x, &[p, shared], now).unwrap();
        provided.add_tree(y, &[shared], now).unwrap();
        assert_eq!(provided.snapshot().len(), 4);
        let added = provided.add_tree(cid(b"z"), &[], now).unwrap();
        assert_eq!(added.dropped, vec![shared]);
        assert!(!holds(&provided, &shared));
        assert_eq!(provided.add_tree(y, &[shared], now).unwrap().listed, 0);
        assert_eq!(provided.add_tree(x, &[p, shared], now).unwrap().listed, 1);
    }

    #[test]
    fn named_cids_push_out_every_listed_one_and_then_the_limit_holds() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 4);
        let now = Instant::now();
        let members = ids(&[b"m1", b"m2", b"m3"]);
        let first = cid(b"first");
        provided.add_tree(first, &members, now).unwrap();
        for name in [b"n1", b"n2", b"n3"] {
            assert_eq!(provided.add(cid(name), now), Ok(AddOutcome::Added));
        }
        assert_eq!(provided.snapshot().len(), 4);
        for member in &members {
            assert!(!holds(&provided, member));
        }
        assert_eq!(provided.add(cid(b"n4"), now), Err(TooMany));
        assert_eq!(
            provided.add_tree(cid(b"n5"), &[], now),
            Err(TooMany),
            "a root is refused the same way"
        );
        assert!(holds(&provided, &first), "a root is never pushed out");
        assert!(provided.remove(&cid(b"n1"), now).was_named);
        assert_eq!(provided.add(cid(b"n4"), now), Ok(AddOutcome::Added));
    }

    #[test]
    fn a_member_asked_for_by_name_is_not_pushed_out() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 3);
        let now = Instant::now();
        let members = ids(&[b"m1", b"m2"]);
        provided.add_tree(cid(b"first"), &members, now).unwrap();
        assert_eq!(provided.add(members[1], now), Ok(AddOutcome::Added));
        let added = provided.add_tree(cid(b"second"), &[], now).unwrap();
        assert_eq!(added.dropped, vec![members[0]]);
        assert!(holds(&provided, &members[1]));
        assert_eq!(provided.add_tree(cid(b"third"), &[], now), Err(TooMany));
    }

    #[test]
    fn room_freed_by_a_delete_goes_back_to_the_tree_that_was_cut() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 6);
        let now = Instant::now();
        let members = ids(&[b"a1", b"a2", b"a3", b"a4", b"a5"]);
        let (first, second) = (cid(b"first"), cid(b"second"));
        provided.add_tree(first, &members, now).unwrap();
        let added = provided.add_tree(second, &ids(&[b"b1", b"b2"]), now).unwrap();
        assert_eq!(added.dropped, vec![members[4]]);
        assert_eq!(added.listed, 0, "a new root's members take only room that is free");
        assert!(!holds(&provided, &members[4]));

        let removed = provided.remove(&second, now);
        assert_eq!(removed.dropped, vec![second]);
        assert!(holds(&provided, &members[4]), "the cut tree is topped up");
        assert_eq!(provided.snapshot().len(), 6);
        assert_eq!(provided.add_tree(first, &members, now).unwrap().listed, 5);
    }

    #[test]
    fn freed_room_goes_to_the_cut_tree_holding_the_fewest_members_first() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 7);
        let now = Instant::now();
        let a = ids(&[b"a1", b"a2", b"a3", b"a4", b"a5", b"a6"]);
        let b = ids(&[b"b1", b"b2"]);
        let c = ids(&[b"c1"]);
        let (root_a, root_b, root_c) = (cid(b"root a"), cid(b"root b"), cid(b"root c"));
        provided.add_tree(root_a, &a, now).unwrap();
        provided.add_tree(root_b, &b, now).unwrap();
        provided.add_tree(root_c, &c, now).unwrap();
        for lost in [&a[5], &a[4], &b[0], &c[0]] {
            assert!(!holds(&provided, lost));
        }

        provided.remove(&root_c, now);
        assert!(holds(&provided, &b[0]), "b holds none, a holds four");
        assert!(!holds(&provided, &a[4]));

        provided.remove(&root_b, now);
        assert!(!holds(&provided, &b[0]));
        assert!(holds(&provided, &a[4]));
        assert!(holds(&provided, &a[5]));
        assert_eq!(provided.snapshot().len(), 7);
    }

    #[test]
    fn a_cut_tree_is_topped_up_when_a_root_shrinks_its_list() {
        let mut provided = Provided::with_max(REANNOUNCE_EVERY, 5);
        let now = Instant::now();
        let a = ids(&[b"a1", b"a2", b"a3", b"a4"]);
        let b = ids(&[b"b1", b"b2", b"b3"]);
        provided.add_tree(cid(b"root a"), &a, now).unwrap();
        provided.add_tree(cid(b"root b"), &b, now).unwrap();
        assert!(!holds(&provided, &a[3]));
        assert!(!holds(&provided, &b[0]));
        let smaller = provided.add_tree(cid(b"root a"), &a[..2], now).unwrap();
        assert_eq!(smaller.listed, 2);
        assert_eq!(smaller.dropped, vec![a[2]]);
        assert!(holds(&provided, &b[0]));
        assert_eq!(provided.snapshot().len(), 5);
    }
}
