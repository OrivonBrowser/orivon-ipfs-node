//! Which blocks of a shared folder to announce: the ones a path below the root can name.
//!
//! A peer that opens `<root>/<path>` finds the root through the DHT and fetches the rest of that
//! one path from whoever answered, so announcing the root is enough for the first file it opens. A
//! peer that asks for another file later starts from the file's own CID and looks for providers of
//! it, and Kubo's broadcast control does not offer such a want to a peer that has not been found as
//! a provider. So the root's directories and the first block of each file are announced too. A
//! file's remaining blocks are not: they are fetched on the connection that answered for its first.

use crate::kept::KeptBlocks;
use cid::Cid;
use orivon_ipfs_core::CODEC_DAG_PB;
use prost::Message;
use std::collections::{HashSet, VecDeque};

#[derive(Clone, PartialEq, Message)]
struct PbNode {
    #[prost(bytes = "vec", optional, tag = "1")]
    data: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "2")]
    links: Vec<PbLink>,
}

#[derive(Clone, PartialEq, Message)]
struct PbLink {
    #[prost(bytes = "vec", optional, tag = "1")]
    hash: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct UnixfsData {
    #[prost(int32, optional, tag = "1")]
    kind: Option<i32>,
}

const UNIXFS_DIRECTORY: i32 = 1;
const UNIXFS_HAMT_SHARD: i32 = 5;

/// The links of `data` when it is a UnixFS directory (or one shard of a large one): the entries,
/// each a file, a symlink or a directory of its own. Any other block has no entries to announce.
fn directory_entries(cid: &Cid, data: &[u8]) -> Vec<Cid> {
    if cid.codec() != CODEC_DAG_PB {
        return Vec::new();
    }
    let Ok(node) = PbNode::decode(data) else {
        return Vec::new();
    };
    let kind = node
        .data
        .as_deref()
        .and_then(|bytes| UnixfsData::decode(bytes).ok())
        .and_then(|unixfs| unixfs.kind);
    if !matches!(kind, Some(UNIXFS_DIRECTORY | UNIXFS_HAMT_SHARD)) {
        return Vec::new();
    }
    node.links
        .iter()
        .filter_map(|link| Cid::try_from(link.hash.as_deref()?).ok())
        .collect()
}

/// The CIDs to announce for `root`, `root` first: its directories and the entries in them, level by
/// level, as far as `limit` allows. Only blocks the node holds are named. The second value is true
/// when `limit` cut the list short.
pub fn announce_set(kept: &KeptBlocks, root: &Cid, limit: usize) -> (Vec<Cid>, bool) {
    let mut set = Vec::new();
    let mut seen = HashSet::new();
    let mut queue = VecDeque::from([*root]);
    while let Some(cid) = queue.pop_front() {
        if !seen.insert(cid.hash().to_bytes()) {
            continue;
        }
        let Some(data) = kept.get(&cid) else {
            continue;
        };
        if set.len() >= limit {
            return (set, true);
        }
        set.push(cid);
        queue.extend(directory_entries(&cid, &data));
    }
    (set, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orivon_ipfs_core::{cid_from_data, CODEC_RAW};

    fn link(cid: &Cid) -> PbLink {
        PbLink {
            hash: Some(cid.to_bytes()),
        }
    }

    fn node(kind: i32, links: &[Cid]) -> (Cid, Vec<u8>) {
        let data = UnixfsData { kind: Some(kind) }.encode_to_vec();
        let encoded = PbNode {
            data: Some(data),
            links: links.iter().map(link).collect(),
        }
        .encode_to_vec();
        (cid_from_data(CODEC_DAG_PB, &encoded), encoded)
    }

    fn raw(data: &[u8]) -> (Cid, Vec<u8>) {
        (cid_from_data(CODEC_RAW, data), data.to_vec())
    }

    fn kept_with(blocks: &[&(Cid, Vec<u8>)]) -> KeptBlocks {
        let kept = KeptBlocks::default();
        kept.insert_all(blocks.iter().map(|b| (*b).clone()).collect())
            .unwrap();
        kept
    }

    #[test]
    fn a_folder_announces_itself_its_subfolders_and_its_files() {
        let (a, b, c) = (raw(b"a"), raw(b"b"), raw(b"c"));
        let inner = node(UNIXFS_DIRECTORY, &[b.0, c.0]);
        let outer = node(UNIXFS_DIRECTORY, &[a.0, inner.0]);
        let kept = kept_with(&[&a, &b, &c, &inner, &outer]);
        let (set, cut) = announce_set(&kept, &outer.0, 100);
        assert!(!cut);
        assert_eq!(set, vec![outer.0, a.0, inner.0, b.0, c.0]);
    }

    #[test]
    fn a_file_is_announced_but_its_chunks_are_not() {
        let (one, two) = (raw(b"chunk one"), raw(b"chunk two"));
        let file = node(2, &[one.0, two.0]);
        let folder = node(UNIXFS_DIRECTORY, &[file.0]);
        let kept = kept_with(&[&one, &two, &file, &folder]);
        let (set, cut) = announce_set(&kept, &folder.0, 100);
        assert!(!cut);
        assert_eq!(set, vec![folder.0, file.0]);
    }

    #[test]
    fn a_hamt_shard_is_walked_like_a_folder() {
        let (a, b) = (raw(b"a"), raw(b"b"));
        let shard = node(UNIXFS_HAMT_SHARD, &[a.0, b.0]);
        let kept = kept_with(&[&a, &b, &shard]);
        assert_eq!(announce_set(&kept, &shard.0, 100).0, vec![shard.0, a.0, b.0]);
    }

    #[test]
    fn blocks_the_node_does_not_hold_are_left_out() {
        let (a, missing) = (raw(b"a"), raw(b"never given"));
        let folder = node(UNIXFS_DIRECTORY, &[a.0, missing.0]);
        let kept = kept_with(&[&a, &folder]);
        assert_eq!(announce_set(&kept, &folder.0, 100).0, vec![folder.0, a.0]);
        let nothing = KeptBlocks::default();
        assert!(announce_set(&nothing, &folder.0, 100).0.is_empty());
    }

    #[test]
    fn the_limit_cuts_the_list_and_says_so() {
        let files: Vec<_> = (0u8..5).map(|i| raw(&[i])).collect();
        let ids: Vec<Cid> = files.iter().map(|f| f.0).collect();
        let folder = node(UNIXFS_DIRECTORY, &ids);
        let mut blocks: Vec<&(Cid, Vec<u8>)> = files.iter().collect();
        blocks.push(&folder);
        let kept = kept_with(&blocks);
        let (set, cut) = announce_set(&kept, &folder.0, 3);
        assert!(cut);
        assert_eq!(set, vec![folder.0, ids[0], ids[1]]);
        let (all, cut) = announce_set(&kept, &folder.0, 6);
        assert!(!cut);
        assert_eq!(all.len(), 6);
    }

    #[test]
    fn a_block_named_twice_is_listed_once_and_a_cycle_ends() {
        let a = raw(b"a");
        let folder = node(UNIXFS_DIRECTORY, &[a.0, a.0]);
        let kept = kept_with(&[&a, &folder]);
        assert_eq!(announce_set(&kept, &folder.0, 100).0, vec![folder.0, a.0]);
    }

    #[test]
    fn a_raw_block_and_garbage_have_no_entries() {
        let a = raw(b"just bytes");
        let kept = kept_with(&[&a]);
        assert_eq!(announce_set(&kept, &a.0, 100).0, vec![a.0]);
        let garbage = (cid_from_data(CODEC_DAG_PB, b"\xff\xff"), b"\xff\xff".to_vec());
        let kept = kept_with(&[&garbage]);
        assert_eq!(announce_set(&kept, &garbage.0, 100).0, vec![garbage.0]);
    }
}
