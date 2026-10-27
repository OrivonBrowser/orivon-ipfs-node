//! The blocks a serve-mode node was given and offers to peers.
//!
//! This table is separate from the read cache and never evicts. Bitswap answers only from here, so
//! a block the node read for a caller is never offered to anyone else.

use bytes::Bytes;
use cid::Cid;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Most bytes the table is charged: each block costs its length plus [`BLOCK_OVERHEAD`].
pub const MAX_KEPT_BYTES: usize = 256 * 1024 * 1024;
/// Most blocks the table holds.
pub const MAX_KEPT_BLOCKS: usize = 200_000;
/// Charged per block on top of its bytes, for the key and the map entry.
pub const BLOCK_OVERHEAD: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeptError {
    #[error("kept blocks would exceed {max} bytes")]
    Full { max: usize },
    #[error("kept blocks would exceed {max} blocks")]
    TooManyBlocks { max: usize },
}

#[derive(Debug, Default)]
struct Inner {
    // Keyed by the multihash bytes, as Bitswap's blockstore is: a CIDv0 and a CIDv1 of the same
    // dag-pb block ask for the same bytes.
    blocks: HashMap<Vec<u8>, Bytes>,
    bytes: usize,
}

#[derive(Debug, Clone)]
pub struct KeptBlocks {
    inner: Arc<Mutex<Inner>>,
    max_bytes: usize,
    max_blocks: usize,
}

impl Default for KeptBlocks {
    fn default() -> Self {
        Self::with_limit(MAX_KEPT_BYTES)
    }
}

impl KeptBlocks {
    pub fn with_limit(max_bytes: usize) -> Self {
        Self::with_limits(max_bytes, MAX_KEPT_BLOCKS)
    }

    pub fn with_limits(max_bytes: usize, max_blocks: usize) -> Self {
        Self {
            inner: Arc::default(),
            max_bytes,
            max_blocks,
        }
    }

    /// Keeps every block or none: when the new blocks would take the table above its byte or block
    /// limit the table is left as it was. A block already kept costs nothing.
    pub fn insert_all(&self, blocks: Vec<(Cid, Vec<u8>)>) -> Result<(), KeptError> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut added: HashMap<Vec<u8>, Bytes> = HashMap::new();
        let mut added_bytes = 0usize;
        for (cid, data) in blocks {
            let key = cid.hash().to_bytes();
            if inner.blocks.contains_key(&key) || added.contains_key(&key) {
                continue;
            }
            added_bytes = added_bytes.saturating_add(data.len() + BLOCK_OVERHEAD);
            added.insert(key, Bytes::from(data));
        }
        if inner.blocks.len() + added.len() > self.max_blocks {
            return Err(KeptError::TooManyBlocks {
                max: self.max_blocks,
            });
        }
        if inner.bytes.saturating_add(added_bytes) > self.max_bytes {
            return Err(KeptError::Full {
                max: self.max_bytes,
            });
        }
        inner.bytes += added_bytes;
        for (key, data) in added {
            inner.blocks.insert(key, data);
        }
        Ok(())
    }

    /// The block's bytes, shared with the table: cloning them copies nothing.
    pub fn get(&self, cid: &Cid) -> Option<Bytes> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.blocks.get(&cid.hash().to_bytes()).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn bytes(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orivon_ipfs_core::{cid_from_data, CODEC_RAW};

    const COST: usize = BLOCK_OVERHEAD;

    fn block(data: &[u8]) -> (Cid, Vec<u8>) {
        (cid_from_data(CODEC_RAW, data), data.to_vec())
    }

    #[test]
    fn keeps_and_returns_blocks() {
        let kept = KeptBlocks::default();
        kept.insert_all(vec![block(b"one"), block(b"two")]).unwrap();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept.bytes(), 6 + 2 * COST);
        let (cid, _) = block(b"one");
        assert_eq!(&kept.get(&cid).unwrap()[..], b"one");
        assert!(kept.get(&block(b"three").0).is_none());
    }

    #[test]
    fn a_post_over_the_limit_keeps_nothing() {
        let kept = KeptBlocks::with_limit(10 + 2 * COST);
        kept.insert_all(vec![block(b"12345")]).unwrap();
        let err = kept
            .insert_all(vec![block(b"abc"), block(b"defghi")])
            .unwrap_err();
        assert!(matches!(err, KeptError::Full { .. }));
        assert_eq!(kept.len(), 1);
        assert_eq!(kept.bytes(), 5 + COST);
        assert!(kept.get(&block(b"abc").0).is_none());
    }

    #[test]
    fn a_block_kept_twice_counts_once() {
        let kept = KeptBlocks::with_limit(5 + COST);
        kept.insert_all(vec![block(b"12345")]).unwrap();
        kept.insert_all(vec![block(b"12345"), block(b"12345")]).unwrap();
        assert_eq!(kept.bytes(), 5 + COST);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn the_limit_is_inclusive() {
        let kept = KeptBlocks::with_limit(6 + 2 * COST);
        kept.insert_all(vec![block(b"123"), block(b"456")]).unwrap();
        assert_eq!(kept.bytes(), 6 + 2 * COST);
        assert!(kept.insert_all(vec![block(b"7")]).is_err());
    }

    #[test]
    fn every_block_is_charged_its_overhead_so_empty_blocks_cannot_be_free() {
        let kept = KeptBlocks::with_limit(3 * COST);
        let distinct: Vec<_> = (0u8..4).map(|i| block(&[i])).collect();
        assert!(matches!(
            kept.insert_all(distinct),
            Err(KeptError::Full { .. })
        ));
        assert!(kept.is_empty());
    }

    #[test]
    fn the_block_count_is_capped() {
        let kept = KeptBlocks::with_limits(MAX_KEPT_BYTES, 3);
        kept.insert_all(vec![block(b"a"), block(b"b"), block(b"c")])
            .unwrap();
        assert_eq!(
            kept.insert_all(vec![block(b"d")]).unwrap_err(),
            KeptError::TooManyBlocks { max: 3 }
        );
        kept.insert_all(vec![block(b"a")]).unwrap();
        assert_eq!(kept.len(), 3);
    }
}
