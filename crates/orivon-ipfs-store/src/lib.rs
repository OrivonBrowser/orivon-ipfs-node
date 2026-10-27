use cid::Cid;
use orivon_ipfs_core::{
    block_data_range, encode_car_v1, parse_car_v1, verify_block, Block, BlockProvider, CarBlock,
    CoreError, Result as CoreResult,
};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_HOT_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const HOT_CACHE_TOUCH_INTERVAL: Duration = Duration::from_secs(30);
const MAX_NAME_CACHE_RECORDS: i64 = 128;
/// Bumped whenever the layout of the cached rows changes. A database with
/// another version is only a cache: its tables are dropped and recreated.
const SCHEMA_VERSION: &str = "2";
const MAX_PARTITION_KEY_LEN: usize = 128;

/// Whose blocks a store handle reads and writes. Every block row is keyed by
/// (partition, cid), so a block kept for one partition is invisible to every
/// other one. The size cap stays global.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BlockPartition {
    /// The single partition of a library user or a test that has none. Rows
    /// keep the bare CID as their key.
    Shared,
    /// An opaque key chosen by the caller: 1 to 128 characters of
    /// `[A-Za-z0-9_-]` (see `BlockPartition::keyed`).
    Keyed(Arc<str>),
    /// Nothing is read and nothing is kept: a read always misses and a write
    /// verifies the block and drops it.
    Discard,
}

impl BlockPartition {
    /// `None` when `key` is not 1 to 128 characters of `[A-Za-z0-9_-]`.
    pub fn keyed(key: &str) -> Option<Self> {
        let valid = !key.is_empty()
            && key.len() <= MAX_PARTITION_KEY_LEN
            && key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
        valid.then(|| Self::Keyed(Arc::from(key)))
    }

    fn key_prefix(&self) -> Vec<u8> {
        match self {
            // A v1 CID key starts with 0x01 and a keyed prefix with 'P', and
            // a partition key holds no 0x00, so the three never collide.
            Self::Keyed(key) => {
                let mut prefix = Vec::with_capacity(key.len() + 2);
                prefix.push(b'P');
                prefix.extend_from_slice(key.as_bytes());
                prefix.push(0);
                prefix
            }
            Self::Shared | Self::Discard => Vec::new(),
        }
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("core: {0}")]
    Core(#[from] CoreError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone)]
pub struct SqliteBlockStore {
    conn: Arc<Mutex<Connection>>,
    max_bytes: u64,
    hot: Arc<Mutex<VerifiedHotCache>>,
    retained: Arc<Mutex<HashMap<Vec<u8>, usize>>>,
    partition: BlockPartition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedProviderRecord {
    pub id: Option<String>,
    pub addrs: Vec<String>,
}

impl SqliteBlockStore {
    pub fn open(path: impl AsRef<Path>, max_bytes: u64) -> Result<Self> {
        let conn = Connection::open(path)?;
        let max_bytes = if max_bytes == 0 {
            DEFAULT_CACHE_BYTES
        } else {
            max_bytes
        };
        let hot_cache = VerifiedHotCache::new(hot_cache_bytes(max_bytes));
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            max_bytes,
            hot: Arc::new(Mutex::new(hot_cache)),
            retained: Arc::new(Mutex::new(HashMap::new())),
            partition: BlockPartition::Shared,
        };
        store.init()?;
        Ok(store)
    }

    pub fn in_memory(max_bytes: u64) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let max_bytes = if max_bytes == 0 {
            DEFAULT_CACHE_BYTES
        } else {
            max_bytes
        };
        let hot_cache = VerifiedHotCache::new(hot_cache_bytes(max_bytes));
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            max_bytes,
            hot: Arc::new(Mutex::new(hot_cache)),
            retained: Arc::new(Mutex::new(HashMap::new())),
            partition: BlockPartition::Shared,
        };
        store.init()?;
        Ok(store)
    }

    /// A handle on the same database and caches that reads and writes blocks
    /// for `partition` only. Provider, bad-provider and name rows stay shared.
    pub fn scoped(&self, partition: BlockPartition) -> Self {
        Self {
            partition,
            ..self.clone()
        }
    }

    pub fn partition(&self) -> &BlockPartition {
        &self.partition
    }

    fn row_key(&self, cid: &Cid) -> Vec<u8> {
        let mut key = self.partition.key_prefix();
        key.extend_from_slice(&block_key(cid));
        key
    }

    fn init(&self) -> Result<()> {
        self.conn.lock().execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );
            "#,
        )?;
        let version = self
            .conn
            .lock()
            .query_row(
                "SELECT value FROM metadata WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if version.as_deref() != Some(SCHEMA_VERSION) {
            self.conn.lock().execute_batch(
                r#"
                DROP TABLE IF EXISTS blocks;
                DROP TABLE IF EXISTS bad_providers;
                DROP TABLE IF EXISTS provider_cache;
                DROP TABLE IF EXISTS name_cache;
                "#,
            )?;
            self.conn.lock().execute(
                "INSERT INTO metadata(key, value) VALUES ('schema_version', ?1)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![SCHEMA_VERSION],
            )?;
        }
        self.conn.lock().execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS blocks (
                cid BLOB PRIMARY KEY NOT NULL,
                codec INTEGER NOT NULL,
                size INTEGER NOT NULL,
                data BLOB NOT NULL,
                inserted_at INTEGER NOT NULL,
                last_accessed_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS blocks_last_accessed
                ON blocks(last_accessed_at);
            CREATE TABLE IF NOT EXISTS bad_providers (
                peer_or_url TEXT PRIMARY KEY NOT NULL,
                reason TEXT NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS provider_cache (
                cid BLOB PRIMARY KEY NOT NULL,
                providers_json TEXT NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS name_cache (
                name TEXT PRIMARY KEY NOT NULL,
                resolved_target TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS name_cache_updated_at
                ON name_cache(updated_at);
            "#,
        )?;
        Ok(())
    }

    pub fn put_block(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        verify_block(cid, data)?;
        if self.partition == BlockPartition::Discard {
            return Ok(());
        }
        let cid_bytes = self.row_key(cid);
        let now = now_secs();
        self.conn.lock().execute(
            r#"
            INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?5)
            ON CONFLICT(cid) DO UPDATE SET
                codec = excluded.codec,
                size = excluded.size,
                data = excluded.data,
                last_accessed_at = excluded.last_accessed_at
            "#,
            params![
                &cid_bytes,
                cid.codec() as i64,
                data.len() as i64,
                data,
                now as i64
            ],
        )?;
        let inserted_block_evicted = self.evict_if_needed_tracking(&cid_bytes)?;
        if inserted_block_evicted {
            self.hot.lock().remove(&cid_bytes);
        } else {
            self.hot.lock().put_verified(cid_bytes, data.to_vec(), now);
        }
        Ok(())
    }

    pub fn put(&self, block: &Block) -> Result<()> {
        self.put_block(block.cid(), block.data())
    }

    pub fn get(&self, cid: &Cid) -> Result<Option<Block>> {
        if self.partition == BlockPartition::Discard {
            return Ok(None);
        }
        let cid_bytes = self.row_key(cid);
        let now = now_secs();
        if let Some(hit) = self.hot.lock().get_verified(&cid_bytes, now) {
            if hit.touch_persistent {
                self.touch_at(cid, now)?;
            }
            return Ok(Some(Block::unchecked(*cid, hit.data)));
        }

        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT data FROM blocks WHERE cid = ?1",
                params![cid_bytes],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        match row {
            Some(data) => {
                verify_block(cid, &data)?;
                self.touch_at(cid, now)?;
                self.hot.lock().put_verified(cid_bytes, data.clone(), now);
                Ok(Some(Block::unchecked(*cid, data)))
            }
            None => Ok(None),
        }
    }

    pub fn get_range(&self, cid: &Cid, start: u64, end: u64) -> Result<Option<Vec<u8>>> {
        if self.partition == BlockPartition::Discard {
            return Ok(None);
        }
        let cid_bytes = self.row_key(cid);
        let now = now_secs();
        if let Some(hit) = self
            .hot
            .lock()
            .get_verified_range(&cid_bytes, now, start, end)
        {
            if hit.touch_persistent {
                self.touch_at(cid, now)?;
            }
            return Ok(Some(hit.data));
        }

        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT data FROM blocks WHERE cid = ?1",
                params![&cid_bytes],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        match row {
            Some(data) => {
                verify_block(cid, &data)?;
                self.touch_at(cid, now)?;
                let range = block_data_range(&data, start, end);
                self.hot.lock().put_verified(cid_bytes, data, now);
                Ok(Some(range))
            }
            None => Ok(None),
        }
    }

    pub fn import_car(&self, bytes: &[u8]) -> Result<Vec<Cid>> {
        let car = parse_car_v1(bytes)?;
        let mut imported = Vec::with_capacity(car.blocks.len());
        for block in car.blocks {
            self.put_block(&block.cid, &block.data)?;
            imported.push(block.cid);
        }
        Ok(imported)
    }

    pub fn export_car(&self) -> Result<Vec<u8>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT cid, data FROM blocks ORDER BY inserted_at ASC")?;
        let blocks = stmt
            .query_map([], |row| {
                let cid_bytes = row.get::<_, Vec<u8>>(0)?;
                let data = row.get::<_, Vec<u8>>(1)?;
                Ok((cid_bytes, data))
            })?
            .map(|row| {
                let (cid_bytes, data) = row?;
                let cid_bytes = match cid_bytes.first() {
                    Some(b'P') => cid_bytes
                        .iter()
                        .position(|byte| *byte == 0)
                        .map(|end| cid_bytes[end + 1..].to_vec())
                        .unwrap_or(cid_bytes),
                    _ => cid_bytes,
                };
                let cid = Cid::read_bytes(&mut Cursor::new(cid_bytes))
                    .map_err(|err| CoreError::InvalidCid(err.to_string()))?;
                Ok(CarBlock { cid, data })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(encode_car_v1(&blocks))
    }

    pub fn put_provider_records(
        &self,
        cid: &Cid,
        providers: &[CachedProviderRecord],
        ttl: Duration,
    ) -> Result<()> {
        let expires_at = now_secs().saturating_add(ttl.as_secs());
        let providers_json = serde_json::to_string(providers)?;
        self.conn.lock().execute(
            r#"
            INSERT INTO provider_cache(cid, providers_json, expires_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(cid) DO UPDATE SET
                providers_json = excluded.providers_json,
                expires_at = excluded.expires_at
            "#,
            params![provider_cache_key(cid), providers_json, expires_at as i64],
        )?;
        Ok(())
    }

    pub fn get_provider_records(&self, cid: &Cid) -> Result<Option<Vec<CachedProviderRecord>>> {
        let cid_bytes = provider_cache_key(cid);
        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT providers_json, expires_at FROM provider_cache WHERE cid = ?1",
                params![cid_bytes],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;

        let Some((providers_json, expires_at)) = row else {
            return Ok(None);
        };
        if expires_at <= now_secs() as i64 {
            self.conn.lock().execute(
                "DELETE FROM provider_cache WHERE cid = ?1",
                params![provider_cache_key(cid)],
            )?;
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&providers_json)?))
    }

    pub fn put_name_record(&self, name: &str, resolved_target: &str, ttl: Duration) -> Result<()> {
        if ttl.is_zero() {
            return Ok(());
        }
        let now = now_secs();
        let expires_at = now.saturating_add(ttl.as_secs());
        self.conn.lock().execute(
            r#"
            INSERT INTO name_cache(name, resolved_target, expires_at, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(name) DO UPDATE SET
                resolved_target = excluded.resolved_target,
                expires_at = excluded.expires_at,
                updated_at = excluded.updated_at
            "#,
            params![name, resolved_target, expires_at as i64, now as i64],
        )?;
        self.prune_name_cache()?;
        Ok(())
    }

    pub fn get_name_record(&self, name: &str) -> Result<Option<String>> {
        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT resolved_target, expires_at FROM name_cache WHERE name = ?1",
                params![name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;

        let Some((resolved_target, expires_at)) = row else {
            return Ok(None);
        };
        if expires_at <= now_secs() as i64 {
            self.conn
                .lock()
                .execute("DELETE FROM name_cache WHERE name = ?1", params![name])?;
            return Ok(None);
        }
        Ok(Some(resolved_target))
    }

    pub fn mark_bad_provider(&self, peer_or_url: &str, reason: &str, ttl: Duration) -> Result<()> {
        let expires_at = now_secs().saturating_add(ttl.as_secs());
        self.conn.lock().execute(
            r#"
            INSERT INTO bad_providers(peer_or_url, reason, expires_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(peer_or_url) DO UPDATE SET
                reason = excluded.reason,
                expires_at = excluded.expires_at
            "#,
            params![peer_or_url, reason, expires_at as i64],
        )?;
        Ok(())
    }

    pub fn is_bad_provider(&self, peer_or_url: &str) -> Result<bool> {
        let expires_at = self
            .conn
            .lock()
            .query_row(
                "SELECT expires_at FROM bad_providers WHERE peer_or_url = ?1",
                params![peer_or_url],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let Some(expires_at) = expires_at else {
            return Ok(false);
        };
        if expires_at <= now_secs() as i64 {
            self.conn.lock().execute(
                "DELETE FROM bad_providers WHERE peer_or_url = ?1",
                params![peer_or_url],
            )?;
            return Ok(false);
        }
        Ok(true)
    }

    pub fn total_bytes(&self) -> Result<u64> {
        let total =
            self.conn
                .lock()
                .query_row("SELECT COALESCE(SUM(size), 0) FROM blocks", [], |row| {
                    row.get::<_, i64>(0)
                })?;
        Ok(total.max(0) as u64)
    }

    pub fn block_count(&self) -> Result<u64> {
        let count = self
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM blocks", [], |row| {
                row.get::<_, i64>(0)
            })?;
        Ok(count.max(0) as u64)
    }

    pub fn clear(&self) -> Result<()> {
        self.conn.lock().execute("DELETE FROM blocks", [])?;
        self.conn.lock().execute("DELETE FROM provider_cache", [])?;
        self.conn.lock().execute("DELETE FROM bad_providers", [])?;
        self.conn.lock().execute("DELETE FROM name_cache", [])?;
        self.hot.lock().clear();
        Ok(())
    }

    pub fn clear_provider_metadata(&self) -> Result<()> {
        self.conn.lock().execute("DELETE FROM provider_cache", [])?;
        self.conn.lock().execute("DELETE FROM bad_providers", [])?;
        self.conn.lock().execute("DELETE FROM name_cache", [])?;
        Ok(())
    }

    pub fn trim_blocks_to(&self, max_bytes: u64) -> Result<()> {
        self.evict_until(max_bytes)
    }

    fn touch_at(&self, cid: &Cid, now: u64) -> Result<()> {
        self.conn.lock().execute(
            "UPDATE blocks SET last_accessed_at = ?1 WHERE cid = ?2",
            params![now as i64, self.row_key(cid)],
        )?;
        Ok(())
    }

    fn evict_if_needed_tracking(&self, tracked_cid_bytes: &[u8]) -> Result<bool> {
        self.evict_until_tracking(self.max_bytes, tracked_cid_bytes)
    }

    fn prune_name_cache(&self) -> Result<()> {
        let now = now_secs() as i64;
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM name_cache WHERE expires_at <= ?1",
            params![now],
        )?;
        conn.execute(
            r#"
            DELETE FROM name_cache
            WHERE name IN (
                SELECT name FROM name_cache
                ORDER BY updated_at DESC, name DESC
                LIMIT -1 OFFSET ?1
            )
            "#,
            params![MAX_NAME_CACHE_RECORDS],
        )?;
        Ok(())
    }

    fn evict_until(&self, max_bytes: u64) -> Result<()> {
        self.evict_until_tracking(max_bytes, &[]).map(|_| ())
    }

    fn evict_until_tracking(&self, max_bytes: u64, tracked_cid_bytes: &[u8]) -> Result<bool> {
        let mut tracked_block_evicted = false;
        loop {
            let total = self.total_bytes()?;
            if total <= max_bytes {
                return Ok(tracked_block_evicted);
            }
            let retained = self.retained.lock().clone();
            let cid_bytes = self.oldest_evictable_cid(&retained)?;
            let Some(cid_bytes) = cid_bytes else {
                return Ok(tracked_block_evicted);
            };
            let deleted = self
                .conn
                .lock()
                .execute("DELETE FROM blocks WHERE cid = ?1", params![&cid_bytes])?;
            if deleted == 0 {
                return Ok(tracked_block_evicted);
            }
            if !tracked_cid_bytes.is_empty() && cid_bytes == tracked_cid_bytes {
                tracked_block_evicted = true;
            }
            self.hot.lock().remove(&cid_bytes);
        }
    }

    fn oldest_evictable_cid(&self, retained: &HashMap<Vec<u8>, usize>) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            r#"
            SELECT cid FROM blocks
            ORDER BY last_accessed_at ASC, inserted_at ASC
            "#,
        )?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let cid_bytes = row.get::<_, Vec<u8>>(0)?;
            if !retained.contains_key(&cid_bytes) {
                return Ok(Some(cid_bytes));
            }
        }
        Ok(None)
    }

    fn retain_cid_bytes(&self, cid_bytes: Vec<u8>) {
        let mut retained = self.retained.lock();
        *retained.entry(cid_bytes).or_insert(0) += 1;
    }

    fn release_cid_bytes(&self, cid_bytes: &[u8]) {
        let mut retained = self.retained.lock();
        let Some(count) = retained.get_mut(cid_bytes) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            retained.remove(cid_bytes);
        }
    }
}

impl BlockProvider for SqliteBlockStore {
    fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
        self.get(cid)
            .map_err(|err| CoreError::Storage(err.to_string()))
    }

    fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> CoreResult<Option<Vec<u8>>> {
        self.get_range(cid, start, end)
            .map_err(|err| CoreError::Storage(err.to_string()))
    }

    fn retain_block(&self, cid: &Cid) -> CoreResult<()> {
        self.retain_cid_bytes(self.row_key(cid));
        Ok(())
    }

    fn release_block(&self, cid: &Cid) {
        self.release_cid_bytes(&self.row_key(cid));
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn provider_cache_key(cid: &Cid) -> Vec<u8> {
    cid.hash().to_bytes()
}

fn block_key(cid: &Cid) -> Vec<u8> {
    Cid::new_v1(cid.codec(), *cid.hash()).to_bytes()
}

fn hot_cache_bytes(max_bytes: u64) -> u64 {
    max_bytes.min(DEFAULT_HOT_CACHE_BYTES)
}

// Private cache for block bytes that were already verified on put or cold read.
struct VerifiedHotCache {
    max_bytes: u64,
    bytes: u64,
    clock: u64,
    entries: HashMap<Vec<u8>, VerifiedHotBlock>,
}

struct VerifiedHotBlock {
    data: Vec<u8>,
    last_accessed: u64,
    last_persistent_touch: u64,
}

struct VerifiedHotCacheHit {
    data: Vec<u8>,
    touch_persistent: bool,
}

impl VerifiedHotCache {
    fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            bytes: 0,
            clock: 0,
            entries: HashMap::new(),
        }
    }

    fn get_verified(&mut self, cid: &[u8], now: u64) -> Option<VerifiedHotCacheHit> {
        let entry = self.entries.get_mut(cid)?;
        self.clock = self.clock.saturating_add(1);
        entry.last_accessed = self.clock;
        let touch_persistent =
            now.saturating_sub(entry.last_persistent_touch) >= HOT_CACHE_TOUCH_INTERVAL.as_secs();
        if touch_persistent {
            entry.last_persistent_touch = now;
        }
        Some(VerifiedHotCacheHit {
            data: entry.data.clone(),
            touch_persistent,
        })
    }

    fn get_verified_range(
        &mut self,
        cid: &[u8],
        now: u64,
        start: u64,
        end: u64,
    ) -> Option<VerifiedHotCacheHit> {
        let entry = self.entries.get_mut(cid)?;
        self.clock = self.clock.saturating_add(1);
        entry.last_accessed = self.clock;
        let touch_persistent =
            now.saturating_sub(entry.last_persistent_touch) >= HOT_CACHE_TOUCH_INTERVAL.as_secs();
        if touch_persistent {
            entry.last_persistent_touch = now;
        }
        Some(VerifiedHotCacheHit {
            data: block_data_range(&entry.data, start, end),
            touch_persistent,
        })
    }

    fn put_verified(&mut self, cid: Vec<u8>, data: Vec<u8>, now: u64) {
        if self.max_bytes == 0 || data.len() as u64 > self.max_bytes {
            self.remove(&cid);
            return;
        }
        if let Some(existing) = self.entries.remove(&cid) {
            self.bytes = self.bytes.saturating_sub(existing.data.len() as u64);
        }
        self.clock = self.clock.saturating_add(1);
        self.bytes = self.bytes.saturating_add(data.len() as u64);
        self.entries.insert(
            cid,
            VerifiedHotBlock {
                data,
                last_accessed: self.clock,
                last_persistent_touch: now,
            },
        );
        self.evict_if_needed();
    }

    fn remove(&mut self, cid: &[u8]) {
        if let Some(block) = self.entries.remove(cid) {
            self.bytes = self.bytes.saturating_sub(block.data.len() as u64);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    fn evict_if_needed(&mut self) {
        while self.bytes > self.max_bytes {
            let Some(cid) = self
                .entries
                .iter()
                .min_by_key(|(_, block)| block.last_accessed)
                .map(|(cid, _)| cid.clone())
            else {
                self.bytes = 0;
                return;
            };
            self.remove(&cid);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orivon_ipfs_core::{cid_from_data, CODEC_DAG_PB, CODEC_RAW};

    #[test]
    fn stores_and_verifies_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"cached block";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let block = store.get(&cid).unwrap().unwrap();
        assert_eq!(block.data(), data);
        assert_eq!(block.cid(), &cid);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn rejected_put_does_not_populate_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");

        assert!(store.put_block(&cid, b"invalid block").is_err());
        assert!(!store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn evicted_put_does_not_populate_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1).unwrap();
        let data = b"larger than cache budget";
        let cid = cid_from_data(CODEC_RAW, data);

        store.put_block(&cid, data).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
        assert!(!store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn cold_read_verifies_before_populating_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");
        let cid_bytes = block_key(&cid);
        store
            .conn
            .lock()
            .execute(
                r#"
                INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                "#,
                params![
                    &cid_bytes,
                    cid.codec() as i64,
                    13i64,
                    b"invalid block".as_slice(),
                    now_secs() as i64
                ],
            )
            .unwrap();

        let err = store.get(&cid).unwrap_err();
        assert!(matches!(
            err,
            StoreError::Core(CoreError::HashMismatch { .. })
        ));
        assert!(!store.hot.lock().entries.contains_key(&cid_bytes));
    }

    #[test]
    fn range_read_verifies_cold_block_before_populating_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");
        let cid_bytes = block_key(&cid);
        store
            .conn
            .lock()
            .execute(
                r#"
                INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                "#,
                params![
                    &cid_bytes,
                    cid.codec() as i64,
                    13i64,
                    b"invalid block".as_slice(),
                    now_secs() as i64
                ],
            )
            .unwrap();

        let err = store.get_range(&cid, 0, 4).unwrap_err();
        assert!(matches!(
            err,
            StoreError::Core(CoreError::HashMismatch { .. })
        ));
        assert!(!store.hot.lock().entries.contains_key(&cid_bytes));
    }

    #[test]
    fn hot_cache_range_hit_returns_requested_bytes() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"0123456789";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);

        assert_eq!(store.get_range(&cid, 2, 5).unwrap().unwrap(), b"2345");
        assert_eq!(block_last_accessed_at(&store, &cid), 1);
    }

    #[test]
    fn cold_range_read_populates_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"cold range block";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        store.hot.lock().clear();

        assert_eq!(store.get_range(&cid, 5, 9).unwrap().unwrap(), b"range");

        assert!(store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn dag_pb_blocks_are_addressable_by_cidv0_and_cidv1() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"dag-pb alias block";
        let cidv1 = cid_from_data(CODEC_DAG_PB, data);
        let cidv0 = Cid::new_v0(*cidv1.hash()).unwrap();

        store.put_block(&cidv0, data).unwrap();

        assert_eq!(store.get(&cidv0).unwrap().unwrap().data(), data);
        let block = store.get(&cidv1).unwrap().unwrap();
        assert_eq!(block.cid(), &cidv1);
        assert_eq!(block.data(), data);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn cidv0_cidv1_aliases_share_retention_state() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cidv1 = cid_from_data(CODEC_DAG_PB, &first);
        let first_cidv0 = Cid::new_v0(*first_cidv1.hash()).unwrap();
        let second_cid = cid_from_data(CODEC_RAW, &second);

        store.put_block(&first_cidv0, &first).unwrap();
        store.retain_block(&first_cidv1).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert_eq!(store.get(&first_cidv0).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cidv1);
        store.put_block(&second_cid, &second).unwrap();

        assert!(store.get(&first_cidv0).unwrap().is_none());
        assert_eq!(store.get(&second_cid).unwrap().unwrap().data(), second);
    }

    #[test]
    fn evicts_lru_blocks_when_over_budget() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert!(store.total_bytes().unwrap() <= 20);
        assert_eq!(store.get(&second_cid).unwrap().unwrap().data(), second);
    }

    #[test]
    fn clear_removes_hot_cache_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache clear";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        store
            .put_name_record(
                "example.test",
                "/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(60),
            )
            .unwrap();
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);
        assert!(store.get_name_record("example.test").unwrap().is_some());

        store.clear().unwrap();

        assert!(store.get(&cid).unwrap().is_none());
        assert!(store.get_name_record("example.test").unwrap().is_none());
    }

    #[test]
    fn hot_cache_hit_skips_redundant_sqlite_touch() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache touch skip";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);

        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        assert_eq!(block_last_accessed_at(&store, &cid), 1);
    }

    #[test]
    fn hot_cache_hit_periodically_refreshes_sqlite_touch() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache touch refresh";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);
        store
            .hot
            .lock()
            .entries
            .get_mut(&block_key(&cid))
            .unwrap()
            .last_persistent_touch = 0;

        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        assert!(block_last_accessed_at(&store, &cid) > 1);
    }

    #[test]
    fn trim_removes_hot_cache_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache trim";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
        assert!(store.get(&cid).unwrap().is_none());
    }

    #[test]
    fn trims_blocks_to_requested_budget() {
        let store = SqliteBlockStore::in_memory(1024).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        store.trim_blocks_to(20).unwrap();

        assert!(store.total_bytes().unwrap() <= 20);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn retained_blocks_are_not_evicted_until_released() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let third = vec![3u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        let third_cid = cid_from_data(CODEC_RAW, &third);

        store.put_block(&first_cid, &first).unwrap();
        store.retain_block(&first_cid).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert_eq!(store.get(&first_cid).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cid);
        store.put_block(&third_cid, &third).unwrap();

        assert!(store.get(&first_cid).unwrap().is_none());
        assert_eq!(store.get(&third_cid).unwrap().unwrap().data(), third);
    }

    #[test]
    fn trim_skips_retained_blocks() {
        let store = SqliteBlockStore::in_memory(1024).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();
        store.retain_block(&first_cid).unwrap();

        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.get(&first_cid).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cid);
        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
    }

    fn set_block_last_accessed_at(store: &SqliteBlockStore, cid: &Cid, value: i64) {
        store
            .conn
            .lock()
            .execute(
                "UPDATE blocks SET last_accessed_at = ?1 WHERE cid = ?2",
                params![value, block_key(cid)],
            )
            .unwrap();
    }

    fn block_last_accessed_at(store: &SqliteBlockStore, cid: &Cid) -> i64 {
        store
            .conn
            .lock()
            .query_row(
                "SELECT last_accessed_at FROM blocks WHERE cid = ?1",
                params![block_key(cid)],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn caches_name_records_with_ttl() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        store
            .put_name_record(
                "example.test",
                "/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(
            store.get_name_record("example.test").unwrap().as_deref(),
            Some("/ipfs/bafkqaddwgevxmmraojswg33smq")
        );
        assert!(store.get_name_record("missing.test").unwrap().is_none());
    }

    #[test]
    fn skips_zero_ttl_name_records() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        store
            .put_name_record("example.test", "/ipfs/bafyroot", Duration::ZERO)
            .unwrap();

        assert!(store.get_name_record("example.test").unwrap().is_none());
    }

    #[test]
    fn bounds_name_cache_records() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        for index in 0..(MAX_NAME_CACHE_RECORDS + 2) {
            store
                .put_name_record(
                    &format!("name-{index:03}.test"),
                    "/ipfs/bafyroot",
                    Duration::from_secs(60),
                )
                .unwrap();
        }

        let count = store
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM name_cache", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(count, MAX_NAME_CACHE_RECORDS);
    }

    fn partition(key: &str) -> BlockPartition {
        BlockPartition::keyed(key).unwrap()
    }

    #[test]
    fn a_block_kept_for_one_partition_is_invisible_to_the_others() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"kept for partition a";
        let cid = cid_from_data(CODEC_RAW, data);
        let a = store.scoped(partition("site-a"));
        let b = store.scoped(partition("site-b"));
        a.put_block(&cid, data).unwrap();

        assert_eq!(a.get(&cid).unwrap().unwrap().data(), data);
        // The second read is served from the hot cache; it is still A's.
        assert!(a.get(&cid).unwrap().is_some());
        assert!(b.get(&cid).unwrap().is_none());
        assert!(b.get_range(&cid, 0, 3).unwrap().is_none());
        assert!(store.get(&cid).unwrap().is_none());
        assert!(store
            .scoped(BlockPartition::Discard)
            .get(&cid)
            .unwrap()
            .is_none());
    }

    #[test]
    fn partitions_hold_their_own_copy_of_the_same_block() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"same bytes in two places";
        let cid = cid_from_data(CODEC_RAW, data);
        store.scoped(partition("one")).put_block(&cid, data).unwrap();
        store.scoped(partition("two")).put_block(&cid, data).unwrap();

        assert_eq!(store.block_count().unwrap(), 2);
        assert_eq!(store.total_bytes().unwrap(), 2 * data.len() as u64);
        assert!(store.scoped(partition("two")).get(&cid).unwrap().is_some());
    }

    #[test]
    fn partition_keys_that_share_a_prefix_stay_apart() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"prefix test";
        let cid = cid_from_data(CODEC_RAW, data);
        store.scoped(partition("ab")).put_block(&cid, data).unwrap();

        assert!(store.scoped(partition("a")).get(&cid).unwrap().is_none());
        assert!(store.scoped(partition("abc")).get(&cid).unwrap().is_none());
    }

    #[test]
    fn the_size_cap_is_global_across_partitions() {
        let first = [1u8; 600];
        let second = [2u8; 600];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        let store = SqliteBlockStore::in_memory(1000).unwrap();
        let a = store.scoped(partition("a"));
        let b = store.scoped(partition("b"));
        a.put_block(&first_cid, &first).unwrap();
        set_partition_block_last_accessed_at(&a, &first_cid, 1);

        b.put_block(&second_cid, &second).unwrap();

        assert!(a.get(&first_cid).unwrap().is_none(), "oldest row goes first");
        assert!(b.get(&second_cid).unwrap().is_some());
        assert_eq!(store.block_count().unwrap(), 1);
    }

    fn set_partition_block_last_accessed_at(store: &SqliteBlockStore, cid: &Cid, value: i64) {
        store
            .conn
            .lock()
            .execute(
                "UPDATE blocks SET last_accessed_at = ?1 WHERE cid = ?2",
                params![value, store.row_key(cid)],
            )
            .unwrap();
    }

    #[test]
    fn a_discarding_handle_keeps_nothing_and_reads_nothing() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"not kept";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        let discard = store.scoped(BlockPartition::Discard);

        assert!(discard.get(&cid).unwrap().is_none());
        discard.put_block(&cid, data).unwrap();
        assert!(discard.put_block(&cid, b"wrong bytes").is_err());
        assert_eq!(store.block_count().unwrap(), 1);
        assert!(discard.hot.lock().entries.len() <= 1);
    }

    #[test]
    fn partition_keys_are_checked() {
        assert!(BlockPartition::keyed("abc_DEF-123").is_some());
        assert!(BlockPartition::keyed(&"a".repeat(128)).is_some());
        assert!(BlockPartition::keyed("").is_none());
        assert!(BlockPartition::keyed(&"a".repeat(129)).is_none());
        for bad in ["a b", "a/b", "a.b", "a:b", "a\0b", "\u{e9}", "a\n"] {
            assert!(BlockPartition::keyed(bad).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn a_retained_block_of_one_partition_is_not_retained_for_another() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"retention is per partition";
        let cid = cid_from_data(CODEC_RAW, data);
        let a = store.scoped(partition("a"));
        a.put_block(&cid, data).unwrap();
        a.retain_block(&cid).unwrap();

        assert!(store.retained.lock().contains_key(&a.row_key(&cid)));
        assert!(!store
            .retained
            .lock()
            .contains_key(&store.scoped(partition("b")).row_key(&cid)));
        a.release_block(&cid);
        assert!(store.retained.lock().is_empty());
    }

    #[test]
    fn an_old_database_is_dropped_and_recreated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blocks.db");
        let data = b"from the old layout";
        let cid = cid_from_data(CODEC_RAW, data);
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE blocks (
                    cid BLOB PRIMARY KEY NOT NULL,
                    codec INTEGER NOT NULL,
                    size INTEGER NOT NULL,
                    data BLOB NOT NULL,
                    inserted_at INTEGER NOT NULL,
                    last_accessed_at INTEGER NOT NULL
                );
                CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL);
                "#,
            )
            .unwrap();
            conn.execute(
                "INSERT INTO blocks VALUES (?1, ?2, ?3, ?4, 1, 1)",
                params![block_key(&cid), CODEC_RAW as i64, data.len() as i64, data.as_slice()],
            )
            .unwrap();
        }

        let store = SqliteBlockStore::open(&path, 1024 * 1024).unwrap();
        assert_eq!(store.block_count().unwrap(), 0);
        assert!(store.get(&cid).unwrap().is_none());
        store.scoped(partition("a")).put_block(&cid, data).unwrap();
        drop(store);

        let reopened = SqliteBlockStore::open(&path, 1024 * 1024).unwrap();
        assert_eq!(reopened.block_count().unwrap(), 1);
        assert!(reopened.scoped(partition("a")).get(&cid).unwrap().is_some());
        assert!(reopened.scoped(partition("b")).get(&cid).unwrap().is_none());
    }

    #[test]
    fn export_reads_keyed_rows_back_as_plain_cids() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"exported from a partition";
        let cid = cid_from_data(CODEC_RAW, data);
        store.scoped(partition("a")).put_block(&cid, data).unwrap();

        let car = store.export_car().unwrap();
        let other = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        assert_eq!(other.import_car(&car).unwrap(), vec![cid]);
        assert!(other.get(&cid).unwrap().is_some());
    }

    #[test]
    fn exports_cache_as_importable_car() {
        let source = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"car export block";
        let cid = cid_from_data(CODEC_RAW, data);
        source.put_block(&cid, data).unwrap();

        let car = source.export_car().unwrap();
        let target = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let imported = target.import_car(&car).unwrap();

        assert_eq!(imported, vec![cid]);
        assert_eq!(target.get(&cid).unwrap().unwrap().data(), data);
    }

    #[test]
    fn caches_provider_records_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"provider cache key");
        let providers = vec![CachedProviderRecord {
            id: Some("peer".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        }];

        store
            .put_provider_records(&cid, &providers, Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), Some(providers));

        store
            .put_provider_records(
                &cid,
                &[CachedProviderRecord {
                    id: None,
                    addrs: vec![],
                }],
                Duration::ZERO,
            )
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
    }

    #[test]
    fn caches_empty_provider_records_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"empty provider cache key");

        store
            .put_provider_records(&cid, &[], Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), Some(Vec::new()));

        store
            .put_provider_records(&cid, &[], Duration::ZERO)
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
    }

    #[test]
    fn provider_cache_key_is_cid_representation_independent() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cidv1 = cid_from_data(orivon_ipfs_core::CODEC_DAG_PB, b"provider multihash key");
        let cidv0 = Cid::new_v0(*cidv1.hash()).unwrap();
        let providers = vec![CachedProviderRecord {
            id: Some("peer".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        }];

        store
            .put_provider_records(&cidv1, &providers, Duration::from_secs(60))
            .unwrap();

        assert_eq!(store.get_provider_records(&cidv0).unwrap(), Some(providers));
    }

    #[test]
    fn tracks_bad_providers_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        store
            .mark_bad_provider("peer", "timeout", Duration::from_secs(60))
            .unwrap();
        assert!(store.is_bad_provider("peer").unwrap());

        store
            .mark_bad_provider("peer", "timeout", Duration::ZERO)
            .unwrap();
        assert!(!store.is_bad_provider("peer").unwrap());
    }

    #[test]
    fn clears_provider_metadata_without_removing_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"provider metadata");
        store.put_block(&cid, b"provider metadata").unwrap();
        store
            .put_provider_records(
                &cid,
                &[CachedProviderRecord {
                    id: Some("peer".to_string()),
                    addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
                }],
                Duration::from_secs(60),
            )
            .unwrap();
        store
            .mark_bad_provider("peer", "timeout", Duration::from_secs(60))
            .unwrap();

        store.clear_provider_metadata().unwrap();

        assert!(store.get(&cid).unwrap().is_some());
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
        assert!(!store.is_bad_provider("peer").unwrap());
    }
}
