//! The server half of Bitswap: wire messages, the wantlist -> reply logic, and message framing.

use crate::kept::KeptBlocks;
use bytes::Bytes;
use cid::Cid;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::StreamProtocol;
use prost::Message;
use std::collections::HashSet;
use std::io;

pub const PROTOCOL_1_2: &str = "/ipfs/bitswap/1.2.0";
pub const PROTOCOL_1_1: &str = "/ipfs/bitswap/1.1.0";
pub const PROTOCOL_1_0: &str = "/ipfs/bitswap/1.0.0";

/// Largest message read from a peer. A wantlist of 1024 entries is about 60 KB.
pub const MAX_INBOUND_MESSAGE: usize = 1024 * 1024;
/// Block bytes put in one reply message before the next block starts another.
const REPLY_SOFT_LIMIT: usize = 1024 * 1024;
/// Block bytes in all the replies to one wantlist. Peers send the rest of their wants again.
pub const REPLY_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Entries answered from one wantlist.
const MAX_ENTRIES: usize = 1024;

const PRESENCE_HAVE: i32 = 0;
const PRESENCE_DONT_HAVE: i32 = 1;

#[derive(Clone, PartialEq, Message)]
pub struct BitswapMessage {
    #[prost(message, optional, tag = "1")]
    pub wantlist: Option<Wantlist>,
    #[prost(bytes = "bytes", repeated, tag = "2")]
    pub blocks: Vec<Bytes>,
    #[prost(message, repeated, tag = "3")]
    pub payload: Vec<BlockPayload>,
    #[prost(message, repeated, tag = "4")]
    pub block_presences: Vec<BlockPresence>,
    #[prost(int32, tag = "5")]
    pub pending_bytes: i32,
    #[prost(bytes = "vec", repeated, tag = "6")]
    pub tokens: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Wantlist {
    #[prost(message, repeated, tag = "1")]
    pub entries: Vec<WantEntry>,
    #[prost(bool, tag = "2")]
    pub full: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct WantEntry {
    #[prost(bytes = "vec", tag = "1")]
    pub block: Vec<u8>,
    #[prost(int32, tag = "2")]
    pub priority: i32,
    #[prost(bool, tag = "3")]
    pub cancel: bool,
    #[prost(enumeration = "WantType", tag = "4")]
    pub want_type: i32,
    #[prost(bool, tag = "5")]
    pub send_dont_have: bool,
    #[prost(int32, repeated, tag = "7")]
    pub tokens: Vec<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum WantType {
    Block = 0,
    Have = 1,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlockPayload {
    #[prost(bytes = "vec", tag = "1")]
    pub prefix: Vec<u8>,
    #[prost(bytes = "bytes", tag = "2")]
    pub data: Bytes,
    #[prost(int32, repeated, tag = "4")]
    pub tokens: Vec<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlockPresence {
    #[prost(bytes = "vec", tag = "1")]
    pub cid: Vec<u8>,
    #[prost(int32, tag = "2")]
    pub type_pb: i32,
    #[prost(int32, repeated, tag = "4")]
    pub tokens: Vec<i32>,
}

impl BlockPresence {
    pub fn is_have(&self) -> bool {
        self.type_pb == PRESENCE_HAVE
    }
}

/// What a protocol version can carry. 1.0.0 sends bare block bytes and has no presences; 1.1.0
/// adds CID prefixes; 1.2.0 adds HAVE and DONT_HAVE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    V1_0,
    V1_1,
    V1_2,
}

impl Version {
    pub fn protocol(self) -> StreamProtocol {
        StreamProtocol::new(match self {
            Version::V1_0 => PROTOCOL_1_0,
            Version::V1_1 => PROTOCOL_1_1,
            Version::V1_2 => PROTOCOL_1_2,
        })
    }

    pub fn all() -> [Version; 3] {
        [Version::V1_2, Version::V1_1, Version::V1_0]
    }
}

/// The CID prefix a payload carries: version, codec, hash code and hash length as varints.
pub fn cid_prefix(cid: &Cid) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buffer = unsigned_varint::encode::u64_buffer();
    for value in [
        cid.version() as u64,
        cid.codec(),
        cid.hash().code(),
        cid.hash().size() as u64,
    ] {
        out.extend_from_slice(unsigned_varint::encode::u64(value, &mut buffer));
    }
    out
}

/// The messages that answer one wantlist, from the kept table alone.
///
/// - want-block, kept: the block (a payload with its CID prefix; bare bytes on 1.0.0).
/// - want-have, kept: HAVE. Not kept and `send_dont_have`: DONT_HAVE (1.2.0 only).
/// - cancels and unreadable entries are ignored.
///
/// Each (block, want type) is answered once per wantlist, whatever codecs or versions the entries
/// name for it. Block bytes are shared with the table, never copied, and the replies stop taking
/// blocks once they hold [`REPLY_MAX_BYTES`]. Presences go in the first message; blocks fill
/// messages of about 1 MiB each.
pub fn reply_to_wantlist(
    message: &BitswapMessage,
    kept: &KeptBlocks,
    version: Version,
) -> Vec<BitswapMessage> {
    let Some(wantlist) = &message.wantlist else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut presences = Vec::new();
    let mut replies = Vec::new();
    let mut current = BitswapMessage::default();
    let mut current_bytes = 0usize;
    let mut total_bytes = 0usize;

    for entry in wantlist.entries.iter().take(MAX_ENTRIES) {
        if entry.cancel {
            continue;
        }
        let Ok(cid) = Cid::read_bytes(io::Cursor::new(&entry.block)) else {
            continue;
        };
        let want_have = entry.want_type == WantType::Have as i32;
        if !seen.insert((cid.hash().to_bytes(), want_have)) {
            continue;
        }
        let found = kept.get(&cid);
        match (found, want_have) {
            (Some(_), true) if version == Version::V1_2 => presences.push(BlockPresence {
                cid: cid.to_bytes(),
                type_pb: PRESENCE_HAVE,
                tokens: Vec::new(),
            }),
            (Some(data), false) => {
                if total_bytes > 0 && total_bytes + data.len() > REPLY_MAX_BYTES {
                    continue;
                }
                if current_bytes > 0 && current_bytes + data.len() > REPLY_SOFT_LIMIT {
                    replies.push(std::mem::take(&mut current));
                    current_bytes = 0;
                }
                current_bytes += data.len();
                total_bytes += data.len();
                match version {
                    Version::V1_0 => current.blocks.push(data),
                    Version::V1_1 | Version::V1_2 => current.payload.push(BlockPayload {
                        prefix: cid_prefix(&cid),
                        data,
                        tokens: Vec::new(),
                    }),
                }
            }
            (None, _) if entry.send_dont_have && version == Version::V1_2 => {
                presences.push(BlockPresence {
                    cid: cid.to_bytes(),
                    type_pb: PRESENCE_DONT_HAVE,
                    tokens: Vec::new(),
                })
            }
            _ => {}
        }
    }

    if !current.payload.is_empty() || !current.blocks.is_empty() {
        replies.push(current);
    }
    if !presences.is_empty() {
        if replies.is_empty() {
            replies.push(BitswapMessage::default());
        }
        replies[0].block_presences = presences;
    }
    replies
}

pub async fn read_message<T>(mut io: T) -> io::Result<Option<BitswapMessage>>
where
    T: AsyncRead + Unpin,
{
    let mut length = 0u64;
    for index in 0..10 {
        let mut byte = [0u8; 1];
        match io.read_exact(&mut byte).await {
            Ok(()) => {}
            Err(err) if index == 0 && err.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None)
            }
            Err(err) => return Err(err),
        }
        length |= u64::from(byte[0] & 0x7f) << (7 * index);
        if byte[0] & 0x80 == 0 {
            let length = usize::try_from(length)
                .ok()
                .filter(|length| *length <= MAX_INBOUND_MESSAGE)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "message too large"))?;
            // Memory follows the bytes that arrive, not the length the peer claims.
            let mut buffer = Vec::new();
            io.take(length as u64).read_to_end(&mut buffer).await?;
            if buffer.len() != length {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            return BitswapMessage::decode(buffer.as_slice())
                .map(Some)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "message length is too long",
    ))
}

pub async fn write_message<T>(io: &mut T, message: &BitswapMessage) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
{
    let bytes = message.encode_to_vec();
    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "message too large"))?;
    let mut buffer = unsigned_varint::encode::u32_buffer();
    io.write_all(unsigned_varint::encode::u32(length, &mut buffer))
        .await?;
    io.write_all(&bytes).await?;
    io.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use orivon_ipfs_core::{cid_from_data, CODEC_DAG_PB, CODEC_RAW};

    fn wants(entries: Vec<WantEntry>) -> BitswapMessage {
        BitswapMessage {
            wantlist: Some(Wantlist {
                entries,
                full: false,
            }),
            ..BitswapMessage::default()
        }
    }

    fn entry(cid: &Cid, want_type: WantType, send_dont_have: bool) -> WantEntry {
        WantEntry {
            block: cid.to_bytes(),
            priority: 1,
            cancel: false,
            want_type: want_type as i32,
            send_dont_have,
            tokens: Vec::new(),
        }
    }

    fn kept_with(blocks: &[(&Cid, &[u8])]) -> KeptBlocks {
        let kept = KeptBlocks::default();
        kept.insert_all(blocks.iter().map(|(c, d)| (**c, d.to_vec())).collect())
            .unwrap();
        kept
    }

    #[test]
    fn want_block_for_a_kept_block_sends_it_with_its_prefix() {
        let cid = cid_from_data(CODEC_DAG_PB, b"directory");
        let kept = kept_with(&[(&cid, b"directory")]);
        let replies = reply_to_wantlist(
            &wants(vec![entry(&cid, WantType::Block, true)]),
            &kept,
            Version::V1_2,
        );
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].payload.len(), 1);
        assert_eq!(&replies[0].payload[0].data[..], b"directory");
        assert_eq!(replies[0].payload[0].prefix, vec![0x01, 0x70, 0x12, 0x20]);
        assert!(replies[0].block_presences.is_empty());
    }

    #[test]
    fn want_have_answers_have_or_dont_have() {
        let have = cid_from_data(CODEC_RAW, b"yes");
        let missing = cid_from_data(CODEC_RAW, b"no");
        let kept = kept_with(&[(&have, b"yes")]);
        let replies = reply_to_wantlist(
            &wants(vec![
                entry(&have, WantType::Have, true),
                entry(&missing, WantType::Have, true),
            ]),
            &kept,
            Version::V1_2,
        );
        assert_eq!(replies.len(), 1);
        let presences = &replies[0].block_presences;
        assert_eq!(presences.len(), 2);
        assert_eq!(presences[0].cid, have.to_bytes());
        assert!(presences[0].is_have());
        assert_eq!(presences[1].cid, missing.to_bytes());
        assert!(!presences[1].is_have());
        assert!(replies[0].payload.is_empty());
    }

    #[test]
    fn a_missing_block_is_silent_unless_dont_have_was_asked_for() {
        let missing = cid_from_data(CODEC_RAW, b"no");
        let kept = KeptBlocks::default();
        assert!(reply_to_wantlist(
            &wants(vec![entry(&missing, WantType::Block, false)]),
            &kept,
            Version::V1_2
        )
        .is_empty());
        let replies = reply_to_wantlist(
            &wants(vec![entry(&missing, WantType::Block, true)]),
            &kept,
            Version::V1_2,
        );
        assert_eq!(replies[0].block_presences.len(), 1);
        assert!(!replies[0].block_presences[0].is_have());
    }

    #[test]
    fn cancels_and_unreadable_entries_are_ignored() {
        let cid = cid_from_data(CODEC_RAW, b"data");
        let kept = kept_with(&[(&cid, b"data")]);
        let mut cancel = entry(&cid, WantType::Block, true);
        cancel.cancel = true;
        let garbage = WantEntry {
            block: vec![0xff, 0xff],
            ..entry(&cid, WantType::Block, true)
        };
        assert!(reply_to_wantlist(&wants(vec![cancel, garbage]), &kept, Version::V1_2).is_empty());
        assert!(reply_to_wantlist(&BitswapMessage::default(), &kept, Version::V1_2).is_empty());
    }

    #[test]
    fn older_versions_send_bare_blocks_and_no_presences() {
        let cid = cid_from_data(CODEC_RAW, b"data");
        let missing = cid_from_data(CODEC_RAW, b"nope");
        let kept = kept_with(&[(&cid, b"data")]);
        let message = wants(vec![
            entry(&cid, WantType::Block, true),
            entry(&missing, WantType::Block, true),
        ]);
        let v10 = reply_to_wantlist(&message, &kept, Version::V1_0);
        assert_eq!(v10.len(), 1);
        assert_eq!(v10[0].blocks, vec![Bytes::from_static(b"data")]);
        assert!(v10[0].payload.is_empty() && v10[0].block_presences.is_empty());
        let v11 = reply_to_wantlist(&message, &kept, Version::V1_1);
        assert_eq!(v11[0].payload.len(), 1);
        assert!(v11[0].block_presences.is_empty());
    }

    #[test]
    fn a_cid_wanted_twice_is_answered_once() {
        let cid = cid_from_data(CODEC_RAW, b"data");
        let kept = kept_with(&[(&cid, b"data")]);
        let replies = reply_to_wantlist(
            &wants(vec![
                entry(&cid, WantType::Block, true),
                entry(&cid, WantType::Block, true),
            ]),
            &kept,
            Version::V1_2,
        );
        assert_eq!(replies[0].payload.len(), 1);
    }

    #[test]
    fn a_v0_cid_finds_the_dag_pb_block_kept_under_v1() {
        let v1 = cid_from_data(CODEC_DAG_PB, b"node");
        let v0 = Cid::new_v0(*v1.hash()).unwrap();
        let kept = kept_with(&[(&v1, b"node")]);
        let replies = reply_to_wantlist(
            &wants(vec![entry(&v0, WantType::Block, false)]),
            &kept,
            Version::V1_2,
        );
        assert_eq!(replies[0].payload[0].prefix, vec![0x00, 0x70, 0x12, 0x20]);
    }

    #[test]
    fn large_answers_split_into_several_messages() {
        let blocks: Vec<Vec<u8>> = (0..5u8).map(|i| vec![i; 600 * 1024]).collect();
        let cids: Vec<Cid> = blocks.iter().map(|b| cid_from_data(CODEC_RAW, b)).collect();
        let kept = KeptBlocks::default();
        kept.insert_all(cids.iter().copied().zip(blocks.clone()).collect())
            .unwrap();
        let replies = reply_to_wantlist(
            &wants(cids.iter().map(|c| entry(c, WantType::Block, false)).collect()),
            &kept,
            Version::V1_2,
        );
        assert!(replies.len() >= 3);
        assert_eq!(replies.iter().map(|m| m.payload.len()).sum::<usize>(), 5);
        assert!(replies.iter().all(|m| m.payload.len() <= 2));
    }

    #[test]
    fn one_block_named_under_many_codecs_is_sent_once() {
        let data = vec![7u8; 2 * 1024 * 1024];
        let raw = cid_from_data(CODEC_RAW, &data);
        let kept = KeptBlocks::default();
        kept.insert_all(vec![(raw, data)]).unwrap();
        let entries = (0..1000u64)
            .map(|codec| {
                let cid = Cid::new_v1(0x70 + (codec % 3), *raw.hash());
                entry(&cid, WantType::Block, false)
            })
            .collect();
        let replies = reply_to_wantlist(&wants(entries), &kept, Version::V1_2);
        assert_eq!(replies.iter().map(|m| m.payload.len()).sum::<usize>(), 1);
    }

    #[test]
    fn the_blocks_sent_share_the_tables_bytes() {
        let data = vec![1u8; 4096];
        let cid = cid_from_data(CODEC_RAW, &data);
        let kept = KeptBlocks::default();
        kept.insert_all(vec![(cid, data)]).unwrap();
        let replies = reply_to_wantlist(
            &wants(vec![entry(&cid, WantType::Block, false)]),
            &kept,
            Version::V1_2,
        );
        let kept_ptr = kept.get(&cid).unwrap().as_ptr();
        assert_eq!(replies[0].payload[0].data.as_ptr(), kept_ptr);
    }

    #[test]
    fn replies_stop_taking_blocks_at_the_cap() {
        let blocks: Vec<Vec<u8>> = (0..12u8).map(|i| vec![i; 1024 * 1024]).collect();
        let cids: Vec<Cid> = blocks.iter().map(|b| cid_from_data(CODEC_RAW, b)).collect();
        let kept = KeptBlocks::default();
        kept.insert_all(cids.iter().copied().zip(blocks).collect())
            .unwrap();
        let replies = reply_to_wantlist(
            &wants(cids.iter().map(|c| entry(c, WantType::Block, false)).collect()),
            &kept,
            Version::V1_2,
        );
        let sent: usize = replies
            .iter()
            .flat_map(|m| m.payload.iter())
            .map(|p| p.data.len())
            .sum();
        assert_eq!(sent, REPLY_MAX_BYTES);
    }

    #[tokio::test]
    async fn a_frame_that_claims_more_than_arrives_is_an_error() {
        let mut bytes = Vec::new();
        let mut buffer = unsigned_varint::encode::u32_buffer();
        bytes.extend_from_slice(unsigned_varint::encode::u32(
            MAX_INBOUND_MESSAGE as u32,
            &mut buffer,
        ));
        bytes.extend_from_slice(b"only a few bytes");
        let mut read = futures::io::Cursor::new(bytes);
        assert!(read_message(&mut read).await.is_err());
    }

    #[tokio::test]
    async fn messages_round_trip_through_the_framing() {
        let message = wants(vec![entry(&cid_from_data(CODEC_RAW, b"x"), WantType::Have, true)]);
        let mut wire = futures::io::Cursor::new(Vec::new());
        write_message(&mut wire, &message).await.unwrap();
        write_message(&mut wire, &message).await.unwrap();
        let mut read = futures::io::Cursor::new(wire.into_inner());
        assert_eq!(read_message(&mut read).await.unwrap(), Some(message.clone()));
        assert_eq!(read_message(&mut read).await.unwrap(), Some(message));
        assert_eq!(read_message(&mut read).await.unwrap(), None);
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused() {
        let mut bytes = Vec::new();
        let mut buffer = unsigned_varint::encode::u32_buffer();
        bytes.extend_from_slice(unsigned_varint::encode::u32(
            (MAX_INBOUND_MESSAGE + 1) as u32,
            &mut buffer,
        ));
        let mut read = futures::io::Cursor::new(bytes);
        assert!(read_message(&mut read).await.is_err());
    }
}
