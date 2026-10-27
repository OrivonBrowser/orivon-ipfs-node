//! CARv1 parsing for `POST /blocks`: every block is checked against its CID before anything is kept.

use cid::Cid;
use orivon_ipfs_core::{CODEC_DAG_PB, CODEC_RAW, DEFAULT_MAX_BLOCK_SIZE, HASH_SHA2_256};
use multihash_codetable::{Code, MultihashDigest};
use serde::Deserialize;
use std::io::Cursor;

/// Largest CARv1 header accepted. Real headers hold a few roots.
const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CarError {
    #[error("not a CARv1 file: {0}")]
    Malformed(String),
    #[error("block {cid} has an unsupported codec 0x{codec:x} (raw and dag-pb only)")]
    UnsupportedCodec { cid: String, codec: u64 },
    #[error("block {cid} has an unsupported hash 0x{code:x} (sha2-256 only)")]
    UnsupportedHash { cid: String, code: u64 },
    #[error("block {cid} is larger than {max} bytes")]
    BlockTooLarge { cid: String, max: usize },
    #[error("block {cid} does not match its hash")]
    HashMismatch { cid: String },
}

#[derive(Debug)]
pub struct VerifiedCar {
    pub roots: Vec<Cid>,
    pub blocks: Vec<(Cid, Vec<u8>)>,
}

#[derive(Deserialize)]
struct Header {
    roots: Vec<Cid>,
    version: u64,
}

fn malformed(reason: &str) -> CarError {
    CarError::Malformed(reason.to_string())
}

fn read_varint(bytes: &[u8], offset: &mut usize) -> Result<usize, CarError> {
    let rest = bytes.get(*offset..).unwrap_or_default();
    let (value, tail) = unsigned_varint::decode::u64(rest).map_err(|_| malformed("bad length"))?;
    *offset += rest.len() - tail.len();
    usize::try_from(value).map_err(|_| malformed("length too large"))
}

/// Takes `len` bytes at `offset`, or fails; no sum of untrusted lengths can overflow.
fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8], CarError> {
    let end = offset
        .checked_add(len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| malformed("section runs past the end"))?;
    let section = &bytes[*offset..end];
    *offset = end;
    Ok(section)
}

pub fn verify_block(cid: &Cid, data: &[u8]) -> Result<(), CarError> {
    let shown = || cid.to_string();
    let codec = cid.codec();
    if codec != CODEC_RAW && codec != CODEC_DAG_PB {
        return Err(CarError::UnsupportedCodec {
            cid: shown(),
            codec,
        });
    }
    let hash = cid.hash();
    if hash.code() != HASH_SHA2_256 || hash.digest().len() != 32 {
        return Err(CarError::UnsupportedHash {
            cid: shown(),
            code: hash.code(),
        });
    }
    if data.len() > DEFAULT_MAX_BLOCK_SIZE {
        return Err(CarError::BlockTooLarge {
            cid: shown(),
            max: DEFAULT_MAX_BLOCK_SIZE,
        });
    }
    if Code::Sha2_256.digest(data).digest() != hash.digest() {
        return Err(CarError::HashMismatch { cid: shown() });
    }
    Ok(())
}

fn parse_header(bytes: &[u8], offset: &mut usize) -> Result<Header, CarError> {
    let header_len = read_varint(bytes, offset)?;
    if header_len == 0 || header_len > MAX_HEADER_BYTES {
        return Err(malformed("bad header length"));
    }
    let header_bytes = take(bytes, offset, header_len)?;
    let header: Header = serde_ipld_dagcbor::from_slice(header_bytes)
        .map_err(|_| malformed("header is not a CAR header"))?;
    if header.version != 1 {
        return Err(malformed("only CAR version 1 is supported"));
    }
    Ok(header)
}

/// The next verified block, or `None` at the end of the file.
fn next_block(bytes: &[u8], offset: &mut usize) -> Result<Option<(Cid, Vec<u8>)>, CarError> {
    if *offset >= bytes.len() {
        return Ok(None);
    }
    let section_len = read_varint(bytes, offset)?;
    if section_len == 0 {
        return Err(malformed("empty block section"));
    }
    let section = take(bytes, offset, section_len)?;
    let mut cursor = Cursor::new(section);
    let cid = Cid::read_bytes(&mut cursor).map_err(|_| malformed("bad block cid"))?;
    let data = &section[cursor.position() as usize..];
    verify_block(&cid, data)?;
    Ok(Some((cid, data.to_vec())))
}

pub fn parse_and_verify_car(bytes: &[u8]) -> Result<VerifiedCar, CarError> {
    let mut offset = 0;
    let header = parse_header(bytes, &mut offset)?;
    let mut blocks = Vec::new();
    while let Some(block) = next_block(bytes, &mut offset)? {
        blocks.push(block);
    }
    Ok(VerifiedCar {
        roots: header.roots,
        blocks,
    })
}

/// Hashing between yields, so a large CAR does not hold the single-threaded runtime for long.
const YIELD_EVERY_BYTES: usize = 256 * 1024;

/// [`parse_and_verify_car`] that gives the runtime back every [`YIELD_EVERY_BYTES`] hashed.
pub async fn parse_and_verify_car_yielding(bytes: &[u8]) -> Result<VerifiedCar, CarError> {
    let mut offset = 0;
    let header = parse_header(bytes, &mut offset)?;
    let mut blocks = Vec::new();
    let mut since_yield = 0usize;
    while let Some((cid, data)) = next_block(bytes, &mut offset)? {
        since_yield += data.len();
        blocks.push((cid, data));
        if since_yield >= YIELD_EVERY_BYTES {
            since_yield = 0;
            tokio::task::yield_now().await;
        }
    }
    Ok(VerifiedCar {
        roots: header.roots,
        blocks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orivon_ipfs_core::{cid_from_data, encode_varint};
    use multihash::Multihash;

    // {"roots": [], "version": 1}
    const EMPTY_ROOTS_HEADER: &[u8] = &[
        0xa2, 0x65, b'r', b'o', b'o', b't', b's', 0x80, 0x67, b'v', b'e', b'r', b's', b'i', b'o',
        b'n', 0x01,
    ];

    fn header_with_root(root: &Cid) -> Vec<u8> {
        let mut out = vec![0xa2, 0x65, b'r', b'o', b'o', b't', b's', 0x81];
        let cid_bytes = root.to_bytes();
        out.extend_from_slice(&[0xd8, 0x2a, 0x58, (cid_bytes.len() + 1) as u8, 0x00]);
        out.extend_from_slice(&cid_bytes);
        out.extend_from_slice(&[0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n', 0x01]);
        out
    }

    fn car(header: &[u8], sections: &[(Cid, Vec<u8>)]) -> Vec<u8> {
        let mut out = encode_varint(header.len());
        out.extend_from_slice(header);
        for (cid, data) in sections {
            let mut section = cid.to_bytes();
            section.extend_from_slice(data);
            out.extend_from_slice(&encode_varint(section.len()));
            out.extend_from_slice(&section);
        }
        out
    }

    #[test]
    fn accepts_a_good_car_and_returns_its_roots() {
        let one = b"hello".to_vec();
        let two = b"world".to_vec();
        let c1 = cid_from_data(CODEC_RAW, &one);
        let c2 = cid_from_data(CODEC_DAG_PB, &two);
        let parsed =
            parse_and_verify_car(&car(&header_with_root(&c2), &[(c1, one), (c2, two)])).unwrap();
        assert_eq!(parsed.roots, vec![c2]);
        assert_eq!(parsed.blocks.len(), 2);
        assert_eq!(parsed.blocks[0].0, c1);
    }

    #[test]
    fn accepts_a_car_with_no_blocks() {
        let parsed = parse_and_verify_car(&car(EMPTY_ROOTS_HEADER, &[])).unwrap();
        assert!(parsed.roots.is_empty());
        assert!(parsed.blocks.is_empty());
    }

    #[test]
    fn refuses_a_block_whose_hash_does_not_match() {
        let good = b"hello".to_vec();
        let cid = cid_from_data(CODEC_RAW, &good);
        let err = parse_and_verify_car(&car(EMPTY_ROOTS_HEADER, &[(cid, b"hellO".to_vec())]))
            .unwrap_err();
        assert!(matches!(err, CarError::HashMismatch { .. }), "{err}");
    }

    #[test]
    fn refuses_a_codec_other_than_raw_and_dag_pb() {
        let data = b"{}".to_vec();
        let cid = cid_from_data(0x71, &data);
        let err = parse_and_verify_car(&car(EMPTY_ROOTS_HEADER, &[(cid, data)])).unwrap_err();
        assert!(
            matches!(err, CarError::UnsupportedCodec { codec: 0x71, .. }),
            "{err}"
        );
    }

    #[test]
    fn refuses_a_hash_other_than_sha2_256() {
        let data = b"abc".to_vec();
        let digest = Multihash::<64>::wrap(0x00, &data).unwrap();
        let cid = Cid::new_v1(CODEC_RAW, digest);
        let err = parse_and_verify_car(&car(EMPTY_ROOTS_HEADER, &[(cid, data)])).unwrap_err();
        assert!(matches!(err, CarError::UnsupportedHash { .. }), "{err}");
    }

    #[test]
    fn refuses_garbage_and_truncation_without_panicking() {
        for bytes in [
            &b""[..],
            &b"\x00"[..],
            &b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01abc"[..],
            &b"\x05hello"[..],
        ] {
            assert!(parse_and_verify_car(bytes).is_err());
        }
        let data = b"hello".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        let mut whole = car(EMPTY_ROOTS_HEADER, &[(cid, data)]);
        whole.truncate(whole.len() - 2);
        assert!(parse_and_verify_car(&whole).is_err());
        let mut huge = encode_varint(EMPTY_ROOTS_HEADER.len());
        huge.extend_from_slice(EMPTY_ROOTS_HEADER);
        huge.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
        assert!(parse_and_verify_car(&huge).is_err());
    }

    #[test]
    fn refuses_another_car_version() {
        // {"roots": [], "version": 2}
        let header = [
            0xa2, 0x65, b'r', b'o', b'o', b't', b's', 0x80, 0x67, b'v', b'e', b'r', b's', b'i',
            b'o', b'n', 0x02,
        ];
        assert!(parse_and_verify_car(&car(&header, &[])).is_err());
    }

    #[tokio::test]
    async fn the_yielding_parser_gives_the_same_answer() {
        let blocks: Vec<Vec<u8>> = (0..6u8).map(|i| vec![i; 100 * 1024]).collect();
        let sections: Vec<(Cid, Vec<u8>)> = blocks
            .into_iter()
            .map(|data| (cid_from_data(CODEC_RAW, &data), data))
            .collect();
        let file = car(EMPTY_ROOTS_HEADER, &sections);
        let plain = parse_and_verify_car(&file).unwrap();
        let yielding = parse_and_verify_car_yielding(&file).await.unwrap();
        assert_eq!(plain.blocks, yielding.blocks);
        let mut bad = file.clone();
        let last = bad.len() - 1;
        bad[last] ^= 1;
        assert!(parse_and_verify_car_yielding(&bad).await.is_err());
    }
}
