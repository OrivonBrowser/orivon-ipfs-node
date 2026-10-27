# Fixtures

Offline tests build their fixtures in Rust, so they are reproducible:

- a raw CIDv1 block, and a minimal CAR v1 with one raw block (including the empty raw block),
- a dag-pb UnixFS file, and a UnixFS directory that points at a file,
- missing and malformed block cases.

Opt-in tests (marked `#[ignore]`) need the public network or a Kubo binary:

- `make kubo-parity`: with `KUBO_BIN` pointing at an `ipfs` binary, Kubo generates UnixFS directory,
  HAMT directory and CIDv0 dag-pb CARs, and the gateway's answers are compared with Kubo's: empty
  files, percent-encoded paths, fixed, open-ended and suffix byte ranges, directory listings, and
  full and ranged `HEAD`.
- `make kubo-bitswap`: a raw block is retrieved over Bitswap from a loopback Kubo daemon in an
  isolated repository.
- `make live-corpus`: the immutable `/ipfs` and DNSLink-backed `/ipns` paths listed in
  `public_corpus.txt` are fetched through the gateway. Each line is `name path min_bytes
  optional_range_bytes`, and a range byte count checks that the ranged response is a prefix of the
  full response.
- `make live-smoke`: ENS names or paths given in `ORIVON_IPFS_LIVE_ENS` or `ORIVON_IPFS_LIVE_PATHS`.
- `make local-soak` and `make live-soak`: repeated requests against generated blocks, or against the
  public network, checking that resident memory growth stays bounded on Linux.
- `ORIVON_IPFS_LIVE_DHT_CID=<cid> cargo test -p orivon-ipfs-routing live_light_dht_finds_public_providers -- --ignored --nocapture`:
  a light-DHT provider lookup for a CID that has providers on the Amino DHT.
