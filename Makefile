LIVE_ENS ?= vitalik.eth,daicowtf.eth
KUBO_BIN ?= target/tools/kubo/kubo/ipfs

.PHONY: test fmt clippy verify local-soak live-smoke live-corpus live-soak kubo-parity kubo-bitswap clean

test:
	cargo test --workspace

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

verify: test clippy

local-soak:
	cargo test -p orivon-ipfs-gateway --test local_soak -- --ignored --nocapture

live-smoke:
	ORIVON_IPFS_LIVE_ENS="$(LIVE_ENS)" cargo test -p orivon-ipfs-gateway --test live_smoke -- --ignored --nocapture

live-corpus:
	cargo test -p orivon-ipfs-gateway --test public_corpus -- --ignored --nocapture

live-soak:
	cargo test -p orivon-ipfs-gateway --test live_soak -- --ignored --nocapture

kubo-parity:
	KUBO_BIN="$(KUBO_BIN)" cargo test -p orivon-ipfs-gateway --test kubo_parity -- --ignored --nocapture

kubo-bitswap:
	KUBO_BIN="$(KUBO_BIN)" cargo test -p orivon-ipfs-retrieval kubo_bitswap_retrieves_raw_block_from_loopback_daemon -- --ignored --nocapture

clean:
	cargo clean
