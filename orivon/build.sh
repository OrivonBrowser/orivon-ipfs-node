#!/bin/sh
# Builds orivon-ipfs-gateway for wasm32-wasip2 (release) and transpiles it for Orivon's WASI host.
#
# Every path comes from an environment variable. Required:
#   RUSTUP_HOME     a rustup install whose `stable` toolchain has the wasm32-wasip2 std
#   CARGO_HOME      cargo's home (registry cache)
#   WASI_SDK_PATH   wasi-sdk 34 or later: the C compiler for the dependencies that carry C (SQLite)
#   ORIVON_JCO_DIR  an npm prefix with @bytecodealliance/jco-transpile@0.15.0 installed
#                   (never a dependency of this repository)
# Optional:
#   CARGO_TARGET_DIR  default <repository>/target
#   OUT_DIR           default <repository>/dist
#   CARGO_JOBS        default 2
#
# Versions this was built and checked with: rustc 1.98.1 (stable), wasi-sdk 34,
# wasmtime 49, jco-transpile 0.15.0 (jco 1.35.0), node 22 or later.
set -eu

: "${RUSTUP_HOME:?set RUSTUP_HOME}"
: "${CARGO_HOME:?set CARGO_HOME}"
: "${WASI_SDK_PATH:?set WASI_SDK_PATH}"
: "${ORIVON_JCO_DIR:?set ORIVON_JCO_DIR}"

HERE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO="$(dirname -- "$HERE")"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO/target}"
OUT_DIR="${OUT_DIR:-$REPO/dist}"
CARGO_JOBS="${CARGO_JOBS:-2}"

export CARGO_TARGET_DIR
export PATH="$RUSTUP_HOME/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH"
export CC_wasm32_wasip2="$WASI_SDK_PATH/bin/clang"
export AR_wasm32_wasip2="$WASI_SDK_PATH/bin/llvm-ar"
# The published files must not carry the builder's directories: every local path is mapped to a
# fixed name, and symbol names are stripped.
export CFLAGS_wasm32_wasip2="--sysroot=$WASI_SDK_PATH/share/wasi-sysroot -Wno-error=incompatible-pointer-types -ffile-prefix-map=$CARGO_HOME=/cargo -ffile-prefix-map=$REPO=/src -ffile-prefix-map=$WASI_SDK_PATH=/wasi-sdk"
# tokio refuses its `net` feature on wasm32-wasip2 unless this cfg is set.
export RUSTFLAGS="--cfg tokio_unstable -C strip=symbols --remap-path-prefix=$CARGO_HOME=/cargo --remap-path-prefix=$RUSTUP_HOME=/rustup --remap-path-prefix=$REPO=/src"

( cd "$REPO" && cargo build --target wasm32-wasip2 -p orivon-ipfs-gateway --bin orivon-ipfs-gateway --release -j "$CARGO_JOBS" )

WASM="$CARGO_TARGET_DIR/wasm32-wasip2/release/orivon-ipfs-gateway.wasm"
[ -f "$WASM" ] || { echo "build.sh: $WASM was not produced" >&2; exit 1; }

mkdir -p "$OUT_DIR"
cp "$WASM" "$OUT_DIR/ipfs-node.wasm"
cp "$REPO/LICENSE-MIT" "$OUT_DIR/LICENSE-MIT"
cp "$REPO/LICENSE-APACHE" "$OUT_DIR/LICENSE-APACHE"

# Transpile exactly as the host's own transpile command does; see transpile.mjs.
node "$HERE/transpile.mjs" "$OUT_DIR/ipfs-node.wasm" "$OUT_DIR/ipfs-node.p2" ipfs-node

echo "build.sh: $OUT_DIR/ipfs-node.wasm and $OUT_DIR/ipfs-node.p2/ written"
