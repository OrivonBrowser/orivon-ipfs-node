#!/usr/bin/env bash
# Checks serve mode end to end against a scratch Kubo: the node is given a CAR, dials Kubo, and
# Kubo fetches the content over the connection the node opened.
#
#   MODE=native NODE_BIN=<path to orivon-ipfs-gateway>  orivon/serve-check.sh
#   MODE=wasm   WASMTIME=<wasmtime> WASM=dist/ipfs-node.wasm  orivon/serve-check.sh
#
# Needs `ipfs` (Kubo) on PATH, curl and python3. Everything lives under a scratch directory that
# is removed at the end; Kubo's repositories are never ~/.ipfs and its ports are random.
set -u
MODE=${MODE:-native}
IPFS=${IPFS:-ipfs}
SCRATCH=${SCRATCH:-$(mktemp -d "${TMPDIR:-/tmp}/serve-check.XXXXXX")}
KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
API="http://127.0.0.1:$PORT"
NODE_PID=""
KUBO_PID=""
FAILED=0

say() { printf '%s\n' "$*"; }
ok() { say "ok   $*"; }
bad() { say "FAIL $*"; FAILED=1; }
expect() { # expect <description> <expected> <actual>
  if [ "$2" = "$3" ]; then ok "$1 ($3)"; else bad "$1: expected '$2', got '$3'"; fi
}
cleanup() {
  [ -n "$KUBO_PID" ] && { IPFS_PATH="$SCRATCH/b" "$IPFS" shutdown >/dev/null 2>&1; sleep 1; kill "$KUBO_PID" 2>/dev/null; }
  [ -n "$NODE_PID" ] && kill "$NODE_PID" 2>/dev/null
  wait 2>/dev/null
  [ -z "${KEEP:-}" ] && rm -rf "$SCRATCH"
}
trap cleanup EXIT
mkdir -p "$SCRATCH"

# Content, built in a Kubo repository that never joins the network (repository a).
export IPFS_PATH="$SCRATCH/a"
"$IPFS" init --profile=test >/dev/null 2>&1 || { say "ipfs init failed"; exit 2; }
mkdir -p "$SCRATCH/site/sub"
echo "<h1>served from a page</h1>" > "$SCRATCH/site/index.html"
echo "nested" > "$SCRATCH/site/sub/note.txt"
head -c 700000 /dev/urandom > "$SCRATCH/site/big.bin"
ROOT=$("$IPFS" add -r -Q --cid-version=1 "$SCRATCH/site")
"$IPFS" dag export "$ROOT" > "$SCRATCH/site.car"
WANT_STAT=$("$IPFS" dag stat "$ROOT" --progress=false 2>/dev/null | grep -E 'Size|Blocks' | tr -s ' \n' ' ')
say "root $ROOT, car $(wc -c < "$SCRATCH/site.car") bytes"

# The node under test.
case "$MODE" in
  native) CMD=("${NODE_BIN:?set NODE_BIN}") ;;
  wasm) CMD=("${WASMTIME:-wasmtime}" run -S inherit-network=y -S allow-ip-name-lookup=y -S tcp=y -S udp=y "${WASM:?set WASM}") ;;
  *) say "MODE must be native or wasm"; exit 2 ;;
esac
"${CMD[@]}" --online --routing-mode offline --serve --allow-local-dial --api-key "$KEY" --addr "127.0.0.1:$PORT" \
  >"$SCRATCH/node.log" 2>&1 &
NODE_PID=$!
for _ in $(seq 1 120); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$API/health")" = 200 ] && break
  sleep 0.5
done
expect "health is open" 200 "$(curl -s -o /dev/null -w '%{http_code}' "$API/health")"

H=(-H "x-orivon-key: $KEY")
expect "id without a key" 401 "$(curl -s -o /dev/null -w '%{http_code}' "$API/id")"
expect "blocks with a wrong key" 401 "$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "x-orivon-key: ${KEY//[0-9a-f]/0}" --data-binary @"$SCRATCH/site.car" "$API/blocks")"
NODE_PEER=$(curl -s "${H[@]}" "$API/id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["peer"])')
say "node peer $NODE_PEER"

printf 'not a car' > "$SCRATCH/junk"
expect "junk CAR is refused" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST "${H[@]}" --data-binary @"$SCRATCH/junk" "$API/blocks")"
python3 - "$SCRATCH/site.car" "$SCRATCH/tampered.car" <<'PY'
import sys
d = bytearray(open(sys.argv[1], "rb").read()); d[-1] ^= 1; open(sys.argv[2], "wb").write(d)
PY
expect "tampered CAR is refused" 400 "$(curl -s -o /dev/null -w '%{http_code}' -X POST "${H[@]}" --data-binary @"$SCRATCH/tampered.car" "$API/blocks")"
RESULT=$(curl -s -X POST "${H[@]}" --data-binary @"$SCRATCH/site.car" "$API/blocks")
say "POST /blocks -> $RESULT"
expect "root reported" "$ROOT" "$(printf '%s' "$RESULT" | python3 -c 'import json,sys; print(json.load(sys.stdin)["roots"][0])')"

# Without --allow-local-dial the node refuses loopback; the unit tests cover that, so the check
# only exercises the flag a local setup needs.
# Kubo with no bootstrap and no way to learn about the content except from the node (repository b).
export IPFS_PATH="$SCRATCH/b"
"$IPFS" init --profile=test >/dev/null 2>&1
"$IPFS" bootstrap rm --all >/dev/null 2>&1
"$IPFS" config --json Routing.Type '"none"' >/dev/null 2>&1
"$IPFS" config --json Swarm.Transports.Network.QUIC false >/dev/null 2>&1
"$IPFS" config --json Swarm.Transports.Network.WebTransport false >/dev/null 2>&1
"$IPFS" config --json Swarm.Transports.Network.WebRTCDirect false >/dev/null 2>&1
"$IPFS" config --json Addresses.Swarm '["/ip4/127.0.0.1/tcp/0"]' >/dev/null 2>&1
"$IPFS" daemon >"$SCRATCH/kubo.log" 2>&1 &
KUBO_PID=$!
for _ in $(seq 1 120); do
  [ -s "$SCRATCH/b/api" ] && "$IPFS" id >/dev/null 2>&1 && break
  sleep 0.5
done
KUBO_PEER=$("$IPFS" id -f '<id>')
KUBO_ADDR=$("$IPFS" id -f '<addrs>' | grep -E '^/ip4/127\.0\.0\.1/tcp/' | head -1)
say "kubo $KUBO_ADDR"
case "$($IPFS cat "$ROOT/index.html" --timeout 2s 2>&1)" in *"served from a page"*) bad "kubo had the content before the dial" ;; *) ok "kubo does not have the content yet" ;; esac

DIAL=$(curl -s -m 40 -X POST "${H[@]}" -d "{\"addr\":\"$KUBO_ADDR\"}" "$API/dial")
say "POST /dial -> $DIAL"
expect "dial answered with kubo's peer" "$KUBO_PEER" "$(printf '%s' "$DIAL" | python3 -c 'import json,sys; print(json.load(sys.stdin)["peer"])' 2>/dev/null)"

"$IPFS" swarm peers | grep -q "$NODE_PEER" && ok "kubo lists the node as a peer" || bad "kubo does not list the node"
"$IPFS" id "$NODE_PEER" -f '<protocols>' --timeout 5s 2>/dev/null | grep -q bitswap && ok "identify advertises bitswap" || say "note: could not read the node's protocols from kubo"

"$IPFS" block get "$ROOT" --timeout 30s >"$SCRATCH/root.block" 2>"$SCRATCH/err"; expect "block get root" 0 "$?"
"$IPFS" ls "$ROOT" --timeout 30s >"$SCRATCH/ls.txt" 2>&1; expect "ls root" 0 "$?"
cat "$SCRATCH/ls.txt"
"$IPFS" cat "$ROOT/index.html" --timeout 30s | cmp - "$SCRATCH/site/index.html" && ok "index.html matches" || bad "index.html differs"
"$IPFS" cat "$ROOT/sub/note.txt" --timeout 30s | cmp - "$SCRATCH/site/sub/note.txt" && ok "sub/note.txt matches" || bad "sub/note.txt differs"
"$IPFS" cat "$ROOT/big.bin" --timeout 60s | cmp - "$SCRATCH/site/big.bin" && ok "big.bin (several blocks) matches" || bad "big.bin differs"
GOT_STAT=$("$IPFS" dag stat "$ROOT" --progress=false --timeout 30s 2>/dev/null | grep -E 'Size|Blocks' | tr -s ' \n' ' ')
expect "dag stat matches" "$WANT_STAT" "$GOT_STAT"

# The node re-dials a peer that dropped the connection.
"$IPFS" swarm disconnect "/p2p/$NODE_PEER" >/dev/null 2>&1
REDIALED=no
for _ in $(seq 1 30); do
  "$IPFS" swarm peers | grep -q "$NODE_PEER" && { REDIALED=yes; break; }
  sleep 1
done
expect "node redialed kubo after a disconnect" yes "$REDIALED"

if [ "$FAILED" = 0 ]; then say "serve-check ($MODE): all passed"; else say "serve-check ($MODE): FAILED (logs: $SCRATCH, rerun with KEEP=1)"; fi
exit "$FAILED"
