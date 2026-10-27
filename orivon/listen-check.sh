#!/usr/bin/env bash
# Checks serve mode with a listener (--p2p-port) end to end against scratch Kubo nodes.
#
#   MODE=native NODE_BIN=<path to orivon-ipfs-gateway>  orivon/listen-check.sh
#   MODE=wasm   WASMTIME=<wasmtime> WASM=dist/ipfs-node.wasm  orivon/listen-check.sh
#
# Part 1, inbound (loopback): the node listens, a Kubo that has nothing but the node's address
# dials it and reads a file the node holds (`ipfs cat`).
#
# Part 2, reachability and announcing, in private network namespaces (Linux, no root: `unshare
# -rn`). Two Kubo nodes with public-looking addresses (93.184.216.1 and .3) form the DHT and are
# the AutoNAT servers, and the node runs in a second namespace behind a virtual cable
# (93.184.216.2). Nothing leaves the machine and no real network is touched. The node is started
# the way it is in use, bootstrapped from the two Kubo nodes and with no test switch:
#   - it learns its address from the two Kubo nodes, Kubo's AutoNAT dials it back, and `GET
#     /status` turns `public` with the address confirmed;
#   - before that nothing is announced;
#   - `POST /provide` is then announced, the other Kubo finds the node with `ipfs routing
#     findprovs` after every connection to the node was cut, and fetches the file from it.
#
# Part 3, the same with the node's port closed (a firewall rule in its namespace): AutoNAT says
# `private`, nothing is announced, and the other Kubo finds no provider. Takes about two minutes;
# SKIP_PRIVATE=1 leaves it out.
#
# Without unprivileged network namespaces parts 2 and 3 are skipped, and say so.
#
# Needs `ipfs` (Kubo) on PATH or $IPFS, curl and python3, and for parts 2 and 3 unshare, nsenter,
# ip and iptables. Everything lives under a scratch directory that is removed at the end. Kubo's
# repositories are never ~/.ipfs and every port is random.
set -u
MODE=${MODE:-native}
IPFS=${IPFS:-ipfs}
PART=${PART:-1}
SCRATCH=${SCRATCH:-$(mktemp -d "${TMPDIR:-/tmp}/listen-check.XXXXXX")}
KEY=${KEY:-$(python3 -c 'import secrets; print(secrets.token_hex(32))')}
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
FAILED=0
PIDS=()

say() { printf '%s\n' "$*"; }
ok() { say "ok   $*"; }
bad() { say "FAIL $*"; FAILED=1; }
expect() { # expect <description> <expected> <actual>
  if [ "$2" = "$3" ]; then ok "$1 ($3)"; else bad "$1: expected '$2', got '$3'"; fi
}
cleanup() {
  for repo in b c d; do
    [ -s "$SCRATCH/$repo/api" ] && IPFS_PATH="$SCRATCH/$repo" "$IPFS" shutdown >/dev/null 2>&1
  done
  sleep 1
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null; done
  wait 2>/dev/null
  [ -z "${KEEP:-}" ] && rm -rf "$SCRATCH"
}
trap cleanup EXIT
mkdir -p "$SCRATCH"

case "$MODE" in
  native) CMD=("${NODE_BIN:?set NODE_BIN}") ;;
  wasm) CMD=("${WASMTIME:-wasmtime}" run -S inherit-network=y -S allow-ip-name-lookup=y -S tcp=y -S udp=y "${WASM:?set WASM}") ;;
  *) say "MODE must be native or wasm"; exit 2 ;;
esac

# A Kubo repository that only talks TCP and starts from no peer.
kubo_init() { # kubo_init <repo> <routing type> [swarm address]
  export IPFS_PATH="$SCRATCH/$1"
  "$IPFS" init --profile=test >/dev/null 2>&1 || { say "ipfs init failed"; exit 2; }
  "$IPFS" bootstrap rm --all >/dev/null 2>&1
  "$IPFS" config --json Routing.Type "\"$2\"" >/dev/null 2>&1
  "$IPFS" config --json AutoConf.Enabled false >/dev/null 2>&1
  "$IPFS" config --json Discovery.MDNS.Enabled false >/dev/null 2>&1
  "$IPFS" config --json Swarm.DisableNatPortMap true >/dev/null 2>&1
  "$IPFS" config --json Swarm.RelayClient.Enabled false >/dev/null 2>&1
  "$IPFS" config --json Swarm.Transports.Network.QUIC false >/dev/null 2>&1
  "$IPFS" config --json Swarm.Transports.Network.WebTransport false >/dev/null 2>&1
  "$IPFS" config --json Swarm.Transports.Network.WebRTCDirect false >/dev/null 2>&1
  "$IPFS" config --json Swarm.Transports.Network.Websocket false >/dev/null 2>&1
  "$IPFS" config --json Addresses.Swarm "[\"${3:-/ip4/127.0.0.1/tcp/0}\"]" >/dev/null 2>&1
  "$IPFS" config Addresses.API /ip4/127.0.0.1/tcp/0 >/dev/null 2>&1
  "$IPFS" config Addresses.Gateway /ip4/127.0.0.1/tcp/0 >/dev/null 2>&1
}
kubo_start() { # kubo_start <repo>
  export IPFS_PATH="$SCRATCH/$1"
  "$IPFS" daemon >"$SCRATCH/$1.log" 2>&1 &
  PIDS+=("$!")
  for _ in $(seq 1 120); do
    [ -s "$SCRATCH/$1/api" ] && "$IPFS" id >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  say "kubo $1 did not start"; exit 2
}
kubo_addr() { IPFS_PATH="$SCRATCH/$1" "$IPFS" id -f '<addrs>' | grep -E "^/ip4/${2:-127\.0\.0\.1}/tcp/" | head -1; }
kubo_peer() { IPFS_PATH="$SCRATCH/$1" "$IPFS" id -f '<id>'; }
ipfs_as() { local repo=$1; shift; IPFS_PATH="$SCRATCH/$repo" "$IPFS" "$@"; }

# Starts the node. NODE_PREFIX (an array) runs it elsewhere, in another network namespace.
NODE_PREFIX=()
NODE_HOST=127.0.0.1
start_node() { # start_node <extra node arguments...>
  API_PORT=$(free_port); P2P_PORT=$(free_port); API="http://$NODE_HOST:$API_PORT"
  "${NODE_PREFIX[@]}" "${CMD[@]}" --serve --api-key "$KEY" --addr "$NODE_HOST:$API_PORT" --p2p-port "$P2P_PORT" \
    "$@" >"$SCRATCH/node.log" 2>&1 &
  NODE_PID=$!
  PIDS+=("$NODE_PID")
  for _ in $(seq 1 240); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$API/health")" = 200 ] && break
    sleep 0.5
  done
  H=(-H "x-orivon-key: $KEY")
  NODE_PEER=$(curl -s "${H[@]}" "$API/id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["peer"])')
  NODE_ADDR="/ip4/$NODE_HOST/tcp/$P2P_PORT/p2p/$NODE_PEER"
}
json_get() { python3 -c 'import json,sys; d=json.load(sys.stdin)
for k in sys.argv[1:]: d=d[int(k)] if isinstance(d,list) else d[k]
print(d if isinstance(d,str) else json.dumps(d))' "$@"; }
features() { curl -s -D - -o /dev/null "$API/health" | tr -d '\r' | awk -F': ' 'tolower($1)=="x-orivon-node-features"{print $2}'; }
status_until() { # status_until <seconds> <python condition on the status d>; sets STATUS
  local end=$((SECONDS + $1))
  while :; do
    STATUS=$(curl -s "${H[@]}" "$API/status")
    printf '%s' "$STATUS" | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if ('"$2"') else 1)' 2>/dev/null && return 0
    [ $SECONDS -ge $end ] && return 1
    sleep 1
  done
}
post_blocks() { curl -s -X POST "${H[@]}" --data-binary @"$1" "$API/blocks" >/dev/null; }
provide_status() { curl -s -o /dev/null -w '%{http_code}' -X POST "${H[@]}" -d "{\"cid\":\"$1\"}" "$API/provide"; }

########################################################################################
# Part 2 and 3 run in a child of this script that is alone in a network namespace.
########################################################################################
if [ "$PART" = 2 ]; then
  KEEP=1  # the parent removes the scratch directory
  ip link set lo up
  # A second namespace for the node, joined to this one by a virtual cable.
  unshare -n sleep 3600 &
  NS_PID=$!
  PIDS+=("$NS_PID")
  for _ in $(seq 1 50); do
    [ "$(readlink /proc/$NS_PID/ns/net)" != "$(readlink /proc/$$/ns/net)" ] && break
    sleep 0.1
  done
  ip link add vA type veth peer name vB || { say "no virtual cable"; exit 3; }
  ip link set vB netns "$NS_PID"
  ip addr add 93.184.216.1/24 dev vA
  ip addr add 93.184.216.3/24 dev vA
  ip link set vA up
  nsenter -t "$NS_PID" -n bash -c 'ip link set lo up && ip addr add 93.184.216.2/24 dev vB && ip link set vB up'
  NODE_PREFIX=(nsenter -t "$NS_PID" -n --)
  NODE_HOST=93.184.216.2

  say "== part 2: reachability and announcing =="
  kubo_init c dhtserver /ip4/93.184.216.1/tcp/0
  kubo_init d dhtserver /ip4/93.184.216.3/tcp/0
  kubo_start c
  kubo_start d
  C_ADDR=$(kubo_addr c 93\\.184\\.216\\.1); C_PEER=$(kubo_peer c)
  D_ADDR=$(kubo_addr d 93\\.184\\.216\\.3); D_PEER=$(kubo_peer d)
  ipfs_as d swarm connect "$C_ADDR/p2p/$C_PEER" >/dev/null 2>&1; expect "the two kubo nodes connect" 0 "$?"
  sleep 2

  start_node --online --routing-mode light-dht --test-bootstrap "$C_ADDR/p2p/$C_PEER,$D_ADDR/p2p/$D_PEER"
  expect "features with a DHT" "serve,listen,provide" "$(features)"
  post_blocks "$SCRATCH/site.car"
  expect "provide a kept block" 202 "$(provide_status "$ROOT")"
  STATUS=$(curl -s "${H[@]}" "$API/status")
  expect "reachability starts unknown" unknown "$(printf '%s' "$STATUS" | json_get reachability)"
  expect "nothing announced yet" null "$(printf '%s' "$STATUS" | json_get providing 0 announced)"

  if status_until 150 "d['reachability']=='public'"; then
    ok "AutoNAT confirms the node is reachable"
  else
    bad "the node never became public"
  fi
  say "GET /status -> $STATUS"
  printf '%s' "$STATUS" | json_get external | grep -q "\"/ip4/93.184.216.2/tcp/$P2P_PORT\"" && ok "the confirmed address is listed" || bad "the confirmed address is not listed"
  if status_until 90 "d['providing'][0]['announced'] is not None"; then
    ok "the node reports the announcement ($(printf '%s' "$STATUS" | json_get providing 0 announced))"
  else
    bad "the node never reported an announcement"
  fi

  # Cut every connection to the node, so the fetch has to find it through the DHT.
  for repo in c d; do ipfs_as $repo swarm disconnect "/p2p/$NODE_PEER" >/dev/null 2>&1; done
  ipfs_as d swarm peers | grep -q "$NODE_PEER" && bad "kubo d is still connected to the node" || ok "kubo d is not connected to the node"
  FOUND=$(ipfs_as d routing findprovs "$ROOT" --num-providers 3 --timeout 40s 2>/dev/null | tr '\n' ' ')
  say "findprovs -> $FOUND"
  case "$FOUND" in *"$NODE_PEER"*) ok "findprovs returns the node" ;; *) bad "findprovs does not return the node" ;; esac
  ipfs_as d cat "$ROOT/index.html" --timeout 60s | cmp - "$SCRATCH/site/index.html" && ok "kubo d fetches index.html through the DHT" || bad "kubo d could not fetch index.html"
  ipfs_as d cat "$ROOT/big.bin" --timeout 60s | cmp - "$SCRATCH/site/big.bin" && ok "kubo d fetches big.bin" || bad "kubo d could not fetch big.bin"

  # DELETE stops the announcement and the CID leaves the status.
  expect "delete a provided CID" 200 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "${H[@]}" "$API/provide/$ROOT")"
  expect "delete again" 404 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "${H[@]}" "$API/provide/$ROOT")"
  expect "nothing provided now" "[]" "$(curl -s "${H[@]}" "$API/status" | json_get providing)"
  kill "$NODE_PID" 2>/dev/null; wait "$NODE_PID" 2>/dev/null

  if [ -z "${SKIP_PRIVATE:-}" ]; then
    say "== part 3: a closed port =="
    start_node --online --routing-mode light-dht --test-bootstrap "$C_ADDR/p2p/$C_PEER,$D_ADDR/p2p/$D_PEER"
    nsenter -t "$NS_PID" -n iptables -A INPUT -p tcp --dport "$P2P_PORT" -j REJECT --reject-with tcp-reset \
      || { say "iptables is not available"; FAILED=1; }
    post_blocks "$SCRATCH/site2.car"
    expect "provide a kept block" 202 "$(provide_status "$ROOT2")"
    if status_until 200 "d['reachability']=='private'"; then
      ok "AutoNAT says the node cannot be reached"
    else
      bad "the node never turned private"
    fi
    say "GET /status -> $STATUS"
    expect "nothing confirmed" "[]" "$(printf '%s' "$STATUS" | json_get external)"
    expect "nothing announced" null "$(printf '%s' "$STATUS" | json_get providing 0 announced)"
    sleep 5
    FOUND=$(ipfs_as d routing findprovs "$ROOT2" --num-providers 1 --timeout 15s 2>/dev/null | tr '\n' ' ')
    expect "kubo finds no provider" "" "${FOUND% }"
  fi
  [ "$FAILED" = 0 ] || exit 1
  exit 0
fi

# Content, built in a Kubo repository that never joins any network (repository a).
kubo_init a none
mkdir -p "$SCRATCH/site/sub" "$SCRATCH/site2"
echo "<h1>served from a computer that listens</h1>" > "$SCRATCH/site/index.html"
echo "nested" > "$SCRATCH/site/sub/note.txt"
head -c 700000 /dev/urandom > "$SCRATCH/site/big.bin"
echo "a second share, for a node nobody can reach" > "$SCRATCH/site2/index.html"
ROOT=$(ipfs_as a add -r -Q --cid-version=1 --offline "$SCRATCH/site")
ROOT2=$(ipfs_as a add -r -Q --cid-version=1 --offline "$SCRATCH/site2")
ipfs_as a dag export "$ROOT" > "$SCRATCH/site.car"
ipfs_as a dag export "$ROOT2" > "$SCRATCH/site2.car"
say "root $ROOT, car $(wc -c < "$SCRATCH/site.car") bytes"

########################################################################################
# Part 1: a peer that only has the node's address reads from it.
########################################################################################
say "== part 1: inbound =="
start_node --online --routing-mode offline
expect "health is open" 200 "$(curl -s -o /dev/null -w '%{http_code}' "$API/health")"
expect "features without a DHT" "serve,listen" "$(features)"
expect "status needs the key" 401 "$(curl -s -o /dev/null -w '%{http_code}' "$API/status")"
expect "provide needs the key" 401 "$(curl -s -o /dev/null -w '%{http_code}' -X POST -d '{}' "$API/provide")"
RESULT=$(curl -s -X POST "${H[@]}" --data-binary @"$SCRATCH/site.car" "$API/blocks")
expect "root kept" "$ROOT" "$(printf '%s' "$RESULT" | json_get roots 0)"

STATUS=$(curl -s "${H[@]}" "$API/status")
say "GET /status -> $STATUS"
expect "status names the peer" "$NODE_PEER" "$(printf '%s' "$STATUS" | json_get peer)"
printf '%s' "$STATUS" | json_get listen | grep -q "/ip4/127.0.0.1/tcp/$P2P_PORT\|/ip4/0.0.0.0/tcp/$P2P_PORT" && ok "status lists the listen address" || bad "status does not list the listen address"
expect "reachability unknown without a DHT" unknown "$(printf '%s' "$STATUS" | json_get reachability)"

kubo_init b none
kubo_start b
case "$(ipfs_as b cat "$ROOT/index.html" --timeout 2s 2>&1)" in *"computer that listens"*) bad "kubo had the content before connecting" ;; *) ok "kubo does not have the content yet" ;; esac
ipfs_as b swarm connect "$NODE_ADDR" >/dev/null 2>&1; expect "kubo connects to the node" 0 "$?"
ipfs_as b swarm peers | grep -q "$NODE_PEER" && ok "kubo lists the node as a peer" || bad "kubo does not list the node"
ipfs_as b id "$NODE_PEER" -f '<protocols>' --timeout 5s 2>/dev/null | grep -q bitswap && ok "identify advertises bitswap" || say "note: could not read the node's protocols from kubo"
ipfs_as b cat "$ROOT/index.html" --timeout 30s | cmp - "$SCRATCH/site/index.html" && ok "index.html matches" || bad "index.html differs"
ipfs_as b cat "$ROOT/sub/note.txt" --timeout 30s | cmp - "$SCRATCH/site/sub/note.txt" && ok "sub/note.txt matches" || bad "sub/note.txt differs"
ipfs_as b cat "$ROOT/big.bin" --timeout 60s | cmp - "$SCRATCH/site/big.bin" && ok "big.bin (several blocks) matches" || bad "big.bin differs"
ipfs_as b shutdown >/dev/null 2>&1
kill "$NODE_PID" 2>/dev/null; wait "$NODE_PID" 2>/dev/null

########################################################################################
# Parts 2 and 3: in network namespaces.
########################################################################################
if unshare -rn true 2>/dev/null && command -v nsenter >/dev/null && command -v ip >/dev/null; then
  PART=2 SCRATCH=$SCRATCH KEY=$KEY ROOT=$ROOT ROOT2=$ROOT2 MODE=$MODE IPFS=$IPFS \
    NODE_BIN=${NODE_BIN:-} WASMTIME=${WASMTIME:-} WASM=${WASM:-} SKIP_PRIVATE=${SKIP_PRIVATE:-} \
    unshare -rn bash "$0"
  [ $? = 0 ] || FAILED=1
else
  say "== parts 2 and 3 skipped: they need unprivileged network namespaces (Linux with unshare, nsenter, ip) =="
fi

if [ "$FAILED" = 0 ]; then say "listen-check ($MODE): all passed"; else say "listen-check ($MODE): FAILED (logs: $SCRATCH, rerun with KEEP=1)"; fi
exit "$FAILED"
