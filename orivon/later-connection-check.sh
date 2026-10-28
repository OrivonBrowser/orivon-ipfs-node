#!/usr/bin/env bash
# Checks that a serve node answers a peer that comes back on a NEW connection after waiting, and
# asks for another file of the folder it shared: the case of a visitor who opens a share, leaves it
# idle, and opens a second file.
#
#   NODE_BIN=<path to orivon-ipfs-gateway> IPFS=<kubo> FETCHER=kubo|node VIA=direct|relay \
#     [RELAY=<address>/p2p/<relay id>] orivon/later-connection-check.sh
#
# The node serves a folder (an index file and eight 40 KB files) the way an app does: it is given
# the blocks, started with --p2p-port (and --relay when VIA=relay), and asked to provide the root.
# One file is then asked for at once, and one more after each wait in $IDLES (default 0 20 60 120
# seconds), each file once.
#
#   FETCHER=kubo  a scratch Kubo asked for `ipfs cat <root>/<file>` each time after being
#                 disconnected from the node for the wait. ROUTING=none (default): Kubo has no content
#                 routing and is connected to the node by address; ROUTING=dht: Kubo finds the node
#                 through the public DHT, as a visitor would. BROADCAST=on|off turns Kubo's bitswap
#                 broadcast control on or off (default off).
#   FETCHER=node  a second copy of the node asked for each raw block over HTTP, the way Orivon's
#                 browser reads a path. A small routing service stands in for the DHT: it names the
#                 node as a provider of exactly the CIDs the node reports in GET /status as provided.
#
# Prints one `LADDER` line per step and exits 1 when a step failed. Needs curl, python3 and cmp.
set -u
NODE_BIN=${NODE_BIN:?set NODE_BIN}
IPFS=${IPFS:-ipfs}
FETCHER=${FETCHER:-kubo}
VIA=${VIA:-direct}
RELAY=${RELAY:-}
IDLES=${IDLES:-"0 20 60 120"}
FETCH_TIMEOUT=${FETCH_TIMEOUT:-40}
BROADCAST=${BROADCAST:-off}
ROUTING=${ROUTING:-none}
ANNOUNCE_WAIT=${ANNOUNCE_WAIT:-240}
TREE=${TREE:-true}
FILES=${FILES:-8}
SCRATCH=${SCRATCH:-$(mktemp -d "${TMPDIR:-/tmp}/later-connection.XXXXXX")}
KEY=${KEY:-$(python3 -c 'import secrets; print(secrets.token_hex(32))')}
TRACE=${TRACE:-orivon_ipfs_serve=debug,libp2p_stream=debug,libp2p_relay=debug,libp2p_swarm=debug,libp2p_ping=debug,warn}
FAILED=0
PIDS=()

say() { printf '%s\n' "$*"; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
json_get() { python3 -c 'import json,sys; d=json.load(sys.stdin)
for k in sys.argv[1:]: d=d[int(k)] if isinstance(d,list) else d[k]
print(d if isinstance(d,str) else json.dumps(d))' "$@"; }
now() { date +%s.%N; }
cleanup() {
  [ -s "$SCRATCH/f/api" ] && IPFS_PATH="$SCRATCH/f" "$IPFS" shutdown >/dev/null 2>&1
  sleep 1
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null; done
  wait 2>/dev/null
  [ -z "${KEEP:-}" ] && rm -rf "$SCRATCH"
}
trap cleanup EXIT
mkdir -p "$SCRATCH"

# One step of the ladder: result <idle> <file> <ok|FAIL> <seconds> [note]
result() {
  say "LADDER fetcher=$FETCHER via=$VIA idle=$1 file=$2 result=$3 seconds=$4 ${5:-}"
  [ "$3" = ok ] || FAILED=1
}

#### the folder, as blocks

export IPFS_PATH="$SCRATCH/src"
"$IPFS" init --profile=test >/dev/null 2>&1 || { say "ipfs init failed"; exit 2; }
mkdir -p "$SCRATCH/site"
echo '<h1>a share</h1>' >"$SCRATCH/site/index.html"
for i in $(seq 0 $((FILES - 1))); do head -c 40000 /dev/urandom >"$SCRATCH/site/f$i"; done
ROOT=$("$IPFS" add -r -Q --cid-version=1 --raw-leaves --pin=false "$SCRATCH/site") || { say "ipfs add failed"; exit 2; }
"$IPFS" dag export "$ROOT" >"$SCRATCH/site.car" 2>/dev/null || { say "ipfs dag export failed"; exit 2; }
declare -A CHILD
while read -r cid _ name; do CHILD[$name]=$cid; done < <("$IPFS" ls "$ROOT")
say "root $ROOT, ${#CHILD[@]} entries"

#### the node

API_PORT=$(free_port); P2P_PORT=$(free_port); API="http://127.0.0.1:$API_PORT"
NODE_ARGS=(--online --routing-mode auto --serve --api-key "$KEY" --addr "127.0.0.1:$API_PORT" --p2p-port "$P2P_PORT" --trace-filter "$TRACE")
[ "$VIA" = relay ] && NODE_ARGS+=(--relay "${RELAY:?set RELAY}")
"$NODE_BIN" "${NODE_ARGS[@]}" >"$SCRATCH/node.log" 2>&1 &
PIDS+=("$!")
for _ in $(seq 1 240); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$API/health")" = 200 ] && break
  sleep 0.5
done
H=(-H "x-orivon-key: $KEY")
PEER=$(curl -s "${H[@]}" "$API/id" | json_get peer)
say "node $PEER"
curl -s -X POST "${H[@]}" --data-binary @"$SCRATCH/site.car" "$API/blocks" >/dev/null
say "provide root (tree=$TREE) -> $(curl -s -o /dev/null -w '%{http_code}' -X POST "${H[@]}" -d "{\"cid\":\"$ROOT\",\"tree\":$TREE}" "$API/provide")"
announced_all() { curl -s "${H[@]}" "$API/status" | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d["providing"] and all(e["announced"] for e in d["providing"]) else 1)'; }
if [ "$VIA" = relay ]; then
  ADDR=""
  for _ in $(seq 1 120); do
    ADDR=$(curl -s "${H[@]}" "$API/status" | json_get relays 0 2>/dev/null) && [ -n "$ADDR" ] && break
    ADDR=""; sleep 1
  done
  [ -n "$ADDR" ] || { say "the relay never gave the node a reservation"; sed -n '1,60p' "$SCRATCH/node.log"; exit 2; }
else
  ADDR="/ip4/127.0.0.1/tcp/$P2P_PORT/p2p/$PEER"
fi
say "the node is reached at $ADDR"
if [ "$ROUTING" = dht ] && [ "$FETCHER" = kubo ]; then
  end=$((SECONDS + ANNOUNCE_WAIT))
  until announced_all; do
    [ $SECONDS -ge $end ] && { say "not every provided CID was announced in ${ANNOUNCE_WAIT}s"; break; }
    sleep 3
  done
  say "provided: $(curl -s "${H[@]}" "$API/status" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["providing"]), "CIDs,", sum(1 for e in d["providing"] if e["announced"]), "announced")')"
fi

#### FETCHER=kubo

kubo_ladder() {
  export IPFS_PATH="$SCRATCH/f"
  if [ "$ROUTING" = dht ]; then
    "$IPFS" init >/dev/null 2>&1 || { say "ipfs init failed"; exit 2; }
    "$IPFS" config --json Routing.Type '"dhtclient"' >/dev/null 2>&1
  else
    "$IPFS" init --profile=test >/dev/null 2>&1 || { say "ipfs init failed"; exit 2; }
    "$IPFS" bootstrap rm --all >/dev/null 2>&1
    "$IPFS" config --json Routing.Type '"none"' >/dev/null 2>&1
    "$IPFS" config --json AutoConf.Enabled false >/dev/null 2>&1
  fi
  "$IPFS" config --json Discovery.MDNS.Enabled false >/dev/null 2>&1
  "$IPFS" config --json Swarm.DisableNatPortMap true >/dev/null 2>&1
  "$IPFS" config --json Swarm.RelayClient.Enabled true >/dev/null 2>&1
  "$IPFS" config --json Swarm.Transports.Network.QUIC false >/dev/null 2>&1
  "$IPFS" config --json Swarm.Transports.Network.WebTransport false >/dev/null 2>&1
  "$IPFS" config --json Swarm.Transports.Network.WebRTCDirect false >/dev/null 2>&1
  "$IPFS" config --json Swarm.Transports.Network.Websocket false >/dev/null 2>&1
  "$IPFS" config --json Addresses.Swarm '["/ip4/127.0.0.1/tcp/0"]' >/dev/null 2>&1
  "$IPFS" config Addresses.API /ip4/127.0.0.1/tcp/0 >/dev/null 2>&1
  "$IPFS" config Addresses.Gateway /ip4/127.0.0.1/tcp/0 >/dev/null 2>&1
  case "$BROADCAST" in
    off) "$IPFS" config --json Internal.Bitswap.BroadcastControl.Enable false >/dev/null 2>&1 ;;
    *) "$IPFS" config --json Internal.Bitswap.BroadcastControl.Enable true >/dev/null 2>&1 ;;
  esac
  "$IPFS" daemon >"$SCRATCH/kubo.log" 2>&1 &
  PIDS+=("$!")
  for _ in $(seq 1 120); do
    [ -s "$SCRATCH/f/api" ] && "$IPFS" id >/dev/null 2>&1 && break
    sleep 0.5
  done

  if [ "$ROUTING" = dht ]; then
    for _ in $(seq 1 60); do
      [ "$("$IPFS" swarm peers 2>/dev/null | wc -l)" -ge 8 ] && break
      sleep 1
    done
    say "kubo has $("$IPFS" swarm peers 2>/dev/null | wc -l) peers"
  fi
  FETCHER_ID=$("$IPFS" id -f '<id>')
  connect() {
    [ "$ROUTING" = dht ] && return 0
    local started; started=$(now)
    if timeout 60 "$IPFS" swarm connect "$ADDR" >"$SCRATCH/connect.out" 2>&1; then
      say "connect ok in $(python3 -c "print(round($(now)-$started,1))") s"
    else
      say "connect FAILED: $(cat "$SCRATCH/connect.out")"
    fi
  }
  cat_file() { # cat_file <idle> <file name>
    local started took
    started=$(now)
    if timeout $((FETCH_TIMEOUT + 15)) "$IPFS" cat "$ROOT/$2" --timeout "${FETCH_TIMEOUT}s" 2>"$SCRATCH/cat.err" | cmp -s - "$SCRATCH/site/$2"; then
      result "$1" "$2" ok "$(python3 -c "print(round($(now)-$started,1))")"
    else
      result "$1" "$2" FAIL "$(python3 -c "print(round($(now)-$started,1))")" "$(head -c 200 "$SCRATCH/cat.err" | tr '\n' ' ')"
      "$IPFS" bitswap stat 2>&1 | head -20
      say "kubo has $("$IPFS" swarm peers 2>/dev/null | wc -l) peers; providers of ${CHILD[$2]} by the DHT:"
      timeout 45 "$IPFS" routing findprovs -n 3 --timeout 30s "${CHILD[$2]}" 2>&1 | head -5
    fi
  }

  connect
  cat_file first f0
  i=1
  for idle in $IDLES; do
    "$IPFS" swarm disconnect "/p2p/$PEER" >/dev/null 2>&1
    sleep "$idle"
    connect
    cat_file "$idle" "f$i"
    i=$((i + 1))
  done
}

#### FETCHER=node

node_ladder() {
  local router_port fetch_port
  router_port=$(free_port); fetch_port=$(free_port)
  cat >"$SCRATCH/router.py" <<'PY'
import json, sys, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

api, key, peer, addr, port = sys.argv[1:6]

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        cid = self.path.split('?')[0].rstrip('/').rsplit('/', 1)[-1]
        request = urllib.request.Request(api + '/status', headers={'x-orivon-key': key})
        provided = {entry['cid'] for entry in json.load(urllib.request.urlopen(request))['providing']}
        providers = []
        if cid in provided:
            providers = [{'Schema': 'peer', 'ID': peer, 'Addrs': [addr], 'Protocols': ['transport-bitswap']}]
        body = json.dumps({'Providers': providers}).encode()
        self.send_response(200)
        self.send_header('content-type', 'application/json')
        self.send_header('content-length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        sys.stderr.write('router: %s -> %d providers\n' % (cid, len(providers)))
        sys.stderr.flush()

ThreadingHTTPServer(('127.0.0.1', int(port)), Handler).serve_forever()
PY
  python3 -I "$SCRATCH/router.py" "$API" "$KEY" "$PEER" "$ADDR" "$router_port" >"$SCRATCH/router.log" 2>&1 &
  PIDS+=("$!")
  for _ in $(seq 1 60); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$router_port/routing/v1/providers/warmup")" = 200 ] && break
    sleep 0.25
  done
  "$NODE_BIN" --online --routing-mode delegated --delegated-router "http://127.0.0.1:$router_port/routing/v1" \
    --addr "127.0.0.1:$fetch_port" --trace-filter "${FETCH_TRACE:-orivon_ipfs_retrieval=info,warn}" >"$SCRATCH/fetcher.log" 2>&1 &
  PIDS+=("$!")
  for _ in $(seq 1 240); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$fetch_port/health")" = 200 ] && break
    sleep 0.5
  done

  get_block() { # get_block <idle> <name> <cid> <file to compare with, or ->
    local started code
    started=$(now)
    code=$(curl -s -m "$FETCH_TIMEOUT" -o "$SCRATCH/got" -w '%{http_code}' "http://127.0.0.1:$fetch_port/ipfs/$3?format=raw")
    if [ "$code" = 200 ] && { [ "$4" = - ] || cmp -s "$SCRATCH/got" "$4"; }; then
      result "$1" "$2" ok "$(python3 -c "print(round($(now)-$started,1))")"
    else
      result "$1" "$2" FAIL "$(python3 -c "print(round($(now)-$started,1))")" "http $code"
    fi
  }

  get_block first root "$ROOT" -
  get_block first f0 "${CHILD[f0]}" "$SCRATCH/site/f0"
  i=1
  for idle in $IDLES; do
    sleep "$idle"
    get_block "$idle" "f$i" "${CHILD[f$i]}" "$SCRATCH/site/f$i"
    i=$((i + 1))
  done
}

case "$FETCHER" in
  kubo) kubo_ladder ;;
  node) node_ladder ;;
  *) say "FETCHER must be kubo or node"; exit 2 ;;
esac

say "---- what the node saw of the fetcher"
python3 -I - "$SCRATCH/node.log" "${FETCHER_ID:-}" <<'PY'
import re, sys
path, known = sys.argv[1], sys.argv[2]
ansi = re.compile(r'\x1b\[[0-9;]*m')
lines = [ansi.sub('', line.rstrip('\n')) for line in open(path, errors='replace')]
peers = {known} if known else set()
for line in lines:
    if 'connection established' in line and ('127.0.0.1' in line or 'p2p-circuit' in line):
        found = re.search(r'peer_id=(\S+)', line)
        if found:
            peers.add(found.group(1))
shown = [line for line in lines
         if any(('peer_id=' + peer + ' ') in line + ' ' or ('peer=' + peer + ' ') in line + ' ' for peer in peers)
         and re.search(r'connection (established|closed)|inbound bitswap stream|bitswap (wantlist|reply|stream)', line)]
print('fetcher peers:', ', '.join(sorted(peers)) or 'none seen')
for line in shown[-120:]:
    print(line[:320])
PY
[ -f "$SCRATCH/router.log" ] && { say "---- router log"; tail -40 "$SCRATCH/router.log"; }
[ -f "$SCRATCH/fetcher.log" ] && { say "---- fetcher log"; sed 's/\x1b\[[0-9;]*m//g' "$SCRATCH/fetcher.log" | grep -a -E 'bitswap_session|bitswap_peer_expand|provider_lookup|bitswap_successful_peer|dial|connection|circuit|bitswap_request|bitswap_fetch' | cut -c1-330 | tail -80; }
if [ -n "${LOG_DIR:-}" ]; then
  mkdir -p "$LOG_DIR"
  cp "$SCRATCH"/*.log "$LOG_DIR"/ 2>/dev/null
fi
exit "$FAILED"
