#!/usr/bin/env python3
"""Times how long a fetcher takes to find the provider of a share announced a moment ago.

  fresh-lookup-check.py --node <orivon-ipfs-gateway> --ipfs <kubo> --relay <addr>/p2p/<relay id>

A serve node (the way Orivon Share runs it: --serve, --p2p-port, --relay) holds ten shares of fresh
random bytes, one after the other. For each share it announces the root, waits until /status says the
announcement reached a peer, and from that moment a fetcher looks the root up through the public
network. The fetchers, each on shares of its own so that none warms a cache for another:

  node-auto   a new copy of the node started the way the browser starts it (routing auto, two
              delegated routers), asked for the root block over HTTP
  node-dht    the same with --routing-mode light-dht
  kubo-find   a Kubo that has run for a while (dhtclient): `ipfs routing findprovs -n 1`
  kubo-cat    the same Kubo asked for `ipfs cat <root>/index.html`
  delegated   polls delegated-ipfs.dev for the root once a second

Prints one RESULT line per share and a SUMMARY per fetcher, and writes results.json and the logs
under --out. Needs only the Python standard library, the two programs and the network.
"""
import argparse
import json
import os
import platform
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROUTERS = "https://delegated-ipfs.dev/routing/v1,https://cid.contact/routing/v1"
METHODS = ["node-auto", "node-dht", "kubo-find", "kubo-cat", "delegated"]
# `node-dht:wide` is node-dht with the environment of the preset named after the colon.
PRESETS = {
    "par10": {"ORIVON_IPFS_DHT_PARALLELISM": "10"},
    "wide": {"ORIVON_IPFS_DHT_PARALLELISM": "10", "ORIVON_IPFS_DHT_CONNECTIONS": "64", "ORIVON_IPFS_DHT_PENDING_CONNECTIONS": "32"},
    "fast": {
        "ORIVON_IPFS_DHT_PARALLELISM": "10", "ORIVON_IPFS_DHT_CONNECTIONS": "64", "ORIVON_IPFS_DHT_PENDING_CONNECTIONS": "32",
        "ORIVON_IPFS_DHT_CONNECT_TIMEOUT_MS": "5000", "ORIVON_IPFS_DHT_IDLE_TIMEOUT_MS": "5000",
    },
}
LOCK = threading.Lock()
RESULTS = []
PROCS = []


def say(*parts):
    with LOCK:
        print(*parts, flush=True)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method, url, headers=None, data=None, timeout=10):
    request = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()
    except (urllib.error.URLError, OSError, TimeoutError) as error:
        return 0, {}, str(error).encode()


def wait_for(url, timeout):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if http("GET", url, timeout=3)[0] == 200:
            return True
        time.sleep(0.3)
    return False


class Proc:
    def __init__(self, name, args, out, env=None):
        self.name = name
        self.log = open(os.path.join(out, name + ".log"), "wb")
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=subprocess.STDOUT, env=env)
        with LOCK:
            PROCS.append(self)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()


class Rig:
    def __init__(self, args):
        self.args = args
        self.out = os.path.abspath(args.out)
        os.makedirs(self.out, exist_ok=True)
        self.tmp = tempfile.mkdtemp(prefix="fresh-lookup.")
        self.key = secrets.token_hex(32)
        self.api_port = free_port()
        self.api = f"http://127.0.0.1:{self.api_port}"
        self.headers = {"x-orivon-key": self.key}
        self.src_repo = os.path.join(self.tmp, "src-repo")
        self.kubo_repo = os.path.join(self.tmp, "kubo-repo")
        self.deadline = time.monotonic() + args.budget_min * 60

    # Kubo as a command line

    def ipfs(self, repo, *argv, timeout=60, data=None):
        env = dict(os.environ, IPFS_PATH=repo)
        return subprocess.run([self.args.ipfs, *argv], env=env, input=data, capture_output=True, timeout=timeout)

    def make_share(self, index):
        site = os.path.join(self.tmp, f"site-{index}")
        os.makedirs(site)
        with open(os.path.join(site, "index.html"), "w") as f:
            f.write(f"<h1>share {index} {secrets.token_hex(16)}</h1>\n")
        for n in range(self.args.files):
            with open(os.path.join(site, f"f{n}"), "wb") as f:
                f.write(os.urandom(40000))
        root = self.ipfs(
            self.src_repo, "add", "-r", "-Q", "--cid-version=1", "--raw-leaves", "--pin=false", site
        ).stdout.decode().strip()
        car = self.ipfs(self.src_repo, "dag", "export", root).stdout
        if not root or not car:
            raise RuntimeError("could not make a share")
        with open(os.path.join(site, "index.html"), "rb") as f:
            index_bytes = f.read()
        return {"root": root, "car": car, "index": index_bytes}

    # The serve node

    def start_serve(self):
        a = self.args
        p2p = free_port()
        argv = [
            a.node, "--online", "--routing-mode", "auto", "--serve", "--api-key", self.key,
            "--addr", f"127.0.0.1:{self.api_port}", "--p2p-port", str(p2p), "--relay", a.relay,
            "--trace-filter", a.serve_trace,
        ]
        self.serve = Proc("serve", argv, self.out)
        if not wait_for(self.api + "/health", 60):
            raise RuntimeError("the serve node did not start")
        end = time.monotonic() + 180
        while time.monotonic() < end:
            status = self.status()
            if status and status.get("relays"):
                say("serve node holds a place on the relay:", status["relays"][0])
                return
            time.sleep(1)
        raise RuntimeError("the relay never gave the serve node a reservation")

    def status(self):
        code, _, body = http("GET", self.api + "/status", self.headers, timeout=10)
        if code != 200:
            return None
        return json.loads(body)

    def announce(self, share):
        """Gives the node the share and returns the seconds until the root's announcement reached a peer."""
        for _ in range(60):
            code, _, body = http("POST", self.api + "/blocks", self.headers, share["car"], timeout=30)
            if code == 200:
                break
            if code != 429:
                raise RuntimeError(f"POST /blocks answered {code} {body[:100]!r}")
            time.sleep(1)
        body = json.dumps({"cid": share["root"], "tree": self.args.tree}).encode()
        code, _, answer = http("POST", self.api + "/provide", {**self.headers, "content-type": "application/json"}, body, timeout=30)
        if code != 202:
            raise RuntimeError(f"POST /provide answered {code} {answer[:100]!r}")
        started = time.monotonic()
        while time.monotonic() - started < self.args.announce_timeout:
            status = self.status()
            for entry in (status or {}).get("providing", []):
                if entry["cid"] == share["root"] and entry["announced"]:
                    return time.monotonic() - started
            time.sleep(0.4)
        return None

    # Fetchers

    def fetch_node(self, method, index, share):
        base, _, preset = method.partition(":")
        mode = "auto" if base == "node-auto" else "light-dht"
        env = dict(os.environ, **PRESETS[preset]) if preset else None
        port = free_port()
        argv = [self.args.node, "--addr", f"127.0.0.1:{port}", "--online", "--routing-mode", mode]
        if mode == "auto":
            argv += ["--delegated-router", ROUTERS]
        argv += ["--db", os.path.join(self.tmp, f"{method.replace(':', '-')}-{index}.db"), "--trace-filter", self.args.fetch_trace]
        proc = Proc(f"{method.replace(':', '-')}-{index:02d}", argv, self.out, env=env)
        try:
            if not wait_for(f"http://127.0.0.1:{port}/health", 60):
                return {"ok": False, "note": "fetch node did not start"}
            announced = self.announce(share)
            if announced is None:
                return {"ok": False, "note": "not announced"}
            started = time.monotonic()
            attempts = []
            while time.monotonic() - started < self.args.fetch_timeout:
                t = time.monotonic()
                remaining = self.args.fetch_timeout - (t - started)
                code, _, body = http("GET", f"http://127.0.0.1:{port}/ipfs/{share['root']}?format=raw", timeout=max(5, remaining))
                attempts.append([code, round(time.monotonic() - t, 1)])
                if code == 200 and body:
                    return {"ok": True, "seconds": round(time.monotonic() - started, 1), "announce_s": round(announced, 1), "attempts": attempts}
                time.sleep(0.5)
            return {"ok": False, "seconds": self.args.fetch_timeout, "announce_s": round(announced, 1), "attempts": attempts}
        finally:
            proc.stop()

    def start_kubo(self):
        a = self.args
        self.ipfs(self.kubo_repo, "init")
        for key, value in [
            ("Routing.Type", '"dhtclient"'),
            ("Discovery.MDNS.Enabled", "false"),
            ("Swarm.DisableNatPortMap", "true"),
            ("Addresses.API", f'"/ip4/127.0.0.1/tcp/{free_port()}"'),
            ("Addresses.Gateway", f'"/ip4/127.0.0.1/tcp/{free_port()}"'),
            ("Addresses.Swarm", f'["/ip4/0.0.0.0/tcp/{free_port()}"]'),
        ]:
            self.ipfs(self.kubo_repo, "config", "--json", key, value)
        env = dict(os.environ, IPFS_PATH=self.kubo_repo)
        self.kubo = Proc("kubo-daemon", [a.ipfs, "daemon"], self.out, env=env)
        end = time.monotonic() + 120
        while time.monotonic() < end:
            if self.ipfs(self.kubo_repo, "id", timeout=20).returncode == 0:
                break
            time.sleep(1)
        end = time.monotonic() + 120
        while time.monotonic() < end:
            peers = self.ipfs(self.kubo_repo, "swarm", "peers", timeout=20).stdout.decode().split()
            if len(peers) >= 20:
                break
            time.sleep(2)
        say("kubo has", len(peers), "peers")
        say(self.ipfs(self.kubo_repo, "version", "--number").stdout.decode().strip())

    def fetch_kubo_find(self, share):
        announced = self.announce(share)
        if announced is None:
            return {"ok": False, "note": "not announced"}
        started = time.monotonic()
        env = dict(os.environ, IPFS_PATH=self.kubo_repo)
        proc = subprocess.Popen(
            [self.args.ipfs, "routing", "findprovs", "-n", "1", "--timeout", f"{self.args.fetch_timeout}s", "--enc=json", "-v", share["root"]],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        events, first = 0, None
        for raw in proc.stdout:
            events += 1
            line = raw.decode(errors="replace")
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("Type") == 4 and event.get("Responses") and first is None:
                first = round(time.monotonic() - started, 1)
        proc.wait()
        return {"ok": first is not None, "seconds": first, "announce_s": round(announced, 1), "events": events}

    def fetch_kubo_cat(self, share):
        announced = self.announce(share)
        if announced is None:
            return {"ok": False, "note": "not announced"}
        started = time.monotonic()
        try:
            done = self.ipfs(self.kubo_repo, "cat", f"{share['root']}/index.html", "--timeout", f"{self.args.fetch_timeout}s", timeout=self.args.fetch_timeout + 20)
            ok = done.returncode == 0 and done.stdout == share["index"]
            note = "" if ok else done.stderr.decode(errors="replace")[:200]
        except subprocess.TimeoutExpired:
            ok, note = False, "timeout"
        seconds = round(time.monotonic() - started, 1)
        extra = {}
        if not ok:
            find = self.ipfs(self.kubo_repo, "routing", "findprovs", "-n", "3", "--timeout", "30s", share["root"], timeout=50)
            extra["findprovs_after"] = find.stdout.decode(errors="replace").split()[:3]
            extra["bitswap"] = self.ipfs(self.kubo_repo, "bitswap", "stat", timeout=20).stdout.decode(errors="replace")[:400]
        return {"ok": ok, "seconds": seconds, "announce_s": round(announced, 1), "note": note, **extra}

    def fetch_delegated(self, share):
        announced = self.announce(share)
        if announced is None:
            return {"ok": False, "note": "not announced"}
        started = time.monotonic()
        polls, headers = 0, None
        while time.monotonic() - started < self.args.fetch_timeout:
            code, head, body = http(
                "GET", "https://delegated-ipfs.dev/routing/v1/providers/" + share["root"],
                {"Accept": "application/json"}, timeout=20,
            )
            polls += 1
            if headers is None:
                headers = {k.lower(): v for k, v in head.items() if k.lower() in ("cache-control", "age", "cf-cache-status", "x-cache", "server", "via")}
            if code == 200:
                try:
                    found = json.loads(body).get("Providers") or []
                except ValueError:
                    found = []
                if found:
                    return {"ok": True, "seconds": round(time.monotonic() - started, 1), "announce_s": round(announced, 1), "polls": polls, "first_headers": headers}
            time.sleep(1)
        return {"ok": False, "seconds": self.args.fetch_timeout, "announce_s": round(announced, 1), "polls": polls, "first_headers": headers}

    # Pipelines

    def pipeline(self, method, shares):
        for index, share in enumerate(shares):
            if time.monotonic() > self.deadline:
                say(f"{method}: out of time budget after {index} shares")
                break
            try:
                if method.startswith("node-"):
                    result = self.fetch_node(method, index, share)
                elif method == "kubo-find":
                    result = self.fetch_kubo_find(share)
                elif method == "kubo-cat":
                    result = self.fetch_kubo_cat(share)
                else:
                    result = self.fetch_delegated(share)
            except Exception as error:  # a broken share must not end the run
                result = {"ok": False, "note": f"error: {error}"}
            result.update({"method": method, "share": index, "root": share["root"]})
            with LOCK:
                RESULTS.append(result)
            say("RESULT", json.dumps({k: v for k, v in result.items() if k not in ("events",)}))

    def run(self):
        a = self.args
        say("platform", platform.platform(), "python", platform.python_version())
        self.ipfs(self.src_repo, "init", "--profile=test")
        methods = [m for m in a.methods.split(",") if m]
        shares = {m: [self.make_share(i) for i in range(a.shares)] for m in methods}
        say("made", sum(len(v) for v in shares.values()), "shares")
        self.diagnostics()
        self.start_serve()
        if any(m.startswith("kubo") for m in methods):
            self.start_kubo()
        threads = [threading.Thread(target=self.pipeline, args=(m, shares[m])) for m in methods]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        status = self.status() or {}
        with open(os.path.join(self.out, "serve-status.json"), "w") as f:
            json.dump(status, f, indent=1)
        self.summary()

    def diagnostics(self):
        """What the runner's network looks like to the delegated router."""
        info = {}
        for name in ("delegated-ipfs.dev", "cid.contact"):
            try:
                info[name] = sorted({r[4][0] for r in socket.getaddrinfo(name, 443, proto=socket.IPPROTO_TCP)})
            except OSError as error:
                info[name] = str(error)
        try:
            with socket.create_connection(("2606:4700:4700::1111", 53), timeout=4):
                info["ipv6"] = "reachable"
        except OSError as error:
            info["ipv6"] = f"no: {error}"
        say("DIAG", json.dumps(info))

    def summary(self):
        lines = []
        by_method = {}
        for r in RESULTS:
            by_method.setdefault(r["method"], []).append(r)
        for method in sorted(by_method, key=lambda m: (METHODS.index(m.split(":")[0]), m)):
            rows = by_method[method]
            ok = sorted(r["seconds"] for r in rows if r.get("ok"))
            fast = sum(1 for s in ok if s <= 15)
            median = ok[len(ok) // 2] if ok else None
            lines.append(
                f"SUMMARY {platform.system()} {method}: found {len(ok)}/{len(rows)}, within 15 s {fast}/{len(rows)}, "
                f"median {median}, all {ok}"
            )
        for line in lines:
            say(line)
        with open(os.path.join(self.out, "results.json"), "w") as f:
            json.dump({"platform": platform.platform(), "results": RESULTS, "summary": lines}, f, indent=1)
        step = os.environ.get("GITHUB_STEP_SUMMARY")
        if step:
            with open(step, "a") as f:
                f.write(f"### {platform.system()}\n```\n" + "\n".join(lines) + "\n```\n")

    def cleanup(self):
        if getattr(self, "kubo", None):
            try:
                self.ipfs(self.kubo_repo, "shutdown", timeout=20)
            except Exception:
                pass
        for proc in PROCS:
            try:
                proc.stop()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--node", required=True, help="path of orivon-ipfs-gateway")
    p.add_argument("--ipfs", required=True, help="path of Kubo")
    p.add_argument("--relay", required=True, help="<address>/p2p/<relay id> of the circuit relay")
    p.add_argument("--out", default="fresh-lookup-out")
    p.add_argument("--shares", type=int, default=10, help="fresh shares per fetcher")
    p.add_argument("--files", type=int, default=2, help="40 KB files in a share")
    p.add_argument("--methods", default=",".join(METHODS))
    p.add_argument("--tree", action="store_true", help="announce the folder's files with the root, as Share does")
    p.add_argument("--fetch-timeout", type=int, default=90)
    p.add_argument("--announce-timeout", type=int, default=300)
    p.add_argument("--budget-min", type=int, default=45, help="no new share is started after this")
    p.add_argument("--serve-trace", default="warn,orivon_ipfs_serve=debug,libp2p_relay=debug,libp2p_kad=debug")
    p.add_argument("--fetch-trace", default="warn,orivon_ipfs_routing=info,orivon_ipfs_retrieval=info")
    args = p.parse_args()
    rig = Rig(args)
    try:
        rig.run()
    finally:
        rig.cleanup()
    return 0


if __name__ == "__main__":
    sys.exit(main())
