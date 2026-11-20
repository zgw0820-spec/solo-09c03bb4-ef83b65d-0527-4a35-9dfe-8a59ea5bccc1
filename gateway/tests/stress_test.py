"""Stress run: large burst with repeated swaps and a failed start mixed in."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def call(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode())


def main() -> int:
    run_dir = tempfile.mkdtemp(prefix="gw-stress-")
    env = dict(os.environ, GATEWAY_RUN_DIR=run_dir, GATEWAY_TCP_PORT="19007",
               GATEWAY_BACKUP_TCP_PORT="19008",
               GATEWAY_WEB_PORT="18007",
               COLLECTOR_FAIL_PATH=os.path.join(run_dir, "fail"))
    gw = subprocess.Popen([sys.executable, "-m", "app.main"], cwd=ROOT,
                          env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True)
    base = "http://127.0.0.1:18007"
    try:
        for _ in range(60):
            try:
                call("GET", base + "/api/status")
                break
            except (urllib.error.URLError, ConnectionResetError):
                time.sleep(0.25)

        total = 200
        sock = socket.create_connection(("127.0.0.1", 19007), timeout=10)
        swap_at = {40: ("v2", False), 90: ("vX", True), 130: ("v3", False),
                   170: ("v4", False)}
        for rid in range(1, total + 1):
            if rid in swap_at:
                ver, fail = swap_at[rid]
                r = call("POST", base + "/api/upgrade",
                         {"new_version": ver, "simulate_fail": fail})
                print(f"swap@{rid} -> {ver} fail={fail}: ok={r.get('ok')} "
                      f"rolled_back={r.get('rolled_back', False)}")
                time.sleep(0.4)
            sock.sendall(json.dumps(
                {"id": rid, "sensor": "fz", "temp_c": -18.0,
                 "ts": time.time()}).encode() + b"\n")
            time.sleep(0.015)
        sock.close()

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            v = call("GET", base + "/api/verify")
            if sum(c["count"] for c in v["connections"]) >= total:
                break
            time.sleep(0.5)
        st = call("GET", base + "/api/status")
        v = call("GET", base + "/api/verify")
        conn = v["connections"][0]
        print("acks:", conn["count"], "contiguous:", conn["contiguous_from_1"],
              "unique:", conn["unique"], "dupes:", len(v["duplicate_acks"]))
        print("version ranges:", {k: (d["first_id"], d["last_id"])
                                  for k, d in conn["versions"].items()})
        print("final active:", st["current_version"])
        ok = (conn["count"] == total and conn["contiguous_from_1"]
              and conn["unique"] and not v["duplicate_acks"]
              and st["current_version"] == "v4"
              and st["tcp_connections_accepted_total"] == 1)
        # Peer tuple identity check (reconnect would change local port).
        print("single connection accepted:",
              st["tcp_connections_accepted_total"] == 1)
        print("STRESS", "PASS" if ok else "FAIL")
        return 0 if ok else 1
    finally:
        gw.terminate()
        try:
            gw.wait(timeout=10)
        except subprocess.TimeoutExpired:
            gw.kill()


if __name__ == "__main__":
    raise SystemExit(main())
