"""HTTP-level smoke test: drives the real FastAPI endpoints end to end.

Starts the gateway as a subprocess, then exercises the operation page,
recorder start/stop, a successful hot swap and a simulated failed start,
using only the public HTTP API. Run:  python tests/smoke_http.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def call(method: str, url: str, body: dict | None = None,
         raw: bool = False) -> int | dict | str:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = resp.read().decode()
            return payload if raw else json.loads(payload)
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode()
        return payload if raw else json.loads(payload)


def wait_up(base: str) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            call("GET", base + "/api/status")
            return
        except (urllib.error.URLError, ConnectionResetError):
            time.sleep(0.25)
    raise RuntimeError("gateway did not start")


def main() -> int:
    run_dir = tempfile.mkdtemp(prefix="gw-smoke-")
    env = dict(os.environ, GATEWAY_RUN_DIR=run_dir,
               GATEWAY_TCP_PORT="19005", GATEWAY_BACKUP_TCP_PORT="19006",
               GATEWAY_WEB_PORT="18005",
               COLLECTOR_FAIL_PATH=os.path.join(run_dir, "fail"))
    gw = subprocess.Popen([sys.executable, "-m", "app.main"],
                          cwd=ROOT, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True)
    base = "http://127.0.0.1:18005"
    failures: list[str] = []

    def check(cond: bool, name: str, detail: str = "") -> None:
        print(("  PASS  " if cond else "  FAIL  ") + name + (f" {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    try:
        wait_up(base)
        print("== static assets ==")
        html = call("GET", base + "/", raw=True)
        check(isinstance(html, str) and "换版操作台" in html, "index page served")
        js = call("GET", base + "/static/app.js", raw=True)
        check(isinstance(js, str) and "api/upgrade" in js, "app.js served")

        print("== initial status ==")
        st = call("GET", base + "/api/status")
        check(st["current_version"] == "v1", "initial version v1", str(st))

        print("== recorder single-connection burst + hot swap ==")
        r = call("POST", base + "/api/recorder/start",
                 {"count": 25, "interval": 0.1, "gap_at": 10, "gap_seconds": 4})
        check(r.get("ok"), "recorder started via API", str(r))
        time.sleep(1.2)
        up = call("POST", base + "/api/upgrade",
                  {"new_version": "v2", "simulate_fail": False})
        check(up.get("ok"), "upgrade accepted", str(up))

        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            v = call("GET", base + "/api/verify")
            if sum(c["count"] for c in v["connections"]) >= 25:
                break
            time.sleep(0.3)
        v = call("GET", base + "/api/verify")
        conn = v["connections"][0]
        check(conn["count"] == 25 and conn["contiguous_from_1"]
              and conn["unique"] and not v["duplicate_acks"],
              "25 readings acked once, in order", str(conn))
        check(set(conn["versions"]) == {"v1", "v2"},
              "both versions appear in acks", str(conn["versions"]))
        st = call("GET", base + "/api/status")
        check(st["tcp_connections_accepted_total"] == 1,
              "exactly one TCP connection accepted",
              str(st["tcp_connections_accepted_total"]))

        print("== simulated failed startup keeps old collector ==")
        bad = call("POST", base + "/api/upgrade",
                   {"new_version": "v3", "simulate_fail": True})
        check(bad.get("ok") is False and bad.get("rolled_back"),
              "failed start rolls back", str(bad))
        time.sleep(1)
        st = call("GET", base + "/api/status")
        check(st["current_version"] == "v2" and not st["fail_marker_present"],
              "old version still active, fail marker cleaned up", str(st))

        print("== dual-entry readiness race via API ==")
        idle_deadline = time.monotonic() + 15
        while time.monotonic() < idle_deadline:
            if not call("GET", base + "/api/status")["recorder_alive"]:
                break
            time.sleep(0.3)
        call("POST", base + "/api/events/reset")
        pe = call("POST", base + "/api/entries/primary", {"delay": 2.0})
        be = call("POST", base + "/api/entries/backup", {"delay": 0.0})
        check(pe.get("ok") and pe.get("delay") == 2.0 and be.get("ok"),
              "entry policies applied", f"{pe} {be}")
        dr = call("POST", base + "/api/recorder/start",
                  {"mode": "dual", "count": 10, "interval": 0.05,
                   "gap_at": 0, "gap_seconds": 0, "failover_after": 0.5,
                   "total_timeout": 6})
        check(dr.get("ok") and dr.get("mode") == "dual",
              "dual recorder started via API", str(dr))
        race_deadline = time.monotonic() + 20
        while time.monotonic() < race_deadline:
            rs = call("GET", base + "/api/status")["recorder"]
            if rs.get("choice") and not rs.get("running"):
                break
            time.sleep(0.2)
        by_entry = {a["entry"]: a for a in rs["attempts"]}
        check(rs.get("choice", {}).get("entry") == "backup",
              "backup entry won the API-driven race", str(rs.get("choice")))
        check(by_entry.get("primary", {}).get("status") == "aborted",
              "primary attempt aborted after the choice",
              str(by_entry.get("primary")))
        v = call("GET", base + "/api/verify")
        check(v["entries"].get("backup", {}).get("count") == 10
              and not v["entries"].get("primary")
              and not v["duplicate_acks"],
              "all acks attributed to the backup entry, none duplicated",
              str(v["entries"]))
        call("POST", base + "/api/entries/primary", {"delay": 0.0, "fail": False})
        call("POST", base + "/api/entries/backup", {"delay": 0.0, "fail": False})

        print("== double start rejected ==")
        # Wait for the previous recorder to finish first; then a concurrent
        # second start must be rejected (one-connection-per-run guarantee).
        idle_deadline = time.monotonic() + 15
        while time.monotonic() < idle_deadline:
            if not call("GET", base + "/api/status")["recorder_alive"]:
                break
            time.sleep(0.3)
        call("POST", base + "/api/recorder/start",
             {"count": 5, "interval": 0.1, "gap_at": 0, "gap_seconds": 0})
        dup = call("POST", base + "/api/recorder/start",
                   {"count": 5, "interval": 0.1, "gap_at": 0, "gap_seconds": 0})
        check(dup.get("ok") is False, "concurrent recorder start rejected",
              str(dup))
        stop = call("POST", base + "/api/recorder/stop")
        check(stop.get("ok"), "recorder stopped via API", str(stop))

        print("== events API cursor pagination ==")
        page1 = call("GET", base + "/api/events?cursor=0")
        check(page1["events"] and page1["cursor"] > 0, "events page 1")
        page2 = call("GET", f"{base}/api/events?cursor={page1['cursor']}")
        check(isinstance(page2["events"], list), "events page 2 (cursor works)")

    finally:
        gw.terminate()
        try:
            out, _ = gw.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            gw.kill()
            out, _ = gw.communicate()

    if failures:
        print(f"\n{len(failures)} FAILURE(S); gateway log tail:")
        print("\n".join(out.splitlines()[-30:]))
        return 1
    print("\nALL HTTP SMOKE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
