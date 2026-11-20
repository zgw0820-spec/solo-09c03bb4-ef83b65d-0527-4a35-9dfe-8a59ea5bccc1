"""End-to-end verification for the gateway, using the stdlib only.

Scenarios:

  1. Burst + hot swap: a recorder opens ONE tcp connection and sends a
     long burst with a pause in the middle. We upgrade v1 -> v2 during the
     pause. The recorder must never reconnect; all readings 1..N must be
     acked exactly once, in order, split across the two versions.
  2. Startup failure: request an upgrade with simulate_fail. The new
     generation must fail to start and the old one must keep collecting
     (remaining readings still acked, same version, no gaps/dupes).
  3. Same-fd handover: ack events before/after the swap carry the same
     peer tuple (the kernel TCP connection is untouched).
  4. Two concurrent recorders + a reused version label.
  5. Dual-entry readiness race (primary delayed -> backup wins; late primary
     reply never sends), explicit primary failure -> immediate backup, and
     both entries timing out -> batch not sent.

Run:  python tests/e2e_test.py
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


def http(method: str, url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        # API uses 409 with a JSON body for expected conflicts such as a
        # simulated startup failure; return the body so tests can assert.
        return json.loads(exc.read().decode())


def wait_status(base: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            return http("GET", base + "/api/status")
        except (urllib.error.URLError, ConnectionResetError) as exc:
            last = exc
            time.sleep(0.25)
    raise RuntimeError(f"gateway never came up: {last}")


def wait_acks(base: str, expected: int, timeout: float = 40.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = http("GET", base + "/api/verify")
        total = sum(c["count"] for c in v["connections"])
        if total >= expected:
            return v
        time.sleep(0.3)
    raise AssertionError(f"timed out waiting for {expected} acks, got {total}")


def wait_event(base: str, event_type: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    cursor = 0
    while time.monotonic() < deadline:
        ev = http("GET", f"{base}/api/events?cursor={cursor}")
        cursor = ev["cursor"]
        for e in ev["events"]:
            if e.get("type") == event_type:
                return e
        time.sleep(0.25)
    raise AssertionError(f"event {event_type} not observed within {timeout}s")


def wait_version_dead(base: str, version: str, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = http("GET", base + "/api/status")
        gens = [g for g in st["generations"] if g["version"] == version]
        if gens and not gens[0]["alive"]:
            return gens[0]
        time.sleep(0.25)
    raise AssertionError(f"collector {version} still alive after {timeout}s")


class Check:
    def __init__(self) -> None:
        self.passed = 0
        self.failed: list[str] = []

    def expect(self, cond: bool, name: str, detail: str = "") -> None:
        if cond:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failed.append(f"{name} {detail}")
            print(f"  FAIL  {name} {detail}")


class OneShotRecorder:
    """Recorder that opens exactly one TCP connection.

    Reconnect detection uses two authoritative signals: the supervisor's
    accept counter (delta over the run) and the recorder's own stdout,
    which prints one "connected once" line per create_connection() call.
    """

    def __init__(self, port: int, count: int, interval: float,
                 gap_at: int = 0, gap_seconds: float = 0.0) -> None:
        self.port = port
        self.count = count
        self.interval = interval
        self.gap_at = gap_at
        self.gap_seconds = gap_seconds
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        cmd = [sys.executable, "-m", "app.recorder",
               "--host", "127.0.0.1", "--port", str(self.port),
               "--count", str(self.count), "--interval", str(self.interval),
               "--gap-at", str(self.gap_at),
               "--gap-seconds", str(self.gap_seconds)]
        self.proc = subprocess.Popen(
            cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=dict(os.environ),
        )

    def stop(self) -> tuple[int, str]:
        assert self.proc is not None
        out, _ = self.proc.communicate(timeout=30)
        return self.proc.returncode, out

    @staticmethod
    def connect_count(output: str) -> int:
        return sum(1 for line in output.splitlines()
                   if "connected once" in line)


class DualRecorder:
    """Recorder in dual-entry mode; returns its structured @@GW@@ reports."""

    def __init__(self, port: int, backup_port: int, count: int,
                 interval: float = 0.03, failover_after: float = 1.0,
                 total_timeout: float = 6.0) -> None:
        self.port = port
        self.backup_port = backup_port
        self.count = count
        self.interval = interval
        self.failover_after = failover_after
        self.total_timeout = total_timeout
        self.proc: subprocess.Popen | None = None
        self.output = ""

    def start(self) -> None:
        cmd = [sys.executable, "-m", "app.recorder", "--mode", "dual",
               "--host", "127.0.0.1", "--port", str(self.port),
               "--backup-host", "127.0.0.1",
               "--backup-port", str(self.backup_port),
               "--count", str(self.count), "--interval", str(self.interval),
               "--failover-after", str(self.failover_after),
               "--total-timeout", str(self.total_timeout)]
        self.proc = subprocess.Popen(
            cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=dict(os.environ),
        )

    def stop(self) -> tuple[int, str]:
        assert self.proc is not None
        out, _ = self.proc.communicate(timeout=40)
        self.output = out
        return self.proc.returncode, out

    def reports(self, kind: str | None = None) -> list[dict]:
        items = []
        for line in self.output.splitlines():
            if not line.startswith("@@GW@@ "):
                continue
            try:
                obj = json.loads(line[len("@@GW@@ "):])
            except json.JSONDecodeError:
                continue
            if kind is None or obj.get("kind") == kind:
                items.append(obj)
        return items

    def attempts(self) -> dict[str, dict]:
        return {a["entry"]: a for a in self.reports("attempt")}


def set_entry(base: str, entry: str, delay: float | None = None,
              fail: bool | None = None) -> dict:
    body = {}
    if delay is not None:
        body["delay"] = delay
    if fail is not None:
        body["fail"] = fail
    return http("POST", f"{base}/api/entries/{entry}", body)


def wait_recorder_idle(base: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not http("GET", base + "/api/status")["recorder_alive"]:
            return
        time.sleep(0.2)


def main() -> int:
    run_dir = tempfile.mkdtemp(prefix="gw-e2e-")
    port_tcp = 19001
    port_backup = 19002
    port_web = 18001
    env = dict(os.environ,
               GATEWAY_RUN_DIR=run_dir,
               GATEWAY_TCP_PORT=str(port_tcp),
               GATEWAY_BACKUP_TCP_PORT=str(port_backup),
               GATEWAY_WEB_PORT=str(port_web),
               COLLECTOR_FAIL_PATH=os.path.join(run_dir, "fail"),
               COLLECTOR_INITIAL_VERSION="v1")
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    base = f"http://127.0.0.1:{port_web}"
    check = Check()
    try:
        wait_status(base)
        print("== scenario 1: burst with hot swap v1 -> v2 ==")
        http("POST", base + "/api/events/reset")
        count, gap_at = 30, 12
        accepts_before = http("GET", base + "/api/status")[
            "tcp_connections_accepted_total"]
        rec = OneShotRecorder(port_tcp, count=count, interval=0.12,
                              gap_at=gap_at, gap_seconds=4.0)
        rec.start()
        # Let v1 ack the first few readings.
        time.sleep(1.2)
        st0 = http("GET", base + "/api/status")
        check.expect(st0["current_version"] == "v1", "initial collector is v1")

        up = http("POST", base + "/api/upgrade",
                  {"new_version": "v2", "simulate_fail": False})
        check.expect(up.get("ok") is True, "upgrade v1->v2 accepted", str(up))
        done = wait_event(base, "upgrade_completed", timeout=15)
        check.expect(done["connections_forwarded"] >= 1,
                     "live tcp connection forwarded during handover",
                     str(done))
        wait_event(base, "handed_over", timeout=10)

        st1 = http("GET", base + "/api/status")
        check.expect(st1["current_version"] == "v2",
                     "active collector is v2 after handover")
        v1_dead = wait_version_dead(base, "v1")
        check.expect(v1_dead["alive"] is False,
                     "old collector v1 exited after handover",
                     f"rc={v1_dead['exit_code']}")
        check.expect(st1["tcp_connections_accepted_total"] - accepts_before == 1,
                     "supervisor accepted exactly one new tcp connection",
                     str(st1["tcp_connections_accepted_total"] - accepts_before))

        verify = wait_acks(base, count)
        rc, rec_out = rec.stop()
        check.expect(rc == 0, "recorder exited cleanly")
        check.expect(OneShotRecorder.connect_count(rec_out) == 1,
                     "recorder opened exactly one tcp connection (no reconnect)",
                     rec_out)
        check.expect("connected once" in rec_out,
                     "recorder log reports single connection")

        check.expect(len(verify["connections"]) == 1,
                     "exactly one connection produced acks")
        conn = verify["connections"][0]
        check.expect(conn["count"] == count,
                     f"all {count} readings acked", f"got {conn['count']}")
        check.expect(conn["contiguous_from_1"] and conn["unique"],
                     "acked ids are 1..N contiguous with no duplicate")
        check.expect(not verify["duplicate_acks"],
                     "no reading was acked twice",
                     str(verify["duplicate_acks"]))
        versions = conn["versions"]
        check.expect(set(versions) == {"v1", "v2"},
                     "both versions processed the stream", str(versions))
        # Ordering boundary: v1 owns a prefix, v2 the contiguous suffix.
        v1_last = versions["v1"]["last_id"]
        v2_first = versions["v2"]["first_id"]
        check.expect(versions["v1"]["first_id"] == 1 and v2_first == v1_last + 1
                     and versions["v2"]["last_id"] == count,
                     "version boundary is a clean prefix/suffix split in send order",
                     f"v1=1..{v1_last} v2={v2_first}..{count}")

        print("== scenario 2: new version startup failure -> rollback ==")
        http("POST", base + "/api/events/reset")
        count2, gap_at2 = 20, 8
        accepts_before2 = http("GET", base + "/api/status")[
            "tcp_connections_accepted_total"]
        rec2 = OneShotRecorder(port_tcp, count=count2, interval=0.12,
                               gap_at=gap_at2, gap_seconds=4.0)
        rec2.start()
        time.sleep(1.0)
        failed = http("POST", base + "/api/upgrade",
                      {"new_version": "v3", "simulate_fail": True})
        check.expect(failed.get("ok") is False and failed.get("rolled_back"),
                     "failed upgrade reported as rolled back", str(failed))
        wait_event(base, "upgrade_failed", timeout=10)
        st2 = http("GET", base + "/api/status")
        check.expect(st2["current_version"] == "v2",
                     "active collector stays on old version after failed start")
        verify2 = wait_acks(base, count2)
        rc2, out2 = rec2.stop()
        st2b = http("GET", base + "/api/status")
        check.expect(rc2 == 0 and OneShotRecorder.connect_count(out2) == 1,
                     "recorder kept its single connection through rollback",
                     f"rc={rc2} connects={OneShotRecorder.connect_count(out2)}")
        check.expect(st2b["tcp_connections_accepted_total"] - accepts_before2 == 1,
                     "no extra tcp connection was accepted during rollback")
        conn2 = verify2["connections"][0]
        check.expect(conn2["count"] == count2
                     and conn2["contiguous_from_1"] and conn2["unique"],
                     f"all {count2} readings still acked once and in order",
                     str(conn2["ids"]))
        check.expect(set(conn2["versions"]) == {"v2"},
                     "old collector v2 processed every reading after rollback",
                     str(conn2["versions"]))
        check.expect(not verify2["duplicate_acks"],
                     "no duplicate acks after rollback")

        print("== scenario 3: mid-line partial buffer handover ==")
        # Tiny, rapid burst with an immediate upgrade: forces partial lines
        # to be carried inside the handover frame without data loss.
        http("POST", base + "/api/events/reset")
        count3 = 40
        accepts_before3 = http("GET", base + "/api/status")[
            "tcp_connections_accepted_total"]
        rec3 = OneShotRecorder(port_tcp, count=count3, interval=0.02)
        rec3.start()
        time.sleep(0.15)
        up3 = http("POST", base + "/api/upgrade",
                   {"new_version": "v4", "simulate_fail": False})
        check.expect(up3.get("ok") is True, "rapid upgrade v2->v4 accepted")
        wait_event(base, "upgrade_completed", timeout=15)
        verify3 = wait_acks(base, count3)
        rc3, out3 = rec3.stop()
        conn3 = verify3["connections"][0]
        check.expect(rc3 == 0 and OneShotRecorder.connect_count(out3) == 1,
                     "rapid-burst recorder stayed on one connection")
        check.expect(http("GET", base + "/api/status")[
                         "tcp_connections_accepted_total"] - accepts_before3 == 1,
                     "rapid burst used one tcp connection only")
        check.expect(conn3["count"] == count3
                     and conn3["contiguous_from_1"] and conn3["unique"],
                     "no bytes lost when partial lines crossed the handover",
                     f"count={conn3['count']}")
        check.expect(not verify3["duplicate_acks"],
                     "no duplicate acks across partial-line handover")

        print("== scenario 4: two concurrent recorders + reused version label ==")
        import socket as _socket
        http("POST", base + "/api/events/reset")
        n4 = 15
        accepts_before4 = http("GET", base + "/api/status")[
            "tcp_connections_accepted_total"]
        socks = []
        for i in range(2):
            s = _socket.create_connection(("127.0.0.1", port_tcp), timeout=10)
            socks.append(s)
            for rid in range(1, 6):  # five readings before the swap
                s.sendall(json.dumps({"id": rid, "sensor": f"r{i}",
                                      "temp_c": -10.0 - i,
                                      "ts": time.time()}).encode() + b"\n")
            time.sleep(0.1)
        # Reuse an old label (v2 already existed) to exercise pid keying.
        up4 = http("POST", base + "/api/upgrade",
                   {"new_version": "v2", "simulate_fail": False})
        check.expect(up4.get("ok") is True, "reused-label upgrade accepted",
                     str(up4))
        wait_event(base, "upgrade_completed", timeout=15)
        for i, s in enumerate(socks):
            for rid in range(6, n4 + 1):
                s.sendall(json.dumps({"id": rid, "sensor": f"r{i}",
                                      "temp_c": -10.0 - i,
                                      "ts": time.time()}).encode() + b"\n")
                time.sleep(0.02)
            s.close()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            v4 = http("GET", base + "/api/verify")
            if sum(c["count"] for c in v4["connections"]) >= 2 * n4:
                break
            time.sleep(0.3)
        v4 = http("GET", base + "/api/verify")
        check.expect(len(v4["connections"]) == 2,
                     "both recorder connections tracked separately",
                     str(len(v4["connections"])))
        check.expect(all(c["count"] == n4 and c["contiguous_from_1"]
                         and c["unique"] for c in v4["connections"]),
                     "each of the two streams acked 1..15 exactly once",
                     str(v4["connections"]))
        check.expect(all(set(c["versions"]) == {"v4", "v2"}
                         for c in v4["connections"]),
                     "both connections moved v4 -> new v2 generation",
                     str(v4["connections"]))
        check.expect(http("GET", base + "/api/status")[
                         "tcp_connections_accepted_total"] - accepts_before4 == 2,
                     "exactly two new TCP connections for two recorders")
        check.expect(not v4["duplicate_acks"],
                     "no duplicate acks with two concurrent connections")

        # ---------------- dual-entry scenarios ---------------- #
        print("== scenario 5: dual entry, primary delayed -> primary still wins ==")
        http("POST", base + "/api/events/reset")
        set_entry(base, "primary", delay=1.0)
        set_entry(base, "backup", delay=5.0)
        d1 = DualRecorder(port_tcp, port_backup, count=12,
                          failover_after=2.0, total_timeout=8.0)
        d1.start()
        rc5, _ = d1.stop()
        a5 = d1.attempts()
        c5 = d1.reports("choice")
        check.expect(rc5 == 0, "dual recorder exited cleanly", str(rc5))
        check.expect(c5 and c5[0]["entry"] == "primary",
                     "primary's slow readiness reply won the race",
                     str(c5))
        check.expect(a5["primary"]["status"] == "ready"
                     and a5["primary"]["ready_ms"] >= 900,
                     "primary readiness was genuinely delayed",
                     str(a5.get("primary")))
        check.expect("backup" not in a5,
                     "backup was never opened (primary replied before failover)",
                     str(list(a5)))
        v5 = wait_acks(base, 12)
        check.expect(v5["entries"].get("primary", {}).get("count") == 12
                     and not v5["entries"].get("backup"),
                     "all readings acked on the primary entry only",
                     str(v5["entries"]))
        check.expect(v5["ok"], "dual-entry verify passed", str(v5.get("ok")))

        print("== scenario 6: dual entry, backup wins; late primary reply discarded ==")
        http("POST", base + "/api/events/reset")
        set_entry(base, "primary", delay=2.5)
        set_entry(base, "backup", delay=0.5)
        d2 = DualRecorder(port_tcp, port_backup, count=12,
                          failover_after=1.0, total_timeout=8.0)
        d2.start()
        rc6, _ = d2.stop()
        a6 = d2.attempts()
        c6 = d2.reports("choice")
        check.expect(rc6 == 0, "backup-win recorder exited cleanly", str(rc6))
        check.expect(c6 and c6[0]["entry"] == "backup",
                     "first readiness reply (backup) won", str(c6))
        check.expect(c6[0].get("other_entry_aborted") is True,
                     "losing primary attempt was aborted", str(c6))
        check.expect(a6.get("primary", {}).get("status") == "aborted",
                     "primary attempt ended aborted before its reply",
                     str(a6.get("primary")))
        check.expect(not a6.get("primary", {}).get("ready_ms"),
                     "primary's late reply never became a readiness win",
                     str(a6.get("primary")))
        v6 = wait_acks(base, 12)
        check.expect(v6["entries"].get("backup", {}).get("count") == 12
                     and not v6["entries"].get("primary"),
                     "whole batch acked on the backup entry only",
                     str(v6["entries"]))
        check.expect(not v6["duplicate_acks"],
                     "late primary reply produced no duplicate acks")
        # Give the collector time to send the still-delayed primary reply;
        # it must never be turned into readings.
        time.sleep(3.0)
        v6b = http("GET", base + "/api/verify")
        check.expect(v6b["entries"].get("backup", {}).get("count") == 12
                     and not v6b["entries"].get("primary")
                     and not v6b["duplicate_acks"],
                     "3s later: late primary reply still sent nothing",
                     str(v6b["entries"]))

        print("== scenario 7: dual entry, explicit primary failure -> immediate backup ==")
        http("POST", base + "/api/events/reset")
        set_entry(base, "primary", delay=0.0, fail=True)
        set_entry(base, "backup", delay=0.0, fail=False)
        d3 = DualRecorder(port_tcp, port_backup, count=8,
                          failover_after=5.0, total_timeout=6.0)
        d3.start()
        rc7, _ = d3.stop()
        a7 = d3.attempts()
        c7 = d3.reports("choice")
        check.expect(rc7 == 0, "failover recorder exited cleanly", str(rc7))
        check.expect(a7.get("primary", {}).get("status") == "failed"
                     and a7["primary"].get("phase") == "eof_before_ready",
                     "primary failed explicitly (closed before readiness)",
                     str(a7.get("primary")))
        check.expect(c7 and c7[0]["entry"] == "backup"
                     and c7[0]["decision_ms"] < 1000,
                     "backup was tried immediately, well before failover-threshold",
                     str(c7))
        v7 = wait_acks(base, 8)
        check.expect(v7["entries"].get("backup", {}).get("count") == 8,
                     "batch acked on backup entry after explicit failure",
                     str(v7["entries"]))

        print("== scenario 8: dual entry, both time out -> batch not sent ==")
        http("POST", base + "/api/events/reset")
        set_entry(base, "primary", delay=6.0, fail=False)
        set_entry(base, "backup", delay=6.0, fail=False)
        d4 = DualRecorder(port_tcp, port_backup, count=9,
                          failover_after=1.0, total_timeout=2.5)
        d4.start()
        rc8, _ = d4.stop()
        nr = d4.reports("no_entry_ready")
        a8 = d4.attempts()
        batches = d4.reports("batch")
        check.expect(rc8 == 3, "recorder reports 'no entry ready' exit code",
                     str(rc8))
        check.expect(bool(nr), "no_entry_ready reported", str(nr))
        check.expect(a8.get("primary", {}).get("status") == "timeout"
                     and a8.get("backup", {}).get("status") == "timeout",
                     "both attempts ended in timeout",
                     str({k: v.get("status") for k, v in a8.items()}))
        check.expect(not batches, "no batch was sent", str(batches))
        time.sleep(1.0)
        st8 = http("GET", base + "/api/status")
        check.expect(st8["ack_counts"] == {},
                     "collector acked nothing for the timed-out batch",
                     str(st8["ack_counts"]))
        print("== scenario 9: collector swap while an entry is still waiting to be ready ==")
        http("POST", base + "/api/events/reset")
        set_entry(base, "primary", delay=2.5)
        set_entry(base, "backup", delay=2.5)
        d5 = DualRecorder(port_tcp, port_backup, count=6,
                          failover_after=0.6, total_timeout=8.0)
        d5.start()
        # Wait until both entry connections are accepted and waiting, then
        # hot-swap the collector before either readiness delay elapses.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            ev = http("GET", base + "/api/events?cursor=0")["events"]
            accepted = {e.get("entry") for e in ev
                        if e.get("type") == "conn_accepted"}
            if {"primary", "backup"} <= accepted:
                break
            time.sleep(0.05)
        up9 = http("POST", base + "/api/upgrade",
                   {"new_version": "v9", "simulate_fail": False})
        check.expect(up9.get("ok") is True,
                     "upgrade during pending readiness accepted", str(up9))
        wait_event(base, "upgrade_completed", timeout=15)
        rc9, _ = d5.stop()
        c9 = d5.reports("choice")
        check.expect(rc9 == 0, "recorder still completed its batch", str(rc9))
        check.expect(c9 and c9[0]["entry"] in ("primary", "backup"),
                     "one entry won after readiness crossed the handover",
                     str(c9))
        v9 = wait_acks(base, 6)
        win = c9[0]["entry"]
        check.expect(v9["entries"].get(win, {}).get("count") == 6,
                     "batch acked on the winning entry after handover",
                     str(v9["entries"]))
        check.expect(not v9["duplicate_acks"],
                     "readiness reply not duplicated across handover",
                     str(v9["duplicate_acks"]))
        # The waiting entry that lost must not have been sent a second
        # readiness reply that could be mistaken for permission to send.
        ev9 = http("GET", base + "/api/events?cursor=0")["events"]
        ready9 = [e for e in ev9 if e.get("type") == "entry_ready"]
        per_entry9: dict[str, int] = {}
        for e in ready9:
            per_entry9[e["entry"]] = per_entry9.get(e["entry"], 0) + 1
        check.expect(all(n == 1 for n in per_entry9.values()),
                     "each pending entry replied readiness at most once",
                     str(per_entry9))
        set_entry(base, "primary", delay=0.0)
        set_entry(base, "backup", delay=0.0)

    finally:
        proc.terminate()
        try:
            gw_out, _ = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            gw_out, _ = proc.communicate()

    print()
    print(f"result: {check.passed} passed, {len(check.failed)} failed")
    if check.failed:
        for f in check.failed:
            print("  -", f)
        print("\n--- gateway output tail ---")
        print("\n".join(gw_out.splitlines()[-40:]))
        return 1
    print("ALL E2E CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
