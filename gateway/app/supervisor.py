"""Gateway supervisor.

Owns the recorder-facing TCP listen socket and the FastAPI operation page,
and orchestrates collector generations as child processes. Collector
processes never accept TCP connections themselves: the supervisor accepts
and passes the connection fd over a Unix control socket (SCM_RIGHTS).

During an upgrade the supervisor relays the old collector's handover
stream -- TCP fds plus unprocessed partial lines -- into the new
collector, so the recorder's single long-lived TCP connection is never
closed and every reading is acked exactly once in send order.
"""
from __future__ import annotations

import collections
import json
import os
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from .frame import recv_frame, send_frame

TCP_PORT = int(os.environ.get("GATEWAY_TCP_PORT", "9000"))
BACKUP_TCP_PORT = int(os.environ.get("GATEWAY_BACKUP_TCP_PORT", "9001"))
WEB_PORT = int(os.environ.get("GATEWAY_WEB_PORT", "8000"))
RUN_DIR = os.environ.get("GATEWAY_RUN_DIR", "/var/run/gateway")
FAIL_PATH = os.environ.get("COLLECTOR_FAIL_PATH",
                          os.path.join(RUN_DIR, "collector-fail"))
INITIAL_VERSION = os.environ.get("COLLECTOR_INITIAL_VERSION", "v1")
READY_TIMEOUT = float(os.environ.get("COLLECTOR_READY_TIMEOUT", "10"))
EVENT_HISTORY = 5000
ENTRIES = ("primary", "backup")


@dataclass
class Generation:
    version: str
    proc: subprocess.Popen
    ctrl: socket.socket          # supervisor side of the control socketpair
    reader: threading.Thread
    ready: threading.Event = field(default_factory=threading.Event)
    failed: bool = False
    fail_reason: str = ""
    dead: bool = False
    exit_code: int | None = None


class Supervisor:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.fail_path = FAIL_PATH
        self.events: collections.deque[dict[str, Any]] = collections.deque(maxlen=EVENT_HISTORY)
        self.seq = 0
        # ack bookkeeping, keyed by peer connection -> {reading_id: event}
        self.acks: dict[str, dict[int, dict[str, Any]]] = {}
        self.duplicates: list[dict[str, Any]] = []
        self.generations: dict[int, Generation] = {}
        self.current: Generation | None = None
        self.handover_in_progress = False
        self.listen_sock: socket.socket | None = None
        self.listen_socks: dict[str, socket.socket] = {}
        # Per-entry readiness policy applied to new recorder connections.
        # ``delay`` postpones the collector's readiness reply; ``fail`` makes
        # the entry explicitly unusable (the connection is closed right after
        # accept, which the dual-entry recorder treats as an immediate
        # failure and answers by trying the other entry).
        self.entries: dict[str, dict[str, Any]] = {
            "primary": {"port": TCP_PORT, "delay": 0.0, "fail": False},
            "backup": {"port": BACKUP_TCP_PORT, "delay": 0.0, "fail": False},
        }
        self._stop = threading.Event()
        self.recorder_proc: subprocess.Popen | None = None
        self.recorder_thread: threading.Thread | None = None
        self.recorder_started_at: float | None = None
        self.recorder_mode = "single"
        self.recorder_attempts: list[dict[str, Any]] = []
        self.recorder_choice: dict[str, Any] | None = None
        self.recorder_no_entry_ready: dict[str, Any] | None = None
        self.recorder_batch: dict[str, Any] | None = None
        self.connection_count_total = 0

    # ------------------------------------------------------------------ #
    # Event store
    # ------------------------------------------------------------------ #
    def record_event(self, event: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            self.seq += 1
            event = dict(event)
            event["seq"] = self.seq
            event.setdefault("ts", time.time())
            if event.get("type") == "ack":
                peer = str(event.get("peer", ""))
                rid = event.get("id")
                bucket = self.acks.setdefault(peer, {})
                if rid in bucket:
                    dup = {"type": "duplicate_ack", "id": rid, "peer": peer,
                           "first_version": bucket[rid].get("version"),
                           "dup_version": event.get("version"),
                           "ts": event["ts"]}
                    self.duplicates.append(dup)
                    self.events.append(dup)
                else:
                    bucket[rid] = event
            self.events.append(event)
            return event

    def log(self, msg: str, **extra: Any) -> None:
        self.record_event({"type": "supervisor_log", "msg": msg, **extra})

    # ------------------------------------------------------------------ #
    # Collector process lifecycle
    # ------------------------------------------------------------------ #
    def spawn_collector(self, version: str) -> Generation:
        parent_sock, child_sock = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_SEQPACKET)
        parent_sock.set_inheritable(False)
        cmd = [sys.executable, "-m", "app.collector", version, str(child_sock.fileno())]
        env = dict(os.environ, COLLECTOR_FAIL_PATH=self.fail_path)
        proc = subprocess.Popen(
            cmd, pass_fds=[child_sock.fileno()], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        # The child holds its own duplicate; parent does not need this end.
        child_sock.close()

        gen = Generation(version=version, proc=proc, ctrl=parent_sock,
                         reader=threading.current_thread())  # placeholder
        gen.reader = threading.Thread(
            target=self._reader_loop, args=(gen,),
            name=f"reader-{version}-{proc.pid}", daemon=True,
        )
        gen.reader.start()
        with self.lock:
            # Keyed by pid: the same version label can legitimately be
            # spawned again (rollback, crash restart) and must not overwrite
            # the previous generation's record.
            self.generations[proc.pid] = gen
        self.log(f"spawned collector {version} pid={proc.pid}")
        return gen

    def _reader_loop(self, gen: Generation) -> None:
        """Read control frames from one collector generation until it exits."""
        try:
            while not self._stop.is_set():
                try:
                    frame, fds = recv_frame(gen.ctrl)
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    self.log(f"control channel error from {gen.version}: {exc}")
                    break
                if frame is None:
                    break
                # Fds may accompany handover-relayed frames only on the
                # handover channel; control-channel frames carry none.
                for fd in fds:
                    os.close(fd)
                ftype = frame.get("type")
                if ftype == "ready":
                    gen.ready.set()
                    self.record_event(frame)
                elif ftype == "start_failed":
                    gen.failed = True
                    gen.fail_reason = frame.get("reason", "unknown")
                    gen.ready.set()
                    self.record_event(frame)
                else:
                    self.record_event(frame)
        finally:
            gen.ready.set()  # unblock anyone waiting, success or failure
            try:
                gen.ctrl.close()
            except OSError:
                pass
            try:
                gen.exit_code = gen.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                gen.proc.kill()
                gen.exit_code = gen.proc.wait()
            gen.dead = True
            self.log(f"collector {gen.version} exited rc={gen.exit_code}")
            self._on_generation_death(gen)

    def _on_generation_death(self, gen: Generation) -> None:
        with self.lock:
            if self.handover_in_progress:
                return
            if self.current is gen:
                # Unexpected crash of the active generation: restart the
                # same version. Its TCP fds died with the process, so the
                # recorder will observe EOF (crash semantics, no data loss
                # for readings already acked).
                self.log(f"active collector {gen.version} crashed; restarting")
                try:
                    new_gen = self.spawn_collector(gen.version)
                except OSError as exc:
                    self.log(f"restart failed: {exc}")
                    return
                self.current = new_gen

    # ------------------------------------------------------------------ #
    # Upgrade / rollback
    # ------------------------------------------------------------------ #
    def upgrade(self, new_version: str, simulate_fail: bool = False) -> dict[str, Any]:
        with self.lock:
            if self.handover_in_progress:
                return {"ok": False, "error": "another handover is in progress"}
            old = self.current
            if old is None:
                return {"ok": False, "error": "no active collector"}
            if old.dead:
                return {"ok": False, "error": "active collector is dead"}
            self.handover_in_progress = True

        try:
            os.makedirs(RUN_DIR, exist_ok=True)
            if simulate_fail:
                with open(self.fail_path, "w") as fh:
                    fh.write(f"requested at {time.time()}\n")
                self.log(f"fail marker installed for {new_version} "
                         f"(simulated startup failure)")

            # 1. Spawn the new generation and require a clean startup.
            try:
                new_gen = self.spawn_collector(new_version)
            except OSError as exc:
                self._clear_fail_marker()
                self.handover_in_progress = False
                return {"ok": False, "error": f"spawn failed: {exc}"}

            if not new_gen.ready.wait(READY_TIMEOUT):
                self.log(f"collector {new_version} did not become ready, killing")
                new_gen.proc.kill()
                self._clear_fail_marker()
                self.handover_in_progress = False
                self.record_event({"type": "upgrade_failed",
                                   "new_version": new_version,
                                   "reason": "ready timeout"})
                return {"ok": False, "error": "new collector startup timed out",
                        "active_version": old.version}

            if new_gen.failed:
                # Give it a moment to finish its exit path.
                new_gen.proc.wait(timeout=5)
                reason = new_gen.fail_reason
                self._clear_fail_marker()
                self.handover_in_progress = False
                self.record_event({"type": "upgrade_failed",
                                   "new_version": new_version, "reason": reason})
                self.log(f"upgrade rolled back: {new_version} failed to start "
                         f"({reason}); {old.version} keeps serving")
                return {"ok": False, "error": reason, "rolled_back": True,
                        "active_version": old.version}

            self._clear_fail_marker()
            self.record_event({"type": "upgrade_started",
                               "old_version": old.version,
                               "new_version": new_version})

            # 2. Give the old collector a private channel to the successor.
            # Keep THIS end open for the whole handover: closing it here
            # would tear down the socketpair before the old collector can
            # forward its connection fds. The relay thread closes it once
            # handover_complete has been relayed (or the channel errors).
            handover_parent, handover_old = socket.socketpair(
                socket.AF_UNIX, socket.SOCK_SEQPACKET)
            try:
                send_frame(old.ctrl,
                           {"type": "takeover", "new_version": new_version},
                           fds=[handover_old.fileno()])
            finally:
                # The old collector now owns its duplicate of the pair end.
                handover_old.close()

            # 3. Relay handover frames (and their TCP fds) into the new gen.
            relay = threading.Thread(
                target=self._relay_handover,
                args=(handover_parent, old, new_gen),
                name=f"handover-{new_version}", daemon=True)
            relay.start()
            return {"ok": True, "from": old.version, "to": new_version,
                    "simulate_fail": simulate_fail}
        except Exception as exc:  # pragma: no cover - defensive
            self._clear_fail_marker()
            self.handover_in_progress = False
            self.log(f"upgrade error: {exc!r}")
            return {"ok": False, "error": repr(exc)}

    def _relay_handover(self, handover: socket.socket,
                        old: Generation, new_gen: Generation) -> None:
        forwarded = 0
        completed = False
        try:
            while True:
                frame, fds = recv_frame(handover)
                if frame is None:
                    self.log("handover channel closed before completion")
                    break
                try:
                    send_frame(new_gen.ctrl, frame, fds=fds)
                finally:
                    for fd in fds:
                        os.close(fd)
                if frame.get("type") == "new_conn":
                    forwarded += 1
                if frame.get("type") == "handover_complete":
                    completed = True
                    break
        except OSError as exc:
            self.record_event({"type": "handover_nack", "reason": str(exc)})
        finally:
            try:
                handover.close()
            except OSError:
                pass
            with self.lock:
                if completed:
                    # Only promote the successor once every connection fd
                    # has actually been relayed. On failure the old
                    # collector keeps its fds and remains current.
                    self.current = new_gen
                self.handover_in_progress = False
            if not completed:
                if old.dead:
                    # The old collector died mid-handover. The successor is
                    # healthy and ready; promote it rather than leave the
                    # gateway with no active collector. Its connection fds
                    # were lost with the crashed process (crash semantics).
                    with self.lock:
                        self.current = new_gen
                    self.record_event({"type": "upgrade_completed",
                                       "old_version": old.version,
                                       "new_version": new_gen.version,
                                       "connections_forwarded": forwarded,
                                       "note": "old collector died during handover"})
                    self.log(f"old collector died mid-handover; promoted "
                             f"{new_gen.version} with {forwarded} fd(s) relayed")
                    return
                self.record_event({"type": "upgrade_failed",
                                   "new_version": new_gen.version,
                                   "reason": "old collector did not complete "
                                             "the handover; keeping old version"})
                self.log(f"handover aborted: {old.version} keeps serving; "
                         f"stopping unused successor {new_gen.version}")
                if new_gen.proc.poll() is None:
                    new_gen.proc.terminate()
                    try:
                        new_gen.proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        new_gen.proc.kill()
                        new_gen.proc.wait()
                return
            self.record_event({"type": "upgrade_completed",
                               "old_version": old.version,
                               "new_version": new_gen.version,
                               "connections_forwarded": forwarded})
            self.log(f"handover complete: {old.version} -> {new_gen.version}, "
                     f"{forwarded} connection(s) forwarded on the same TCP fd(s)")

            # The old generation should exit on its own; reap it.
            try:
                old.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.log(f"old collector {old.version} did not exit, killing")
                old.proc.kill()
                old.proc.wait()

    def _clear_fail_marker(self) -> None:
        try:
            os.unlink(self.fail_path)
        except FileNotFoundError:
            pass

    # ------------------------------------------------------------------ #
    # Recorder-facing TCP accept loops (one per entry)
    # ------------------------------------------------------------------ #
    def accept_loop(self, entry: str = "primary") -> None:
        listen_sock = self.listen_socks[entry]
        while not self._stop.is_set():
            try:
                conn, addr = listen_sock.accept()
            except OSError:
                return
            threading.Thread(
                target=self._handle_accepted,
                args=(entry, conn, addr), daemon=True,
                name=f"accept-{entry}").start()

    def _handle_accepted(self, entry: str, conn: socket.socket,
                         addr: tuple[str, int]) -> None:
        peer = f"{addr[0]}:{addr[1]}"
        with self.lock:
            policy = dict(self.entries.get(entry, {}))
            wait_for_handover = self.handover_in_progress
        # During a handover the active collector is about to give its fds
        # away and exit; hold the new connection in the kernel backlog until
        # the successor is current, so nothing is handed to a dying process.
        # The wait must NOT hold self.lock: the relay thread needs it to
        # clear handover_in_progress.
        if wait_for_handover:
            deadline = time.monotonic() + READY_TIMEOUT
            while time.monotonic() < deadline:
                with self.lock:
                    if not self.handover_in_progress:
                        break
                time.sleep(0.05)
        with self.lock:
            gen = self.current
        self.record_event({"type": "entry_connect", "entry": entry,
                           "peer": peer, "delay": policy.get("delay", 0.0),
                           "simulated_fail": policy.get("fail", False)})
        if policy.get("fail"):
            # Explicit entry failure: refuse to serve this entry. The
            # recorder sees EOF before any readiness reply and fails over.
            self.record_event({"type": "entry_failed", "entry": entry,
                               "peer": peer,
                               "reason": "simulated entry failure"})
            self.log(f"entry {entry} refused connection {peer} "
                     f"(simulated failure)")
            try:
                conn.close()
            except OSError:
                pass
            return
        if gen is None or gen.dead:
            self.log("no live collector, rejecting recorder connection")
            conn.close()
            return
        try:
            send_frame(gen.ctrl,
                       {"type": "new_conn", "peer": peer, "leftover": "",
                        "entry": entry, "delay": policy.get("delay", 0.0)},
                       fds=[conn.fileno()])
        except OSError as exc:
            self.log(f"failed to hand connection to {gen.version}: {exc}")
            conn.close()
            return
        finally:
            conn.close()
        with self.lock:
            self.connection_count_total += 1
        self.record_event({"type": "conn_accepted", "peer": peer,
                           "entry": entry, "version": gen.version})

    def set_entry_policy(self, entry: str, delay: float | None = None,
                         fail: bool | None = None) -> dict[str, Any]:
        if entry not in self.entries:
            return {"ok": False, "error": f"unknown entry {entry!r}"}
        with self.lock:
            if delay is not None:
                self.entries[entry]["delay"] = max(0.0, float(delay))
            if fail is not None:
                self.entries[entry]["fail"] = bool(fail)
            policy = dict(self.entries[entry])
        self.log(f"entry {entry} policy: delay={policy['delay']}s "
                 f"simulated_fail={policy['fail']}")
        return {"ok": True, "entry": entry, **policy}

    # ------------------------------------------------------------------ #
    # Simulated recorder management (driven from the web page)
    # ------------------------------------------------------------------ #
    def start_recorder(self, mode: str = "single", count: int = 60,
                       interval: float = 0.2, gap_at: int = 0,
                       gap_seconds: float = 4.0,
                       failover_after: float = 2.0,
                       total_timeout: float = 10.0) -> dict[str, Any]:
        with self.lock:
            if self.recorder_proc is not None and self.recorder_proc.poll() is None:
                return {"ok": False, "error": "a recorder is already running; "
                                              "it connects exactly once by design"}
            cmd = [sys.executable, "-m", "app.recorder",
                   "--mode", mode,
                   "--host", "127.0.0.1", "--port", str(TCP_PORT),
                   "--backup-host", "127.0.0.1",
                   "--backup-port", str(self.entries["backup"]["port"]),
                   "--count", str(count), "--interval", str(interval),
                   "--gap-at", str(gap_at), "--gap-seconds", str(gap_seconds),
                   "--failover-after", str(failover_after),
                   "--total-timeout", str(total_timeout)]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True)
            self.recorder_proc = proc
            self.recorder_started_at = time.time()
            self.recorder_attempts = []
            self.recorder_choice = None
            self.recorder_no_entry_ready = None
            self.recorder_batch = None
            self.recorder_mode = mode
            self.recorder_thread = threading.Thread(
                target=self._pump_recorder_output, args=(proc,), daemon=True)
            self.recorder_thread.start()
        if mode == "dual":
            self.log(f"dual-entry recorder started pid={proc.pid}, "
                     f"entries=primary:{TCP_PORT} backup:"
                     f"{self.entries['backup']['port']}, "
                     f"failover_after={failover_after}s, "
                     f"total_timeout={total_timeout}s")
        else:
            self.log(f"simulated recorder started pid={proc.pid}, "
                     f"count={count}, interval={interval}s (single entry)")
        return {"ok": True, "pid": proc.pid, "mode": mode}

    def _pump_recorder_output(self, proc: subprocess.Popen) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            msg = line.rstrip()
            if msg.startswith("@@GW@@ "):
                try:
                    report = json.loads(msg[len("@@GW@@ "):])
                except json.JSONDecodeError:
                    self.record_event({"type": "recorder_log", "msg": msg})
                    continue
                self._record_recorder_report(report)
                continue
            self.record_event({"type": "recorder_log", "msg": msg})
        rc = proc.wait()
        self.record_event({"type": "recorder_exited", "rc": rc})

    def _record_recorder_report(self, report: dict[str, Any]) -> None:
        kind = report.get("kind")
        with self.lock:
            if kind == "attempt":
                entry = report.get("entry")
                snapshot = dict(report)
                # Keep the latest status per entry in one place; keep every
                # transition in the event stream for timing inspection.
                prev = next((a for a in self.recorder_attempts
                             if a.get("entry") == entry), None)
                if prev is None:
                    self.recorder_attempts.append(snapshot)
                else:
                    prev.update(snapshot)
            elif kind == "choice":
                self.recorder_choice = dict(report)
            elif kind == "no_entry_ready":
                self.recorder_no_entry_ready = dict(report)
            elif kind == "batch":
                self.recorder_batch = dict(report)
        self.record_event({"type": "recorder_report", **report})

    def recorder_state(self) -> dict[str, Any]:
        with self.lock:
            proc = self.recorder_proc
            return {
                "mode": getattr(self, "recorder_mode", "single"),
                "running": proc is not None and proc.poll() is None,
                "attempts": [dict(a) for a in self.recorder_attempts],
                "choice": dict(self.recorder_choice)
                if self.recorder_choice else None,
                "no_entry_ready": dict(self.recorder_no_entry_ready)
                if self.recorder_no_entry_ready else None,
                "batch": dict(self.recorder_batch) if self.recorder_batch else None,
                "entries": {name: dict(p) for name, p in self.entries.items()},
            }

    def stop_recorder(self) -> dict[str, Any]:
        with self.lock:
            proc = self.recorder_proc
        if proc is None or proc.poll() is not None:
            return {"ok": False, "error": "no running recorder"}
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        self.log("simulated recorder stopped")
        return {"ok": True}

    # ------------------------------------------------------------------ #
    # Snapshot for the web page / verification
    # ------------------------------------------------------------------ #
    def status(self) -> dict[str, Any]:
        with self.lock:
            gens = []
            for g in self.generations.values():
                gens.append({"version": g.version, "pid": g.proc.pid,
                             "alive": g.proc.poll() is None,
                             "ready": g.ready.is_set(), "failed": g.failed,
                             "exit_code": g.exit_code,
                             "current": self.current is g})
            recorder_alive = (self.recorder_proc is not None
                              and self.recorder_proc.poll() is None)
            acked_per_peer = {peer: sorted(ids) for peer, ids in
                              ((p, list(m.keys())) for p, m in self.acks.items())}
            return {
                "current_version": self.current.version if self.current else None,
                "handover_in_progress": self.handover_in_progress,
                "generations": gens,
                "entries": {name: {"port": p["port"], "delay": p["delay"],
                                   "simulated_fail": p["fail"]}
                            for name, p in self.entries.items()},
                "entry_ports": {"primary": TCP_PORT, "backup": BACKUP_TCP_PORT},
                "recorder_alive": recorder_alive,
                "recorder_started_at": self.recorder_started_at,
                "recorder": self.recorder_state(),
                "tcp_connections_accepted_total": self.connection_count_total,
                "ack_counts": {p: len(m) for p, m in self.acks.items()},
                "acked_ids_per_connection": acked_per_peer,
                "duplicate_acks": list(self.duplicates),
                "fail_marker_present": os.path.exists(self.fail_path),
                "ts": time.time(),
            }

    def events_since(self, cursor: int) -> dict[str, Any]:
        with self.lock:
            items = [e for e in self.events if e["seq"] > cursor]
            next_cursor = self.events[-1]["seq"] if self.events else cursor
            return {"events": items, "cursor": next_cursor}

    def verify(self) -> dict[str, Any]:
        """Independently check ack completeness, order and uniqueness."""
        with self.lock:
            report: dict[str, Any] = {"connections": [], "entries": {}}
            ok = True
            for peer, ids_map in self.acks.items():
                ids = sorted(ids_map)
                contiguous = ids == list(range(1, len(ids) + 1)) if ids else True
                # Version boundary: which ids were acked by which version.
                per_version: dict[str, list[int]] = {}
                for rid in ids:
                    v = ids_map[rid].get("version", "?")
                    per_version.setdefault(v, []).append(rid)
                # Entry attribution: which entry's connection carried them.
                per_entry: dict[str, list[int]] = {}
                for rid in ids:
                    e = ids_map[rid].get("entry", "primary")
                    per_entry.setdefault(e, []).append(rid)
                conn_report = {
                    "peer": peer,
                    "count": len(ids),
                    "ids": ids,
                    "contiguous_from_1": contiguous,
                    "unique": len(ids) == len(set(ids)),
                    "entry": ids_map[ids[0]].get("entry", "primary") if ids else None,
                    "entries": {e: {"count": len(r), "ids": r}
                                for e, r in sorted(per_entry.items())},
                    "versions": {v: {"count": len(r), "first_id": min(r),
                                     "last_id": max(r)}
                                 for v, r in sorted(per_version.items())},
                }
                report["connections"].append(conn_report)
                for e, r in per_entry.items():
                    slot = report["entries"].setdefault(
                        e, {"count": 0, "ids": [], "versions": {}})
                    slot["count"] += len(r)
                    slot["ids"].extend(r)
                    for rid in r:
                        v = ids_map[rid].get("version", "?")
                        vs = slot["versions"].setdefault(
                            v, {"count": 0, "first_id": rid, "last_id": rid})
                        vs["count"] += 1
                        vs["first_id"] = min(vs["first_id"], rid)
                        vs["last_id"] = max(vs["last_id"], rid)
                ok = ok and contiguous and conn_report["unique"]
            for e in report["entries"].values():
                e["ids"].sort()
            report["duplicate_acks"] = list(self.duplicates)
            report["ok"] = ok and not self.duplicates
            return report

    def reset_events(self) -> None:
        with self.lock:
            self.events.clear()
            self.acks.clear()
            self.duplicates.clear()
            self.seq = 0

    # ------------------------------------------------------------------ #
    def shutdown(self) -> None:
        self._stop.set()
        for sock in list(self.listen_socks.values()):
            try:
                sock.close()
            except OSError:
                pass
        if self.listen_sock is not None:
            try:
                self.listen_sock.close()
            except OSError:
                pass
        for gen in list(self.generations.values()):
            if gen.proc.poll() is None:
                gen.proc.terminate()
        if self.recorder_proc and self.recorder_proc.poll() is None:
            self.recorder_proc.terminate()


def create_supervisor() -> Supervisor:
    sup = Supervisor()
    os.makedirs(RUN_DIR, exist_ok=True)
    sup._clear_fail_marker()
    env = dict(os.environ)
    env["COLLECTOR_FAIL_PATH"] = sup.fail_path
    os.environ["COLLECTOR_FAIL_PATH"] = sup.fail_path

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", TCP_PORT))
    srv.listen(16)
    sup.listen_socks["primary"] = srv
    sup.listen_sock = srv  # backward-compatible alias for the primary entry

    # Backup entry: a second recorder-facing listener. It is a truly separate
    # endpoint (its own port), so the recorder's failover race is a real
    # network-level choice, not just a flag.
    backup_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    backup_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    backup_srv.bind(("0.0.0.0", BACKUP_TCP_PORT))
    backup_srv.listen(16)
    sup.listen_socks["backup"] = backup_srv

    first = sup.spawn_collector(INITIAL_VERSION)
    if not first.ready.wait(READY_TIMEOUT):
        raise RuntimeError("initial collector failed to become ready")
    if first.failed:
        raise RuntimeError(f"initial collector failed: {first.fail_reason}")
    sup.current = first
    sup.log(f"supervisor up: tcp=:{TCP_PORT} (primary), "
            f"tcp=:{BACKUP_TCP_PORT} (backup), web=:{WEB_PORT}, "
            f"active collector {INITIAL_VERSION}")

    for entry in ENTRIES:
        threading.Thread(target=sup.accept_loop, args=(entry,),
                         name=f"accept-{entry}", daemon=True).start()
    return sup
