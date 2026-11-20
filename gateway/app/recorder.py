"""Simulated cold-storage temperature recorder.

Two entry modes:

``--mode single`` (default, backward compatible)
    Opens exactly one TCP connection to ``--host/--port`` and starts sending
    readings immediately. This is the legacy single-entry recorder; it never
    reads from the socket and is unaffected by the collector's readiness
    reply line.

``--mode dual``
    Competes on two gateway entries and sends the whole batch over exactly
    one of them:

    1. Connect to the primary entry first and wait for the collector's
       readiness reply (one JSON line ``{"type": "ready", ...}``).
    2. If the primary connection fails outright (refused / reset / closed
       before the reply), try the backup immediately.
    3. If the primary is connected but has not replied within
       ``--failover-after`` seconds, start the backup too -- while still
       waiting on the primary.
    4. The first readiness reply within ``--total-timeout`` wins. The losing
       attempt is aborted and its socket closed; a reply that arrives on it
       afterwards is discarded and can never trigger sending.
    5. If neither entry is ready before the deadline, the batch is not sent
       at all.

The recorder never reconnects after the connection is chosen. It reports
structured progress lines (``@@GW@@ {...}``) so the operation page can show
each entry's attempt result and how the winner was picked.

Line format on the chosen connection (one JSON object per line, ``\\n``)::

    {"id": 7, "sensor": "freezer-1", "temp_c": -18.4, "ts": 1731...}
"""
from __future__ import annotations

import argparse
import json
import random
import socket
import sys
import threading
import time

REPORT_PREFIX = "@@GW@@ "


def emit(kind: str, **fields) -> None:
    """Structured attempt/batch report consumed by the operation page."""
    payload = {"kind": kind, "ts": round(time.time(), 3), **fields}
    print(REPORT_PREFIX + json.dumps(payload, ensure_ascii=False), flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="simulated temperature recorder")
    p.add_argument("--host", default="127.0.0.1",
                   help="primary (single mode: the only) entry host")
    p.add_argument("--port", type=int, default=9000,
                   help="primary (single mode: the only) entry port")
    p.add_argument("--mode", choices=["single", "dual"], default="single",
                   help="single = legacy one-entry recorder; dual = race two "
                        "entries until the first readiness reply")
    p.add_argument("--backup-host", default=None,
                   help="backup entry host (defaults to --host)")
    p.add_argument("--backup-port", type=int, default=9001,
                   help="backup entry port")
    p.add_argument("--failover-after", type=float, default=2.0,
                   help="dual: seconds to wait for the primary reply before "
                        "starting the backup as well (primary keeps waiting)")
    p.add_argument("--total-timeout", type=float, default=10.0,
                   help="dual: total seconds to wait for any readiness reply")
    p.add_argument("--connect-timeout", type=float, default=3.0,
                   help="TCP connect timeout per entry")
    p.add_argument("--count", type=int, default=60,
                   help="total readings to send (ids are 1..count)")
    p.add_argument("--interval", type=float, default=0.2,
                   help="seconds between readings")
    p.add_argument("--gap-at", type=int, default=0,
                   help="insert a pause BEFORE sending this id (0 = no gap)")
    p.add_argument("--gap-seconds", type=float, default=3.0,
                   help="length of the pause; upgrade during this window")
    p.add_argument("--sensor", default="freezer-1")
    return p.parse_args()


# ---------------------------------------------------------------------- #
# Dual-entry race
# ---------------------------------------------------------------------- #
class EntryAttempt:
    """One connection attempt to one entry, run in its own thread."""

    def __init__(self, name: str, host: str, port: int, connect_timeout: float,
                 total_deadline: float, shared: "RaceState") -> None:
        self.name = name
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.total_deadline = total_deadline
        self.shared = shared
        self.sock: socket.socket | None = None
        self.won = False
        self.finished = threading.Event()

    def run(self) -> None:
        try:
            self._race()
        finally:
            self.finished.set()

    def _race(self) -> None:
        t0 = time.monotonic()
        emit("attempt", entry=self.name, status="connecting",
             host=self.host, port=self.port)
        try:
            sock = socket.create_connection(
                (self.host, self.port), timeout=self.connect_timeout)
        except OSError as exc:
            self.shared.failed(self.name)
            emit("attempt", entry=self.name, status="failed",
                 phase="connect", error=f"{type(exc).__name__}: {exc}",
                 connect_ms=round((time.monotonic() - t0) * 1000, 1))
            return
        self.sock = sock
        connected_at = time.monotonic()
        emit("attempt", entry=self.name, status="connected",
             connect_ms=round((connected_at - t0) * 1000, 1))

        # Non-blocking-ish reads: short timeouts keep the cancel flag and
        # the overall deadline responsive.
        sock.settimeout(0.05)
        buf = b""
        try:
            while True:
                if self.shared.cancelled.is_set():
                    late_deadline = self.shared.deadline_hit
                    emit("attempt", entry=self.name,
                         status="timeout" if late_deadline else "aborted",
                         phase="waiting_ready" if late_deadline
                         else "before_ready",
                         waited_ms=round((time.monotonic() - t0) * 1000, 1))
                    return
                if time.monotonic() >= self.total_deadline:
                    emit("attempt", entry=self.name, status="timeout",
                         phase="waiting_ready",
                         waited_ms=round((time.monotonic() - t0) * 1000, 1))
                    return
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    continue
                except OSError as exc:
                    if self.shared.cancelled.is_set():
                        emit("attempt", entry=self.name, status="aborted",
                             phase="socket_closed_after_choice",
                             waited_ms=round((time.monotonic() - t0) * 1000, 1))
                        return
                    emit("attempt", entry=self.name, status="failed",
                         phase="before_ready",
                         error=f"{type(exc).__name__}: {exc}",
                         waited_ms=round((time.monotonic() - t0) * 1000, 1))
                    self.shared.failed(self.name)
                    return
                if not chunk:
                    if self.shared.cancelled.is_set():
                        emit("attempt", entry=self.name, status="aborted",
                             phase="closed_after_choice",
                             waited_ms=round((time.monotonic() - t0) * 1000, 1))
                        return
                    # Peer closed before any readiness reply: an explicit
                    # failure for this entry, try the other one right away.
                    emit("attempt", entry=self.name, status="failed",
                         phase="eof_before_ready",
                         waited_ms=round((time.monotonic() - t0) * 1000, 1))
                    self.shared.failed(self.name)
                    return
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    reply = self._parse_ready(line)
                    if reply is None:
                        continue
                    if self.shared.claim(self.name):
                        self.won = True
                        emit("attempt", entry=self.name, status="ready",
                             phase="chosen",
                             connect_ms=round((connected_at - t0) * 1000, 1),
                             ready_ms=round((time.monotonic() - t0) * 1000, 1),
                             reply=reply)
                        return
                    # Another entry already won (or we were aborted): this
                    # readiness reply is late. Report it and stop here; this
                    # thread closes its own socket in ``finally``.
                    emit("attempt", entry=self.name, status="ready_late",
                         phase="after_choice",
                         ready_ms=round((time.monotonic() - t0) * 1000, 1),
                         reply=reply)
                    return
        finally:
            if not self.won:
                # Losing attempt: never leave its socket open. This is what
                # guarantees a late readiness reply on it cannot trigger a
                # send (the reader is gone and the fd is closed).
                _close_quietly(sock)
            if self.sock is sock and not self.won:
                self.sock = None
            self.shared.thread_done(self.name)

    @staticmethod
    def _parse_ready(line: bytes) -> dict | None:
        line = line.strip()
        if not line:
            return None
        try:
            obj = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if isinstance(obj, dict) and obj.get("type") == "ready":
            return obj
        return None


class RaceState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.winner: str | None = None
        self.cancelled = threading.Event()
        self.deadline_hit = False
        self.failed_entries: set[str] = set()
        self.done: set[str] = set()
        self.threads: dict[str, threading.Thread] = {}

    def claim(self, name: str) -> bool:
        with self.lock:
            if self.winner is not None or self.cancelled.is_set():
                return False
            self.winner = name
            return True

    def failed(self, name: str) -> None:
        with self.lock:
            self.failed_entries.add(name)

    def thread_done(self, name: str) -> None:
        with self.lock:
            self.done.add(name)

    def start(self, attempt: EntryAttempt) -> None:
        th = threading.Thread(target=attempt.run, name=f"entry-{attempt.name}",
                              daemon=True)
        self.threads[attempt.name] = th
        th.start()


def race_entries(args: argparse.Namespace) -> tuple[socket.socket, str] | None:
    """Try both entries; return (socket, entry_name) of the winner or None."""
    t0 = time.monotonic()
    total_deadline = t0 + max(0.1, args.total_timeout)
    failover_at = t0 + max(0.0, args.failover_after)
    backup_host = args.backup_host or args.host
    emit("race", status="start", primary=f"{args.host}:{args.port}",
         backup=f"{backup_host}:{args.backup_port}",
         failover_after=args.failover_after, total_timeout=args.total_timeout)

    state = RaceState()
    primary = EntryAttempt("primary", args.host, args.port,
                           args.connect_timeout, total_deadline, state)
    state.start(primary)

    backup: EntryAttempt | None = None
    while time.monotonic() < total_deadline:
        with state.lock:
            winner = state.winner
            primary_failed = "primary" in state.failed_entries
            primary_done = "primary" in state.done
        if winner is not None:
            break
        now = time.monotonic()
        if backup is None and (primary_failed or now >= failover_at):
            if primary_failed:
                emit("race", status="primary_failed_try_backup",
                     at_ms=round((now - t0) * 1000, 1))
            else:
                emit("race", status="failover_started",
                     at_ms=round((now - t0) * 1000, 1),
                     note="primary still waiting; backup starts in parallel")
            backup = EntryAttempt("backup", backup_host, args.backup_port,
                                  args.connect_timeout, total_deadline, state)
            state.start(backup)
        elif primary_done and backup is None:
            # Primary ended without a usable reply (its failure already
            # requested a backup when explicit; this covers the timeout /
            # abort path) -- make sure the backup still gets tried.
            backup = EntryAttempt("backup", backup_host, args.backup_port,
                                  args.connect_timeout, total_deadline, state)
            state.start(backup)
        time.sleep(0.01)

    with state.lock:
        winner = state.winner
    winner_attempt = primary if winner == "primary" else backup
    if winner is None or winner_attempt is None or winner_attempt.sock is None:
        # Absolute deadline reached with no readiness reply: send nothing.
        state.deadline_hit = True
        state.cancelled.set()
        for th in state.threads.values():
            th.join(timeout=0.5)
        for attempt in (primary, backup):
            if attempt is not None:
                _close_quietly(attempt.sock)
                attempt.sock = None
        emit("no_entry_ready", waited_ms=round((time.monotonic() - t0) * 1000, 1),
             entries_tried=sorted(state.threads),
             failed_entries=sorted(state.failed_entries))
        return None

    # Winner found: abort the other attempt immediately. The losing thread
    # polls the cancel flag every 50ms and closes its own socket on exit;
    # wait for it so no second reader can outlive the choice.
    state.cancelled.set()
    loser = backup if winner == "primary" else primary
    loser_name = "backup" if winner == "primary" else "primary"
    loser_joined = True
    if loser is not None:
        loser_joined = loser.finished.wait(
            timeout=max(1.0, args.connect_timeout + 0.5))
    # Guarantee the losing socket is closed even if its thread is somehow
    # still busy. Once closed, a late readiness reply on it has no reader
    # and can never trigger a send.
    if loser is not None and loser.sock is not None:
        _close_quietly(loser.sock)
        loser.sock = None
    emit("choice", entry=winner,
         decision_ms=round((time.monotonic() - t0) * 1000, 1),
         other_entry_closed=loser is not None,
         other_entry_aborted=loser_joined,
         entries_tried=sorted(state.threads))
    return winner_attempt.sock, winner


def _close_quietly(sock: socket.socket | None) -> None:
    if sock is None:
        return
    try:
        sock.close()
    except OSError:
        pass


# ---------------------------------------------------------------------- #
# Sending
# ---------------------------------------------------------------------- #
def send_batch(sock: socket.socket, args: argparse.Namespace,
               entry: str) -> int:
    base_temp = -18.0
    sent = 0
    try:
        for rid in range(1, args.count + 1):
            if args.gap_at and rid == args.gap_at:
                print(f"recorder: pausing {args.gap_seconds}s before id {rid} "
                      f"-- safe window to switch the collector", flush=True)
                time.sleep(args.gap_seconds)
            reading = {
                "id": rid,
                "sensor": args.sensor,
                "temp_c": round(base_temp + random.uniform(-1.5, 1.5), 2),
                "ts": round(time.time(), 3),
            }
            line = json.dumps(reading, ensure_ascii=False) + "\n"
            try:
                sock.sendall(line.encode("utf-8"))
            except OSError as exc:
                emit("batch", status="send_failed", entry=entry, sent=sent,
                     error=f"{type(exc).__name__}: {exc}")
                return sent
            sent += 1
            print(f"recorder: sent id={rid} temp={reading['temp_c']}C",
                  flush=True)
            time.sleep(args.interval)
        print(f"recorder: all {args.count} readings sent over entry "
              f"{entry}; holding connection open for 2s then closing",
              flush=True)
        emit("batch", status="sent", entry=entry, sent=sent,
             expected=args.count)
        time.sleep(2)
    finally:
        _close_quietly(sock)
    return sent


def main() -> int:
    args = parse_args()

    if args.mode == "single":
        # Legacy path: one connection, send immediately, never read back.
        sock = socket.create_connection((args.host, args.port), timeout=10)
        peer = sock.getpeername()
        print(f"recorder: connected once to {peer[0]}:{peer[1]} "
              f"(local fd stays open for the entire run)", flush=True)
        emit("race", status="single_entry", entry="primary",
             endpoint=f"{args.host}:{args.port}")
        emit("choice", entry="primary", decision_ms=0, single_entry=True)
        sent = send_batch(sock, args, "primary")
        print("recorder: connection closed, exiting (no reconnect)", flush=True)
        return 0 if sent == args.count else 1

    result = race_entries(args)
    if result is None:
        print("recorder: no entry became ready before the deadline; "
              "this batch was not sent", flush=True)
        return 3
    sock, entry = result
    peer = sock.getpeername()
    print(f"recorder: entry {entry} won readiness race, sending whole batch "
          f"on this single connection to {peer[0]}:{peer[1]}", flush=True)
    try:
        sock.settimeout(None)
    except OSError:
        pass
    sent = send_batch(sock, args, entry)
    print("recorder: connection closed, exiting (no reconnect)", flush=True)
    return 0 if sent == args.count else 1


if __name__ == "__main__":
    sys.exit(main())
