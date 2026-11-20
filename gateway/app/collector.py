"""Temperature collector process.

Each generation of the collector runs as a separate process with its own
version label (e.g. ``v1`` / ``v2``). The supervisor owns the listen socket
and hands accepted recorder connections to the *current* collector through a
Unix control channel using SCM_RIGHTS.

Upgrade flow (the recorder's TCP connection never drops):

1. Supervisor spawns the new collector; if it fails to become ready, the
   supervisor aborts the switch and the old collector keeps serving.
2. Supervisor creates a handover socketpair and passes one end to the old
   collector in a ``takeover`` frame.
3. The old collector finishes every *fully received* reading already in its
   buffer, then forwards each still-open connection over that channel:
   the TCP fd plus any partial-line bytes already read. Data sitting in the
   kernel socket buffer travels with the fd automatically. The supervisor
   relays the frames (and fds) into the new collector's control channel.
4. The new collector continues reading from the exact same TCP connection
   and acks remaining readings in send order. The old collector exits.

Ack rule: one ``ack`` event per reading id, each id acked exactly once --
the successor drains its handed-over partial buffer before reading more.
"""
from __future__ import annotations

import json
import os
import random
import select
import socket
import sys
import time
from typing import Any

from .frame import recv_frame, send_frame

# Tuned via environment by the supervisor / tests.
FAIL_PATH = os.environ.get("COLLECTOR_FAIL_PATH", "/tmp/gateway-collector-fail")


class Collector:
    def __init__(self, version: str, control_fd: int) -> None:
        self.version = version
        self.control = socket.socket(fileno=control_fd)
        # Connections keyed by their local-side TCP fd, which stays stable
        # across the handover (the same kernel file description is passed).
        self.conns: dict[int, dict[str, Any]] = {}
        self.running = True

    # ------------------------------------------------------------------ #
    # Event reporting to the supervisor (also serves as liveness pings).
    # ------------------------------------------------------------------ #
    def emit(self, event: dict[str, Any]) -> None:
        event.setdefault("ts", time.time())
        event["version"] = self.version
        try:
            send_frame(self.control, event)
        except OSError:
            # Supervisor gone: nothing useful we can do.
            self.running = False

    def log(self, msg: str) -> None:
        self.emit({"type": "log", "msg": msg})

    # ------------------------------------------------------------------ #
    # Connection bookkeeping
    # ------------------------------------------------------------------ #
    def add_conn(self, fd: int, leftover: bytes = b"", peer: str = "") -> None:
        sock = socket.socket(fileno=fd)
        self.conns[fd] = {
            "sock": sock,
            "buf": bytearray(leftover),
            "peer": peer,
            "closed": False,
        }

    def close_conn(self, fd: int) -> None:
        entry = self.conns.pop(fd, None)
        if entry is not None:
            try:
                entry["sock"].close()
            except OSError:
                pass

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        # Simulated boot of a new release: presence of FAIL_PATH makes this
        # generation "fail to start" so the supervisor can test rollback.
        if os.path.exists(FAIL_PATH):
            try:
                send_frame(
                    self.control,
                    {"type": "start_failed",
                     "reason": "simulated startup failure (fail marker present)",
                     "version": self.version},
                )
            except OSError:
                pass
            time.sleep(0.2)
            sys.exit(2)

        self.emit({"type": "ready"})
        self.log(f"collector {self.version} ready, pid={os.getpid()}")

        while self.running:
            watches = [self.control]
            watches.extend(e["sock"] for e in self.conns.values())
            try:
                readable, _, _ = select.select(watches, [], [], 1.0)
            except OSError:
                break
            for sock in readable:
                if sock is self.control:
                    self.handle_control()
                    if not self.running:
                        break
                else:
                    self.handle_data(sock)

        self.emit({"type": "exiting"})
        try:
            self.control.close()
        except OSError:
            pass

    # ------------------------------------------------------------------ #
    # Control channel: new connections and handover commands
    # ------------------------------------------------------------------ #
    def handle_control(self) -> None:
        try:
            frame, fds = recv_frame(self.control)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            self.log(f"control channel error: {exc}")
            self.running = False
            return
        if frame is None:
            self.running = False
            return

        ftype = frame.get("type")
        if ftype == "new_conn":
            if not fds:
                self.log("new_conn frame without fd, ignored")
                return
            fd = fds[0]
            leftover = frame.get("leftover", "").encode("latin-1")
            self.add_conn(fd, leftover=leftover, peer=frame.get("peer", ""))
            self.log(f"accepted recorder connection fd={fd}, "
                     f"leftover={len(leftover)} byte(s)")
        elif ftype == "takeover":
            if not fds:
                self.log("takeover without successor fd, refused")
                self.emit({"type": "handover_nack",
                           "reason": "missing successor fd"})
                return
            self.do_handover(fds[0], frame.get("new_version", "?"))
        elif ftype in ("handover_complete", "ping"):
            pass  # acknowledgements / keepalives, nothing to do
        else:
            self.log(f"unknown control frame: {ftype!r}")

    # ------------------------------------------------------------------ #
    # Handover: drain complete readings, pass the live fd to the successor
    # ------------------------------------------------------------------ #
    def do_handover(self, successor_fd: int, new_version: str) -> None:
        successor = socket.socket(fileno=successor_fd)
        self.log(f"handover to {new_version} starting, {len(self.conns)} connection(s)")

        # 1. Finish every reading whose full JSON line is already buffered,
        #    and pull any further complete lines already delivered into the
        #    kernel buffer. A partial last line stays in `buf` and travels
        #    with the fd to the successor.
        for entry in list(self.conns.values()):
            self.drain_available(entry)

        # 2. Forward each live connection: same TCP fd + unprocessed bytes.
        forwarded = 0
        try:
            for fd, entry in list(self.conns.items()):
                if entry["closed"]:
                    continue
                sock = entry["sock"]
                send_frame(
                    successor,
                    {"type": "new_conn", "peer": entry.get("peer", ""),
                     "leftover": bytes(entry["buf"]).decode("latin-1")},
                    fds=[sock.fileno()],
                )
                forwarded += 1
            send_frame(successor,
                       {"type": "handover_complete", "old_version": self.version,
                        "connection_count": forwarded})
        except OSError as exc:
            self.emit({"type": "handover_nack", "reason": f"forward failed: {exc}"})
            try:
                successor.close()
            except OSError:
                pass
            return
        successor.close()

        # 3. Release our references. The kernel keeps the sockets alive
        #    because the successor now holds duplicate fds.
        for fd in list(self.conns):
            self.close_conn(fd)

        self.emit({"type": "handed_over", "new_version": new_version,
                   "connection_count": forwarded})
        self.running = False

    def drain_available(self, entry: dict[str, Any]) -> None:
        """Process every complete line currently readable on one connection."""
        sock: socket.socket = entry["sock"]
        while True:
            nl = entry["buf"].find(b"\n")
            if nl >= 0:
                self.process_line(entry, entry["buf"][:nl])
                del entry["buf"][: nl + 1]
                continue
            try:
                readable, _, _ = select.select([sock], [], [], 0)
            except OSError:
                return
            if not readable:
                return
            try:
                chunk = sock.recv(65536)
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                self.emit({"type": "conn_error", "peer": entry.get("peer", "")})
                entry["closed"] = True
                return
            if not chunk:
                # Recorder closed the connection.
                if entry["buf"]:
                    self.process_line(entry, bytes(entry["buf"]))
                    entry["buf"].clear()
                self.emit({"type": "conn_closed", "peer": entry.get("peer", "")})
                entry["closed"] = True
                return
            entry["buf"].extend(chunk)

    # ------------------------------------------------------------------ #
    # Normal steady-state reading
    # ------------------------------------------------------------------ #
    def handle_data(self, sock: socket.socket) -> None:
        entry = self.conns.get(sock.fileno())
        if entry is None:
            return
        try:
            chunk = sock.recv(65536)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            self.emit({"type": "conn_error", "peer": entry.get("peer", "")})
            self.close_conn(sock.fileno())
            return
        if not chunk:
            if entry["buf"]:
                self.process_line(entry, bytes(entry["buf"]))
                entry["buf"].clear()
            self.emit({"type": "conn_closed", "peer": entry.get("peer", "")})
            self.close_conn(sock.fileno())
            return

        entry["buf"].extend(chunk)
        while True:
            nl = entry["buf"].find(b"\n")
            if nl < 0:
                break
            line = bytes(entry["buf"][:nl])
            del entry["buf"][: nl + 1]
            self.process_line(entry, line)

    def process_line(self, entry: dict[str, Any], line: bytes) -> None:
        line = line.strip()
        if not line:
            return
        try:
            reading = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self.emit({"type": "bad_line", "error": str(exc),
                       "raw": line[:200].decode("latin-1", "replace")})
            return
        reading_id = reading.get("id")
        if reading_id is None:
            self.emit({"type": "bad_line", "error": "missing id",
                       "raw": line[:200].decode("latin-1", "replace")})
            return

        # Small bounded jitter to prove acks stay per-line and in send order
        # even when per-line processing time varies; never reorder or batch.
        time.sleep(random.uniform(0.001, 0.006))
        self.emit({
            "type": "ack",
            "id": reading_id,
            "peer": entry.get("peer", ""),
            "reading": reading,
        })


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: python -m app.collector <version> <control_fd>",
              file=sys.stderr)
        return 64
    version = sys.argv[1]
    try:
        control_fd = int(sys.argv[2])
    except ValueError:
        print("control_fd must be an integer", file=sys.stderr)
        return 64

    Collector(version, control_fd).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
