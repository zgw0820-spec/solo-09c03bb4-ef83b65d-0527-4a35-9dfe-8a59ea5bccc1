"""Simulated cold-storage temperature recorder.

Opens exactly one TCP connection to the gateway and streams numbered
newline-delimited JSON readings over it. It never reconnects: the whole
point of the demo is that the gateway can swap its collector process
while this single long-lived connection stays open.

Line format (one JSON object per line, ``\\n`` terminated)::

    {"id": 7, "sensor": "freezer-1", "temp_c": -18.4, "ts": 1731...}
"""
from __future__ import annotations

import argparse
import json
import random
import socket
import sys
import time


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="simulated temperature recorder")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
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


def main() -> int:
    args = parse_args()
    # One connection, for the whole run, no retries.
    sock = socket.create_connection((args.host, args.port), timeout=10)
    peer = sock.getpeername()
    print(f"recorder: connected once to {peer[0]}:{peer[1]} "
          f"(local fd stays open for the entire run)", flush=True)

    base_temp = -18.0
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
            sock.sendall(line.encode("utf-8"))
            print(f"recorder: sent id={rid} temp={reading['temp_c']}C",
                  flush=True)
            time.sleep(args.interval)
        print(f"recorder: all {args.count} readings sent; holding connection "
              f"open for 2s then closing", flush=True)
        time.sleep(2)
    finally:
        sock.close()
        print("recorder: connection closed, exiting (no reconnect)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
