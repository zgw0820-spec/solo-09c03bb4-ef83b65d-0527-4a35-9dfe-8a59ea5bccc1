"""Length-prefixed JSON frames over Unix SOCK_SEQPACKET sockets.

SOCK_SEQPACKET is used for every control/handover channel because it gives
us, simultaneously:

* connection-oriented, reliable delivery (like SOCK_STREAM);
* preserved message boundaries (like SOCK_DGRAM), so one ``recvmsg`` call
  returns exactly one frame together with its SCM_RIGHTS ancillary data;
* SCM_RIGHTS support for passing the recorder TCP connection fds.

On stream sockets the separate 4-byte length read consumes the kernel
message and discards any attached ancillary data, so a length-prefixed
protocol must run over a message-boundary socket to carry fds reliably.

Wire layout of one frame (a single sendmsg message)::

    [4-byte big-endian payload length][JSON payload] (+ optional SCM_RIGHTS fds)
"""
from __future__ import annotations

import array
import json
import socket
import struct
from typing import Any, Iterable

MAX_FRAME = 1 * 1024 * 1024  # control frames are tiny JSON; 1 MiB is ample
# Ancillary buffer: room for a handful of int fds plus cmsg headers.
CMSG_BUF = 65536


def send_frame(sock: socket.socket, obj: Any, fds: Iterable[int] = ()) -> None:
    payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    if len(payload) > MAX_FRAME:
        raise ValueError("frame too large")
    message = struct.pack(">I", len(payload)) + payload
    fd_list = list(fds)
    anc = (
        [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", fd_list))]
        if fd_list
        else []
    )
    # One sendmsg message so ancillary data stays attached to this frame.
    sock.sendmsg([message], anc)


def recv_frame(sock: socket.socket) -> tuple[dict | None, list[int]]:
    """Return (json_object, fds). (None, []) means the peer closed the channel."""
    try:
        data, ancdata, flags, _addr = sock.recvmsg(MAX_FRAME + 4, CMSG_BUF)
    except BlockingIOError:
        return None, []
    except OSError:
        return None, []
    if not data:  # clean peer shutdown / EOF
        return None, []
    if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
        raise ValueError(
            f"frame or ancillary data truncated (flags={flags}); "
            f"raise MAX_FRAME/CMSG_BUF")
    if len(data) < 4:
        raise ValueError(f"truncated frame header: {len(data)} bytes")
    (length,) = struct.unpack(">I", data[:4])
    if length > MAX_FRAME or len(data) - 4 < length:
        raise ValueError(
            f"truncated frame: header says {length} bytes, "
            f"got {len(data) - 4}")

    fds: list[int] = []
    # CPython recvmsg ancillary entries are (level, type, packed_bytes).
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
            fds.extend(array.array("i", cmsg_data))
    return json.loads(data[4:4 + length].decode("utf-8")), fds
