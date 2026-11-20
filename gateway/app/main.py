"""Entry point: start the supervisor and the FastAPI/uvicorn web server."""
from __future__ import annotations

import uvicorn

from . import webapp
from .supervisor import WEB_PORT, create_supervisor


def main() -> None:
    supervisor = create_supervisor()
    webapp.SUP = supervisor
    config = uvicorn.Config(
        webapp.app, host="0.0.0.0", port=WEB_PORT,
        log_level="info", access_log=False,
    )
    server = uvicorn.Server(config)
    try:
        server.run()
    finally:
        supervisor.shutdown()


if __name__ == "__main__":
    main()
