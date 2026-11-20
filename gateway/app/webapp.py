"""FastAPI operation page for the cold-storage gateway."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .supervisor import RUN_DIR, Supervisor

WEB_DIR = Path(__file__).parent / "web"

app = FastAPI(title="冷库网关 · 采集进程换版操作台", version="1.0.0")
SUP: Supervisor | None = None  # injected by app.main


def sup() -> Supervisor:
    if SUP is None:  # pragma: no cover - always injected at startup
        raise HTTPException(503, "supervisor not initialized")
    return SUP


class UpgradeReq(BaseModel):
    new_version: str = Field(..., examples=["v2"])
    simulate_fail: bool = False


class RecorderReq(BaseModel):
    count: int = Field(60, ge=1, le=10000)
    interval: float = Field(0.2, ge=0.01, le=10)
    gap_at: int = Field(20, ge=0)
    gap_seconds: float = Field(4.0, ge=0, le=120)


class FailMarkerReq(BaseModel):
    present: bool


@app.get("/")
def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/api/status")
def get_status() -> dict:
    return sup().status()


@app.get("/api/events")
def get_events(cursor: int = 0) -> dict:
    return sup().events_since(cursor)


@app.get("/api/verify")
def get_verify() -> dict:
    return sup().verify()


@app.post("/api/upgrade")
def post_upgrade(req: UpgradeReq) -> dict:
    if req.new_version == sup().status().get("current_version"):
        raise HTTPException(400, "new_version equals the active version")
    result = sup().upgrade(req.new_version, simulate_fail=req.simulate_fail)
    if not result.get("ok"):
        return JSONResponse(result, status_code=409)
    return result


@app.post("/api/recorder/start")
def post_recorder_start(req: RecorderReq) -> dict:
    result = sup().start_recorder(req.count, req.interval,
                                  req.gap_at, req.gap_seconds)
    if not result.get("ok"):
        return JSONResponse(result, status_code=409)
    return result


@app.post("/api/recorder/stop")
def post_recorder_stop() -> dict:
    result = sup().stop_recorder()
    if not result.get("ok"):
        return JSONResponse(result, status_code=409)
    return result


@app.post("/api/fail-marker")
def post_fail_marker(req: FailMarkerReq) -> dict:
    os.makedirs(RUN_DIR, exist_ok=True)
    path = sup().fail_path
    if req.present:
        with open(path, "w") as fh:
            fh.write("manual marker\n")
    else:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
    return {"ok": True, "present": os.path.exists(path)}


@app.post("/api/events/reset")
def post_reset() -> dict:
    sup().reset_events()
    return {"ok": True}


app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")
