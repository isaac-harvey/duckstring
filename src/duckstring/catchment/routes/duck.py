"""Duck protocol endpoints.

``GET  /api/duck/{name}/{major}/jobs``     — the Duck polls for queued commands (``begin_run`` / ``shutdown``).
``POST /api/duck/{name}/{major}/events``   — the Duck reports ``ripple`` / ``run_completed`` events.
``GET  /api/duck/{name}/{major}/artifact`` — the deployed version's source bundle (a tar), for a
remote Duck that can't read the Catchment's disk (prereqs D6). The Catchment stays the artifact
authority — no object-store coupling here.

A Duck serves one major line of a Pond, so all routes address the pond key ``name@major``. All are
Duck-initiated, so the same shape works for a local subprocess or a remote Duck. Job delivery is a
short poll (the Duck re-polls on a backing-off interval); events are idempotent on freshness.

**Why this is not a long poll.** ``take_jobs`` DRAINS the queue, so holding the connection opens a window
where a Duck that was killed mid-poll wakes on the next job and swallows work meant for its replacement,
which then waits forever for a job that no longer exists. Verified: holding the connection broke reset
and missing-source recovery, and ASGI gives no reliable disconnect signal for an idle GET to close the
window with. Doing this properly needs a spawn token so the server can tell WHICH Duck owns the queue —
worth doing, but it is a protocol change, not a tweak. Until then the Duck backs off when idle, which
removes most of the traffic with none of the delivery risk.
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request, Response

from ...keys import pond_key
from .. import auth

router = APIRouter()


@router.get("/duck/{name}/{major}/jobs", dependencies=[auth.duck])
def jobs(name: str, major: int, request: Request):
    driver = request.app.state.driver
    return {"jobs": driver.take_jobs(pond_key(name, major))}


@router.post("/duck/{name}/{major}/events", dependencies=[auth.duck])
async def events(name: str, major: int, request: Request):
    driver = request.app.state.driver
    payload = await request.json()
    driver.on_event(pond_key(name, major), payload)
    return {"ok": True}


@router.get("/duck/{name}/{major}/artifact", dependencies=[auth.duck])
def artifact(name: str, major: int, request: Request):
    """The selected version's source bundle for this major line, as an uncompressed tar (code is
    small; the fetch is boot-time-critical). The Duck unpacks it at its own root and proceeds —
    the registry and ledger it then builds are ephemeral caches over the published plane."""
    driver = request.app.state.driver
    meta = driver.meta.get(pond_key(name, major))
    if meta is None:
        raise HTTPException(status_code=404, detail=f"unknown pond {name}@{major}")
    source_dir = Path(driver.root) / meta["source_path"]
    if not source_dir.is_dir():
        raise HTTPException(status_code=404, detail="artifact not on disk")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        tar.add(source_dir, arcname=".")
    return Response(
        content=buf.getvalue(), media_type="application/x-tar",
        headers={"X-Duckstring-Version": meta["version"], "X-Duckstring-Source-Path": meta["source_path"]},
    )
