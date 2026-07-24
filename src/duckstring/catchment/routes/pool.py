"""Pool-agent protocol endpoints (plans/pool-agent.md).

``GET  /api/pool/{name}/jobs``   — the Pool agent polls for supervision commands (``ensure`` /
``terminate`` / ``shutdown``).
``POST /api/pool/{name}/events`` — the agent reports child-process events (``duck_started`` /
``duck_exit`` / ``agent_up`` / ``agent_down``).

Agent-initiated (dial-back), symmetric with the Duck channel — the same shape works for a local
subprocess agent and a remote machine, and needs no inbound networking to a Fargate task. Authenticated
with the same internal Duck token (the agent is infrastructure, not a user surface)."""

from __future__ import annotations

from fastapi import APIRouter, Request

from .. import auth

router = APIRouter()


def _pool_launcher(request: Request, name: str):
    launcher = request.app.state.driver.launcher
    getter = getattr(launcher, "pool_launcher", None)
    return getter(name) if getter is not None else None


@router.get("/pool/{name}/jobs", dependencies=[auth.duck])
def pool_jobs(name: str, request: Request):
    pl = _pool_launcher(request, name)
    return {"jobs": pl.take_jobs() if pl is not None else []}


@router.post("/pool/{name}/events", dependencies=[auth.duck])
async def pool_events(name: str, request: Request):
    pl = _pool_launcher(request, name)
    if pl is not None:
        pl.on_event(await request.json())
    return {"ok": True}
