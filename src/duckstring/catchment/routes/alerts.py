"""Alert channels — failure & freshness notifications. Catchment-wide, all ``full``-gated.

A channel destination is an outbound egress surface (it can carry a token / e-mail an error), so managing
channels needs full access, like Spouts and secrets. See plans/alerts.md.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from .. import auth

router = APIRouter()


def _driver(request: Request):
    return request.app.state.driver


class _ChannelBody(BaseModel):
    name: str
    destination: str
    scope: Optional[str] = None          # "name@major" (one line), "name" (any major), or None (catchment-wide)
    events: str = "all"                  # CSV of kinds, or "all"
    stale_ms: Optional[int] = None       # freshness-SLA bound; None = no freshness monitoring


@router.get("/alerts", dependencies=[auth.full])
def list_alerts(request: Request):
    return {"channels": _driver(request).list_channels()}


@router.post("/alerts", dependencies=[auth.full])
def add_alert(request: Request, body: _ChannelBody):
    try:
        _driver(request).add_channel(body.name, body.destination, body.scope, body.events, body.stale_ms)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"ok": True, "name": body.name}


@router.delete("/alerts/{name}", dependencies=[auth.full])
def remove_alert(name: str, request: Request):
    if not _driver(request).remove_channel(name):
        raise HTTPException(status_code=404, detail=f"No alert channel '{name}'")
    return {"ok": True}


@router.post("/alerts/{name}/test", dependencies=[auth.full])
async def test_alert(name: str, request: Request):
    """Send a test notification through the channel — validates connectivity + credentials. A connection
    problem is a 200 ``{ok: false, error}`` (a sanitised message), not a 5xx."""
    destination = _driver(request).channel_destination(name)
    if destination is None:
        raise HTTPException(status_code=404, detail=f"No alert channel '{name}'")

    def _test() -> None:
        from ...alerts import get_notifier

        get_notifier(destination).test()

    try:
        await run_in_threadpool(_test)
    except Exception as exc:  # noqa: BLE001 — a connection/credential problem is a result, not a server error
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": True}


@router.get("/alerts/deliveries", dependencies=[auth.full])
def list_deliveries(request: Request, limit: int = 100):
    return {"deliveries": _driver(request).deliveries(limit)}
