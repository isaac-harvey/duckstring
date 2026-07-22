"""The write-only Catchment secret store (see catchment/secrets.py). Full-gated, catchment-wide.

``GET`` lists **names only** — there is no endpoint that returns a value (write-only by design, so even a
full key can't exfiltrate a stored secret). ``POST`` sets/overwrites; the value travels in the request, so
use HTTPS. Referenced from a Spout destination as ``${secret:NAME}``, resolved only at egress time.
"""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from .. import auth, cloud

router = APIRouter()


class _SecretBody(BaseModel):
    name: str
    value: str


@router.get("/secrets", dependencies=[auth.full])
def list_secrets(request: Request):
    """``{"secrets": [{"name", "set_at"}]}`` — names only, never values."""
    return {"secrets": request.app.state.secret_store.names()}


@router.post("/secrets", dependencies=[auth.full])
def set_secret(request: Request, body: _SecretBody):
    """Set/overwrite a secret. The value is stored write-only — never returned."""
    try:
        request.app.state.secret_store.set(body.name, body.value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    # An AWS_* secret is a control-plane credential — apply it to the live environment immediately (an
    # explicit set wins over any stale ambient value) so boto3 uses it without a restart. This is what
    # makes the UI "enable cloud" flow actually work.
    if body.name.startswith(cloud.AWS_SECRET_PREFIX):
        os.environ[body.name] = body.value
        cloud._chain_has_credentials.cache_clear()
    return {"ok": True, "name": body.name}


@router.delete("/secrets/{name}", dependencies=[auth.full])
def remove_secret(name: str, request: Request):
    if not request.app.state.secret_store.remove(name):
        raise HTTPException(status_code=404, detail=f"No secret '{name}'")
    return {"ok": True}
