"""Access levels & API-key auth for the Catchment.

The single-key model is split into a **total-ordered** ladder — ``read ⊂ demand ⊂ full`` — so the
authorization check stays one integer comparison:

- **read** — read & query data only (no ducts, no demand).
- **demand** — read + create demand (tap/wave/pulse/tide) + connect a downstream duct.
- **full** — everything: deploy, the control verbs, windows, ducts, key rotation.

Each route declares a *minimum* level via the ready-made dependencies ``read`` / ``demand`` / ``full``
(or ``duck`` for the internal worker channel). A request's level is the matched key's level. Routes are
**fail-closed**: :func:`audit_routes` runs at app construction and raises if any ``/api`` route (bar the
public allowlist and the duck channel) carries no level guard — a new route added without classification
fails to boot rather than leaking.

The three user keys are stored as **hashes** in ``catchment_key`` (the plaintext is printed once at
generation). The Duck's internal dial-back channel uses a **separate** token (:func:`ensure_duck_token`,
persisted in ``catchment_meta`` so a Duck surviving a Catchment restart keeps authenticating) — decoupled
from the user keys so rerolling a user key never disrupts running Ducks and no subprocess carries a
full-access user key. Backward compatibility: a single ``api_key`` (``--key`` / ``DUCKSTRING_API_KEY``)
still works and means **full**.
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
from dataclasses import dataclass
from enum import IntEnum

from fastapi import Depends, HTTPException, Request

# Routes that need no credential at all (parity with the old open `/api/health`).
PUBLIC_PATHS = {"/api/health"}


class Level(IntEnum):
    READ = 1
    DEMAND = 2
    FULL = 3


NAME_TO_LEVEL = {"read": Level.READ, "demand": Level.DEMAND, "full": Level.FULL}
LEVEL_TO_NAME = {v: k for k, v in NAME_TO_LEVEL.items()}


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


# ─── Generation / persistence ────────────────────────────────────────────────


def generate(con: sqlite3.Connection, levels: list[str] | None = None) -> dict[str, str]:
    """Mint fresh keys for ``levels`` (default all three), store their hashes, return the plaintext
    once. Used both for initial generation and for rerolling a subset (``rotate-keys``)."""
    chosen = levels if levels is not None else list(NAME_TO_LEVEL)
    bad = [lvl for lvl in chosen if lvl not in NAME_TO_LEVEL]
    if bad:
        raise ValueError(f"unknown level(s): {', '.join(bad)} — use read/demand/full")
    out: dict[str, str] = {}
    for lvl in chosen:
        key = secrets.token_urlsafe(24)
        con.execute(
            "INSERT INTO catchment_key (level, hash) VALUES (?, ?) "
            "ON CONFLICT(level) DO UPDATE SET hash = excluded.hash",
            (lvl, hash_key(key)),
        )
        out[lvl] = key
    con.commit()
    return out


def ensure_duck_token(con: sqlite3.Connection) -> str:
    """Mint the internal Duck dial-back token once (persisted in ``catchment_meta``), then return it.
    Stable across restarts so a Duck that outlived the Catchment keeps authenticating."""
    row = con.execute("SELECT value FROM catchment_meta WHERE key = 'duck_token'").fetchone()
    if row is not None:
        return row[0]
    token = secrets.token_urlsafe(32)
    con.execute("INSERT INTO catchment_meta (key, value) VALUES ('duck_token', ?)", (token,))
    con.commit()
    return token


def _key_hashes(con: sqlite3.Connection) -> dict[str, str]:
    return {level: khash for level, khash in con.execute("SELECT level, hash FROM catchment_key")}


def auth_configured(con: sqlite3.Connection | None, api_key: str | None) -> bool:
    """Whether any user credential gates this Catchment. When False the Catchment is fully open (the
    bare, no-auth mode) — get_principal grants full and the duck channel is ungated. A missing db
    (a minimally-constructed app) is treated as unconfigured/open."""
    if api_key:
        return True
    if con is None:
        return False
    return con.execute("SELECT 1 FROM catchment_key LIMIT 1").fetchone() is not None


# ─── Request-time resolution ─────────────────────────────────────────────────


@dataclass
class Principal:
    level: Level | None  # the access level of a user credential (None = unauthenticated)
    is_duck: bool        # the internal worker token (only valid on the duck channel)


def _extract_token(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    supplied = auth[7:] if auth.lower().startswith("bearer ") else ""
    return supplied or request.headers.get("x-duck-token", "")


def get_principal(request: Request) -> Principal:
    """Resolve the caller from the request headers + the Catchment's keys. Cached per request by
    FastAPI (the level guards depend on it), so the key lookup runs once."""
    state = request.app.state
    con: sqlite3.Connection | None = getattr(state, "db", None)
    api_key: str | None = getattr(state, "api_key", None)
    duck_token: str | None = getattr(state, "duck_token", None)
    supplied = _extract_token(request)

    if duck_token and supplied and secrets.compare_digest(supplied, duck_token):
        return Principal(level=None, is_duck=True)

    if not auth_configured(con, api_key):
        return Principal(level=Level.FULL, is_duck=False)  # open mode — no credential required

    if supplied:
        if con is not None:
            supplied_hash = hash_key(supplied)
            for level, khash in _key_hashes(con).items():
                if secrets.compare_digest(khash, supplied_hash):
                    return Principal(level=NAME_TO_LEVEL[level], is_duck=False)
        if api_key and secrets.compare_digest(supplied, api_key):
            return Principal(level=Level.FULL, is_duck=False)

    return Principal(level=None, is_duck=False)


def level_for_key(con: sqlite3.Connection | None, api_key: str | None, supplied: str) -> Level | None:
    """The access :class:`Level` for a raw key — the reusable core of :func:`get_principal`, for
    non-HTTP front doors (the pg / Flight wire servers, where the key arrives as a password). Open mode
    (no keys configured) → FULL; a bad/absent key → None."""
    if not auth_configured(con, api_key):
        return Level.FULL
    if supplied:
        if con is not None:
            h = hash_key(supplied)
            for level, khash in _key_hashes(con).items():
                if secrets.compare_digest(khash, h):
                    return NAME_TO_LEVEL[level]
        if api_key and secrets.compare_digest(supplied, api_key):
            return Level.FULL
    return None


# ─── Route guards (declared on each route as `dependencies=[...]`) ────────────


def _level_guard(required: Level):
    def dep(principal: Principal = Depends(get_principal)) -> Principal:
        if principal.is_duck:  # the worker token is for the duck channel only
            raise HTTPException(status_code=403, detail="endpoint not available to the worker token")
        if principal.level is None:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")
        if principal.level < required:
            raise HTTPException(status_code=403, detail=f"requires {LEVEL_TO_NAME[required]} access")
        return principal

    dep._duckstring_level = required  # type: ignore[attr-defined]  # marker for audit_routes
    return Depends(dep)


def _duck_guard():
    def dep(request: Request) -> None:
        state = request.app.state
        if not auth_configured(getattr(state, "db", None), getattr(state, "api_key", None)):
            return  # open mode — the duck channel is ungated, like the rest of the API
        token = getattr(state, "duck_token", None)
        supplied = _extract_token(request)
        if not (token and supplied and secrets.compare_digest(supplied, token)):
            raise HTTPException(status_code=401, detail="Invalid or missing worker token")

    dep._duckstring_level = "duck"  # type: ignore[attr-defined]
    return Depends(dep)


# Ready-made guards: `dependencies=[auth.read]` etc.
read = _level_guard(Level.READ)
demand = _level_guard(Level.DEMAND)
full = _level_guard(Level.FULL)
duck = _duck_guard()


def _has_level_guard(dependant) -> bool:
    if getattr(dependant.call, "_duckstring_level", None) is not None:
        return True
    return any(_has_level_guard(sub) for sub in dependant.dependencies)


def audit_routes(app) -> None:
    """Fail-closed: every ``/api`` route (bar the public allowlist and the duck channel) must carry a
    level guard. Raise at construction if one doesn't — so a route added without classification can't
    silently ship open."""
    from fastapi.routing import APIRoute

    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.path.startswith("/api"):
            continue
        if route.path in PUBLIC_PATHS:
            continue
        if not _has_level_guard(route.dependant):
            raise RuntimeError(
                f"route {sorted(route.methods)} {route.path} has no access-level guard — "
                "add `dependencies=[auth.read|demand|full|duck]` (see catchment/auth.py)"
            )
