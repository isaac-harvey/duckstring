"""The duct poller — the consuming Catchment's bridge to its upstreams.

One async task. Each cycle, per duct:

1. GET the upstream's ``/api/status`` and mirror each drawn Pond's freshness + reachability into its
   local Pond Draw (``Driver.observe_remote``). That cascade may start a transfer if there's
   downstream demand and the upstream is fresher.
2. Perform any pending transfers (``Driver.take_transfers``): fetch the upstream's exported Parquet
   over ``/api/draw`` and land it in the local landing zone, then report completion. Data lands
   **before** the Draw's freshness advances, so a Sink never reads stale/absent Parquet.
3. Solicit the upstream (forward a Tap) for any Draw with unmet downstream demand.

Demand flows up; data flows down. The poller is the Draw's "Duck" — Draws are never run by a
subprocess. See plans/cross-catchment-ducts.md.
"""

from __future__ import annotations

import asyncio
import io
import zipfile
from datetime import datetime

import httpx

from .registry import pond_data_dir

_MAX_WAIT = 25.0  # ceiling on a held wait (refresh baselines / survive a silently-dropped connection)
_ERR_BACKOFF = 2.0  # after a failed wait, pause before re-issuing so an unreachable upstream isn't hammered
_DOWN_STATES = {"failed", "killed", "blocked"}


def _parse_f(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


async def _fetch_status(client: httpx.AsyncClient, url: str, auth: dict) -> dict | None:
    try:
        resp = await client.get(f"{url}/api/status", headers=auth)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError:
        return None


async def _land_transfer(client: httpx.AsyncClient, url: str, auth: dict, root, name: str, major: int,
                         data_root: str | None = None) -> None:
    """Fetch an upstream Pond line's exported Parquet (+ the Trickle sidecar) and land it. **Incremental
    for Trickle sources**: the consumer sends the freshness it has already landed (``after``); the producer
    ships only the append-only parts newer than that (append history / ``__changelog`` / ``__droplog``),
    which the consumer drops into its own parts directory. A merge main / plain Ripple output is a single
    file, landed wholesale (replace). ``after = None`` (bootstrap / no Trickle source) → whole set."""
    from ..objects import OBJECTS_DIR
    from ..trickle_io import (
        BASE_SUFFIX,
        SIDECAR,
        changelog_name,
        landed_after,
        load_sidecar,
        part_f,
        warm_name,
    )

    data_dir = pond_data_dir(root, name, major, data_root)
    after = landed_after(data_dir)  # what we already hold; None → wholesale (bootstrap)
    # The cold base is wholesale but rewritten rarely — tell the producer the oldest cold-base freshness we
    # already hold so it re-ships a base only when its fold watermark has advanced past it.
    held = load_sidecar(data_dir)
    f_bases = [m.get("f_base") for m in held.values() if m.get("mode") == "merge" and isinstance(m, dict)]
    base_after = min(f_bases) if f_bases and all(f_bases) else None
    # Objects are overwrite-only; tell the producer the oldest Object freshness we hold so it re-ships an
    # Object only when its freshness advanced past that (see draw._ship_objects).
    obj_fs = [e.get("f") for e in (held.get("objects") or {}).values() if e.get("f")]
    objects_after = min(obj_fs) if obj_fs else None
    params = {k: v for k, v in (("after", after), ("base_after", base_after), ("objects_after", objects_after)) if v}
    resp = await client.get(f"{url}/api/draw/{name}/{major}", params=params, headers=auth)
    resp.raise_for_status()
    data_dir.mkdir()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        # Each entry is written to its path atomically: a top-level "{table}.parquet" replaces a wholesale
        # table; a nested "{table}/{f}.parquet" adds an append-only part (incremental — no merge needed,
        # parts are immutable and idempotent by name). A "{table}__base/{chunk}" is a chunk of a merge
        # main's wholesale cold base — shipped in full only when it changed, so after landing we drop any
        # stale chunk the producer no longer has (its names change every compaction; a leftover resurrects).
        # A re-shipped Object arrives as its whole subtree; clear the old copy first (its file set may have
        # changed — a dir-Object's members, or a single-file Object's extension) so no stale file survives.
        for reshipped in {p.split("/")[1] for p in zf.namelist()
                          if p.split("/")[0] == OBJECTS_DIR and len(p.split("/")) > 2}:
            data_dir.rmtree(OBJECTS_DIR, reshipped)
        landed_base: dict[str, set[str]] = {}
        for info in zf.infolist():
            parts = info.filename.split("/")
            is_object = parts[0] == OBJECTS_DIR and len(parts) > 2
            if not (info.filename.endswith(".parquet") or info.filename == SIDECAR or is_object):
                continue
            data_dir.write_bytes(zf.read(info), *parts)  # atomic per-object write (rename local / PUT object)
            if len(parts) > 1 and parts[0].endswith(BASE_SUFFIX):
                landed_base.setdefault(parts[0], set()).add(parts[-1])
        for base_name, kept in landed_base.items():  # prune the previous compaction's chunks (overlap-safe)
            base = data_dir.child(base_name)
            for old in base.parquet_names():
                if old not in kept:
                    base.remove(old)
    # Reclaim warm bands / hot changelog parts now subsumed by the (possibly advanced) cold base: anything
    # at-or-below a merge main's published f_base is folded into the base, so reconstruct ignores it
    # (it filters `> f_base`). Storage-only — correctness already holds via that filter.
    for main, m in load_sidecar(data_dir).items():
        if m.get("mode") != "merge" or not m.get("f_base"):
            continue
        f_base = datetime.fromisoformat(m["f_base"])
        for companion in (warm_name(main), changelog_name(main)):
            comp = data_dir.child(companion)
            for part in comp.parquet_names():
                if part_f(part) <= f_base:
                    comp.remove(part)


async def poll_once(driver, root, client: httpx.AsyncClient, solicited: dict | None = None) -> None:
    # `solicited` (persisted across cycles by run_poller) is the last demand forwarded per Draw, so we
    # never re-send the same push/pull. Re-sending the SAME target would re-add it on the upstream
    # mid-run and trigger a spurious second run at the same freshness.
    if solicited is None:
        solicited = {}
    targets = driver.duct_targets()
    if not targets:
        return

    # 1. Mirror upstream freshness + reachability into each Draw.
    statuses: dict[str, dict | None] = {}
    for duct in targets:
        statuses[duct["origin"]] = await _fetch_status(client, duct["remote_url"], duct["auth"])
    for duct in targets:
        status = statuses[duct["origin"]]
        by_key = {}
        if status is not None:
            for p in status.get("ponds", []):
                by_key[(p["name"], p["major"])] = p
        for m in duct["members"]:
            key = f"{m['name']}@{m['major']}"
            if status is None or by_key.get((m["name"], m["major"])) is None:  # unreachable / not deployed
                driver.observe_remote(key, None, down=True)
                solicited.pop(key, None)  # forget what we sent so it re-forwards on recovery
                continue
            up = by_key[(m["name"], m["major"])]
            driver.observe_remote(key, _parse_f(up.get("end_f")), down=up.get("status") in _DOWN_STATES)

    # 2. Perform pending transfers (fetch + land), then report completion.
    url_by_origin = {d["origin"]: d for d in targets}
    for t in driver.take_transfers():
        origin = _origin_for(targets, t["name"], t["major"])
        duct = url_by_origin.get(origin) if origin else None
        if duct is None:
            driver.fail_draw_transfer(t["key"], t["f"], "no duct for this Draw")
            continue
        try:
            await _land_transfer(client, duct["remote_url"], duct["auth"], root, t["name"], t["major"],
                                 getattr(driver, "data_root", None))
            driver.complete_draw_transfer(t["key"], t["f"])
        except Exception as exc:  # noqa: BLE001 — any failure fails the transfer (blocks downstream)
            driver.fail_draw_transfer(t["key"], t["f"], f"transfer failed: {exc}")

    # 3. Solicit upstreams for Draws with unmet downstream demand — forwarding the demand's epoch so
    #    the upstream Inlet mints the SAME freshness (push target → pulse-at-T; pull → tap-with-m).
    #    Each distinct demand is forwarded exactly once (see `solicited`): re-sending the same epoch
    #    would re-add the target on a mid-run upstream and cause a duplicate run.
    for d in driver.draws():
        key = d["key"]
        origin = _origin_for(targets, d["name"], d["major"])
        duct = url_by_origin.get(origin) if origin else None
        if duct is None:
            continue
        demand = (d["target"], d["pull_m"])
        if demand == (None, None) or solicited.get(key) == demand:
            solicited[key] = demand  # nothing to send, or already sent this exact demand
            continue
        try:
            if d["target"] is not None:
                await client.post(
                    f"{duct['remote_url']}/api/ponds/{d['name']}/pulse",
                    params={"major": d["major"], "at": d["target"]}, headers=duct["auth"],
                )
            if d["pull_m"] is not None:
                await client.post(
                    f"{duct['remote_url']}/api/ponds/{d['name']}/tap",
                    params={"major": d["major"], "m": d["pull_m"]}, headers=duct["auth"],
                )
            solicited[key] = demand
        except httpx.HTTPError:
            pass  # don't record → retry next cycle


def _origin_for(targets: list[dict], name: str, major: int) -> str | None:
    for duct in targets:
        if any(m["name"] == name and m["major"] == major for m in duct["members"]):
            return duct["origin"]
    return None


async def _wait_member(client: httpx.AsyncClient, url: str, params: dict, auth: dict) -> None:
    """Hold one freshness long-poll against an upstream Pond; back off on error so an unreachable
    upstream isn't hammered (cancellation propagates — it must not be swallowed into the backoff)."""
    try:
        await client.get(url, params=params, headers=auth)
    except httpx.HTTPError:
        await asyncio.sleep(_ERR_BACKOFF)


async def _wait_for_change(driver, client: httpx.AsyncClient, wake: asyncio.Event) -> None:
    """Block until something a Draw cares about may have changed: an upstream Pond's freshness
    advances (a held ``…/wait`` returns), local demand arrives (``wake`` is set, so a Draw solicits at
    once), or a ceiling elapses. Then return — ``poll_once`` does the actual observe/transfer/solicit."""
    targets = driver.duct_targets()
    tasks: list[asyncio.Task] = []
    for duct in targets:
        for m in duct["members"]:
            # Pass both the freshness baseline and the last-known down-state: the wait returns only on
            # a CHANGE (freshness advance or a down transition), so a durably-blocked upstream holds the
            # connection instead of returning instantly and spinning the poller.
            params: dict = {"down": m["remote_down"]}
            if m["remote_f"]:
                params["after"] = m["remote_f"]
            url = f"{duct['remote_url']}/api/draw/{m['name']}/{m['major']}/wait"
            tasks.append(asyncio.ensure_future(_wait_member(client, url, params, duct["auth"])))
    wake_task = asyncio.ensure_future(wake.wait())
    tasks.append(wake_task)
    try:
        await asyncio.wait(tasks, timeout=_MAX_WAIT, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        wake.clear()


async def run_poller(driver, root, wake: asyncio.Event) -> None:
    """The poller loop. Each cycle observes/transfers/solicits, then waits — on an upstream freshness
    long-poll or a local-demand wake — rather than sleeping a fixed interval. Cancelled on shutdown."""
    solicited: dict[str, tuple] = {}  # last demand forwarded per Draw — persisted so we don't re-send
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0)) as client:
        while True:
            try:
                await poll_once(driver, root, client, solicited)
            except Exception as exc:  # keep the loop alive
                print(f"[catchment] poller error: {exc}", flush=True)
            try:
                await _wait_for_change(driver, client, wake)
            except Exception as exc:
                print(f"[catchment] poller wait error: {exc}", flush=True)
                await asyncio.sleep(_ERR_BACKOFF)
