"""Persist (plans/persist.md phase 2): the local→durable mirror (`persist_tree`) and the local-first
read resolution (`resolve_data_dir` / `Pond._source_data_dir`). The end-to-end proof (real Ducks,
async persist, persisted_f) is `test_runtime.test_local_first_publish_persists_to_data_root`."""

from __future__ import annotations

import json

from duckstring.dataplane import persist_tree
from duckstring.storage import LocalStorage


def _seed_local(root):
    """A representative local publish: sidecar + wholesale table + a parts dir + state snapshots + the
    (local-only) Iceberg catalog entries."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "_trickle.json").write_text(json.dumps({"t": {"mode": "append"}}))
    (root / "whole.parquet").write_bytes(b"v1")
    (root / "t").mkdir()
    (root / "t" / "2026-01-01T00_00_00+00_00.parquet").write_bytes(b"p1")
    (root / "t" / "2026-01-02T00_00_00+00_00.parquet").write_bytes(b"p2")
    (root / "state" / "agg" / "t").mkdir(parents=True)
    (root / "state" / "agg" / "t" / "2026-01-02T00_00_00+00_00.parquet").write_bytes(b"s1")
    (root / "catalog.json").write_text("{}")  # iceberg pointer — local-only
    (root / "pond").mkdir()  # iceberg warehouse — local-only
    (root / "pond" / "meta.json").write_text("{}")


def test_persist_tree_mirrors_flat_layout_and_skips_iceberg(tmp_path):
    local, dest = tmp_path / "local", LocalStorage(tmp_path / "durable")
    _seed_local(local)
    n = persist_tree(LocalStorage(local), dest)
    assert n > 0
    d = tmp_path / "durable"
    assert (d / "_trickle.json").exists() and (d / "whole.parquet").read_bytes() == b"v1"
    assert sorted(p.name for p in (d / "t").glob("*.parquet")) == [
        "2026-01-01T00_00_00+00_00.parquet", "2026-01-02T00_00_00+00_00.parquet"]
    assert (d / "state" / "agg" / "t" / "2026-01-02T00_00_00+00_00.parquet").exists()
    # The Iceberg catalog layer is local-only (its metadata embeds absolute local paths).
    assert not (d / "catalog.json").exists() and not (d / "pond").exists()


def test_persist_tree_incremental_and_floor_anchored_pruning(tmp_path):
    """The mirror prunes on EXPLICIT signals only (plans/s3-resident-state.md step 1): a destination part
    goes when the local sidecar's floor covers it (retention/fold — inclusive) or, for a whole table
    directory, when the sidecar no longer declares the table. A part merely ABSENT locally is kept —
    absence is what a partial local looks like, and pruning on it would delete plane history. The
    ``state/`` snapshot tree keeps exact keep-latest semantics (rewritten wholesale every run)."""
    import shutil

    local, dest = tmp_path / "local", LocalStorage(tmp_path / "durable")
    _seed_local(local)
    persist_tree(LocalStorage(local), dest)
    d = tmp_path / "durable"

    # Parts are idempotent by name: a re-mirror does not rewrite them (content marker probe).
    (d / "t" / "2026-01-01T00_00_00+00_00.parquet").write_bytes(b"REMOTE-KEPT")
    # Wholesale files are always re-uploaded (rewritten per run).
    (local / "whole.parquet").write_bytes(b"v2")
    # A new part appears; one goes ABSENT locally with NO floor (the partial-local case); the state
    # snapshot rolls forward (keep-latest).
    (local / "t" / "2026-01-03T00_00_00+00_00.parquet").write_bytes(b"p3")
    (local / "t" / "2026-01-02T00_00_00+00_00.parquet").unlink()
    (local / "state" / "agg" / "t" / "2026-01-02T00_00_00+00_00.parquet").unlink()
    (local / "state" / "agg" / "t" / "2026-01-03T00_00_00+00_00.parquet").write_bytes(b"s2")
    persist_tree(LocalStorage(local), dest)

    assert (d / "t" / "2026-01-01T00_00_00+00_00.parquet").read_bytes() == b"REMOTE-KEPT"  # not re-sent
    assert (d / "whole.parquet").read_bytes() == b"v2"  # wholesale re-uploaded
    assert sorted(p.name for p in (d / "t").glob("*.parquet")) == [
        "2026-01-01T00_00_00+00_00.parquet", "2026-01-02T00_00_00+00_00.parquet",
        "2026-01-03T00_00_00+00_00.parquet"], \
        "an absent-with-no-floor part must be KEPT (absence is not a retention signal)"
    assert sorted(p.name for p in (d / "state" / "agg" / "t").glob("*.parquet")) == [
        "2026-01-03T00_00_00+00_00.parquet"]  # snapshots: keep-latest — local is authoritative

    # Retention raises the FLOOR → append parts strictly below it are genuinely dropped everywhere; the
    # AT-floor part survives (it holds real rows a full read needs — strict boundary for append).
    (local / "_trickle.json").write_text(json.dumps(
        {"t": {"mode": "append", "floor": "2026-01-02T00:00:00+00:00"}}))
    (local / "t" / "2026-01-01T00_00_00+00_00.parquet").unlink(missing_ok=True)
    persist_tree(LocalStorage(local), dest)
    assert sorted(p.name for p in (d / "t").glob("*.parquet")) == [
        "2026-01-02T00_00_00+00_00.parquet", "2026-01-03T00_00_00+00_00.parquet"], \
        "below-floor pruned; the at-floor append part survives (strict boundary)"

    # A merge __changelog's floor is INCLUSIVE: a warm fold moved the boundary part's rows into the
    # band, so the at-floor part is pruned (keeping it would double-count on plane reads).
    (local / "_trickle.json").write_text(json.dumps(
        {"t": {"mode": "append", "floor": "2026-01-02T00:00:00+00:00"},
         "m": {"mode": "merge", "pk": ["id"], "floor": "2026-01-02T00:00:00+00:00"}}))
    (local / "m__changelog").mkdir()
    (local / "m__changelog" / "2026-01-03T00_00_00+00_00.parquet").write_bytes(b"hot")
    persist_tree(LocalStorage(local), dest)
    (d / "m__changelog" / "2026-01-02T00_00_00+00_00.parquet").write_bytes(b"folded")  # absent locally
    persist_tree(LocalStorage(local), dest)
    assert sorted(p.name for p in (d / "m__changelog").glob("*.parquet")) == [
        "2026-01-03T00_00_00+00_00.parquet"], "an at-floor changelog part is fold-covered — pruned"

    # A table the sidecar no longer DECLARES (dropped/unpublished) loses its directory; a declared
    # table with a merely-missing local dir keeps it.
    shutil.rmtree(local / "t")
    persist_tree(LocalStorage(local), dest)
    assert (d / "t").exists(), "a declared table's missing local dir must not nuke the plane copy"
    (local / "_trickle.json").write_text(json.dumps({}))
    # (an empty sidecar still exists → the mirror runs; the table is undeclared → its dir goes)
    persist_tree(LocalStorage(local), dest)
    assert not (d / "t").exists()


def test_persist_tree_base_chunks_prune_strictly_older_tokens(tmp_path):
    """Base chunks (``{token}__{i}.parquet``): a checkpoint supersedes STRICTLY older tokens; the current
    token (== f_base) is never pruned even when absent locally — it is the live base."""
    import json as _json

    local, dest = tmp_path / "local", LocalStorage(tmp_path / "durable")
    local.mkdir(parents=True)
    (local / "_trickle.json").write_text(_json.dumps(
        {"m": {"mode": "merge", "pk": ["id"], "f_base": "2026-01-02T00:00:00+00:00"}}))
    (local / "m__base").mkdir()
    (local / "m__base" / "2026-01-02T00_00_00+00_00__0.parquet").write_bytes(b"cur0")
    persist_tree(LocalStorage(local), dest)
    d = tmp_path / "durable"
    # The destination also holds an older token's chunk and a current-token chunk absent locally.
    (d / "m__base" / "2026-01-01T00_00_00+00_00__0.parquet").write_bytes(b"old")
    (d / "m__base" / "2026-01-02T00_00_00+00_00__1.parquet").write_bytes(b"cur1")
    persist_tree(LocalStorage(local), dest)
    names = sorted(p.name for p in (d / "m__base").glob("*.parquet"))
    assert "2026-01-01T00_00_00+00_00__0.parquet" not in names, "older tokens are superseded"
    assert "2026-01-02T00_00_00+00_00__1.parquet" in names, "the current token is never pruned"


def test_persist_tree_refuses_an_empty_local(tmp_path):
    """A fresh/lost box (no local sidecar) must never wipe the durable layer."""
    local, dest = tmp_path / "local", LocalStorage(tmp_path / "durable")
    _seed_local(local)
    persist_tree(LocalStorage(local), dest)
    empty = tmp_path / "empty"
    empty.mkdir()
    assert persist_tree(LocalStorage(empty), dest) == 0
    assert (tmp_path / "durable" / "_trickle.json").exists(), "an empty local wiped the durable layer"


def test_resolve_data_dir_prefers_the_local_publish(tmp_path):
    from duckstring.catchment.registry import resolve_data_dir

    remote = tmp_path / "durable"
    # No local publish → the data root.
    dd = resolve_data_dir(tmp_path, "p", 1, str(remote))
    assert str(remote) in dd.uri()
    # A local publish (sidecar present) → the local layout, data root notwithstanding.
    local = tmp_path / "ponds" / "p" / "m1" / "data"
    local.mkdir(parents=True)
    (local / "_trickle.json").write_text("{}")
    dd = resolve_data_dir(tmp_path, "p", 1, str(remote))
    assert str(local) in dd.uri()
    # No data root at all → always local.
    assert str(local) in resolve_data_dir(tmp_path, "p", 1, None).uri()


def test_pond_source_read_resolves_local_first(tmp_path):
    """`Pond._source_data_dir` (the Duck-side foreign read): a co-located Source published locally is
    read from the local layout even with a data root configured."""
    from duckstring.core import Pond

    local = tmp_path / "ponds" / "src" / "m1" / "data"
    local.mkdir(parents=True)
    (local / "_trickle.json").write_text("{}")
    pond = Pond("me", "1.0.0", con=None, root=str(tmp_path),
                source_majors={"src": 1, "faraway": 2}, data_root=str(tmp_path / "durable"))
    assert str(local) in pond._source_data_dir("src").uri()
    assert str(tmp_path / "durable") in pond._source_data_dir("faraway").uri()


def test_driver_persist_event_releases_cross_pool_sink(tmp_path, monkeypatch):
    """The wiring behind the gating rule (plans/persist.md phase 3): a cross-Pool Sink's demand is NOT
    satisfied by its Source's run completion alone (end_f) — only the Duck's `persist` event (advancing
    persisted_f, via Driver._record_persist → engine record_persist → _process) releases its BeginRun.
    A same-Pool Sink under identical demand runs immediately."""
    from datetime import datetime, timezone

    from duckstring.catchment.db import connect, migrate
    from duckstring.catchment.driver import Driver
    from duckstring.catchment.launcher import NoopLauncher
    from duckstring.catchment.routes.deploy import _register

    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = connect(tmp_path / "duck.db")
    migrate(db)
    cfg = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "pond"}
    _register(db, "sales", "1.0.0", "pond", "ponds/sales/1.0.0", cfg,
              [{"func": "f", "name": "agg", "parents": []}])
    snk = {**cfg, "kind": "outlet", "sources": {"rpt_src": "1.0.0"}, "sources_map": {"sales": "1.0.0"}}
    snk["sources"] = {"sales": "1.0.0"}
    _register(db, "rpt", "1.0.0", "outlet", "ponds/rpt/1.0.0", snk,
              [{"func": "f", "name": "out", "parents": []}])
    # The sink resolves to its own Pool (as a remote spawn would); the source stays on the Catchment Pool.
    monkeypatch.setattr(Driver, "_pool_of",
                        lambda self, key: "duck:rpt@1" if key.startswith("rpt") else "catchment")
    driver = Driver(db, tmp_path, "http://x", NoopLauncher(), data_root=str(tmp_path / "durable"))

    assert driver.state.ponds["sales@1"].async_persist is True   # catchment Pool + data root
    assert driver.state.ponds["rpt@1"].pool == "duck:rpt@1"

    # The source has completed a run locally (end_f advanced; mirror not yet landed).
    with driver.lock:
        ss = driver.state.pond_states["sales@1"]
        ss.start_f = ss.end_f = t0
    driver.tap("rpt@1")
    begin_runs = [j for j in driver.jobs.get("rpt@1", []) if j.get("kind") == "begin_run"]
    assert not begin_runs, "the cross-Pool sink must hold until the source's mirror lands"

    # The Duck reports the persist → the watermark advances → the sink's BeginRun releases.
    driver.on_event("sales@1", {"kind": "persist", "f": t0.isoformat(), "status": "success"})
    begin_runs = [j for j in driver.jobs.get("rpt@1", []) if j.get("kind") == "begin_run"]
    assert begin_runs, "the persist event must release the waiting cross-Pool sink"
    assert begin_runs[0]["f"] == t0.isoformat()

    # And the watermark is persisted + surfaced.
    assert driver.persisted["sales@1"] == t0.isoformat()
    st = next(p for p in driver.status()["ponds"] if p["id"] == "sales@1")
    assert st["persisted_f"] == t0.isoformat()


def _persist_driver(tmp_path, monkeypatch=None, data_root=None):
    """A Driver over one registered pond line 'sales@1' (one ripple 'agg'), NoopLauncher."""
    from duckstring.catchment.db import connect, migrate
    from duckstring.catchment.driver import Driver
    from duckstring.catchment.launcher import NoopLauncher
    from duckstring.catchment.routes.deploy import _register

    db = connect(tmp_path / "duck.db")
    migrate(db)
    cfg = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet"}
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", cfg,
              [{"func": "f", "name": "agg", "parents": []}])
    return db, Driver(db, tmp_path, "http://x", NoopLauncher(), data_root=data_root)


def _set_freshness(db, driver, key, f_iso, persisted_iso=None):
    pond_id = driver.meta[key]["pond_id"]
    db.execute("INSERT INTO pond_state (pond_id, start_f, end_f, changed_f, persisted_f) "
               "VALUES (?, ?, ?, ?, ?) ON CONFLICT(pond_id) DO UPDATE SET "
               "start_f=excluded.start_f, end_f=excluded.end_f, changed_f=excluded.changed_f, "
               "persisted_f=excluded.persisted_f",
               (pond_id, f_iso, f_iso, f_iso, persisted_iso))
    db.commit()


def test_pool_loss_regresses_to_the_persisted_floor(tmp_path):
    """Pool-loss rollback (plans/persist.md phase 4): a Pond whose local publish died in the
    publish→persist window claims content (changed_f) that no surviving bytes back — on reload it
    regresses to the floor of what local ∪ plane actually holds (here: the durable plane through
    persisted_f), its failure state above the floor clears, and its Ripples clamp with it."""
    import json
    from datetime import datetime, timezone

    from duckstring.catchment.registry import pond_data_dir

    f_old = "2026-06-01T00:00:00+00:00"   # mirrored to the plane before the loss
    f_new = "2026-06-02T00:00:00+00:00"   # published locally, lost with the box
    db, driver = _persist_driver(tmp_path, data_root=str(tmp_path / "durable"))
    # The durable plane holds content through f_old (what the last completed mirror landed).
    plane = pond_data_dir(tmp_path, "sales", 1, str(tmp_path / "durable"))
    plane.write_text(json.dumps({"t": {"mode": "overwrite", "f": f_old}}), "_trickle.json")
    # The DB claims f_new (the run completed before the box died); no local publish survives.
    _set_freshness(db, driver, "sales@1", f_new, persisted_iso=f_old)
    driver.reload()

    ps = driver.state.pond_states["sales@1"]
    want = datetime(2026, 6, 1, tzinfo=timezone.utc)
    assert ps.end_f == want and ps.changed_f == want and ps.start_f == want, \
        "the lost window must regress to the persisted floor"
    assert driver.state.ripple_states["sales@1.agg"].end_f <= want
    # Persisted to the DB too (a second reload is a no-op, not a second regression).
    row = db.execute("SELECT end_f FROM pond_state WHERE pond_id=?",
                     (driver.meta["sales@1"]["pond_id"],)).fetchone()
    assert row[0] == f_old


def test_pool_loss_healthy_local_publish_is_untouched(tmp_path):
    """The healthy path: the local publish backs the claim → no plane read, no regression — even though
    the persisted watermark lags (the mirror simply hasn't landed yet; that is the normal async window)."""
    import json
    from datetime import datetime, timezone

    from duckstring.catchment.registry import pond_data_dir

    f = "2026-06-02T00:00:00+00:00"
    db, driver = _persist_driver(tmp_path, data_root=str(tmp_path / "durable"))
    local = pond_data_dir(tmp_path, "sales", 1, None)
    local.mkdir()
    local.write_text(json.dumps({"t": {"mode": "overwrite", "f": f}}), "_trickle.json")
    _set_freshness(db, driver, "sales@1", f, persisted_iso=None)  # mirror not yet landed
    driver.reload()
    assert driver.state.pond_states["sales@1"].end_f == datetime(2026, 6, 2, tzinfo=timezone.utc)


def test_plane_truth_advances_persisted_after_adopt(tmp_path):
    """The plane is truth: content adopted/hand-copied onto the durable plane (no recorded watermark, no
    local publish) advances persisted_f to the plane's freshness instead of regressing — un-parking
    cross-Pool Sinks without waiting for a first post-adopt mirror."""
    import json
    from datetime import datetime, timezone

    from duckstring.catchment.registry import pond_data_dir

    f = "2026-06-03T00:00:00+00:00"
    db, driver = _persist_driver(tmp_path, data_root=str(tmp_path / "durable"))
    plane = pond_data_dir(tmp_path, "sales", 1, str(tmp_path / "durable"))
    plane.write_text(json.dumps({"t": {"mode": "overwrite", "f": f}}), "_trickle.json")
    _set_freshness(db, driver, "sales@1", f, persisted_iso=None)  # adopt wrote freshness, no watermark
    driver.reload()
    ps = driver.state.pond_states["sales@1"]
    want = datetime(2026, 6, 3, tzinfo=timezone.utc)
    assert ps.end_f == want, "adopted content backed by the plane must NOT regress"
    assert ps.persisted_f == want, "the plane's freshness must advance the watermark"
    assert driver.persisted["sales@1"] == f


def test_total_loss_regresses_to_never(tmp_path):
    """Nothing survives anywhere (never mirrored, local gone) → a full cold regress: the honest state is
    'this Pond has never published', and demand rebuilds from scratch."""
    db, driver = _persist_driver(tmp_path, data_root=str(tmp_path / "durable"))
    _set_freshness(db, driver, "sales@1", "2026-06-02T00:00:00+00:00", persisted_iso=None)
    driver.reload()
    from duckstring.engine import NEVER
    assert driver.state.pond_states["sales@1"].end_f == NEVER


def test_disk_pressure_evicts_idle_reconstructible_state(tmp_path, monkeypatch):
    """Registry eviction under disk pressure (plans/persist.md phase 6): below the free-space floor, idle
    Ponds shed their reconstructible hot state LRU — the registry whenever a publish backs it; the local
    publish too only when the durable plane holds everything (persisted >= changed). A Pond whose registry
    is the only copy of anything is never touched. No pressure → no eviction."""
    import json
    import os as _os
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from duckstring.catchment.registry import pond_data_dir, pond_registry_path

    f = "2026-06-02T00:00:00+00:00"
    db, driver = _persist_driver(tmp_path, data_root=str(tmp_path / "durable"))

    class FakeLauncher:
        manages_processes = True
        def is_running(self, key): return False
        def terminate(self, key, wait=False): pass
    driver.launcher = FakeLauncher()

    # An idle pond: registry on disk, local publish current, and FULLY MIRRORED to the plane.
    reg = pond_registry_path(tmp_path, "sales", 1)
    reg.parent.mkdir(parents=True, exist_ok=True)
    reg.write_bytes(b"x" * 4096)
    local = pond_data_dir(tmp_path, "sales", 1, None)
    local.mkdir()
    local.write_text(json.dumps({"t": {"mode": "overwrite", "f": f}}), "_trickle.json")
    plane = pond_data_dir(tmp_path, "sales", 1, str(tmp_path / "durable"))
    plane.write_text(json.dumps({"t": {"mode": "overwrite", "f": f}}), "_trickle.json")
    _set_freshness(db, driver, "sales@1", f, persisted_iso=f)
    driver.reload()
    driver.launcher = FakeLauncher()  # reload does not touch the launcher, but be explicit
    driver.state.pond_states["sales@1"].completion_times.append(datetime(2026, 6, 2, tzinfo=timezone.utc))

    # No pressure → nothing evicted.
    monkeypatch.setattr(_os, "statvfs", lambda p: SimpleNamespace(f_bavail=10**9, f_frsize=4096))
    driver._last_evict_check = None
    driver.scheduler_tick()
    assert reg.exists() and (local.root / "_trickle.json").exists()

    # Pressure → the mirrored pond sheds registry AND local publish (reads fall to the plane).
    monkeypatch.setattr(_os, "statvfs", lambda p: SimpleNamespace(f_bavail=1, f_frsize=4096))
    driver._last_evict_check = None
    driver.scheduler_tick()
    assert not reg.exists(), "the registry must be shed under pressure"
    assert not (local.root / "_trickle.json").exists(), "a fully-mirrored local publish is shed too"

    # Un-mirrored content (persisted behind changed): the registry is shed (local publish backs it) but
    # the local publish MUST survive — it is the only copy.
    reg.parent.mkdir(parents=True, exist_ok=True)
    reg.write_bytes(b"x" * 4096)
    local.mkdir()
    local.write_text(json.dumps({"t": {"mode": "overwrite", "f": f}}), "_trickle.json")
    _set_freshness(db, driver, "sales@1", f, persisted_iso=None)
    driver.reload()
    driver.launcher = FakeLauncher()
    driver.state.pond_states["sales@1"].completion_times.append(datetime(2026, 6, 2, tzinfo=timezone.utc))
    driver._last_evict_check = None
    driver.scheduler_tick()
    assert not reg.exists(), "the registry is reconstructible from the local publish"
    assert (local.root / "_trickle.json").exists(), "an un-mirrored local publish must NEVER be evicted"


def test_export_keeps_unhydrated_parts_and_prunes_only_below_floor(tmp_path):
    """The registry-side half of the decoupling (`_export_parts`): a published part the registry merely
    LACKS (a partially-hydrated box) is kept; only a part below the floor (genuine retention) is pruned."""
    from datetime import datetime, timezone

    import duckdb

    from duckstring import trickle_io as T
    from duckstring.dataplane import ParquetDataPlane

    def ts(d):
        return datetime(2026, 1, d, tzinfo=timezone.utc)

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    data_dir = tmp_path / "data"
    for d in (1, 2, 3):
        T.append_table(con, "ev", con.sql(f"SELECT {d} AS id"), ts(d), ("id",))
    ParquetDataPlane().export(con, data_dir, f=ts(3))
    assert len(list((data_dir / "ev").glob("*.parquet"))) == 3

    # Simulate partial hydration: the registry loses the f1/f2 rows WITHOUT retention (floor unmoved).
    con.execute("DELETE FROM ev WHERE id IN (1, 2)")
    ParquetDataPlane().export(con, data_dir, f=ts(3))
    assert len(list((data_dir / "ev").glob("*.parquet"))) == 3, \
        "registry absence must NOT prune published parts (partial hydration is not retention)"

    # Genuine retention: keep only the newest run → the floor advances → below-floor parts pruned.
    T.append_table(con, "ev", con.sql("SELECT 4 AS id"), ts(4), ("id",), retain_n=1)
    ParquetDataPlane().export(con, data_dir, f=ts(4))
    kept = sorted(p.name for p in (data_dir / "ev").glob("*.parquet"))
    assert kept == ["2026-01-04T00_00_00+00_00.parquet"], f"retention must prune below the floor: {kept}"
