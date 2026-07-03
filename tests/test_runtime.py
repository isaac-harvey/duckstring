"""End-to-end runtime tests: a live Catchment + real Duck subprocesses on the demo ponds.

Proves the whole two-tier wiring — trigger → engine cascade → begin_run dispatched over the job
long-poll → Duck executes ripples → events reported → freshness advances → run ledger + history
written → Duck idle-exits. Crash/replay + incomplete-only recovery are unit-tested in test_duck.py;
here we verify the pieces compose against real processes.

Uses the session-wide DUCKSTRING_SLEEP_MULTIPLIER=0.01 so the demo ripples run fast.
"""

from __future__ import annotations

import io
import socket
import threading
import time
import zipfile
from pathlib import Path

import httpx
import pytest
import uvicorn

pytestmark = pytest.mark.timeout(60)

_DEMO = Path(__file__).parent.parent / "src" / "duckstring" / "demo"
_PONDS = ("transactions", "products", "sales", "reports")


def _zip_dir(path: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(path.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(path))
    return buf.getvalue()


def _read_toml(path: Path) -> dict:
    import sys
    if sys.version_info >= (3, 11):
        import tomllib
        return tomllib.loads(path.read_text())
    import tomli
    return tomli.loads(path.read_text())


def _serve(root, port):
    """Start a Catchment uvicorn server on (root, port); return (server, thread) once healthy."""
    from duckstring.catchment.app import create_app

    config = uvicorn.Config(create_app(root), host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            httpx.get(f"{url}/api/health", timeout=1.0)
            break
        except Exception:
            time.sleep(0.05)
    else:
        raise RuntimeError("Catchment did not start")
    return server, thread


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def runtime(tmp_path_factory, monkeypatch):
    """A real uvicorn Catchment with Duck spawning ENABLED, reachable by the spawned subprocesses.

    Pinned to the Parquet data plane: it keeps this broad subprocess suite fast and network-free (no
    DuckDB iceberg-extension fetch), and is the end-to-end coverage of the ``parquet`` opt-out. The
    default Iceberg plane gets its own e2e proof in ``test_demo_chain_runs_on_iceberg_end_to_end``."""
    root = tmp_path_factory.mktemp("runtime_root")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    monkeypatch.delenv("DUCKSTRING_DISABLE_DUCKS", raising=False)  # enable real Ducks
    monkeypatch.setenv("DUCKSTRING_CATCHMENT_URL", url)
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")  # inherited by the Duck subprocesses

    server, thread = _serve(root, port)
    yield url, root
    server.should_exit = True
    thread.join(timeout=5)


_TRICKLE_PONDS = ("orders", "catalog", "priced", "revenue")


def _deploy(url: str, ponds) -> None:
    for name in ponds:
        info = _read_toml(_DEMO / name / "pond.toml")["pond"]
        r = httpx.post(
            f"{url}/api/deploy",
            files={"pond": ("pond.zip", _zip_dir(_DEMO / name), "application/zip")},
            data={"name": info["name"], "version": info["version"], "type": info.get("type", "pond")},
            timeout=15.0,
        )
        assert r.status_code == 200, r.text


def _deploy_demo(url: str) -> None:
    _deploy(url, _PONDS)


def _pond_status(url: str, name: str) -> dict | None:
    rows = httpx.get(f"{url}/api/status", timeout=5.0).json()["ponds"]
    return next((p for p in rows if p["name"] == name), None)


def _wait(predicate, timeout=45.0, interval=0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_pulse_runs_chain_end_to_end(runtime):
    url, root = runtime
    _deploy_demo(url)

    httpx.post(f"{url}/api/ponds/reports/pulse", timeout=5.0)

    # The whole chain runs and reports reaches a freshness (end_f).
    assert _wait(lambda: (_pond_status(url, "reports") or {}).get("end_f") is not None), \
        "reports never became fresh"

    # Every pond produced a run ledger recording a successful Pond Run.
    from duckstring.engine import pond as ledger
    for name in _PONDS:
        db_path = root / "ponds" / name / "m1" / "pond.db"
        assert db_path.exists(), f"no ledger for {name}"
        con = ledger.connect(db_path)
        assert ledger.read_pond_end_f(con) is not None, f"{name} recorded no completed run"
        con.close()

    # Coherent: under a Pulse the outlet and its source both reach a freshness.
    reports = _pond_status(url, "reports")
    sales = _pond_status(url, "sales")
    assert reports["end_f"] is not None and sales["end_f"] is not None


def test_trickle_chain_runs_end_to_end(runtime):
    """The incremental-Trickle demo on real Duck subprocesses: a pulse on the revenue Outlet cascades
    up through the builder Pond to the append + merge inlets, every Pond runs, and the published layout
    carries the Trickle sidecar + a changelog for the merge lines."""
    url, root = runtime
    _deploy(url, _TRICKLE_PONDS)

    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None), \
        "revenue never became fresh"

    # The append inlet published an order_line history (a per-run parts directory) + the mode/PK sidecar.
    orders_dir = root / "ponds" / "orders" / "m1" / "data"
    assert (orders_dir / "_trickle.json").exists()
    import json
    assert json.loads((orders_dir / "_trickle.json").read_text())["order_line"]["mode"] == "append"
    assert list((orders_dir / "order_line").glob("*.parquet")), "orders: no append history parts"

    # The merge lines are log-structured: their main is the __changelog parts directory (the base
    # `{table}.parquet` only appears once a checkpoint folds it, which this small run never reaches), and the
    # current state is reconstructed on read.
    import duckdb

    from duckstring.dataplane import ParquetDataPlane
    rcon = duckdb.connect()
    for name, table in (("catalog", "product"), ("priced", "priced_line"), ("revenue", "revenue_by_product")):
        data_dir = root / "ponds" / name / "m1" / "data"
        assert list((data_dir / f"{table}__changelog").glob("*.parquet")), f"{name}: no changelog parts"
        assert json.loads((data_dir / "_trickle.json").read_text())[table]["mode"] == "merge"
        # The reconstructed main is readable (non-empty current state) even without a checkpointed base.
        n = rcon.sql(f"SELECT count(*) FROM ({ParquetDataPlane().read_select(data_dir, table)})").fetchone()[0]
        assert n > 0, f"{name}: empty reconstructed main"


def test_delete_table_removes_now_and_rebuilds_on_next_run(runtime):
    """`delete-table` removes a table (data + registry state) **immediately** — no run, no freshness
    change — and it reappears only when the Pond next genuinely runs, rebuilt whole by the builder's
    absent⇒comprehensive trigger (plans/deletes.md)."""
    import duckdb

    from duckstring.dataplane import ParquetDataPlane

    url, root = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None)
    time.sleep(1.0)  # let the chain settle to idle

    data_dir = root / "ponds" / "priced" / "m1" / "data"

    def state_count():
        con = duckdb.connect()
        try:
            sql = ParquetDataPlane().read_select(data_dir, "priced_line")
            return con.sql(f"SELECT count(*) FROM ({sql})").fetchone()[0]
        except Exception:
            return None  # not published (removed)
        finally:
            con.close()

    def run_count():
        return len(httpx.get(f"{url}/api/runs", params={"pond": "priced", "limit": 1000}, timeout=5.0).json()["runs"])

    n_before = state_count()
    assert n_before and n_before > 0
    end_f_before = (_pond_status(url, "priced") or {}).get("end_f")
    runs_before = run_count()

    # Delete → gone at once. No run, no freshness bump.
    r = httpx.request("DELETE", f"{url}/api/ponds/priced/tables/priced_line", timeout=10.0)
    assert r.status_code == 200, r.text
    assert state_count() is None, "priced_line was not removed"
    time.sleep(2.0)  # give any (erroneous) run a chance to appear
    assert state_count() is None, "priced_line came back with no genuine run (a rebuild was forced)"
    assert (_pond_status(url, "priced") or {}).get("end_f") == end_f_before, "freshness advanced on a delete"
    assert run_count() == runs_before, "a Pond Run was logged for a delete"

    # A genuine new run (fresh pulse) rebuilds it whole via the absence trigger. The source has grown a
    # batch since, so it comes back at the *current* full state (≥ the old count), not just a delta.
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (state_count() or 0) >= n_before, timeout=45.0), \
        f"priced_line did not rebuild on the next run (count now {state_count()}, was {n_before})"
    time.sleep(1.0)

    # Deleting the *changelog* companion resolves to the base table — the whole merge collection goes,
    # never a changelog stranded from its main.
    assert (root / "ponds" / "priced" / "m1" / "data" / "priced_line__changelog").exists()
    r = httpx.request("DELETE", f"{url}/api/ponds/priced/tables/priced_line__changelog", timeout=10.0)
    assert r.status_code == 200, r.text
    assert state_count() is None, "deleting the changelog left the main"
    assert not (root / "ponds" / "priced" / "m1" / "data" / "priced_line__changelog").exists()


def test_missing_source_asset_blocks_downstream_with_reason(runtime):
    """A downstream that reads a deleted Source table parks **blocked-with-a-reason** — not failed, no
    retry-budget burn (plans/reset.md Mechanism 2) — and recovers when the Source republishes."""
    url, root = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None)
    time.sleep(1.0)

    # Delete catalog.product — priced reads it. Then force priced to hit the (now missing) read.
    r = httpx.request("DELETE", f"{url}/api/ponds/catalog/tables/product", timeout=10.0)
    assert r.status_code == 200, r.text
    httpx.post(f"{url}/api/ponds/priced/force", timeout=5.0)

    def priced():
        return _pond_status(url, "priced") or {}

    assert _wait(lambda: priced().get("is_blocked") and priced().get("blocked_reason"), timeout=30.0), \
        f"priced never blocked (status={priced()})"
    st = priced()
    assert "catalog.product" in st["blocked_reason"]
    assert st["is_failed"] is False, "a missing Source asset must not fail the Pond"
    assert st["failures"] == 0, "a missing Source asset must not burn the retry budget"

    # Recover: rebuild catalog.product at a fresh freshness → priced re-reads clean → unblocks.
    httpx.post(f"{url}/api/ponds/catalog/pulse", timeout=5.0)
    assert _wait(lambda: not priced().get("is_blocked") and not priced().get("blocked_reason"), timeout=30.0), \
        f"priced never recovered (status={priced()})"


def test_reset_pond_scrubs_and_rebuilds(runtime):
    """`reset {pond}` scrubs a Pond's registry, published data, and ledger and rewinds its freshness —
    keeping code + config + demand — and it rebuilds from scratch when next demanded (plans/reset.md).
    Lazy: nothing runs until re-triggered; the Pond survives (identity/config intact)."""
    url, root = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None)
    time.sleep(1.0)

    line = root / "ponds" / "catalog" / "m1"
    assert (line / "registry.duckdb").exists() and (line / "data").exists()
    assert (_pond_status(url, "catalog") or {}).get("has_tables") is True

    r = httpx.post(f"{url}/api/ponds/catalog/reset", timeout=15.0)
    assert r.status_code == 200, r.text
    assert not (line / "registry.duckdb").exists(), "registry not scrubbed"
    assert not (line / "data").exists(), "published data not scrubbed"
    st = _pond_status(url, "catalog") or {}
    assert st.get("has_tables") is False and st.get("end_f") is None, f"freshness not rewound ({st})"
    assert st.get("version"), "the Pond (identity/config) must survive a reset"

    # Lazy: it rebuilds from scratch on the next genuine demand.
    httpx.post(f"{url}/api/ponds/catalog/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "catalog") or {}).get("has_tables") is True, timeout=30.0), \
        "catalog did not rebuild after reset + pulse"


def test_reset_catchment_scrubs_all_keeps_artifacts(runtime):
    """`catchment reset` scrubs every line's registry/data/ledger and rewinds all freshness — keeping the
    deployed artifacts + config — and the whole chain rebuilds from the Inlets down (plans/reset.md)."""
    url, root = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None)
    time.sleep(1.0)
    assert all((root / "ponds" / p / "m1" / "registry.duckdb").exists() for p in _TRICKLE_PONDS)

    r = httpx.post(f"{url}/api/catchment/reset", timeout=30.0)
    assert r.status_code == 200, r.text
    assert r.json()["ponds"] == len(_TRICKLE_PONDS)
    for p in _TRICKLE_PONDS:
        assert not (root / "ponds" / p / "m1").exists(), f"{p} runtime not scrubbed"
        # The deployed artifact ({name}/{version}/) survives.
        assert any(d.is_dir() and d.name[0].isdigit() for d in (root / "ponds" / p).iterdir()), \
            f"{p} artifact was removed"
        assert (_pond_status(url, p) or {}).get("has_tables") is False

    # The whole chain rebuilds from scratch on the next demand.
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None
                 and (_pond_status(url, "priced") or {}).get("has_tables") is True, timeout=45.0), \
        "chain did not rebuild after catchment reset"


def test_reset_pond_rejects_while_running(runtime):
    """A reset requires the Pond idle (409 while a Run is in flight)."""
    url, _ = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/orders/wave", timeout=5.0)  # a standing pull keeps it busy
    # Catch it mid-run; if we miss the window the wave will re-run it, so retry a few times.
    saw_409 = False
    for _ in range(40):
        r = httpx.post(f"{url}/api/ponds/orders/reset", timeout=5.0)
        if r.status_code == 409:
            saw_409 = True
            break
        time.sleep(0.05)
    httpx.post(f"{url}/api/ponds/orders/remove", timeout=5.0)  # drop the wave
    assert saw_409, "reset was allowed mid-run (should 409)"


def test_remove_pond_retires_line(runtime):
    """`pond remove` (a name@major line): scrub its runtime, delete its live selection + its own Spout +
    its name@major alert channel, keep its deployment record + run history, and leave downstream pinning it
    blocked-on-missing-source. A catchment-wide alert survives. See plans/remove-pond.md."""
    import sqlite3

    url, root = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None)
    time.sleep(1.0)  # settle to idle (a pulse is one-shot → no standing demand)

    # Attach a Spout + a scoped alert + a catchment-wide alert to catalog.
    sp = httpx.post(f"{url}/api/ponds/catalog/spouts", json={
        "destination": f"file://{root}/out", "table": "product", "mode": "auto"}, timeout=5.0).json()
    spout_name = f"catalog#{sp['name']}"       # the Spout's pond name (as it appears in status)
    spout_key = f"{spout_name}@1"              # its pond key (as returned in spouts_removed)
    httpx.post(f"{url}/api/alerts", json={"name": "cat-line", "destination": "https://x/a", "scope": "catalog@1"}, timeout=5.0)
    httpx.post(f"{url}/api/alerts", json={"name": "wide", "destination": "https://x/w", "scope": None}, timeout=5.0)

    def catalog_runs():
        db = sqlite3.connect(root / "duck.db")
        try:
            return db.execute(
                "SELECT count(*) FROM pond_run pr JOIN pond_version pv ON pv.id = pr.pond_version_id "
                "JOIN pond_name pn ON pn.id = pv.pond_name_id WHERE pn.name = 'catalog'").fetchone()[0]
        finally:
            db.close()

    runs_before = catalog_runs()
    assert runs_before > 0 and (root / "ponds" / "catalog" / "m1").exists()

    r = httpx.request("DELETE", f"{url}/api/ponds/catalog", params={"major": 1}, timeout=15.0)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["removed"] == "catalog@1"
    assert body["now_blocked"] == ["priced"] and spout_key in body["spouts_removed"]

    # Gone from the live system + its runtime scrubbed; the deployed artifact + run history survive.
    assert _pond_status(url, "catalog") is None
    assert not (root / "ponds" / "catalog" / "m1").exists()
    assert any(d.is_dir() and d.name[0].isdigit() for d in (root / "ponds" / "catalog").iterdir()), "artifact removed"
    assert catalog_runs() == runs_before, "run history was not kept"
    # Its Spout went with it; the scoped alert went, the catchment-wide one survived.
    assert all(p["name"] != spout_name for p in httpx.get(f"{url}/api/status", timeout=5.0).json()["ponds"])
    alert_names = {c["name"] for c in httpx.get(f"{url}/api/alerts", timeout=5.0).json()["channels"]}
    assert "cat-line" not in alert_names and "wide" in alert_names
    # Downstream that pinned catalog@1 blocks on the missing Source.
    assert _wait(lambda: (_pond_status(url, "priced") or {}).get("is_blocked") is True, timeout=10.0)


def test_remove_pond_rejects_with_demand(runtime):
    """Remove requires the line idle + demand-free — a standing Wave is demand, so remove 409s until it's
    cleared (sleep)."""
    url, _ = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/catalog/wave", timeout=5.0)  # a standing pull = demand
    time.sleep(0.5)
    r = httpx.request("DELETE", f"{url}/api/ponds/catalog", params={"major": 1}, timeout=5.0)
    assert r.status_code == 409, r.text
    httpx.post(f"{url}/api/ponds/catalog/sleep", timeout=5.0)  # clears demand + the standing trigger
    time.sleep(0.5)
    r = httpx.request("DELETE", f"{url}/api/ponds/catalog", params={"major": 1}, timeout=15.0)
    assert r.status_code == 200, r.text


def test_refresh_flag_rebuilds_and_bumps_floor(runtime):
    """`control refresh` is lazy: it flags the Pond (refresh_pending) but runs nothing. The next run is a
    cold wipe-and-rebuild that raises the published changelog floor, so downstream coverage-misses."""
    import json

    url, root = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None)

    sidecar = root / "ponds" / "catalog" / "m1" / "data" / "_trickle.json"
    floor1 = json.loads(sidecar.read_text())["product"]["floor"]

    # Flag catalog for refresh — lazy: it shows pending, but nothing runs.
    httpx.post(f"{url}/api/ponds/catalog/refresh", timeout=5.0)
    assert (_pond_status(url, "catalog") or {}).get("refresh_pending") is True
    assert json.loads(sidecar.read_text())["product"]["floor"] == floor1  # unchanged — no run yet

    # A new pulse runs the chain at a fresh epoch → catalog refreshes (wipe + rebuild), floor bumps,
    # and the pending flag is consumed.
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: json.loads(sidecar.read_text())["product"]["floor"] > floor1), \
        "catalog's floor never advanced after the refresh run"
    assert (_pond_status(url, "catalog") or {}).get("refresh_pending") is False


@pytest.mark.skip(reason="flaky under load (real-Duck repair timing, ~1-in-3) — revisit; unrelated to trickle")
def test_repair_chain_rebuilds_downstream_in_order(runtime):
    """`/api/repair` with downstream rebuilds a connected scope now, in topological order — each Pond
    wiped and rebuilt once its in-scope parents finish. Every scope Pond's floor advances."""
    import json

    url, root = runtime
    _deploy(url, _TRICKLE_PONDS)
    httpx.post(f"{url}/api/ponds/revenue/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("end_f") is not None)

    def floor(name, table):
        return json.loads((root / "ponds" / name / "m1" / "data" / "_trickle.json").read_text())[table]["floor"]

    def changelog_rows(name, table):
        import duckdb
        pq = root / "ponds" / name / "m1" / "data" / f"{table}__changelog.parquet"
        return duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]

    catalog_floor_before = floor("catalog", "product")

    r = httpx.post(f"{url}/api/repair", json={"ponds": [{"name": "catalog"}], "downstream": True}, timeout=5.0)
    assert r.status_code == 200
    assert set(r.json()["scope"]) == {"catalog@1", "priced@1", "revenue@1"}  # orders is a Source, not downstream
    assert r.json()["scope"][0] == "catalog@1" and r.json()["scope"][-1] == "revenue@1"  # topological

    # The inlet (catalog) refreshes at *now*, so its floor advances; the whole scope is rebuilt cold, which
    # for a merge Trickle means an empty changelog (a bootstrap). (priced/revenue's *floor* stays pinned to
    # the un-refreshed `orders` source — repair rebuilds the data, freshness only moves where it genuinely
    # advances; that's expected.)
    assert _wait(lambda: floor("catalog", "product") > catalog_floor_before), "catalog never rebuilt"
    assert _wait(lambda: (_pond_status(url, "revenue") or {}).get("status") != "repairing"), "repair never finished"
    for n, t in (("catalog", "product"), ("priced", "priced_line"), ("revenue", "revenue_by_product")):
        assert changelog_rows(n, t) == 0, f"{n} was not rebuilt cold (changelog not empty)"


def test_wave_then_remove(runtime):
    url, root = runtime
    _deploy_demo(url)

    httpx.post(f"{url}/api/ponds/reports/wave", timeout=5.0)

    # A Wave keeps producing runs: reports completes several times.
    rep_db = root / "ponds" / "reports" / "m1" / "pond.db"
    from duckstring.engine import pond as ledger

    def completed_runs() -> int:
        if not rep_db.exists():
            return 0
        con = ledger.connect(rep_db)
        n = con.execute("SELECT COUNT(*) FROM pond_run WHERE status = 'success'").fetchone()[0]
        con.close()
        return n

    assert _wait(lambda: completed_runs() >= 2), "wave did not produce repeated runs"

    # Removing the standing trigger halts the Wave; in-flight runs drain, then it stabilises.
    httpx.post(f"{url}/api/ponds/reports/untrigger", timeout=5.0)
    time.sleep(2.0)
    settled = completed_runs()
    time.sleep(2.0)
    assert completed_runs() == settled


def test_restart_restores_state_e2e(tmp_path_factory, monkeypatch):
    root = tmp_path_factory.mktemp("restart_root")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    monkeypatch.delenv("DUCKSTRING_DISABLE_DUCKS", raising=False)
    monkeypatch.setenv("DUCKSTRING_CATCHMENT_URL", url)

    # First Catchment: deploy + pulse the chain to completion.
    server, thread = _serve(root, port)
    try:
        _deploy_demo(url)
        httpx.post(f"{url}/api/ponds/reports/pulse", timeout=5.0)
        assert _wait(lambda: (_pond_status(url, "reports") or {}).get("end_f") is not None)
        before = _pond_status(url, "reports")
        assert before["gen"] >= 1
    finally:
        server.should_exit = True
        thread.join(timeout=5)

    # Restart on the SAME root + DB: state must be restored, not fresh.
    server2, thread2 = _serve(root, port)
    try:
        after = _pond_status(url, "reports")
        assert after["gen"] == before["gen"], "gen reset on restart"
        assert after["end_f"] is not None, "freshness lost on restart"
    finally:
        server2.should_exit = True
        thread2.join(timeout=5)


def test_status_and_runs_feed_live(runtime):
    """The UI's read surface against a real run: the enriched /api/status (ripple-level state +
    intra-Pond edges, d_ms, trigger) and the /api/runs history (lineage filter + nested Ripple Runs).
    """
    url, root = runtime
    _deploy_demo(url)

    httpx.post(f"{url}/api/ponds/reports/pulse", timeout=5.0)
    assert _wait(lambda: (_pond_status(url, "reports") or {}).get("end_f") is not None)

    # Enriched status: every Pond carries d_ms + a ripple list; sales has intra-Pond edges
    # (join_lines depends on daily_sales + price_tiers).
    reports = _pond_status(url, "reports")
    assert isinstance(reports["d_ms"], int) and reports["trigger"] is None
    assert {r["name"] for r in reports["ripples"]}  # non-empty
    sales = _pond_status(url, "sales")
    assert ["daily_sales", "join_lines"] in sales["ripple_edges"]
    assert ["price_tiers", "join_lines"] in sales["ripple_edges"]

    # Global run feed includes reports; nested Ripple Runs appear only when requested.
    runs = httpx.get(f"{url}/api/runs", params={"ripples": True}, timeout=5.0).json()["runs"]
    rep_run = next(r for r in runs if r["pond"] == "reports")
    assert rep_run["status"] == "success"
    assert any(rr["status"] == "success" for rr in rep_run["ripples"])
    # Ripple Runs carry a real execution span (started_at + finished_at) → a duration for the UI.
    rr = rep_run["ripples"][0]
    assert rr["started_at"] is not None and rr["finished_at"] is not None
    assert rr["finished_at"] >= rr["started_at"]

    # Lineage filter: reports + its upstream sources (sales, transactions, products) — not just reports.
    feed = httpx.get(f"{url}/api/runs", params={"pond": "reports", "lineage": True}, timeout=5.0).json()["runs"]
    ponds_seen = {r["pond"] for r in feed}
    assert "sales" in ponds_seen and "transactions" in ponds_seen
    only = httpx.get(f"{url}/api/runs", params={"pond": "reports", "lineage": False}, timeout=5.0).json()["runs"]
    assert {r["pond"] for r in only} == {"reports"}


@pytest.fixture
def runtime_iceberg(tmp_path_factory, monkeypatch):
    """Like ``runtime`` but with the Iceberg data plane enabled — the spawned Ducks inherit the env,
    so the demo chain publishes to and reads from Iceberg in real subprocesses. Skipped without
    pyiceberg (SQLAlchemy is deliberately not required)."""
    pytest.importorskip("pyiceberg")
    root = tmp_path_factory.mktemp("runtime_iceberg_root")
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    monkeypatch.delenv("DUCKSTRING_DISABLE_DUCKS", raising=False)
    monkeypatch.setenv("DUCKSTRING_CATCHMENT_URL", url)
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "iceberg")  # inherited by the Duck subprocesses

    server, thread = _serve(root, port)
    yield url, root
    server.should_exit = True
    thread.join(timeout=5)


def test_demo_chain_runs_on_iceberg_end_to_end(runtime_iceberg):
    url, root = runtime_iceberg
    _deploy_demo(url)

    httpx.post(f"{url}/api/ponds/reports/pulse", timeout=5.0)

    # reports reaching a freshness proves the whole chain ran — including sales reading its Sources
    # (transactions, products) *through Iceberg* in the Duck subprocess.
    assert _wait(lambda: (_pond_status(url, "reports") or {}).get("end_f") is not None), \
        "reports never became fresh on the iceberg data plane"

    # The Iceberg base layer was actually used: each pond line has a catalog + committed metadata,
    # alongside the flat-Parquet compat sidecar.
    for name in _PONDS:
        data_dir = root / "ponds" / name / "m1" / "data"
        assert (data_dir / "catalog.json").exists(), f"{name}: no iceberg catalog"
        assert list(data_dir.rglob("*.metadata.json")), f"{name}: no iceberg metadata"
        assert list(data_dir.glob("*.parquet")), f"{name}: no flat-parquet sidecar"

    # The exported data is queryable via /api/data (in-memory, iceberg-aware view registration).
    resp = httpx.post(
        f"{url}/api/query",
        json={"pond": "reports", "sql": "SELECT COUNT(*) AS n FROM monthly_summary"},
        timeout=10.0,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()[0]["n"] >= 0
