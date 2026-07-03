"""Objects — non-tabular Pond outputs (write_object / read_object / object_path), the sidecar section,
the local-runner commit, cross-Pond reads, abort safety, and the read API. See plans/objects.md."""

from __future__ import annotations

import pickle
import textwrap
from pathlib import Path

import pytest

from duckstring.local import hydrate, load_project, run_pond

pytestmark = pytest.mark.timeout(15)


def _make_pond(path: Path, name: str, pond_py: str, sources: dict[str, str] | None = None,
               puddles_py: str = "") -> Path:
    toml = f'[pond]\nname = "{name}"\nversion = "0.1.0"\n'
    if sources:
        toml += "[sources]\n" + "".join(f'{s} = "{v}"\n' for s, v in sources.items())
    (path / "pond.toml").write_text(toml)
    (path / "src").mkdir(exist_ok=True)
    (path / "src" / "pond.py").write_text(textwrap.dedent(pond_py))
    if puddles_py:
        (path / "src" / "puddles.py").write_text(textwrap.dedent(puddles_py))
    return path


def _objects(project) -> dict:
    from duckstring.objects import list_objects

    return list_objects(project.out_dir)


# ── the Pond handle: write during a ripple, commit at export ───────────────────


def test_write_object_committed_and_readable(tmp_path):
    pond_py = """
        import pickle
        from duckstring import ripple

        @ripple
        def train(pond):
            pond.write_table("stat", pond.con.sql("SELECT 1 AS n"))
            pond.write_object("model", pickle.dumps({"w": [1, 2, 3]}))
    """
    project = load_project(_make_pond(tmp_path, "trainer", pond_py))
    result = run_pond(project)
    assert [r.status for r in result.ripples] == ["ok"]

    objs = _objects(project)
    assert set(objs) == {"model"}
    assert objs["model"]["is_dir"] is False
    # published under objects/model/data
    assert (project.out_dir / "objects" / "model" / "data").exists()

    from duckstring.objects import read_object

    assert pickle.loads(read_object(project.out_dir, "model")) == {"w": [1, 2, 3]}


def test_directory_object_round_trips_as_a_unit(tmp_path):
    pond_py = """
        import tempfile
        from pathlib import Path
        from duckstring import ripple

        @ripple
        def bundle(pond):
            pond.write_table("t", pond.con.sql("SELECT 1 AS n"))
            d = Path(tempfile.mkdtemp())
            (d / "config.json").write_text("{}")
            (d / "weights.bin").write_bytes(b"\\x00\\x01\\x02")
            pond.write_object("hf", d)
    """
    project = load_project(_make_pond(tmp_path, "bundler", pond_py))
    result = run_pond(project)
    assert [r.status for r in result.ripples] == ["ok"], result.ripples
    objs = _objects(project)
    assert objs["hf"]["is_dir"] is True
    root = project.out_dir / "objects" / "hf"
    assert (root / "config.json").read_text() == "{}"
    assert (root / "weights.bin").read_bytes() == b"\x00\x01\x02"


# ── persistence across runs ────────────────────────────────────────────────────


def test_object_persists_when_a_later_run_does_not_rewrite_it(tmp_path):
    pond_py = """
        from duckstring import ripple

        @ripple
        def step(pond):
            pond.write_table("t", pond.con.sql("SELECT 1 AS n"))
            if pond.previous_f is None or pond.previous_f.year < 2000:
                pond.write_object("model", b"first-run-bytes")
    """
    project = load_project(_make_pond(tmp_path, "keeper", pond_py))
    run_pond(project)                    # full run: writes model
    assert set(_objects(project)) == {"model"}
    run_pond(project, ripple="step")     # incremental run: does NOT write model
    from duckstring.objects import read_object

    assert set(_objects(project)) == {"model"}
    assert read_object(project.out_dir, "model") == b"first-run-bytes"


# ── abort safety: the staging→commit gate that backs "last-good on failure" ────
#
# The deployed Duck only calls commit_objects *after* the run's table export passes the contract gate
# (RippleExecutor.export). A failed run reports `failed` and never exports, so the staged write is never
# committed and last-good survives. That gate is exercised here directly (the local runner deliberately
# exports partial output for inspection, so it is not the vehicle for this guarantee).


def test_staged_write_is_not_visible_until_commit(tmp_path):
    from datetime import datetime, timezone

    from duckstring import objects
    from duckstring.storage import LocalStorage

    now = lambda: datetime.now(timezone.utc)  # noqa: E731
    data_dir = LocalStorage(tmp_path / "data")
    staging = tmp_path / "staging"

    objects.write_object_now(data_dir, "model", b"good", now())  # last-good published
    objects.stage_object(staging, "model", b"BAD")               # a new run stages an update…
    # …but its run aborts before export, so commit_objects never runs → last-good intact.
    assert objects.read_object(data_dir, "model") == b"good"
    assert set(objects.list_objects(data_dir)) == {"model"}

    objects.commit_objects(staging, data_dir, now())             # only a successful export swaps it
    assert objects.read_object(data_dir, "model") == b"BAD"


# ── delete ─────────────────────────────────────────────────────────────────────


def test_delete_object_removes_payload_and_sidecar_entry(tmp_path):
    from datetime import datetime, timezone

    from duckstring import objects
    from duckstring.storage import LocalStorage

    now = lambda: datetime.now(timezone.utc)  # noqa: E731
    data_dir = LocalStorage(tmp_path / "data")
    objects.write_object_now(data_dir, "model", b"m", now())
    objects.write_object_now(data_dir, "keep", b"k", now())

    objects.delete_object(data_dir, "model")
    assert set(objects.list_objects(data_dir)) == {"keep"}
    assert not (tmp_path / "data" / "objects" / "model").exists()
    with pytest.raises(FileNotFoundError):
        objects.read_object(data_dir, "model")
    assert objects.read_object(data_dir, "keep") == b"k"  # untouched
    objects.delete_object(data_dir, "model")  # idempotent


# ── cross-Pond read (major/flat-layout resolution) ─────────────────────────────


def test_read_object_from_source(tmp_path):
    # A source Pond `up` seeded via a puddle publishes `vec`; the consumer reads `up.vec`.
    up = tmp_path / "up"
    up.mkdir()
    puddles_py = """
        from duckstring import puddle

        @puddle("up.seed")
        def up_seed(p):
            p.write_table("seed", p.con.sql("SELECT 1 AS x"))
            p.write_object("vec", b"source-object-bytes")
    """
    # The consumer lives at tmp_path/down and reads up.vec through the flat puddles layout.
    down = tmp_path / "down"
    down.mkdir()
    pond_py = """
        from duckstring import ripple

        @ripple
        def use(pond):
            data = pond.read_object("up.vec")
            pond.write_table("out", pond.con.sql(f"SELECT {len(data)} AS n"))
    """
    _make_pond(down, "down", pond_py, sources={"up": "0.1.0"}, puddles_py=puddles_py)
    project = load_project(down)
    # hydrate seeds puddles/ponds/up/data/{seed.parquet, objects/vec/...}
    hydrate(project)
    result = run_pond(project)
    assert [r.status for r in result.ripples] == ["ok"], result.ripples
    import duckdb

    (n,) = duckdb.sql(f"SELECT n FROM read_parquet('{project.out_dir / 'out.parquet'}')").fetchone()
    assert n == len(b"source-object-bytes")


# ── read API ───────────────────────────────────────────────────────────────────


def test_list_and_download_endpoints(tmp_path):
    # Publish two objects directly into a pond line's data dir, then serve them via the app.
    from datetime import datetime, timezone

    from duckstring import objects
    from duckstring.storage import LocalStorage

    root = tmp_path / "cat"
    data_dir = LocalStorage(root / "ponds" / "trainer" / "m1" / "data")
    objects.write_object_now(data_dir, "model", b"blobbytes", datetime.now(timezone.utc))
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "a.txt").write_text("A")
    objects.write_object_now(data_dir, "bundle", tree, datetime.now(timezone.utc))

    from fastapi.testclient import TestClient

    from duckstring.catchment.app import create_app

    app = create_app(root)
    client = TestClient(app)
    r = client.get("/api/ponds/trainer/objects", params={"major": 1})
    assert r.status_code == 200
    names = {o["name"]: o for o in r.json()["objects"]}
    assert set(names) == {"model", "bundle"}
    assert names["model"]["is_dir"] is False and names["bundle"]["is_dir"] is True

    r = client.get("/api/ponds/trainer/objects/model", params={"major": 1})
    assert r.status_code == 200
    assert r.content == b"blobbytes"

    r = client.get("/api/ponds/trainer/objects/bundle", params={"major": 1})
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"

    r = client.get("/api/ponds/trainer/objects/missing", params={"major": 1})
    assert r.status_code == 404

    # Delete one Object via the API (Pond is idle) → gone from the listing, others kept.
    r = client.delete("/api/ponds/trainer/objects/model", params={"major": 1})
    assert r.status_code == 200, r.text
    names = {o["name"] for o in client.get("/api/ponds/trainer/objects", params={"major": 1}).json()["objects"]}
    assert names == {"bundle"}
    assert client.get("/api/ponds/trainer/objects/model", params={"major": 1}).status_code == 404
