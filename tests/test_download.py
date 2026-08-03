"""`duckstring catchment download` — pull a Catchment's entire root as a tar stream, with a size
confirmation first. SQLite files arrive as consistent snapshots (WAL content included)."""

from __future__ import annotations

import io
import sqlite3
import tarfile

import duckdb
import pytest

from duckstring.cli import app

pytestmark = pytest.mark.timeout(10)


def _seed(root, pond: str, table: str) -> None:
    data_dir = root / "ponds" / pond / "m1" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    duckdb.sql("SELECT 1 AS id, 'a' AS val").write_parquet(str(data_dir / f"{table}.parquet"))


def test_usage_reports_size_and_count(catchment_client):
    _seed(catchment_client.app.state.root, "outlet", "daily")
    use = catchment_client.get("/api/catchment/usage").json()
    assert use["file_count"] >= 2  # duck.db + the parquet
    assert use["total_bytes"] > 0
    assert use["archive_bytes"] > use["total_bytes"]  # headers + padding on top of the content


def test_archive_roundtrip_with_consistent_sqlite(catchment_client, tmp_path):
    root = catchment_client.app.state.root
    _seed(root, "outlet", "daily")

    resp = catchment_client.get("/api/catchment/archive")
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/x-tar"

    with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r") as tar:
        names = tar.getnames()
        tar.extractall(tmp_path, filter="data")
    assert "duck.db" in names
    assert "ponds/outlet/m1/data/daily.parquet" in names
    assert not any(n.endswith((".db-wal", ".db-shm")) for n in names)  # subsumed by the snapshot

    # The snapshot is a coherent database (WAL content checkpointed in by the backup API).
    con = sqlite3.connect(tmp_path / "duck.db")
    assert con.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] >= 1
    con.close()
    rows = duckdb.sql(f"SELECT * FROM read_parquet('{tmp_path / 'ponds/outlet/m1/data/daily.parquet'}')").fetchall()
    assert rows == [(1, "a")]


def test_cli_download(runner, catchment_root, live_catchment, tmp_path, monkeypatch):
    _seed(catchment_root, "outlet", "daily")
    dest = tmp_path / "state"
    result = runner.invoke(app, ["catchment", "download", "--path", str(dest), "--yes"])
    assert result.exit_code == 0, result.output
    assert (dest / "duck.db").exists()
    assert (dest / "ponds" / "outlet" / "m1" / "data" / "daily.parquet").exists()


def test_cli_download_shows_size_and_can_decline(runner, catchment_root, live_catchment, tmp_path):
    _seed(catchment_root, "outlet", "daily")
    dest = tmp_path / "state"
    result = runner.invoke(app, ["catchment", "download", "--path", str(dest)], input="n\n")
    assert result.exit_code != 0  # declined → aborted
    assert "files" in result.output  # the size estimate was shown before asking
    assert not dest.exists()


def test_cli_download_default_path_is_dot_duckstring(runner, catchment_root, live_catchment, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["catchment", "download", "--yes"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".duckstring" / "duck.db").exists()


def test_serving_spill_is_not_bundled(tmp_path):
    """The sandbox spills into the root; that scratch is transient and can be large, so it must not ride
    a state bundle the way real state does."""
    from duckstring.catchment.routes.catchment import _root_files

    (tmp_path / ".serving-tmp").mkdir()
    (tmp_path / ".serving-tmp" / "duckdb_temp_block-1.tmp").write_bytes(b"spill")
    (tmp_path / "duck.db").write_bytes(b"state")
    names = [arc for _p, arc in _root_files(tmp_path)]
    assert "duck.db" in names
    assert not any(n.startswith(".serving-tmp") for n in names)
