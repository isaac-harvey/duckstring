"""The storage seam (plans/storage-decoupling.md Part 1): a URI-addressable data location with a local
and an object-store (fsspec) backend, and the state/data root split that routes the data plane to it.

DuckDB cannot ``COPY`` to fsspec's in-memory filesystem, so the object-store backend is exercised here
for its **metadata** operations against ``memory://``; the full data-plane read/write pipeline is exercised
against a **separate local data root** (the Databricks-Volume-via-FUSE path, and any local data dir),
which proves the data root is genuinely decoupled from the state root. Real ``s3://`` read/write is the
CI follow-up (same posture as the egress real-backend tests)."""

from __future__ import annotations

import duckdb

from duckstring.dataplane import ParquetDataPlane
from duckstring.storage import LocalStorage, ObjectStorage, get_storage, is_object_uri


def test_is_object_uri_classification():
    assert is_object_uri("s3://bucket/p")
    assert is_object_uri("gs://bucket/p")
    assert is_object_uri("abfss://c@acct.dfs.core.windows.net/p")
    assert not is_object_uri("/Volumes/cat/schema/vol")  # a Volume FUSE path is local
    assert not is_object_uri("file:///tmp/x")
    assert not is_object_uri("/local/path")


def test_get_storage_dispatch(tmp_path):
    assert isinstance(get_storage(tmp_path), LocalStorage)
    assert isinstance(get_storage("s3://bucket/prefix"), ObjectStorage)
    # credentials/options ride the query and are split off the addressable base.
    s = get_storage("s3://bucket/prefix?region=eu-west-1&key_id=${env:K}")
    assert isinstance(s, ObjectStorage)
    assert s.base == "s3://bucket/prefix"
    assert s.params["region"] == "eu-west-1"


def test_local_storage_roundtrip(tmp_path):
    s = get_storage(tmp_path)
    child = s.child("t")
    child.mkdir()
    child.write_text("hello", "a.txt")
    assert child.read_text("a.txt") == "hello"
    child.write_bytes(b"PAR1", "x.parquet")
    assert child.exists("x.parquet")
    assert child.parquet_names() == ["x.parquet"]
    assert s.subdir_names() == ["t"]
    # atomic copy_to: yields a tmp uri, commits on exit
    with child.copy_to("y.parquet") as uri:
        open(uri.replace("''", "'"), "wb").write(b"PAR1yy")
    assert child.size("y.parquet") == 6
    child.remove("x.parquet")
    assert not child.exists("x.parquet")
    s.rmtree("t")
    assert not s.is_dir("t")


def test_object_storage_metadata_ops_memory():
    o = ObjectStorage("memory://bucket/prefix")
    o.write_text("hi", "sub", "a.txt")
    assert o.read_text("sub", "a.txt") == "hi"
    o.write_bytes(b"PAR1", "sub", "x.parquet")
    assert o.parquet_names("sub") == ["x.parquet"]
    assert "sub" in o.subdir_names()
    assert o.size("sub", "x.parquet") == 4
    assert o.uri("sub", "x.parquet") == "memory://bucket/prefix/sub/x.parquet"
    assert o.glob("*.parquet", "sub") == "memory://bucket/prefix/sub/*.parquet"
    # move_into across two object dirs (an fsspec mv)
    dest = o.child("dst")
    o.child("sub").move_into(dest, "x.parquet", "moved.parquet")
    assert dest.parquet_names() == ["moved.parquet"]
    o.remove("sub", "a.txt")
    assert not o.exists("sub", "a.txt")
    o.rmtree("dst")
    assert not o.is_dir("dst")


def test_object_storage_missing_size_is_zero():
    o = ObjectStorage("memory://b2/p")
    assert o.size("nope.parquet") == 0
    assert o.read_text("nope.txt") is None
    assert o.parquet_names("nope") == []


def test_warehouse_location_raw_vs_uri(tmp_path):
    o = ObjectStorage("s3://bucket/prefix")
    assert o.warehouse_location() == "s3://bucket/prefix"  # raw, for pyiceberg warehouse / FileIO
    assert get_storage(tmp_path).warehouse_location().startswith("file://")  # local → file:// URI


def test_iceberg_credential_property_mapping(monkeypatch):
    monkeypatch.setenv("AWS_KEY", "AKIA")
    monkeypatch.setenv("AWS_SECRET", "shh")
    o = ObjectStorage("s3://bucket/p", {"region": "eu-west-1", "key_id": "${env:AWS_KEY}",
                                        "secret": "${env:AWS_SECRET}"})
    props = o.iceberg_properties()
    assert props["s3.access-key-id"] == "AKIA"
    assert props["s3.secret-access-key"] == "shh"
    assert props["s3.region"] == "eu-west-1"
    # a local data root carries no FileIO credentials
    assert get_storage("/tmp/x").iceberg_properties() == {}


def test_iceberg_plane_on_separate_local_data_root(tmp_path):
    """The Iceberg plane writes its catalog + table metadata/data through the Storage seam at a data root
    separate from the state root, and DuckDB reads it back — the same code path an object store takes,
    validated here on a local (Volume-FUSE-equivalent) data root."""
    from duckstring.iceberg_plane import IcebergDataPlane

    data_root = tmp_path / "bucket"
    storage = get_storage(data_root).child("sales", "m1", "data")
    dp = IcebergDataPlane()
    con = _con_with("CREATE TABLE revenue AS SELECT 1 AS id, 10 AS amt")
    dp.prepare(con)
    dp.export(con, storage)

    # the catalog pointer + the Iceberg table live under the (separate) data root
    assert (data_root / "sales" / "m1" / "data" / "catalog.json").exists()
    assert (data_root / "sales" / "m1" / "data" / "pond" / "revenue" / "metadata").is_dir()
    assert dp.list_tables(storage) == ["revenue"]
    assert con.sql(dp.read_select(storage, "revenue")).fetchall() == [(1, 10)]

    # a second overwrite commits a new snapshot and the GC reclaims the superseded data file
    con.execute("DROP TABLE revenue")
    con.execute("CREATE TABLE revenue AS SELECT 2 AS id, 20 AS amt")
    dp.export(con, storage)
    assert con.sql(dp.read_select(storage, "revenue")).fetchall() == [(2, 20)]


def _con_with(table_sql):
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute(table_sql)
    return con


def test_data_plane_writes_to_separate_local_data_root(tmp_path):
    """The data plane publishes/reads through a Storage pointed at a *different* directory than the state
    root — the Volume-FUSE path. Nothing lands under the (separate) state root."""
    state_root = tmp_path / "state"
    data_root = tmp_path / "bucket"
    storage = get_storage(data_root).child("sales", "m1", "data")
    dp = ParquetDataPlane()
    con = _con_with("CREATE TABLE revenue AS SELECT 1 AS id, 10 AS amt")
    dp.export(con, storage)

    assert (data_root / "sales" / "m1" / "data" / "revenue.parquet").exists()
    assert not state_root.exists()  # the state root is untouched
    assert dp.list_tables(storage) == ["revenue"]
    assert con.sql(dp.read_select(storage, "revenue")).fetchall() == [(1, 10)]


def test_pond_data_dir_default_vs_custom(tmp_path):
    from duckstring.catchment.registry import pond_data_dir

    default = pond_data_dir(tmp_path, "p", 2)
    assert isinstance(default, LocalStorage)
    assert default.root == tmp_path / "ponds" / "p" / "m2" / "data"

    custom = pond_data_dir(tmp_path, "p", 2, data_root="s3://lake/ds")
    assert isinstance(custom, ObjectStorage)
    assert custom.base == "s3://lake/ds/p/m2/data"


def test_trickle_parts_roundtrip_on_separate_data_root(tmp_path):
    """A merge Trickle's changelog parts + base land and read correctly through a non-default data root."""
    from datetime import datetime, timezone

    import duckstring.trickle_io as T

    f = datetime(2026, 1, 1, tzinfo=timezone.utc)
    reg = duckdb.connect()
    reg.execute("SET TimeZone='UTC'")
    T.merge_table(reg, "dim", reg.sql("SELECT 1 AS id, 'a' AS v"), f, ("id",))
    storage = get_storage(tmp_path / "bucket").child("dims", "m1", "data")
    dp = ParquetDataPlane()
    dp.export(reg, storage, f=f)

    assert T.load_sidecar(storage)["dim"]["mode"] == "merge"
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    rows = rcon.sql(f"SELECT id, v FROM ({dp.read_select(storage, 'dim')})").fetchall()
    assert rows == [(1, "a")]


def test_object_storage_region_goes_to_client_kwargs(monkeypatch):
    """Regression (found against real S3): a `region` in the URI query must reach s3fs as
    client_kwargs.region_name, not a bare `region=` kwarg (which aiobotocore rejects). Other backends
    derive region themselves, so it isn't forwarded to them."""
    import fsspec

    from duckstring.storage import get_storage

    captured = {}

    def fake_filesystem(protocol, **kwargs):
        captured["protocol"] = protocol
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(fsspec, "filesystem", fake_filesystem)

    _ = get_storage("s3://bucket/p?region=ap-southeast-2").fs
    assert captured["protocol"] == "s3"
    # Listings cache is always disabled (cross-process write/read correctness — see ObjectStorage.fs).
    assert captured["kwargs"] == {"use_listings_cache": False, "client_kwargs": {"region_name": "ap-southeast-2"}}
    assert "region" not in captured["kwargs"]

    captured.clear()
    _ = get_storage("gs://bucket/p?region=us-central1").fs
    assert captured["protocol"] == "gcs" and "region" not in captured["kwargs"]  # not forwarded to gcs

    captured.clear()
    _ = get_storage("s3://bucket/p?key_id=AK&secret=SK&region=eu-west-1").fs
    assert captured["kwargs"]["key"] == "AK" and captured["kwargs"]["secret"] == "SK"
    assert captured["kwargs"]["client_kwargs"] == {"region_name": "eu-west-1"}
