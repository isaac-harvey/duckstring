"""The object-store data plane, against a REAL S3 server.

Everything about S3 has been proved by hand on a live AWS box, one session at a time — which is how a
missing region default, an unsigned write, and a placement bug all shipped. A local S3-compatible server
(moto) makes the same paths a normal test: publish to a bucket, read back through the data plane, mirror
via the Persist, and query it.

By default it runs against **moto** — pure Python, no Docker, starts in a second. Set ``DUCKSTRING_TEST_S3``
to point the same tests at a stricter server instead (CI uses MinIO); the fixture then skips starting one.

**What this does and does not prove.** It covers the object-store code paths: the endpoint override for
both fsspec and DuckDB, the published layout, sidecars, the parquet round trip, and Duck subprocesses
reaching the bucket. It does NOT prove anything about credentials — moto accepts unsigned requests
(measured, not assumed), so the production bug where a worker wrote UNSIGNED would still have passed
here. MinIO rejects anonymous writes, which is why CI runs against it; against moto, treat a pass as
"the paths work", not "the auth works".
"""

from __future__ import annotations

import os
import socket
import threading
import time

import httpx
import pytest

pytestmark = pytest.mark.timeout(120)

if not os.environ.get("DUCKSTRING_TEST_S3"):
    pytest.importorskip("moto.server", reason="needs moto[server] (dev extra) or DUCKSTRING_TEST_S3")
pytest.importorskip("boto3")

_BUCKET = "duckstring-test"
# MinIO in CI is started with these; moto accepts anything.
_KEY = os.environ.get("DUCKSTRING_TEST_S3_KEY", "duckstringtest")
_SECRET = os.environ.get("DUCKSTRING_TEST_S3_SECRET", "duckstringtest")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def s3_server():
    """A real S3 API on localhost: an external one via ``DUCKSTRING_TEST_S3`` (CI points this at MinIO,
    which enforces signing), else an in-process moto."""
    import boto3

    external = os.environ.get("DUCKSTRING_TEST_S3")
    server = None
    if external:
        endpoint = external.rstrip("/")
    else:
        from moto.server import ThreadedMotoServer

        port = _free_port()
        server = ThreadedMotoServer(port=port, verbose=False)
        server.start()
        endpoint = f"http://127.0.0.1:{port}"

    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1",
                      aws_access_key_id=_KEY, aws_secret_access_key=_SECRET)
    try:
        s3.create_bucket(Bucket=_BUCKET)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    yield endpoint, s3
    if server is not None:
        server.stop()


@pytest.fixture
def s3_env(s3_server, monkeypatch):
    """Point the whole process (and any Duck it spawns) at the local S3."""
    endpoint, client = s3_server
    monkeypatch.setenv("DUCKSTRING_S3_ENDPOINT", endpoint)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", _KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", _SECRET)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    return endpoint, client


def _keys(client, prefix: str = "") -> list[str]:
    out = client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    return [o["Key"] for o in out.get("Contents", [])]


# ─── the storage layer itself ────────────────────────────────────────────────────


def test_storage_round_trips_through_a_real_s3(s3_env, tmp_path):
    """Write, read, list and delete over the S3 API — the fsspec half of the data plane."""
    from duckstring.storage import get_storage

    endpoint, client = s3_env
    store = get_storage(f"s3://{_BUCKET}/roundtrip")
    store.write_bytes(b"hello", "a.txt")

    assert store.exists("a.txt")
    assert store.read_bytes("a.txt") == b"hello"
    assert "roundtrip/a.txt" in _keys(client, "roundtrip/")
    store.remove("a.txt")
    assert not store.exists("a.txt")


def test_duckdb_reads_parquet_from_a_real_s3(s3_env, tmp_path):
    """The OTHER half: DuckDB's own httpfs path, which needs the endpoint in its secret rather than in
    fsspec's client kwargs. Without it DuckDB addresses bucket.host and silently resolves nowhere."""
    import duckdb

    from duckstring.storage import get_storage

    endpoint, _client = s3_env
    store = get_storage(f"s3://{_BUCKET}/duckread")
    con = duckdb.connect()
    store.duckdb_setup(con)
    con.execute(f"COPY (SELECT 1 AS id, 'x' AS name) TO '{store.uri('t.parquet')}' (FORMAT PARQUET)")
    assert con.sql(f"SELECT id, name FROM read_parquet('{store.uri('t.parquet')}')").fetchone() == (1, "x")


# ─── the full runtime, publishing to S3 ──────────────────────────────────────────


def _serve(root, port):
    import uvicorn

    from duckstring.catchment.app import create_app

    server = uvicorn.Server(uvicorn.Config(create_app(root), host="127.0.0.1", port=port,
                                           log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            httpx.get(f"{url}/api/health", timeout=1.0)
            break
        except Exception:
            time.sleep(0.05)
    else:
        raise RuntimeError("Catchment did not start")
    return server, thread, url


def test_a_chain_publishes_and_persists_to_s3(s3_env, tmp_path_factory, monkeypatch):
    """The whole runtime with a bucket as its data plane, on real Duck subprocesses: the chain runs, the
    local-first publish mirrors to S3 (the Persist), and the watermark advances. This is the shape that
    only a live AWS box could exercise before — including that the Ducks, which are separate PROCESSES,
    inherit enough environment to sign their own requests."""
    from test_runtime import _deploy, _pond_status, _wait

    endpoint, client = s3_env
    root = tmp_path_factory.mktemp("s3_runtime")
    port = _free_port()
    monkeypatch.delenv("DUCKSTRING_DISABLE_DUCKS", raising=False)   # real Duck subprocesses
    monkeypatch.setenv("DUCKSTRING_CATCHMENT_URL", f"http://127.0.0.1:{port}")
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")
    monkeypatch.setenv("DUCKSTRING_DEMO_PRODUCTS", "500")           # keep the fixture small
    monkeypatch.setenv("DUCKSTRING_DEMO_ORDERS", "500")

    server, thread, url = _serve(root, port)
    try:
        r = httpx.put(f"{url}/api/catchment/settings",
                      json={"data_root": f"s3://{_BUCKET}/plane", "mode": "empty"}, timeout=15.0)
        assert r.status_code == 200, r.text

        _deploy(url, ("orders", "catalog"))
        httpx.post(f"{url}/api/ponds/catalog/pulse", timeout=10.0)
        assert _wait(lambda: (_pond_status(url, "catalog") or {}).get("end_f") is not None, timeout=90.0), \
            "catalog never became fresh against an S3 data plane"

        # The Persist mirrors the local publish into the bucket and advances the watermark.
        assert _wait(lambda: (_pond_status(url, "catalog") or {}).get("persisted_f") is not None,
                     timeout=60.0), "the Persist never landed in S3"
        keys = _keys(client, "plane/catalog/")
        assert any(k.endswith("_trickle.json") for k in keys), f"no sidecar in the bucket: {keys[:5]}"
        assert any(k.endswith(".parquet") for k in keys), f"no data in the bucket: {keys[:5]}"

        # And it is readable back THROUGH the Catchment (the data plane's read path over S3).
        q = httpx.post(f"{url}/api/query", json={"pond": "catalog", "sql": "SELECT count(*) AS n FROM product"},
                       timeout=60.0)
        assert q.status_code == 200, q.text
        assert q.json()[0]["n"] > 0
    finally:
        server.should_exit = True
        thread.join(timeout=5)
