"""Cloud config increment 3a (plans/cloud-config.md): Duck Pool CRUD — the Catchment-level named
remote-compute config a pond.toml `duck = "<pool>"` resolves against. The EC2 launcher that acts on
a pool is separate (increment 3b). Here: the driver CRUD, the routes, and the resolution wiring
(a defined pool is used; removing it falls back to the Catchment Duck)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes import router
from duckstring.catchment.routes.deploy import _register

pytestmark = pytest.mark.timeout(5)

_RIPPLES = [{"func": "f1", "name": "r1", "parents": []}]


def _cfg(**declared):
    cfg = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "inlet"}
    cfg.update(declared)
    return cfg


def _driver(tmp_path, cfg=None):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", cfg or _cfg(), _RIPPLES)
    return Driver(db, tmp_path, "http://x", NoopLauncher())


def _client(driver):
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.driver = driver
    app.state.db = driver.db
    return TestClient(app)


def test_add_list_remove_pool(tmp_path):
    d = _driver(tmp_path)
    d.add_pool("heavy", instance_type="m6i.large", min_instances=1, max_instances=4, keep_warm=1)
    pools = d.list_pools()
    assert len(pools) == 1
    p = pools[0]
    assert p["name"] == "heavy" and p["instance_type"] == "m6i.large"
    assert p["min_instances"] == 1 and p["max_instances"] == 4 and p["keep_warm"] == 1
    # Upsert updates in place.
    d.add_pool("heavy", instance_type="m6i.xlarge", max_instances=8)
    assert d.get_pool("heavy")["instance_type"] == "m6i.xlarge"
    assert d.get_pool("heavy")["max_instances"] == 8
    d.remove_pool("heavy")
    assert d.list_pools() == []


def test_pool_defaults_and_validation(tmp_path):
    d = _driver(tmp_path)
    p = d.add_pool("basic")
    assert p["min_instances"] == 0 and p["max_instances"] == 1 and p["keep_warm"] == 0
    with pytest.raises(ValueError):
        d.add_pool("bad", min_instances=5, max_instances=2)
    with pytest.raises(ValueError):
        d.add_pool("bad", max_instances=-1)
    with pytest.raises(ValueError):
        d.add_pool("")


def test_pool_names_cache_drives_resolution(tmp_path):
    # A Pond declaring `duck = "heavy"` resolves to that pool ONLY once the pool exists; removing it
    # falls back to the Catchment Duck (the same portable-pond.toml rule).
    d = _driver(tmp_path, cfg=_cfg(duck_pool="heavy"))
    assert d.duck_config("src@1")["duck_target"] == "catchment"  # not defined yet
    d.add_pool("heavy", instance_type="m6i.large")
    assert d.duck_config("src@1")["duck_target"] == "heavy"
    d.remove_pool("heavy")
    assert d.duck_config("src@1")["duck_target"] == "catchment"


def test_pool_survives_reload(tmp_path):
    d = _driver(tmp_path)
    d.add_pool("heavy", instance_type="m6i.large")
    d.reload()
    assert d.duck_pool_names == {"heavy"}
    assert d.get_pool("heavy")["instance_type"] == "m6i.large"


def test_pool_routes_roundtrip(tmp_path):
    d = _driver(tmp_path)
    client = _client(d)
    assert client.get("/api/catchment/duck-pools").json()["pools"] == []
    r = client.post("/api/catchment/duck-pools",
                    json={"name": "heavy", "instance_type": "m6i.large", "max_instances": 4})
    assert r.status_code == 200 and r.json()["name"] == "heavy"
    assert len(client.get("/api/catchment/duck-pools").json()["pools"]) == 1
    assert client.post("/api/catchment/duck-pools",
                       json={"name": "x", "min_instances": 9, "max_instances": 1}).status_code == 422
    assert client.delete("/api/catchment/duck-pools/heavy").status_code == 200
    assert client.get("/api/catchment/duck-pools").json()["pools"] == []
