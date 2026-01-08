import json
from pathlib import Path

import pytest

from duckstring import (
    Catchment,
    ContractResolver,
    Pond,
    PondContract,
    Species,
    TableContract,
)
from duckstring import utils as ds_utils


class _FakeResolver(ContractResolver):
    def __init__(self, contracts):
        self._contracts = dict(contracts)

    def resolve_contract(self, pond_name: str, constraint: str) -> PondContract:
        if pond_name not in self._contracts:
            raise KeyError(pond_name)
        c = self._contracts[pond_name]
        if c.version != constraint:
            raise ValueError("constraint mismatch")
        return c


def _manifest_dict(
    *,
    name: str,
    version: str,
    sources: dict,
    exported_tables: dict,
    private_tables: dict | None = None,
    stages: list | None = None,
    description: str | None = None,
):
    private_tables = private_tables or {}
    stages = stages or []
    return {
        "name": name,
        "version": version,
        "description": description,
        "sources": sources,
        "stages": stages,
        "exported_tables": exported_tables,
        "private_tables": private_tables,
    }


def _write_manifest(repo_dir: Path, manifest: dict):
    (repo_dir / "duckstring.manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def test_pond_source_rejects_self():
    p = Pond(name="a", description=None, version="1.0.0")
    with pytest.raises(ValueError):
        p.source({"a": "1.0.0"})


def test_pond_flow_and_build_creates_implicit_final_stage():
    p = Pond(name="a", description="desc", version="1.0.0")

    p.sink_private({"staging": object()})
    p.flow([None], notes="stage1")

    p.sink({"final": object()})
    mf = p.build()

    assert mf.name == "a"
    assert mf.version == "1.0.0"

    # stage1 captured staging
    assert len(mf.stages) == 2
    assert mf.stages[0].outputs == ["staging"]
    assert mf.stages[0].notes == "stage1"

    # implicit stage captured final
    assert mf.stages[1].outputs == ["final"]
    assert mf.stages[1].notes == "implicit final stage"

    assert "final" in mf.exported_tables
    assert "staging" in mf.private_tables


def test_pond_sink_duplicate_table_raises():
    p = Pond(name="a", description=None, version="1.0.0")
    p.sink({"t": object()})
    with pytest.raises(ValueError):
        p.sink({"t": object()})


def test_upstream_get_requires_resolved_contract():
    p = Pond(name="derived", description=None, version="1.0.0")
    p.source({"base": "1.0.0"})
    with pytest.raises(RuntimeError):
        _ = p.upstream["base"].get("t", {"x": "a"})


@pytest.mark.skipif(not ds_utils._HAVE_IBIS, reason="ibis not installed")
def test_upstream_get_validates_columns_and_aliases():
    base_contract = PondContract(
        name="base",
        version="1.0.0",
        description=None,
        tables={
            "t": TableContract(
                name="t",
                schema={"a": "int64", "b": "string"},
            )
        },
    )
    resolver = _FakeResolver({"base": base_contract})

    p = Pond(name="derived", description=None, version="1.0.0")
    p.source({"base": "1.0.0"})
    p.attach_resolver(resolver)

    t = p.upstream["base"].get("t", {"x": "a"})
    sch = t.schema()
    assert "x" in sch.names
    assert "a" not in sch.names

    with pytest.raises(KeyError):
        _ = p.upstream["base"].get("t", {"x": "missing"})


def test_catchment_json_roundtrip(tmp_path: Path):
    spec = tmp_path / "catchment.json"
    c = Catchment(root_dir=str(tmp_path / "c_root"))

    c.ponds = {"p1": "/tmp/p1"}
    c.set_species({"local": Species(kind="local", engine="duckdb")})
    c.set_default_species("local")

    c.save(spec)
    c2 = Catchment.load(spec)

    assert c2.to_dict() == c.to_dict()


def test_catchment_mode_rejects_non_pulse():
    c = Catchment()
    c.set_species({"local": Species()})
    c.set_default_species("local")

    with pytest.raises(ValueError):
        c.set_modes({"weekly": {"type": "scheduled", "schedule": "* * * * *"}})


def test_basin_plan_toposort_from_manifests(tmp_path: Path):
    # Build two fake pond repos with manifests:
    # base <- derived
    base_repo = tmp_path / "base_repo"
    derived_repo = tmp_path / "derived_repo"
    base_repo.mkdir()
    derived_repo.mkdir()

    _write_manifest(
        base_repo,
        _manifest_dict(
            name="base",
            version="1.0.0",
            sources={},
            exported_tables={
                "t": {"name": "t", "schema": {"id": "int64", "val": "int64"}, "description": None}
            },
            stages=[{"index": 0, "parallelizable": True, "outputs": ["t"], "notes": None}],
        ),
    )

    _write_manifest(
        derived_repo,
        _manifest_dict(
            name="derived",
            version="1.0.0",
            sources={"base": "1.0.0"},
            exported_tables={
                "out": {"name": "out", "schema": {"total": "int64"}, "description": None}
            },
            stages=[{"index": 0, "parallelizable": True, "outputs": ["out"], "notes": None}],
        ),
    )

    c = Catchment(root_dir=str(tmp_path / "catchment_root"))
    c.ponds = {"base": str(base_repo), "derived": str(derived_repo)}
    c.set_species({"local": Species()})
    c.set_default_species("local")

    b = c.basin(outlets={"derived": "1.0.0"}, name="test")
    plan = b.plan()

    assert list(plan.ponds_topo) == ["base", "derived"]
    assert plan.outlets == {"derived": "1.0.0"}
    assert "base" in plan.manifests
    assert "derived" in plan.manifests


def test_basin_version_mismatch_raises(tmp_path: Path):
    repo = tmp_path / "p_repo"
    repo.mkdir()

    _write_manifest(
        repo,
        _manifest_dict(
            name="p",
            version="1.0.1",
            sources={},
            exported_tables={},
            stages=[],
        ),
    )

    c = Catchment(root_dir=str(tmp_path / "catchment_root"))
    c.ponds = {"p": str(repo)}
    c.set_species({"local": Species()})
    c.set_default_species("local")

    b = c.basin(outlets={"p": "1.0.0"})
    with pytest.raises(ValueError, match="does not match required"):
        b.resolve()


def test_basin_constraint_conflict_raises(tmp_path: Path):
    # base has v1.0.0; derived requires base v2.0.0 but outlet pins base v1.0.0
    base_repo = tmp_path / "base_repo"
    derived_repo = tmp_path / "derived_repo"
    base_repo.mkdir()
    derived_repo.mkdir()

    _write_manifest(
        base_repo,
        _manifest_dict(
            name="base",
            version="1.0.0",
            sources={},
            exported_tables={},
            stages=[],
        ),
    )

    _write_manifest(
        derived_repo,
        _manifest_dict(
            name="derived",
            version="1.0.0",
            sources={"base": "2.0.0"},
            exported_tables={},
            stages=[],
        ),
    )

    c = Catchment(root_dir=str(tmp_path / "catchment_root"))
    c.ponds = {"base": str(base_repo), "derived": str(derived_repo)}
    c.set_species({"local": Species()})
    c.set_default_species("local")

    b = c.basin(outlets={"base": "1.0.0", "derived": "1.0.0"})
    with pytest.raises(ValueError, match="Constraint conflict"):
        b.resolve()


@pytest.mark.skipif(not (ds_utils._HAVE_IBIS and ds_utils._HAVE_DUCKDB), reason="ibis/duckdb not installed")
def test_basin_pulse_materializes_duckdb_and_parquet(tmp_path: Path):
    # Create two pond repos with pond.py + manifests:
    # base exports t via memtable
    # derived consumes base.t and exports out = sum(val)
    base_repo = tmp_path / "base_repo"
    derived_repo = tmp_path / "derived_repo"
    base_repo.mkdir()
    derived_repo.mkdir()

    # pond.py files
    (base_repo / "pond.py").write_text(
        """
import ibis
from duckstring import Pond

def pond(resolver=None):
    p = Pond(name="base", description=None, version="1.0.0")
    if resolver is not None:
        p.attach_resolver(resolver)

    t = ibis.memtable(
        [{"id": 1, "val": 10}, {"id": 2, "val": 20}],
        schema=ibis.schema({"id": "int64", "val": "int64"}),
    )
    p.sink({"t": t})
    p.flow([None])
    return p
""".lstrip(),
        encoding="utf-8",
    )

    (derived_repo / "pond.py").write_text(
        """
from duckstring import Pond

def pond(resolver=None):
    p = Pond(name="derived", description=None, version="1.0.0")
    p.source({"base": "1.0.0"})
    if resolver is not None:
        p.attach_resolver(resolver)

    base = p.upstream["base"].get("t", {"id": "id", "val": "val"})
    out = base.aggregate(total=base.val.sum())
    p.sink({"out": out})
    p.flow([None])
    return p
""".lstrip(),
        encoding="utf-8",
    )

    # manifests
    _write_manifest(
        base_repo,
        _manifest_dict(
            name="base",
            version="1.0.0",
            sources={},
            exported_tables={
                "t": {"name": "t", "schema": {"id": "int64", "val": "int64"}, "description": None}
            },
            stages=[{"index": 0, "parallelizable": True, "outputs": ["t"], "notes": None}],
        ),
    )

    _write_manifest(
        derived_repo,
        _manifest_dict(
            name="derived",
            version="1.0.0",
            sources={"base": "1.0.0"},
            exported_tables={
                "out": {"name": "out", "schema": {"total": "int64"}, "description": None}
            },
            stages=[{"index": 0, "parallelizable": True, "outputs": ["out"], "notes": None}],
        ),
    )

    catchment_root = tmp_path / "catchment_root"
    c = Catchment(root_dir=str(catchment_root))
    c.ponds = {"base": str(base_repo), "derived": str(derived_repo)}
    c.set_species({"local": Species()})
    c.set_default_species("local")

    b = c.basin(outlets={"derived": "1.0.0"}, name="pulse_test")
    _ = b.pulse()

    # parquet outputs exist
    assert (catchment_root / "data" / "base" / "t.parquet").exists()
    assert (catchment_root / "data" / "derived" / "out.parquet").exists()

    # duckdb outputs exist and are queryable
    db_path = catchment_root / "state" / "duckstring.duckdb"
    assert db_path.exists()

    import duckdb

    con = duckdb.connect(str(db_path))
    try:
        total = con.execute('SELECT total FROM "derived"."out"').fetchone()[0]
        assert total == 30

        cnt = con.execute('SELECT COUNT(*) FROM "base"."t"').fetchone()[0]
        assert cnt == 2

        # Physical tables should exist
        physical_base = ds_utils.physical_table_name("base", "t")
        physical_derived = ds_utils.physical_table_name("derived", "out")
        assert con.execute(f"SELECT COUNT(*) FROM \"{physical_base}\"").fetchone()[0] == 2
        assert con.execute(f"SELECT COUNT(*) FROM \"{physical_derived}\"").fetchone()[0] == 1
    finally:
        con.close()
