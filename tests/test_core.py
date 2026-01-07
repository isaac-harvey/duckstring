import pytest

ibis = pytest.importorskip("ibis")

from duckstring import Pond, PondContract, TableContract  # adjust if your package path differs


class FakeBasin:
    def __init__(self, contracts):
        self.contracts = contracts

    def resolve_contract(self, pond_name: str, constraint: str) -> PondContract:
        # Constraint resolution is Basin responsibility; tests can ignore constraint semantics.
        return self.contracts[pond_name]


def _t(schema: dict, name: str = "t"):
    return ibis.table(ibis.schema(schema), name=name)


def test_source_cannot_reference_self():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    with pytest.raises(ValueError):
        p.source({"order_daily": "1.0.0"})


def test_upstream_registry_unknown_pond_raises():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    p.source({"order": "1.0.0"})
    with pytest.raises(KeyError):
        _ = p.upstream["customer"]


def test_upstream_get_requires_resolved_contract():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    p.source({"order": "1.0.0"})

    with pytest.raises(RuntimeError):
        p.upstream["order"].get("orders", {"order_id": "order_id"})


def test_attach_basin_resolves_and_upstream_get_selects_and_aliases():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    p.source({"order": "1.0.0"})

    contracts = {
        "order": PondContract(
            name="order",
            version="1.0.0",
            tables={
                "orders": TableContract(
                    name="orders",
                    schema={
                        "order_id": "int64",
                        "order_date": "date",
                        "sales_amount": "float64",
                    },
                )
            },
        )
    }
    p.attach_basin(FakeBasin(contracts))

    expr = p.upstream["order"].get(
        "orders",
        {
            "id": "order_id",
            "amount": "sales_amount",
        },
    )

    sch = expr.schema()
    assert list(sch.names) == ["id", "amount"]
    assert str(sch["id"]) == "int64"
    assert str(sch["amount"]) == "float64"


def test_upstream_get_missing_columns_raises_keyerror():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    p.source({"order": "1.0.0"})

    contracts = {
        "order": PondContract(
            name="order",
            version="1.0.0",
            tables={
                "orders": TableContract(
                    name="orders",
                    schema={
                        "order_id": "int64",
                        "order_date": "date",
                    },
                )
            },
        )
    }
    p.attach_basin(FakeBasin(contracts))

    with pytest.raises(KeyError):
        p.upstream["order"].get(
            "orders",
            {
                "id": "order_id",
                "missing": "sales_amount",  # not in contract
            },
        )


def test_sink_and_flow_capture_stage_outputs():
    p = Pond(name="order_daily", description="x", version="1.2.3")

    p.sink_private({"staging": _t({"a": "int64", "b": "string"}, "staging")})
    p.flow([None])  # stage boundary

    p.sink({"final": _t({"a": "int64"}, "final")})
    p.flow([None])  # stage boundary

    manifest = p.build()

    assert [s.outputs for s in manifest.stages] == [["staging"], ["final"]]
    assert "final" in manifest.exported_tables
    assert "staging" in manifest.private_tables


def test_build_creates_implicit_final_stage_when_pending_outputs():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    p.sink({"final": _t({"a": "int64"}, "final")})

    manifest = p.build()
    assert len(manifest.stages) == 1
    assert manifest.stages[0].outputs == ["final"]
    assert manifest.stages[0].notes == "implicit final stage"


def test_duplicate_table_name_raises():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    p.sink_private({"t": _t({"a": "int64"}, "t")})
    with pytest.raises(ValueError):
        p.sink({"t": _t({"a": "int64"}, "t2")})


def test_local_get_missing_columns_raises():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    p.sink_private({"staging": _t({"a": "int64", "b": "string"}, "staging")})

    with pytest.raises(KeyError):
        p.get("staging", {"a_out": "a", "c_out": "c"})  # "c" doesn't exist


def test_local_get_selects_and_aliases():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    p.sink_private({"staging": _t({"a": "int64", "b": "string"}, "staging")})

    expr = p.get("staging", {"a_out": "a"})
    sch = expr.schema()
    assert list(sch.names) == ["a_out"]
    assert str(sch["a_out"]) == "int64"


def test_local_get_unknown_table_raises():
    p = Pond(name="order_daily", description="x", version="1.2.3")
    with pytest.raises(KeyError):
        p.get("does_not_exist", {"x": "x"})
