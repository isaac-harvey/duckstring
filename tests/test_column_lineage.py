"""Static column lineage walked from the capture IR (plans/lineage.md Phase 2). Exact or absent: a
provable column maps to its source (ref, column) set; a constant maps to the empty set; a kind:sql
statement or an unenumerable star maps to None (opaque — we know we don't know)."""

from __future__ import annotations

import pytest

from duckstring import acc, agg
from duckstring.trickle.capture import capture_plan
from duckstring.trickle.lineage import column_lineage

SCHEMAS = {
    "orders.order_line": ["id", "pid", "qty"],
    "prices.price": ["id", "unit"],
    "src.event": ["id", "tag", "amt"],
    "src.txn": ["id", "acct", "amt"],
}


def _src(ref):
    pond = ref.split(".", 1)[0]
    entry = {"location": f"__SRC__/{pond}", "mode": "merge", "pk": ["id"], "floor": "F0", "f": "F1"}
    if ref in SCHEMAS:
        entry["schema"] = {c: "VARCHAR" for c in SCHEMAS[ref]}  # the Extension-2 sidecar shape
    return entry


def _lineage(run_python, schemas=None):
    body = capture_plan(run_python, source_catalog=_src, own_location="__OWN__")
    return column_lineage(body, schemas=schemas)


def test_join_pipeline_columns():
    def run(host):
        (host.trickle("orders.order_line", p=1.0).alias("ol")
         .join(host.trickle("prices.price", p=1.0).alias("pr"), on={"pid": "id"})
         .filter("ol.qty > 1")
         .mutate(revenue="ol.qty * pr.unit")
         .select("ol.id AS id, ol.pid, revenue")
         .merge("priced", pk="id"))
        return []

    lin = _lineage(run)["priced"]
    assert lin["id"] == {("orders.order_line", "id")}
    # pid is an equi-join key — it carries values from BOTH sides
    assert lin["pid"] == {("orders.order_line", "pid"), ("prices.price", "id")}
    # the mutated column unions its expression's refs (qty × the joined key-unified unit)
    assert lin["revenue"] == {("orders.order_line", "qty"), ("prices.price", "unit")}


def test_star_output_dedups_join_key_and_carries_mutate():
    def run(host):
        (host.trickle("orders.order_line", p=1.0).alias("ol")
         .join(host.trickle("prices.price", p=1.0).alias("pr"), on={"pid": "id"})
         .mutate(rev="ol.qty * pr.unit")
         .merge("wide", pk="id"))
        return []

    lin = _lineage(run)["wide"]
    assert set(lin) == {"id", "pid", "qty", "unit", "rev"}
    assert lin["qty"] == {("orders.order_line", "qty")}
    assert lin["rev"] == {("orders.order_line", "qty"), ("prices.price", "unit")}


def test_aggregate_and_accumulate_metrics():
    def run(host):
        (host.trickle("orders.order_line", p=1.0).alias("l")
         .aggregate(by="pid", total=agg.sum("qty"), n=agg.count())
         .merge("by_product"))
        (host.trickle("src.txn", p=1.0).alias("t").along("id")
         .accumulate(by="acct", running=acc.sum("amt"))
         .append("scored", pk="id"))
        return []

    lin = _lineage(run)
    byp = lin["by_product"]
    assert byp["pid"] == {("orders.order_line", "pid")}
    assert byp["total"] == {("orders.order_line", "qty")}
    assert byp["n"] == set()  # count derives from row existence, not a column
    sc = lin["scored"]
    assert sc["amt"] == {("src.txn", "amt")}
    assert sc["running"] == {("src.txn", "amt"), ("src.txn", "id")}  # the metric col + the order axis


def test_chained_statements_resolve_transitively():
    def run(host):
        ab = host.trickle("orders.order_line", p=1.0).filter("s0.qty > 1").merge("ab", pk="id")
        (ab.join(host.trickle("prices.price", p=1.0).alias("pr"), on={"pid": "id"})
         .select("s0.id AS id, pr.unit").merge("abc", pk="id"))
        return []

    lin = _lineage(run)
    assert lin["ab"]["qty"] == {("orders.order_line", "qty")}
    # abc reads "ab" (an own output) — its columns bottom out at the EXTERNAL source
    assert lin["abc"]["id"] == {("orders.order_line", "id")}
    assert lin["abc"]["unit"] == {("prices.price", "unit")}


def test_constants_and_sql_and_unknown_schema():
    def run(host):
        host.trickle("orders.order_line", p=1.0).alias("o") \
            .select("o.id AS id, 1 AS one").merge("m", pk="id")
        host.trickle("orders.order_line", p=1.0).alias("t") \
            .sql("SELECT id, count(*) AS n FROM t GROUP BY id").merge("agg_sql", pk="id")
        return []

    lin = _lineage(run)
    assert lin["m"]["one"] == set()      # a constant: derives from exactly nothing
    assert lin["agg_sql"] is None        # kind: sql — opaque until Phase 3

    def run2(host):  # a source with NO schema and no select → the star is unenumerable
        host.trickle("mystery.table", p=1.0).merge("m2", pk="id")
        return []

    body = capture_plan(run2, source_catalog=lambda r: {"location": "x", "mode": "merge",
                                                        "pk": ["id"], "floor": None, "f": None},
                        own_location="__OWN__")
    assert column_lineage(body)["m2"] is None


def test_explicit_select_survives_unknown_schema():
    """Explicit alias.col references stay provable even when the source schema is unknown —
    exactness doesn't depend on metadata, only enumeration does."""
    def run(host):
        host.trickle("mystery.table", p=1.0).alias("m") \
            .select("m.a AS a, m.b").merge("out", pk="a")
        return []

    body = capture_plan(run, source_catalog=lambda r: {"location": "x", "mode": "merge",
                                                       "pk": ["a"], "floor": None, "f": None},
                        own_location="__OWN__")
    lin = column_lineage(body)["out"]
    assert lin["a"] == {("mystery.table", "a")}
    assert lin["b"] == {("mystery.table", "b")}


def test_semi_join_output_is_left_only():
    def run(host):
        (host.trickle("orders.order_line", p=1.0).alias("a")
         .join(host.trickle("prices.price", p=1.0).alias("b"), on={"pid": "id"}, how="semi")
         .select("a.id AS id, a.qty").merge("t_semi", pk="id"))
        return []

    lin = _lineage(run)["t_semi"]
    # a semi join filters rows; its output values come only from the left side
    assert lin["id"] == {("orders.order_line", "id")}
    assert lin["qty"] == {("orders.order_line", "qty")}


def test_filter_after_mutate_adds_no_edges():
    def run(host):
        (host.trickle("orders.order_line", p=1.0).alias("o")
         .mutate(v="o.qty * 2").filter("v > 3")
         .select("o.id AS id, v").merge("f", pk="id"))
        return []

    lin = _lineage(run)["f"]
    assert lin["v"] == {("orders.order_line", "qty")}
    assert lin["id"] == {("orders.order_line", "id")}


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_outer_join_key_unifies(how):
    def run(host):
        (host.trickle("orders.order_line", p=1.0).alias("a")
         .join(host.trickle("prices.price", p=1.0).alias("b"), on={"pid": "id"}, how=how)
         .select("a.pid AS k").merge("o", pk="k"))
        return []

    assert _lineage(run)["o"]["k"] == {("orders.order_line", "pid"), ("prices.price", "id")}


# ── deploy-time capture + storage + the API surface ─────────────────────────────


def test_deploy_capture_stores_and_serves_column_lineage(tmp_path):
    from duckstring.catchment.db import connect, migrate
    from duckstring.catchment.driver import Driver
    from duckstring.catchment.launcher import NoopLauncher
    from duckstring.catchment.routes.deploy import _capture_column_lineage, _register

    def priced(pond):  # a capturable Trickle ripple
        (pond.trickle("orders.order_line", p=1.0).alias("ol")
         .join(pond.trickle("prices.price", p=1.0).alias("pr"), on={"pid": "id"})
         .mutate(revenue="ol.qty * pr.unit")
         .select("ol.id AS id, revenue")
         .merge("priced", pk="id"))

    def raw(pond):  # classic read/write code — NOT capturable: absent, never guessed
        pond.write_table("t", pond.read_table("orders.order_line"))

    db = connect(tmp_path / "duck.db")
    migrate(db)
    cfg = {"sources": {"orders": "1", "prices": "1"}, "immediate_retries": 0, "source_retries": 0, "kind": "pond"}
    ripples = [{"func": priced, "name": "priced", "parents": []},
               {"func": raw, "name": "raw", "parents": []}]
    _register(db, "sales", "1.0.0", "pond", "ponds/sales/1.0.0", cfg, ripples)
    _capture_column_lineage(db, "sales", "1.0.0", ripples)

    d = Driver(db, tmp_path, "http://x", NoopLauncher())
    cols = d.lineage(pond="sales", columns=True)["ponds"][0]["columns"]
    assert "priced" in cols
    assert cols["priced"]["id"] == [{"ref": "orders.order_line", "column": "id"}]
    assert sorted((s["ref"], s["column"]) for s in cols["priced"]["revenue"]) == [
        ("orders.order_line", "qty"), ("prices.price", "unit")]
    assert "t" not in cols  # the raw ripple contributed nothing — absent, not wrong

    # recomputed wholesale on redeploy (no stale rows)
    _capture_column_lineage(db, "sales", "1.0.0", ripples[:1])
    n = db.execute("SELECT count(DISTINCT \"table\") FROM pond_version_column_lineage").fetchone()[0]
    assert n == 1


def test_deploy_capture_never_fails_the_deploy(tmp_path):
    from duckstring.catchment.db import connect, migrate
    from duckstring.catchment.routes.deploy import _capture_column_lineage, _register

    def explodes(pond):
        raise RuntimeError("ripple import-time chaos")

    db = connect(tmp_path / "duck.db")
    migrate(db)
    cfg = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "inlet"}
    ripples = [{"func": explodes, "name": "boom", "parents": []}]
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", cfg, ripples)
    _capture_column_lineage(db, "src", "1.0.0", ripples)  # must not raise
    assert db.execute("SELECT count(*) FROM pond_version_column_lineage").fetchone()[0] == 0
