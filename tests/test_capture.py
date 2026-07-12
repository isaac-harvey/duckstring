"""Plan capture → DuckFlock plan IR (``duckflock_plan: 1``). Capture runs a ripple's builder chain
against a recording handle and serialises it to the logical plan IR (the duckstring↔duckflock contract,
duckflock ``plans/plan-ir.md``). Comparisons are **order-sensitive** (JSON string equality) — duckflock
deserialises with ``preserve_order``, so mutate-column / metric / dict-key order is semantic."""

from __future__ import annotations

import json

import pytest

from duckstring import acc, agg
from duckstring.trickle.capture import NonCapturable, capture_plan, envelope


def _src(ref):
    """A source catalog resolver for tests: mode/pk/floor/f keyed off the ref (tokens like conformance)."""
    facts = {
        "orders.order_line": ("merge", ["id"], "F0", "F1"),
        "prices.price": ("merge", ["id"], "F0", "F0"),
        "src.event": ("append", ["id"], "F0", "F1"),
        "src.line": ("merge", ["id"], "F0", "F2"),
        "src.txn": ("append", ["id"], "F0", "F1"),
        "cat.product": ("overwrite", None, None, "F1"),
    }
    pond = ref.split(".", 1)[0]
    mode, pk, floor, f = facts[ref]
    return {"location": f"__SRC__/{pond}", "mode": mode, "pk": pk, "floor": floor, "f": f}


def _cap(run_python):
    return capture_plan(run_python, source_catalog=_src, own_location="__OWN__")


def _eq(got, want):
    """Order-sensitive equality (JSON string) — the duckflock acceptance contract."""
    assert json.dumps(got) == json.dumps(want), f"\n got={json.dumps(got, indent=2)}\nwant={json.dumps(want, indent=2)}"


def test_capture_join_pipeline():
    def run(host):
        return [(
            "priced",
            host.trickle("orders.order_line", p=1.0).alias("ol")
            .join(host.trickle("prices.price", p=1.0).alias("pr"), on={"pid": "id"})
            .filter("ol.qty > 1")
            .mutate(revenue="ol.qty * pr.unit")
            .select("ol.id AS id, ol.pid, revenue")
            .merge("priced", pk="id").was_changed(),
        )]

    body = _cap(run)
    _eq(body["catalog"], {
        "orders.order_line": {"location": "__SRC__/orders", "mode": "merge", "pk": ["id"], "floor": "F0", "f": "F1"},
        "prices.price": {"location": "__SRC__/prices", "mode": "merge", "pk": ["id"], "floor": "F0", "f": "F0"},
        "priced": {"location": "__OWN__", "mode": "merge", "pk": ["id"]},
    })
    _eq(body["statements"], [{
        "kind": "trickle",
        "output": {"name": "priced", "terminal": "merge", "pk": ["id"], "options": {}},
        "dag": {
            "node": "join", "how": "inner",
            "on": [{"left": "pid", "right": "id"}],
            "left": {"node": "source", "ref": "orders.order_line", "alias": "ol", "p": 1.0},
            "right": {"node": "source", "ref": "prices.price", "alias": "pr", "p": 1.0},
        },
        "pipeline": [
            {"op": "filter", "predicate": "ol.qty > 1"},
            {"op": "mutate", "columns": {"revenue": "ol.qty * pr.unit"}},
            {"op": "select", "projection": "ol.id AS id, ol.pid, revenue"},
        ],
    }])


def test_capture_append_scan_defaults_empty_options():
    def run(host):
        host.trickle("src.event", p=1.0).alias("e").filter("e.amt > 2") \
            .select("e.id AS id, e.tag, e.amt").append("big_events", pk="id")
        return []

    body = _cap(run)
    _eq(body["statements"], [{
        "kind": "trickle",
        "output": {"name": "big_events", "terminal": "append", "pk": ["id"], "options": {}},
        "dag": {"node": "source", "ref": "src.event", "alias": "e", "p": 1.0},
        "pipeline": [
            {"op": "filter", "predicate": "e.amt > 2"},
            {"op": "select", "projection": "e.id AS id, e.tag, e.amt"},
        ],
    }])


def test_capture_aggregate_metric_order_and_none_pk():
    def run(host):
        host.trickle("src.line", p=1.0).alias("l").aggregate(
            by="pid",
            total=agg.sum("qty"), n=agg.count(), avg=agg.mean("qty"),
            lo=agg.min("qty"), hi=agg.max("qty"), sd=agg.stddev("qty"),
        ).merge("by_product")
        return []

    body = _cap(run)
    # aggregate merge with no pk → statement output.pk is None; the own catalog pk is the effective `by`.
    _eq(body["catalog"]["by_product"], {"location": "__OWN__", "mode": "merge", "pk": ["pid"]})
    _eq(body["statements"], [{
        "kind": "trickle",
        "output": {"name": "by_product", "terminal": "merge", "pk": None, "options": {}},
        "dag": {"node": "source", "ref": "src.line", "alias": "l", "p": 1.0},
        "pipeline": [],
        "aggregate": {"by": ["pid"], "metrics": {
            "total": {"kind": "sum", "col": "qty"},
            "n": {"kind": "count"},
            "avg": {"kind": "mean", "col": "qty"},
            "lo": {"kind": "min", "col": "qty"},
            "hi": {"kind": "max", "col": "qty"},
            "sd": {"kind": "stddev", "col": "qty", "how": "sample"},
        }},
    }])


def test_capture_agg_families_key_order():
    def run(host):
        host.trickle("src.line", p=1.0).alias("l").aggregate(
            by="g",
            wsum=agg.weighted_sum("x", "w"),
            cov=agg.covariance("x", "y"),
            corr=agg.pearson_correlation("x", "y"),
            y_at_min_x=agg.argmin("y", "x"),
            band=agg.bit_and("bits"),
            vpop=agg.var("x", "pop"),
        ).merge("gstats")
        return []

    metrics = _cap(run)["statements"][0]["aggregate"]["metrics"]
    _eq(metrics, {
        "wsum": {"kind": "weighted_sum", "col": "x", "col2": "w"},
        "cov": {"kind": "covariance", "col": "x", "col2": "y", "how": "sample"},   # col2 before how
        "corr": {"kind": "pearson_correlation", "col": "x", "col2": "y"},
        "y_at_min_x": {"kind": "argmin", "col": "y", "col2": "x"},
        "band": {"kind": "bit_and", "col": "bits"},
        "vpop": {"kind": "var", "col": "x", "how": "pop"},
    })


def test_capture_accumulate_windowed():
    def run(host):
        host.trickle("src.txn", p=1.0).alias("t").along("id").accumulate(
            by="acct", running=acc.sum("amt"), seen=acc.count(), worst=acc.min("amt"),
        ).append("scored", pk="id")
        return []

    body = _cap(run)
    _eq(body["statements"], [{
        "kind": "trickle",
        "output": {"name": "scored", "terminal": "append", "pk": ["id"], "options": {}},
        "dag": {"node": "source", "ref": "src.txn", "alias": "t", "p": 1.0},
        "pipeline": [],
        "along": "id",
        "accumulate": {"by": ["acct"], "metrics": {
            "running": {"kind": "sum", "col": "amt"},
            "seen": {"kind": "count"},
            "worst": {"kind": "min", "col": "amt"},
        }},
    }])


def test_capture_acc_extras_native_folds():
    def run(host):
        host.trickle("src.txn", p=1.0).alias("e").along("t").accumulate(
            by="acct",
            decayed=acc.tema("amt", lam=0.05),
            fir=acc.convolution("amt", [0.5, 0.25]),
            two_back=acc.lag("amt", 2),
            prv=acc.prev("amt"),
            opener=acc.first("amt"),
            emm=acc.ema("amt", 0.5),
        ).append("enriched", pk="id")
        return []

    metrics = _cap(run)["statements"][0]["accumulate"]["metrics"]
    _eq(metrics, {
        "decayed": {"kind": "tema", "col": "amt", "lam": 0.05},
        "fir": {"kind": "convolution", "col": "amt", "kernel": [0.5, 0.25]},
        "two_back": {"kind": "lag", "col": "amt", "n": 2},
        "prv": {"kind": "prev", "col": "amt"},     # acc.prev canonicalises to kind "prev", no n
        "opener": {"kind": "first", "col": "amt"},
        "emm": {"kind": "ema", "col": "amt", "alpha": 0.5},
    })


def test_capture_chained_merge_owns_and_positional_alias():
    def run(host):
        ab = host.trickle("orders.order_line", p=1.0).filter("s0.qty > 1").merge("ab", pk="id")
        ab.join(host.trickle("prices.price", p=1.0).alias("pr"), on={"pid": "id"}) \
          .select("s0.id AS id, s0.pid, s0.qty, pr.unit").merge("abc", pk="id")
        return []

    body = _cap(run)
    # catalog: sources first (order of first appearance), then own outputs in statement order.
    assert list(body["catalog"]) == ["orders.order_line", "prices.price", "ab", "abc"]
    _eq(body["statements"], [
        {
            "kind": "trickle",
            "output": {"name": "ab", "terminal": "merge", "pk": ["id"], "options": {}},
            "dag": {"node": "source", "ref": "orders.order_line", "alias": "s0", "p": 1.0},
            "pipeline": [{"op": "filter", "predicate": "s0.qty > 1"}],
        },
        {
            "kind": "trickle",
            "output": {"name": "abc", "terminal": "merge", "pk": ["id"], "options": {}},
            "dag": {
                "node": "join", "how": "inner",
                "on": [{"left": "pid", "right": "id"}],
                "left": {"node": "source", "ref": "ab", "alias": "s0", "p": 1.0},   # own operand, p disabled
                "right": {"node": "source", "ref": "prices.price", "alias": "pr", "p": 1.0},
            },
            "pipeline": [{"op": "select", "projection": "s0.id AS id, s0.pid, s0.qty, pr.unit"}],
        },
    ])


def test_capture_sql_statement_left_leaf_alias():
    def run(host):
        host.trickle("orders.order_line", p=1.0).alias("ol") \
            .join(host.trickle("prices.price", p=1.0).alias("pr"), on="pid") \
            .sql("SELECT pid, COUNT(*) AS n FROM ol GROUP BY pid").merge("pid_rev", pk="pid")
        return []

    body = _cap(run)
    _eq(body["statements"], [{
        "kind": "sql",
        "alias": "ol",   # after .join(), .sql() exposes the composed relation under the left leaf's alias
        "input_dag": {
            "node": "join", "how": "inner",
            "on": [{"left": "pid", "right": "pid"}],
            "left": {"node": "source", "ref": "orders.order_line", "alias": "ol", "p": 1.0},
            "right": {"node": "source", "ref": "prices.price", "alias": "pr", "p": 1.0},
        },
        "pipeline": [],
        "query": "SELECT pid, COUNT(*) AS n FROM ol GROUP BY pid",
        "output": {"name": "pid_rev", "terminal": "merge", "pk": ["pid"], "options": {}},
    }])


def test_capture_sql_with_pipeline_before():
    def run(host):
        host.trickle("src.line", p=1.0).alias("t").filter("t.v > 2") \
            .sql("SELECT id, g FROM t").merge("ranked", pk="id")
        return []

    stmt = _cap(run)["statements"][0]
    assert stmt["kind"] == "sql"
    _eq(stmt["pipeline"], [{"op": "filter", "predicate": "t.v > 2"}])
    assert stmt["alias"] == "t"


def test_capture_append_options_waived_conflicts():
    def run(host):
        host.trickle("orders.order_line", p=1.0).alias("ol") \
            .join(host.trickle("prices.price", p=1.0).alias("pr"), on={"pid": "id"}) \
            .select("ol.id AS id, pr.unit") \
            .append("snap", pk="id", fail_on_conflict=False, log_drops=False)
        host.trickle("src.event", p=1.0).alias("e").append("log", pk="id", fail_on_conflict=False, log_drops=True)
        return []

    stmts = _cap(run)["statements"]
    _eq(stmts[0]["output"]["options"], {"fail_on_conflict": False, "log_drops": False})
    _eq(stmts[1]["output"]["options"], {"fail_on_conflict": False, "log_drops": True})


def test_capture_merge_option_escapes():
    def run(host):
        b = host.trickle("src.line", p=1.0)
        b.merge("keep_n", pk="id", retain_n=2)
        host.trickle("src.line", p=1.0).merge("no_ivm", pk="id", ivm=False)
        host.trickle("src.line", p=1.0).merge("no_kf", pk="id", key_filter=False)
        return []

    stmts = _cap(run)["statements"]
    _eq(stmts[0]["output"]["options"], {"retain_n": 2})
    _eq(stmts[1]["output"]["options"], {"ivm": False})
    _eq(stmts[2]["output"]["options"], {"key_filter": False})


def test_capture_join_types_how_passthrough():
    for how in ("left", "right", "full", "semi", "anti"):
        def run(host, how=how):
            host.trickle("src.line", p=1.0).alias("a") \
                .join(host.trickle("prices.price", p=1.0).alias("b"), on={"id": "id"}, how=how) \
                .select("a.id AS id").merge("o", pk="id")
            return []

        assert _cap(run)["statements"][0]["dag"]["how"] == how


def test_capture_envelope_wraps_body():
    def run(host):
        host.trickle("src.line", p=1.0).merge("m", pk="id")
        return []

    body = _cap(run)
    plan = envelope(
        body, job_id="j-1", tenant="acme",
        pond={"name": "m", "major": 1, "version": "0.0.1"},
        epoch={"f": "2026-07-03T10:15:00+00:00", "previous_f": "0001-01-01T00:00:00+00:00"},
        config={"refresh": False, "keep_warm_ttl_s": 0, "limits": {}},
    )
    assert plan["duckflock_plan"] == 1
    assert list(plan) == ["duckflock_plan", "job_id", "tenant", "pond", "epoch", "catalog", "statements", "config"]
    assert plan["statements"] == body["statements"]


def test_capture_reduce_metric_is_non_capturable():
    def run(host):
        host.trickle("src.line", p=1.0).along("id") \
            .aggregate(by="g", r=agg.reduce(lambda s, row: (s, s), 0)).merge("o")
        return []

    with pytest.raises(NonCapturable, match="reduce"):
        _cap(run)


def test_capture_scan_metric_is_non_capturable():
    def run(host):
        host.trickle("src.txn", p=1.0).along("t") \
            .accumulate(by="g", s=acc.scan(lambda st, row: (st, st), 0)).append("o", pk="id")
        return []

    with pytest.raises(NonCapturable, match="scan"):
        _cap(run)


def test_capture_raw_read_is_non_capturable():
    def run(host):
        host.read_table("src.line")
        return []

    with pytest.raises(NonCapturable, match="read_table"):
        _cap(run)
