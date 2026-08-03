"""Flock conformance: **DuckDB is the authority**.

Dispatching decides *where* a comprehensive recompute runs, never *what* gets published. So an engine's
result must be indistinguishable from DuckDB's — same columns, same order, same types, same values — and
anything we cannot prove equivalent must not be dispatched at all. Three layers enforce that, and this
file is the gate for each:

1. ``flock.semantic_reason`` — only expression forms proven equivalent are eligible (the whitelist).
2. ``flock.conform`` — the engine's result is cast to DuckDB's own schema for the terminal, or rejected.
3. the fallback contract — a rejected result degrades to local compute and is COUNTED as a failure, so a
   non-conformant engine is visible on /metrics rather than silently in your data.

The misbehaving engines below are the point: a fake that simply re-runs DuckDB proves nothing. Each one
reproduces a real cross-engine divergence (a widened decimal, a reordered projection, a dropped column)
and asserts we either normalise it or refuse it — never publish it.
"""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from duckstring import flock
from duckstring.core import Pond

pytestmark = pytest.mark.timeout(10)

UTC = timezone.utc
FLOCK_ENV = {
    "DUCKSTRING_FLOCK_ATHENA_WORKGROUP": "wg",
    "DUCKSTRING_FLOCK_ATHENA_DATABASE": "db",
    "DUCKSTRING_FLOCK_ATHENA_SCRATCH": "s3://bucket/scratch",
}


@pytest.fixture(autouse=True)
def _reset_stats():
    flock._stats.update({"dispatched": 0, "failed": 0, "last_error": None})
    yield


def _pond(tmp_path, flock_mode="always"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(tmp_path / "reg.duckdb"))
    return Pond(name="p", version="1", con=con, root=tmp_path,
                f=datetime(2026, 7, 19, tzinfo=UTC), flock=flock_mode)


def _seed(pond):
    pond.merge_table("src", pond.con.sql(
        "SELECT range AS id, (range % 7) AS grp, CAST(range AS DECIMAL(10,2)) AS amount FROM range(50)"
    ), pk="id")


class _Engine:
    """Runs the query on DuckDB (so values are right by construction) and then applies ``distort`` — the
    way a different engine would differ. ``None`` = a faithful engine."""

    last_error = None

    def __init__(self, distort=None):
        self.distort, self.dispatched = distort, False

    def enabled(self):
        return True

    def eligible(self, builder):
        return None

    def estimate_rows(self, builder):
        return 10_000_000

    def dispatch(self, builder, out_pk):
        self.dispatched = True
        rel = builder._full_join()
        return self.distort(rel) if self.distort else rel


def _run(tmp_path, monkeypatch, engine, projection="s0.id, s0.grp, s0.amount"):
    for k, v in FLOCK_ENV.items():
        monkeypatch.setenv(k, v)
    pond = _pond(tmp_path)
    _seed(pond)
    monkeypatch.setattr(flock, "get_engine", lambda env=None: engine)
    pond.trickle("src").select(projection).merge("out", pk="id")
    return pond


def _published(pond):
    """The published output as (schema, rows) — what a consumer would actually see."""
    schema = {c: str(t) for c, t in zip(*_describe(pond, "out__changelog"), strict=True)}
    rows = pond.con.execute(
        "SELECT id, grp, amount FROM out__changelog WHERE _duckstring_d = 1 ORDER BY id"
    ).fetchall()
    return schema, rows


def _describe(pond, table):
    rel = pond.con.sql(f'SELECT * EXCLUDE (_duckstring_d, _duckstring_f) FROM "{table}"')
    return rel.columns, rel.types


# ─── the oracle: a faithful engine must be indistinguishable from local compute ──


def test_a_faithful_engine_publishes_exactly_what_duckdb_would(tmp_path, monkeypatch):
    """The baseline the whole tier rests on: dispatching must be invisible in the output. Same schema,
    same column order, same rows as the identical Pond computed locally."""
    engine = _Engine()
    dispatched = _run(tmp_path / "dispatched", monkeypatch, engine)
    assert engine.dispatched

    monkeypatch.setattr(flock, "get_engine", lambda env=None: None)  # Flock off → the local path
    local = _run(tmp_path / "local", monkeypatch, None)

    assert _published(dispatched) == _published(local), "dispatch changed the published result"


# ─── the divergences that actually happen ───────────────────────────────────────


def test_a_widened_decimal_is_normalised_to_the_duckdb_type(tmp_path, monkeypatch):
    """The live incident's shape: an engine returns DECIMAL(36,2) where DuckDB says DECIMAL(35,2). The
    values are fine; the TYPE is not ours to change, because the published schema is the version
    contract. Conform casts it back rather than letting it reach the contract."""
    engine = _Engine(lambda rel: rel.project(
        "id, grp, CAST(amount AS DECIMAL(30,4)) AS amount"))
    pond = _run(tmp_path, monkeypatch, engine)
    schema, _rows = _published(pond)
    assert schema["amount"] == "DECIMAL(10,2)", f"engine type leaked into the published schema: {schema}"
    assert flock.stats()["dispatched"] == 1 and flock.stats()["failed"] == 0


def test_a_reordered_projection_is_restored_to_the_duckdb_order(tmp_path, monkeypatch):
    engine = _Engine(lambda rel: rel.project("amount, grp, id"))
    pond = _run(tmp_path, monkeypatch, engine)
    schema, rows = _published(pond)
    assert list(schema) == ["id", "grp", "amount"], f"column order came from the engine: {list(schema)}"
    assert rows[0] == (0, 0, pytest.approx(0)) or rows[0][0] == 0


def test_a_dropped_column_is_refused_not_published(tmp_path, monkeypatch):
    """A different column set means the engine computed a different query. Nothing to normalise — refuse
    it, run local, and count the failure so it surfaces."""
    engine = _Engine(lambda rel: rel.project("id, grp"))
    pond = _run(tmp_path, monkeypatch, engine)
    schema, _ = _published(pond)
    assert set(schema) == {"id", "grp", "amount"}, "the local result must have been used"
    assert flock.stats()["failed"] == 1
    assert "conform" in (flock.stats()["last_error"] or "").lower()


def test_an_extra_column_is_refused(tmp_path, monkeypatch):
    engine = _Engine(lambda rel: rel.project("id, grp, amount, 1 AS sneaky"))
    _run(tmp_path, monkeypatch, engine)
    assert flock.stats()["failed"] == 1


# ─── the whitelist: what is allowed to be dispatched at all ─────────────────────


class _Builder:
    def __init__(self, ops):
        self._ops = ops
        self._materialised = self._agg = self._acc = None


def _athena_reason(expr_kind, expr):
    """Athena's own verdict — equivalence is a property of the expression AND the engine that runs it,
    so the lists live with the engine rather than in a universal 'safe SQL' notion."""
    from duckstring.flock import equivalence
    from duckstring.flock.engines.athena import _ALLOWED_FUNCS, _ALLOWED_NODES

    return equivalence.unproven(_Builder([(expr_kind, expr)]), _ALLOWED_NODES, _ALLOWED_FUNCS)


@pytest.mark.parametrize("expr", [
    "s0.a, s1.b",                          # bare columns
    "s0.a AS x, s1.b AS y",                # renamed
    "s0.q * s1.p AS gross",                # decimal arithmetic: same result-type rule, exact
    "round(s0.q * s1.p, 2) AS r",          # both round half away from zero
    "coalesce(s0.a, 0) AS a",              # null semantics agree
])
def test_expressions_trino_evaluates_like_duckdb_are_dispatchable(expr):
    assert _athena_reason("select", expr) is None


@pytest.mark.parametrize("expr,why", [
    ("s0.a / s0.b AS r",
     "DuckDB `/` is TRUE division (7/2 = 3.5 DOUBLE); Trino's is integer division (3). conform would "
     "cast Trino's 3 to DOUBLE and publish 3.0 — right type, WRONG VALUE"),
    ("CAST(s0.a AS INTEGER) AS a",
     "at .5 DuckDB casts to even (CAST(2.5 AS INTEGER)=2), Trino rounds half up (3)"),
    ("upper(trim(s0.name)) AS n", "trimming and collation are unverified"),
])
def test_expressions_with_a_known_divergence_stay_local(expr, why):
    assert _athena_reason("select", expr) is not None, f"{expr} must not dispatch: {why}"


def test_filters_are_judged_too():
    assert _athena_reason("filter", "s0.a > 5 AND s1.b IS NOT NULL") is None
    assert _athena_reason("filter", "s0.a / 2 > 5") is not None


def test_unparseable_sql_is_never_assumed_safe():
    assert _athena_reason("select", "this is not sql (((") is not None
