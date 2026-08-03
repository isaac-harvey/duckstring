"""Phase 2 version contract — schema side. The additive compatibility check, the Catchment capturing a
Pond version's published schema, the forward-only contract it ships in begin_run (and skips on a
rollback), and a contract violation failing the Source + blocking its Sink."""

from __future__ import annotations

import pytest

from duckstring.catchment.db import connect, migrate
from duckstring.catchment.driver import Driver
from duckstring.catchment.launcher import NoopLauncher
from duckstring.catchment.routes.deploy import _register
from duckstring.schema_contract import contract_violations

pytestmark = pytest.mark.timeout(5)


# ── the additive check (pure) ──────────────────────────────────────────────────


def test_no_contract_never_fails():
    assert contract_violations({"t": {"a": "INTEGER"}}, None) == []
    assert contract_violations({}, {}) == []


def test_additive_changes_are_compatible():
    contract = {"event": {"id": "INTEGER"}}
    output = {"event": {"id": "INTEGER", "extra": "VARCHAR"}, "new_table": {"x": "DOUBLE"}}
    assert contract_violations(output, contract) == []


def test_dropped_column_removed_table_and_narrowing_are_violations():
    contract = {"event": {"id": "BIGINT", "val": "VARCHAR"}, "side": {"k": "INTEGER"}}
    output = {"event": {"id": "INTEGER"}}  # val dropped, id NARROWED, side table gone
    msgs = " | ".join(contract_violations(output, contract))
    assert "event.val" in msgs and "dropped" in msgs
    assert "event.id" in msgs and "BIGINT → INTEGER" in msgs
    assert "side" in msgs and "no longer produced" in msgs


def test_a_lossless_widening_is_additive_not_breaking():
    """The production wedge: a Ripple whose SQL takes a data-dependent branch emitted DECIMAL(24,2) on
    one run and DECIMAL(25,2) on the next — identical semantics, every value still representable. Judged
    by string equality that was a "type change", which fails the Pond and, because the contract is
    forward-only, wedges it permanently with no way back short of DB surgery."""
    contract = {"t": {"revenue": "DECIMAL(24,2)", "n": "INTEGER", "ts": "TIMESTAMP_S"}}
    output = {"t": {"revenue": "DECIMAL(25,2)", "n": "BIGINT", "ts": "TIMESTAMP"}}
    assert contract_violations(output, contract) == []


def test_reset_contract_unwedges_a_narrowed_line(tmp_path):
    """A narrowing IS breaking, so the gate keeps failing it — and a failed run publishes nothing, so the
    contract can never re-capture. Without an escape hatch the line is wedged forever (the live incident
    needed hand-edited SQLite). reset-contract drops the recorded schema and clears the failure."""
    from duckstring.keys import pond_key

    db = connect(tmp_path / "duck.db")
    migrate(db)
    cfg = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "outlet"}
    _register(db, "sales", "1.0.0", "outlet", "ponds/sales/1.0.0", cfg,
              [{"func": "f", "name": "agg", "parents": []}])
    d = Driver(db, tmp_path, "http://x", NoopLauncher())
    key = pond_key("sales", 1)

    d._capture_schema(key, {"out": {"amount": "DECIMAL(25,2)"}})
    assert d._contract_for(key) == {"out": {"amount": "DECIMAL(25,2)"}}
    # A narrowing still fails the gate — the rule did not go soft.
    assert contract_violations({"out": {"amount": "DECIMAL(24,2)"}}, d._contract_for(key))

    d.pulse(key)
    f = d.state.pond_states[key].start_f.isoformat()
    d.on_event(key, {"kind": "contract_failed", "f": f, "status": "failed", "error": "narrowed"})
    assert d.state.pond_states[key].is_failed

    res = d.reset_contract(key)
    assert res["columns_cleared"] == 1
    assert d._contract_for(key) is None       # the next accepted run re-freezes it
    assert not d.state.pond_states[key].is_failed  # and the line can run again


def test_widening_rules_are_conservative():
    from duckstring.schema_contract import is_widening

    assert is_widening("DECIMAL(10,2)", "DECIMAL(11,3)")      # both parts grow
    assert not is_widening("DECIMAL(10,2)", "DECIMAL(10,3)")  # scale grew at the integer part's expense
    assert not is_widening("DECIMAL(25,2)", "DECIMAL(24,2)")  # shrinking is still breaking
    assert is_widening("UINTEGER", "BIGINT")                  # unsigned into a wider signed
    assert not is_widening("BIGINT", "DOUBLE")                # a large BIGINT loses precision as a double
    assert not is_widening("INTEGER", "DECIMAL(20,0)")        # cross-family: not asserted
    assert not is_widening("VARCHAR", "INTEGER")


# ── extract_schema over a live registry (Trickle-aware) ─────────────────────────


def test_extract_schema_covers_uncompacted_merge_main():
    """Regression: a merge Trickle's physical base table (same name) is only materialised at the first
    checkpoint, so before then the registry holds only ``{name}__changelog``. The main's user schema
    must still be captured (from the changelog) — else the contract, catalog, and column lineage all
    miss it until it compacts. Append history + companions behave as expected."""
    from datetime import datetime, timezone

    import duckdb

    from duckstring.schema_contract import extract_schema
    from duckstring.trickle import io as tio

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    f = datetime(2020, 1, 1, tzinfo=timezone.utc)
    tio.merge_table(con, "product", con.sql("SELECT 1 AS product_id, 'p' AS name"), f, ("product_id",))
    tio.append_table(con, "order_line", con.sql("SELECT 2 AS order_id, 5 AS qty"), f, pk=("order_id",))

    schema = extract_schema(con)
    # The merge main is present with its user columns — no physical base table exists yet.
    assert schema["product"] == {"product_id": "INTEGER", "name": "VARCHAR"}
    assert schema["order_line"] == {"order_id": "INTEGER", "qty": "INTEGER"}
    # No framework companions (changelog/droplog/band/base) or system columns leak in.
    assert not any(k.startswith("_") or "__" in k for k in schema)


# ── Catchment: capture + forward-only contract + block ──────────────────────────

_CFG_INLET = {"sources": {}, "immediate_retries": 0, "source_retries": 0, "kind": "inlet"}
_EVENT = [{"func": "event", "name": "event", "parents": []}]


def _driver(tmp_path, extra=None):
    db = connect(tmp_path / "duck.db")
    migrate(db)
    _register(db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", _CFG_INLET, _EVENT)
    if extra:
        extra(db)
    return Driver(db, tmp_path, "http://x", NoopLauncher())


def _run_with_schema(driver, key, ripple, schema):
    """Drive a Pond Run to completion via simulated Duck events, publishing ``schema``."""
    driver.pulse(key)
    f = driver.state.pond_states[key].start_f.isoformat()
    driver.on_event(key, {"kind": "ripple", "f": f, "ripple": ripple, "status": "success"})
    driver.on_event(key, {"kind": "run_completed", "f": f, "schema": schema})
    return f


def test_schema_captured_on_run_completed(tmp_path):
    d = _driver(tmp_path)
    _run_with_schema(d, "src@1", "event", {"event": {"id": "INTEGER", "val": "VARCHAR"}})
    rows = d.db.execute(
        'SELECT "table", "column", type FROM pond_version_schema ORDER BY "column"'
    ).fetchall()
    assert rows == [("event", "id", "INTEGER"), ("event", "val", "VARCHAR")]


def test_first_run_has_no_contract(tmp_path):
    d = _driver(tmp_path)
    assert d._contract_for("src@1") is None  # nothing captured yet → nothing to enforce


def test_forward_version_carries_the_high_water_contract(tmp_path):
    d = _driver(tmp_path)
    _run_with_schema(d, "src@1", "event", {"event": {"id": "INTEGER", "val": "VARCHAR"}})
    # Deploy a forward version on the same major; its next run must keep {id, val}.
    _register(d.db, "src", "1.1.0", "inlet", "ponds/src/1.1.0", _CFG_INLET, _EVENT)
    d.reload()
    assert d._contract_for("src@1") == {"event": {"id": "INTEGER", "val": "VARCHAR"}}
    # And it rides the begin_run job (the latest — NoopLauncher never drains the queue).
    d.pulse("src@1")
    job = [j for j in d.jobs["src@1"] if j["kind"] == "begin_run"][-1]
    assert job["contract"] == {"event": {"id": "INTEGER", "val": "VARCHAR"}}


def test_rollback_skips_the_schema_gate(tmp_path):
    d = _driver(tmp_path)
    # 1.0.0 then forward 1.1.0, both accepted (so 1.1.0 is high-water).
    _run_with_schema(d, "src@1", "event", {"event": {"id": "INTEGER"}})
    _register(d.db, "src", "1.1.0", "inlet", "ponds/src/1.1.0", _CFG_INLET, _EVENT)
    d.reload()
    _run_with_schema(d, "src@1", "event", {"event": {"id": "INTEGER", "val": "VARCHAR"}})
    # Roll back to 1.0.0 (a previously-accepted lower version): the additive gate is skipped — a
    # downgrade is governed by min_version, not the forward-only schema check.
    _register(d.db, "src", "1.0.0", "inlet", "ponds/src/1.0.0", _CFG_INLET, _EVENT)
    d.reload()
    assert d._contract_for("src@1") is None


def test_contract_violation_fails_source_and_blocks_sink(tmp_path):
    def add_sink(db):
        cfg = {"sources": {"src": "1.0.0"}, "immediate_retries": 0, "source_retries": 0, "kind": "pond"}
        _register(db, "snk", "1.0.0", "pond", "ponds/snk/1.0.0", cfg, _EVENT)

    d = _driver(tmp_path, extra=add_sink)
    # The Duck refused to publish (output broke the contract) and reported contract_failed.
    d.pulse("src@1")
    f = d.state.pond_states["src@1"].start_f.isoformat()
    d.on_event("src@1", {"kind": "contract_failed", "f": f, "error": "column 'event.val' was dropped"})

    src = next(p for p in d.status()["ponds"] if p["name"] == "src")
    snk = next(p for p in d.status()["ponds"] if p["name"] == "snk")
    assert src["is_failed"] and "val" in (src["error"] or "")
    assert snk["is_blocked"] and "src@1" in snk["blocked_by"]


# ── the real publish gate (live DuckDB registry) ────────────────────────────────


def test_executor_export_gate_aborts_publish_and_preserves_last_good(tmp_path):
    """The real executor path: extract_schema reads a live DuckDB registry, a violating output raises
    ContractViolation *before* publishing (so last-good stays), and a compatible output publishes and
    returns the schema."""
    from duckstring.duck.executor import RippleExecutor
    from duckstring.schema_contract import ContractViolation

    ex = RippleExecutor("src", 1, "1.0.0", "ponds/src/1.0.0", tmp_path)
    data = ex.registry_path.parent / "data"
    cur = ex._cursor()
    cur.execute("CREATE TABLE event AS SELECT 1 AS id")  # only {id}; the contract wants {id, val}
    cur.close()

    with pytest.raises(ContractViolation, match="val"):
        ex.export(contract={"event": {"id": "INTEGER", "val": "VARCHAR"}})
    assert not (data / "event.parquet").exists()  # nothing published — last-good intact

    schema = ex.export(contract={"event": {"id": "INTEGER"}})  # additive-compatible → publishes
    assert schema == {"event": {"id": "INTEGER"}}
    assert (data / "event.parquet").exists()
    ex.shutdown()


def test_contract_failure_kind_surfaces_in_status(tmp_path):
    """The status API derives the `failure_kind` sub-reason from the stored message's stable prefix —
    "contract" for a Duck-refused publish, "error" for any other failure. Restart-proof (it rides the
    persisted error text, not ephemeral state)."""
    from duckstring.schema_contract import CONTRACT_PREFIX

    d = _driver(tmp_path)
    d.pulse("src@1")
    f = d.state.pond_states["src@1"].start_f.isoformat()
    d.on_event("src@1", {"kind": "contract_failed", "f": f,
                         "error": f"{CONTRACT_PREFIX}column 'event.val' was dropped"})
    src = next(p for p in d.status()["ponds"] if p["name"] == "src")
    assert src["is_failed"] and src["failure_kind"] == "contract"

    # ...and it survives a restart (reload rebuilds from SQLite; the kind re-derives from the message).
    d.reload()
    src = next(p for p in d.status()["ponds"] if p["name"] == "src")
    assert src["failure_kind"] == "contract"


def test_plain_failure_kind_is_error(tmp_path):
    d = _driver(tmp_path)
    d.pulse("src@1")
    f = d.state.pond_states["src@1"].start_f.isoformat()
    d.on_event("src@1", {"kind": "pond_failed", "f": f, "error": "boom"})
    src = next(p for p in d.status()["ponds"] if p["name"] == "src")
    assert src["is_failed"] and src["failure_kind"] == "error"
    healthy = [p for p in d.status()["ponds"] if not p["is_failed"]]
    assert all(p["failure_kind"] is None for p in healthy)


def test_executor_gate_message_carries_contract_prefix(tmp_path):
    from duckstring.duck.executor import RippleExecutor
    from duckstring.schema_contract import CONTRACT_PREFIX, ContractViolation

    (tmp_path / "ponds" / "src" / "1.0.0").mkdir(parents=True, exist_ok=True)
    (tmp_path / "ponds" / "src" / "1.0.0" / "pond.toml").write_text('[pond]\nname = "src"\nversion = "1.0.0"\n')
    ex = RippleExecutor("src", 1, "1.0.0", "ponds/src/1.0.0", tmp_path)
    cur = ex._cursor()
    cur.execute("CREATE TABLE event AS SELECT 1 AS id")
    cur.close()
    with pytest.raises(ContractViolation) as exc:
        ex.export(contract={"event": {"id": "INTEGER", "val": "VARCHAR"}})
    assert str(exc.value).startswith(CONTRACT_PREFIX)
    ex.shutdown()
