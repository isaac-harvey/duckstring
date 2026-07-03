"""Trickle: incremental I/O via Z-sets / DBSP-style joins (plans/trickle-dbsp.md). Covers the append/merge
write paths, the ``_duckstring_d`` Z-set changelog, the windowed ``read_delta`` (consolidation / coverage
fallback / bootstrap / overwrite-source change detection), the DBSP builder (any-key joins, the fact+dim
both-change composition, deletes via full-row retraction, the comprehensive fallback, the change-fraction
threshold), retention, and the incremental draw window. Pinned to the Parquet plane (the window read is a
content predicate over ``_duckstring_f``, so it's plane-agnostic; Parquet is fast/offline)."""

from __future__ import annotations

import textwrap
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from duckstring import trickle_io as T
from duckstring.core import Pond
from duckstring.dataplane import ParquetDataPlane
from duckstring.engine.core import NEVER
from duckstring.local import hydrate, load_project, run_pond
from duckstring.trickle_builder import BuildError

UTC = timezone.utc


def ts(hour: int) -> datetime:
    return datetime(2026, 6, 16, hour, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _parquet_plane(monkeypatch):
    monkeypatch.setenv("DUCKSTRING_DATA_PLANE", "parquet")


@pytest.fixture
def reg(tmp_path):
    """A persistent producer registry (so Trickle history accumulates across simulated runs)."""
    con = duckdb.connect(str(tmp_path / "reg.duckdb"))
    yield con
    con.close()


def publish(con, data_dir, f=None):
    ParquetDataPlane().export(con, data_dir, f=f)
    return data_dir


def rows(con, data_dir, table, proj="*"):
    sql = ParquetDataPlane().read_select(data_dir, table)
    return sorted(con.sql(f"SELECT {proj} FROM ({sql})").fetchall())


def _state(con, pairs):
    if not pairs:
        return con.sql("SELECT CAST(NULL AS INTEGER) AS id, CAST(NULL AS VARCHAR) AS v WHERE 1=0")
    values = ", ".join(f"({i}, '{v}')" for i, v in pairs)
    return con.sql(f"SELECT * FROM (VALUES {values}) AS s(id, v)")


# ─── append ───────────────────────────────────────────────────────────────────


def test_append_accumulates_history_stamped_with_f(reg):
    T.append_table(reg, "event", reg.sql("SELECT 1 AS id, 'a' AS v"), ts(1), ("id",))
    T.append_table(reg, "event", reg.sql("SELECT 2 AS id, 'b' AS v"), ts(2), ("id",))
    assert sorted(reg.sql("SELECT id, v FROM event").fetchall()) == [(1, "a"), (2, "b")]


def test_append_idempotent_replay_at_same_f(reg):
    rel = reg.sql("SELECT 1 AS id, 'a' AS v")
    T.append_table(reg, "event", rel, ts(1), ("id",))
    T.append_table(reg, "event", rel, ts(1), ("id",))  # replay at the same freshness
    assert reg.sql("SELECT count(*) FROM event").fetchone()[0] == 1


def test_append_fail_on_conflict_catches_duplicates(reg):
    # fail_on_conflict defaults True; with a pk it asserts uniqueness. Duplicate within the appended batch:
    with pytest.raises(T.DeltaError, match="duplicate"):
        T.append_table(reg, "event", reg.sql("SELECT 1 AS id UNION ALL SELECT 1 AS id"), ts(1), ("id",))
    assert not reg.sql("SELECT 1 FROM information_schema.tables WHERE table_name = 'event'").fetchall()

    # Collision against existing history (a different run's row).
    T.append_table(reg, "event", reg.sql("SELECT 1 AS id"), ts(1), ("id",))
    with pytest.raises(T.DeltaError, match="already present"):
        T.append_table(reg, "event", reg.sql("SELECT 1 AS id"), ts(2), ("id",))

    # A replay at the same f re-appends its own rows without tripping the check.
    T.append_table(reg, "event", reg.sql("SELECT 1 AS id"), ts(1), ("id",))
    assert reg.sql("SELECT count(*) FROM event").fetchone()[0] == 1

    # fail_on_conflict=False is the trust-the-writer fast path — a colliding pk is appended without a check.
    T.append_table(reg, "event", reg.sql("SELECT 1 AS id"), ts(3), ("id",), fail_on_conflict=False)
    assert reg.sql("SELECT count(*) FROM event").fetchone()[0] == 2


def test_append_delta_window_is_plus_one(reg, tmp_path):
    for h in (1, 2, 3):
        T.append_table(reg, "event", reg.sql(f"SELECT {h} AS id"), ts(h), ("id",))
    data_dir = publish(reg, tmp_path / "data", f=ts(3))
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    d = T.read_delta(rcon, data_dir, "event", previous_f=ts(1), f=ts(3), dp=ParquetDataPlane())
    assert sorted(d.zset.fetchall()) == [(2, 1), (3, 1)]  # rows 2,3 at weight +1 (append never retracts)
    assert not d.is_full


# ─── merge: the Z-set changelog ─────────────────────────────────────────────────


def test_merge_changelog_is_full_image_zset(reg, tmp_path):
    T.merge_table(reg, "dim", _state(reg, [(1, "a"), (2, "b")]), ts(1), ("id",))           # bootstrap
    T.merge_table(reg, "dim", _state(reg, [(1, "a"), (2, "B"), (3, "c")]), ts(2), ("id",))  # 2 upd, 3 ins
    T.merge_table(reg, "dim", _state(reg, [(2, "B")]), ts(3), ("id",))                       # 1,3 del

    data_dir = publish(reg, tmp_path / "data", f=ts(3))
    assert rows(reg, data_dir, "dim", "id, v") == [(2, "B")]  # reconstructed current state, no system columns
    clog = reg.sql(
        f"SELECT id, v, {T.D_COL}, {T.F_COL} FROM dim__changelog ORDER BY {T.F_COL}, id, {T.D_COL}"
    ).fetchall()
    assert clog == [
        # The main is log-structured: run 1 (bootstrap) writes the initial state to the changelog as +1s
        # (it IS the main until a checkpoint folds it into a base).
        (1, "a", 1, ts(1)), (2, "b", 1, ts(1)),                       # run 1: initial inserts
        (2, "b", -1, ts(2)), (2, "B", 1, ts(2)), (3, "c", 1, ts(2)),  # run 2: update 2 = -old +new; insert 3
        (1, "a", -1, ts(3)), (3, "c", -1, ts(3)),                      # run 3: delete 1 and 3 (retractions)
    ]
    assert T.load_sidecar(data_dir)["dim"]["floor"] == ts(1).isoformat()


def test_merge_main_carries_freshness_stripped_for_consumers(reg, tmp_path):
    T.merge_table(reg, "dim", _state(reg, [(1, "a")]), ts(1), ("id",))
    data_dir = publish(reg, tmp_path / "data", f=ts(1))
    rcon = duckdb.connect()
    full = rcon.sql(ParquetDataPlane().read_select(data_dir, "dim"))
    # The (reconstructed) main carries _duckstring_f — each row's last-write freshness, for as-of reads and
    # the data viewer — while every other column is a user column.
    assert set(full.columns) == {"id", "v", "_duckstring_f"}
    # A consumer strips the system column (read_table does this), seeing pure user columns + the right state.
    assert set(T._strip_system(full).columns) == {"id", "v"}
    assert full.filter("id = 1").fetchone()[2] == ts(1)  # _duckstring_f = the run that wrote it


def test_merge_main_checkpoint_folds_into_base(reg, tmp_path, monkeypatch):
    """The log-structured main reconstructs the correct current state across update/insert/delete at every
    publish — whether a change is in the hot changelog, folded into a warm band, or compacted into the cold
    base. The bootstrap builds the first cold base; the reconstructed read is the durable contract."""
    monkeypatch.setenv("DUCKSTRING_COMPACT_THRESHOLD", "1")  # tiny → eager fold/compaction
    snk_dir = tmp_path / "data"

    def run(state, f):
        T.merge_table(reg, "dim", _state(reg, state), f, ("id",))
        publish(reg, snk_dir, f=f)
        return T.load_sidecar(snk_dir)["dim"]

    sc = run([(1, "a"), (2, "b")], ts(1))           # bootstrap → the first cold base
    assert T.base_chunks(snk_dir, "dim") and sc["f_base"] == ts(1).isoformat()
    assert rows(reg, snk_dir, "dim", "id, v") == [(1, "a"), (2, "b")]
    assert "_duckstring_f" in reg.sql("SELECT * FROM dim LIMIT 0").columns

    run([(1, "A"), (2, "b"), (3, "c")], ts(2))      # update 1, insert 3, keep 2
    assert rows(reg, snk_dir, "dim", "id, v") == [(1, "A"), (2, "b"), (3, "c")]

    run([(1, "A"), (3, "c")], ts(3))                # delete 2
    assert rows(reg, snk_dir, "dim", "id, v") == [(1, "A"), (3, "c")]

    # The cold base advances (the fold watermark moved past the bootstrap) and a warm fold occurred, yet
    # every read above resolved the current state regardless of which tier each row lives in.
    sc = T.load_sidecar(snk_dir)["dim"]
    assert sc["f_base"] >= ts(1).isoformat() and sc["floor"] is not None


def test_per_table_compact_threshold_overrides_catchment_default(reg, tmp_path, monkeypatch):
    """A per-table ``compact_threshold`` (recorded at the merge write) overrides the catchment env default
    for that main's checkpoint trigger: a huge env default would never checkpoint these tiny tables, but a
    tiny per-table override folds the base on the first publish."""
    monkeypatch.setenv("DUCKSTRING_COMPACT_THRESHOLD", str(1 << 40))  # 1 TiB env → never fires by size
    snk_dir = tmp_path / "data"
    # No override → the env default holds, so no base is folded (the changelog is still the whole main).
    T.merge_table(reg, "plain", _state(reg, [(1, "a")]), ts(1), ("id",))
    publish(reg, snk_dir, f=ts(1))
    assert T.load_sidecar(snk_dir)["plain"]["f_base"] is None
    # A tiny per-table override → checkpoint fires and folds the base.
    T.merge_table(reg, "over", _state(reg, [(1, "a")]), ts(1), ("id",), compact_threshold=1)
    publish(reg, snk_dir, f=ts(1))
    assert T.read_meta(reg)["over"]["compact_threshold"] == 1
    assert T.load_sidecar(snk_dir)["over"]["f_base"] == ts(1).isoformat()
    assert T.base_chunks(snk_dir, "over")


def test_chunked_base_splits_by_size_and_replaces_on_checkpoint(reg, tmp_path, monkeypatch):
    """A merge base bigger than the chunk size splits into multiple freshness-ordered chunks; a later
    checkpoint rewrites the base under a fresh token and drops the previous chunks (no stale chunk left to
    resurrect a deleted PK). The reconstruct read sees the current state throughout, and the base directory
    is never treated as incremental per-run parts."""
    chunk = 512 * 1024
    snk_dir = tmp_path / "data"
    reg.execute("SET TimeZone='UTC'")

    def state(pred, suffix=""):  # ~200k incompressible rows → several row groups, so FILE_SIZE_BYTES splits
        return reg.sql(
            f"SELECT i AS id, hash(i)::VARCHAR || hash(i*7)::VARCHAR || '{suffix}' AS v "
            f"FROM range(200000) t(i) WHERE {pred}"
        )

    T.merge_table(reg, "dim", state("1=1"), ts(1), ("id",), compact_threshold=chunk)
    publish(reg, snk_dir, f=ts(1))
    chunks1 = T.base_chunks(snk_dir, "dim")
    assert len(chunks1) > 1, "a base over the chunk size must split into multiple chunks (bootstrap)"
    assert "dim__base" not in T.part_tables(snk_dir)  # the base is not a per-run-parts table
    assert rows(reg, snk_dir, "dim", "count(*)") == [(200000,)]

    # Two large churns (full re-write, then delete the odds) accumulate a warm tier ≥ the cold base, so a
    # later publish cold-compacts: the base is rebuilt under a new token and the old chunks are dropped. The
    # read reconstructs correctly from cold ⊎ warm ⊎ hot throughout.
    T.merge_table(reg, "dim", state("1=1", "v2"), ts(2), ("id",), compact_threshold=chunk)
    publish(reg, snk_dir, f=ts(2))
    assert rows(reg, snk_dir, "dim", "count(*) FILTER (WHERE v LIKE '%v2')") == [(200000,)]
    T.merge_table(reg, "dim", state("i % 2 = 0", "v2"), ts(3), ("id",), compact_threshold=chunk)
    publish(reg, snk_dir, f=ts(3))
    assert rows(reg, snk_dir, "dim", "count(*)") == [(100000,)]
    # Drive further publishes until the warm tier has been folded into a freshly-chunked cold base.
    for h in range(4, 8):
        T.merge_table(reg, "dim", state("i % 2 = 0", "v2"), ts(h), ("id",), compact_threshold=chunk)
        publish(reg, snk_dir, f=ts(h))
    chunksN = T.base_chunks(snk_dir, "dim")
    assert set(chunks1).isdisjoint(set(chunksN)), "cold compaction replaced chunks"
    # No deleted (odd) PK is resurrected from a stale chunk across all the folding/compaction.
    assert rows(reg, snk_dir, "dim", "count(*)") == [(100000,)]
    assert rows(reg, snk_dir, "dim", "count(*) FILTER (WHERE id % 2 = 1)") == [(0,)]


def test_warm_tier_folds_changelog_and_reconstructs_across_all_tiers(reg, tmp_path, monkeypatch):
    """The warm tier: incremental merges fold older changelog into warm bands (cold base untouched), and the
    read reconstructs the current state from cold ⊎ warm ⊎ hot at every step. The fold raises the delta
    floor (far-behind windows coverage-miss → full read) while a caught-up consumer's hot-window delta still
    reads. A later cold compaction folds warm into the clean base."""
    monkeypatch.setenv("DUCKSTRING_COMPACT_THRESHOLD", "1")  # tiny → fold/compact eagerly across many runs
    snk_dir = tmp_path / "data"
    expect = {}
    saw_all_three_tiers = False
    for h in range(1, 13):
        expect[h] = f"v{h}"          # insert a new key each run
        if h > 3:
            expect[h - 3] = f"u{h}"  # and update an older one (churn the warm/cold tiers)
        rel = reg.sql("SELECT * FROM (VALUES " + ", ".join(f"({k}, '{v}')" for k, v in expect.items())
                      + ") AS s(id, v)")
        T.merge_table(reg, "dim", rel, ts(h), ("id",))
        publish(reg, snk_dir, f=ts(h))
        assert rows(reg, snk_dir, "dim", "id, v") == sorted(expect.items())  # correct at every step
        if T.base_chunks(snk_dir, "dim") and T.table_parts(snk_dir, "dim__band") \
                and T.table_parts(snk_dir, "dim__changelog"):
            saw_all_three_tiers = True
    assert saw_all_three_tiers, "cold base + warm bands + hot changelog should coexist mid-stream"

    sc = T.load_sidecar(snk_dir)["dim"]
    floor = datetime.fromisoformat(sc["floor"])
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    # A caught-up consumer reading the still-hot window gets a real (non-full) delta...
    hot = T.read_delta(rcon, snk_dir, "dim", previous_f=ts(11), f=ts(12), dp=ParquetDataPlane())
    assert not hot.is_full
    # ...while a consumer behind the (raised) floor coverage-misses → a full read.
    behind = T.read_delta(rcon, snk_dir, "dim", previous_f=ts(1), f=ts(12), dp=ParquetDataPlane())
    assert behind.is_full and floor > ts(1)


def test_stale_warm_band_below_f_base_does_not_corrupt_read(reg, tmp_path, monkeypatch):
    """A consumer that has not yet pruned warm bands subsumed by an advanced cold base stays correct: a band
    file stamped ``≤ f_base`` is ignored by reconstruct (it filters ``> f_base``), so no superseded/deleted
    row resurfaces. This is what lets the cross-Catchment draw ship the base only when it changes."""
    monkeypatch.setenv("DUCKSTRING_COMPACT_THRESHOLD", "1")  # force a cold base
    snk = tmp_path / "data"
    T.merge_table(reg, "dim", _state(reg, [(1, "a"), (2, "b")]), ts(5), ("id",))
    publish(reg, snk, f=ts(5))  # bootstrap → cold base at f_base = ts5
    assert T.load_sidecar(snk)["dim"]["f_base"] == ts(5).isoformat()

    # Inject a stale warm band (f = ts3 ≤ f_base) that, if read, would delete id=1 and add a bogus id=9.
    band_dir = snk / "dim__band"
    band_dir.mkdir(exist_ok=True)
    reg.execute(
        f"COPY (SELECT * FROM (VALUES (1,'a',-1,TIMESTAMPTZ '{ts(3).isoformat()}'), "
        f"(9,'zz',1,TIMESTAMPTZ '{ts(3).isoformat()}')) AS s(id,v,_duckstring_d,_duckstring_f)) "
        f"TO '{band_dir / T.part_name(ts(3))}' (FORMAT PARQUET)"
    )
    # The stale band is below f_base → ignored; the read is exactly the cold base state.
    assert rows(reg, snk, "dim", "id, v") == [(1, "a"), (2, "b")]


def test_merge_idempotent_replay(reg, tmp_path):
    T.merge_table(reg, "dim", _state(reg, [(1, "a"), (2, "b")]), ts(1), ("id",))
    state2 = [(1, "a"), (2, "B")]
    T.merge_table(reg, "dim", _state(reg, state2), ts(2), ("id",))
    before = reg.sql("SELECT count(*) FROM dim__changelog").fetchone()[0]
    T.merge_table(reg, "dim", _state(reg, state2), ts(2), ("id",))  # replay: diff vs advanced main = empty
    after = reg.sql("SELECT count(*) FROM dim__changelog").fetchone()[0]
    assert before == after  # the empty-diff guard preserves the first attempt's changelog
    assert rows(reg, publish(reg, tmp_path / "d", f=ts(2)), "dim", "id, v") == [(1, "a"), (2, "B")]


def test_merge_window_consolidates_multiple_updates(reg, tmp_path):
    T.merge_table(reg, "dim", _state(reg, [(1, "a")]), ts(1), ("id",))
    T.merge_table(reg, "dim", _state(reg, [(1, "b")]), ts(2), ("id",))  # a→b
    T.merge_table(reg, "dim", _state(reg, [(1, "c")]), ts(3), ("id",))  # b→c
    data_dir = publish(reg, tmp_path / "data", f=ts(3))
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    # Window (f1, f3]: -a +b -b +c consolidates to -a +c (the intermediate b cancels).
    d = T.read_delta(rcon, data_dir, "dim", previous_f=ts(1), f=ts(3), dp=ParquetDataPlane())
    assert sorted(d.zset.fetchall()) == [(1, "a", -1), (1, "c", 1)]
    assert sorted(d.upserts.fetchall()) == [(1, "c")]
    assert d.deletes.fetchall() == []


def test_merge_delete_then_readd_resolves_present(reg, tmp_path):
    T.merge_table(reg, "dim", _state(reg, [(1, "a")]), ts(1), ("id",))
    T.merge_table(reg, "dim", _state(reg, []), ts(2), ("id",))         # delete 1
    T.merge_table(reg, "dim", _state(reg, [(1, "z")]), ts(3), ("id",))  # re-add 1
    data_dir = publish(reg, tmp_path / "data", f=ts(3))
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    d = T.read_delta(rcon, data_dir, "dim", previous_f=ts(1), f=ts(3), dp=ParquetDataPlane())
    assert sorted(d.upserts.fetchall()) == [(1, "z")]  # net: was 'a', now 'z'
    assert d.deletes.fetchall() == []


def test_merge_bootstrap_and_coverage_fallback_full_read(reg, tmp_path):
    T.merge_table(reg, "dim", _state(reg, [(1, "a"), (2, "b")]), ts(2), ("id",))
    data_dir = publish(reg, tmp_path / "data", f=ts(2))
    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    boot = T.read_delta(rcon, data_dir, "dim", previous_f=NEVER, f=ts(9), dp=ParquetDataPlane())
    assert boot.is_full and sorted(boot.upserts.fetchall()) == [(1, "a"), (2, "b")]
    miss = T.read_delta(rcon, data_dir, "dim", previous_f=ts(1), f=ts(9), dp=ParquetDataPlane())
    assert miss.is_full and sorted(miss.upserts.fetchall()) == [(1, "a"), (2, "b")]


def test_merge_requires_primary_key(reg):
    with pytest.raises(T.DeltaError):
        T.merge_table(reg, "dim", _state(reg, [(1, "a")]), ts(1), ())


class _CrashOn:
    """Proxy a DuckDB connection, raising when an executed statement contains ``needle`` — to inject a
    crash mid-apply and prove the changelog write commits/aborts atomically."""

    def __init__(self, con, needle):
        self._c, self._needle = con, needle

    def execute(self, sql, *a, **k):
        if self._needle in sql:
            raise RuntimeError("injected crash")
        return self._c.execute(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._c, name)


def test_merge_apply_changelog_write_is_atomic(reg, tmp_path):
    """The main is log-structured (changelog only; no main upsert), so a crash mid-apply can only affect the
    changelog. The DELETE+INSERT of this f's window is one transaction: a crash rolls it back, the
    reconstructed state is unchanged, and a clean replay recovers."""
    T.merge_table(reg, "dim", _state(reg, [(1, "a"), (2, "b")]), ts(1), ("id",))  # bootstrap
    T.merge_table(reg, "dim", _state(reg, [(1, "A"), (2, "b")]), ts(2), ("id",))

    # Crash on the changelog INSERT — the DELETE that precedes it in the same transaction must roll back too.
    with pytest.raises(RuntimeError):
        T.merge_table(_CrashOn(reg, 'INSERT INTO "dim__changelog"'),
                      "dim", _state(reg, [(1, "Z"), (2, "b")]), ts(3), ("id",))

    # Rolled back: no ts(3) changelog rows, and the reconstructed state is still the ts(2) state.
    assert reg.execute(
        f"SELECT count(*) FROM dim__changelog WHERE {T.F_COL} = {T._ts(ts(3))}"
    ).fetchone()[0] == 0
    assert sorted(T.reconstruct_current(reg, "dim").fetchall()) == [(1, "A", ts(2)), (2, "b", ts(1))]

    # A clean replay recovers fully.
    T.merge_table(reg, "dim", _state(reg, [(1, "Z"), (2, "b")]), ts(3), ("id",))
    assert sorted(T._strip_system(T.reconstruct_current(reg, "dim")).fetchall()) == [(1, "Z"), (2, "b")]
    assert sorted(reg.execute(
        f"SELECT id, v, {T.D_COL} FROM dim__changelog WHERE {T.F_COL} = {T._ts(ts(3))}"
    ).fetchall()) == [(1, "A", -1), (1, "Z", 1)]


# ─── builder DSL (pond.trickle) — DBSP composition ──────────────────────────────


def _producer(tmp_path, source, major=1):
    data_dir = tmp_path / "ponds" / source / f"m{major}" / "data"
    return duckdb.connect(str(tmp_path / f"{source}.duckdb")), data_dir


def _star_sources(tmp_path):
    ol_con, ol_dir = _producer(tmp_path, "sales")
    pr_con, pr_dir = _producer(tmp_path, "catalog")

    def ol(state, f):
        vals = ", ".join(f"({o}, '{p}', {q})" for o, p, q in state)
        T.merge_table(ol_con, "order_line",
                      ol_con.sql(f"SELECT * FROM (VALUES {vals}) v(order_id, product_id, qty)"), f, ("order_id",))
        publish(ol_con, ol_dir, f=f)

    def pr(state, f):
        vals = ", ".join(f"('{p}', {pr_})" for p, pr_ in state)
        T.merge_table(pr_con, "product",
                      pr_con.sql(f"SELECT * FROM (VALUES {vals}) v(product_id, price)"), f, ("product_id",))
        publish(pr_con, pr_dir, f=f)

    return (ol_con, pr_con), ol, pr


def _priced(tmp_path, f, previous_f, snk_con, snk_dir, *, spine_p=0.3, dim_p=0.3):
    pond = Pond("priced", "1.0.0", snk_con, root=tmp_path,
                source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=previous_f)
    (pond.trickle("sales.order_line", p=spine_p)
         .join(pond.trickle("catalog.product", p=dim_p), on="product_id")
         .select("s0.order_id, s0.product_id, s0.qty, s1.price, s0.qty * s1.price AS total")
         .merge("priced", pk="order_id"))
    publish(snk_con, snk_dir, f=f)


def test_builder_dimension_change_incremental(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    snk_con = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"

    _priced(tmp_path, ts(1), NEVER, snk_con, snk_dir)  # bootstrap
    assert rows(snk_con, snk_dir, "priced", "order_id, total") == [(10, 10), (11, 9)]

    pr([("p1", 50), ("p2", 9)], ts(2))  # p1 price 5 → 50; only order 10 uses p1
    _priced(tmp_path, ts(2), ts(1), snk_con, snk_dir)
    assert rows(snk_con, snk_dir, "priced", "order_id, total") == [(10, 100), (11, 9)]
    snk_con.close()


def test_builder_key_prefilter_restricts_spine_on_dim_change(tmp_path):
    """The general-purpose performance lever: the affected-key recompute pre-filters BOTH join inputs to the
    changed join keys (``IN (SELECT k0 …)``) before the join — so a small dimension change never drives a
    full spine scan. Guards the pushdown by inspecting the generated SQL."""
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    snk_con = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"
    _priced(tmp_path, ts(1), NEVER, snk_con, snk_dir)  # bootstrap

    pr([("p1", 50), ("p2", 9)], ts(2))   # only the dimension changes this run
    pond = Pond("priced", "1.0.0", snk_con, root=tmp_path,
                source_majors={"sales": 1, "catalog": 1}, f=ts(2), previous_f=ts(1))
    b = (pond.trickle("sales.order_line").join(pond.trickle("catalog.product", p=1.0), on="product_id")
             .select("s0.order_id, s1.price"))
    seen: list[str] = []
    orig_view = b._view
    b._view = lambda sql: seen.append(sql) or orig_view(sql)  # capture the raw generated SQL
    kind, _rel = b._compute(("order_id",), "priced")
    assert kind == "incremental"
    # The spine (s0) is pre-filtered to the changed dim's keys (IN (SELECT k0 …)) before the join.
    assert any('"s0.product_id") IN (SELECT k0' in s for s in seen)
    snk_con.close()


def test_builder_propagates_spine_delete(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    snk_con = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"
    _priced(tmp_path, ts(1), NEVER, snk_con, snk_dir)

    ol([(10, "p1", 2)], ts(2))  # order 11 removed from the spine
    _priced(tmp_path, ts(2), ts(1), snk_con, snk_dir)
    assert rows(snk_con, snk_dir, "priced", "order_id, total") == [(10, 10)]  # 11 dropped
    snk_con.close()


def test_builder_fact_and_dim_both_change(tmp_path):
    """The worked example (plans/trickle-dbsp.md): fact and dim both change in one run. The telescoping
    sum needs the dim's *old* state — the intermediate priced-at-old-price row must cancel."""
    f_con, f_dir = _producer(tmp_path, "fact")
    d_con, d_dir = _producer(tmp_path, "dim")
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'A',10),(2,'A',5),(3,'B',7)) v(id,k,qty)"),
                  ts(1), ("id",))
    T.merge_table(d_con, "d", d_con.sql("SELECT * FROM (VALUES ('A',100),('B',200)) v(k,price)"), ts(1), ("k",))
    publish(f_con, f_dir, f=ts(1))
    publish(d_con, d_dir, f=ts(1))

    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "o" / "m1" / "data"

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"fact": 1, "dim": 1}, f=f, previous_f=pf)
        (p.trickle("fact.f").join(p.trickle("dim.d"), on="k")
           .select("s0.id, s0.k, s0.qty, s1.price").merge("o", pk="id"))
        publish(snk, snk_dir, f=f)

    run(ts(1), NEVER)
    # run 2: fact updates id2 qty 5→8 and inserts id4; dim updates A price 100→120.
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'A',10),(2,'A',8),(3,'B',7),(4,'B',2)) v(id,k,qty)"),
                  ts(2), ("id",))
    T.merge_table(d_con, "d", d_con.sql("SELECT * FROM (VALUES ('A',120),('B',200)) v(k,price)"), ts(2), ("k",))
    publish(f_con, f_dir, f=ts(2))
    publish(d_con, d_dir, f=ts(2))
    run(ts(2), ts(1))

    assert rows(snk, snk_dir, "o", "id, k, qty, price") == [
        (1, "A", 10, 120), (2, "A", 8, 120), (3, "B", 7, 200), (4, "B", 2, 200),
    ]
    snk.close()


def test_builder_rebuilds_whole_when_output_dropped(tmp_path):
    """A delete drops a merge Trickle's registry collection (see plans/deletes.md). The next run must
    rebuild the WHOLE table via the comprehensive path — not persist just the run's small delta — even
    though the Sources only changed a little. Guards the absence⇒comprehensive trigger in builder._compute."""
    f_con, f_dir = _producer(tmp_path, "fact")
    d_con, d_dir = _producer(tmp_path, "dim")
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'A',10),(2,'A',5),(3,'B',7)) v(id,k,qty)"),
                  ts(1), ("id",))
    T.merge_table(d_con, "d", d_con.sql("SELECT * FROM (VALUES ('A',100),('B',200)) v(k,price)"), ts(1), ("k",))
    publish(f_con, f_dir, f=ts(1))
    publish(d_con, d_dir, f=ts(1))

    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "o" / "m1" / "data"

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"fact": 1, "dim": 1}, f=f, previous_f=pf)
        (p.trickle("fact.f").join(p.trickle("dim.d"), on="k")
           .select("s0.id, s0.k, s0.qty, s1.price").merge("o", pk="id"))
        publish(snk, snk_dir, f=f)

    run(ts(1), NEVER)  # bootstrap: full table
    full = [(1, "A", 10, 100), (2, "A", 5, 100), (3, "B", 7, 200)]
    assert rows(snk, snk_dir, "o", "id, k, qty, price") == full

    T.drop_table(snk, "o")  # the delete: drop the sink's whole registry collection
    assert "o" not in T.read_meta(snk)

    # A *small* source change (id2 qty 5→8) with a recent previous_f — the incremental path would compose
    # only that one delta and, applied to the empty log, would leave the table with just id2. The trigger
    # forces a comprehensive rebuild, so the whole join comes back.
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'A',10),(2,'A',8),(3,'B',7)) v(id,k,qty)"),
                  ts(2), ("id",))
    publish(f_con, f_dir, f=ts(2))
    run(ts(2), ts(1))

    assert rows(snk, snk_dir, "o", "id, k, qty, price") == [
        (1, "A", 10, 100), (2, "A", 8, 100), (3, "B", 7, 200),
    ]
    snk.close()


def test_unpublish_table_removes_published_collection(tmp_path):
    """unpublish_table drops every published form + the sidecar entry (the data half of a delete)."""
    from duckstring.dataplane import unpublish_table
    from duckstring.trickle_io import load_sidecar

    con, data_dir = _producer(tmp_path, "src")
    T.merge_table(con, "dim", con.sql("SELECT * FROM (VALUES ('A',1),('B',2)) v(k,x)"), ts(1), ("k",))
    T.append_table(con, "log", con.sql("SELECT 1 AS id"), ts(1), ("id",))
    publish(con, data_dir, f=ts(1))
    # A merge main surfaces its main + its raw-navigable __changelog; the append is a parts table.
    assert set(ParquetDataPlane().list_tables(data_dir)) == {"dim", "dim__changelog", "log"}

    unpublish_table(data_dir, "dim")  # takes the main + every companion with it
    assert ParquetDataPlane().list_tables(data_dir) == ["log"]
    assert "dim" not in load_sidecar(data_dir)
    assert not (data_dir / "dim__changelog").exists()
    unpublish_table(data_dir, "dim")  # idempotent


def test_base_table_name_resolves_companions():
    assert T.base_table_name("priced_line") == "priced_line"
    assert T.base_table_name("priced_line__changelog") == "priced_line"
    assert T.base_table_name("priced_line__band") == "priced_line"
    assert T.base_table_name("order_line__droplog") == "order_line"
    assert T.base_table_name("priced_line__base") == "priced_line"
    assert T.base_table_name("__changelog") == "__changelog"  # no base → unchanged


def test_drop_table_clears_registry_collection(tmp_path):
    con, _ = _producer(tmp_path, "src")
    T.merge_table(con, "dim", con.sql("SELECT * FROM (VALUES ('A',1)) v(k,x)"), ts(1), ("k",))
    assert "dim" in T.read_meta(con)
    assert con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'dim__changelog'").fetchone()[0] == 1

    T.drop_table(con, "dim")
    assert "dim" not in T.read_meta(con)
    assert con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE 'dim%'").fetchone()[0] == 0
    T.drop_table(con, "dim")  # idempotent


def test_builder_count(tmp_path):
    """``.count()``: a bare stored Trickle counts via metadata + changelog net weight (no scan); a composed
    query is evaluated in full and counted. Both track inserts and deletes, and the merge-main count matches
    the rows actually written."""
    f_con, f_dir = _producer(tmp_path, "fact")
    d_con, d_dir = _producer(tmp_path, "dim")
    T.merge_table(d_con, "d", d_con.sql("SELECT * FROM (VALUES ('A',100),('B',200)) v(k,price)"), ts(1), ("k",))
    publish(d_con, d_dir, f=ts(1))

    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "o" / "m1" / "data"
    seen = {}

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"fact": 1, "dim": 1}, f=f, previous_f=pf)
        proj = "s0.id, s0.k, s0.qty, s1.price"
        (p.trickle("fact.f").join(p.trickle("dim.d"), on="k").select(proj).merge("o", pk="id"))
        seen["merge"] = p.trickle("o").count()  # bare local merge main → metadata fast path
        seen["source"] = p.trickle("fact.f").count()  # bare cross-pond source → host count_table (data plane)
        seen["query"] = p.trickle("fact.f").join(p.trickle("dim.d"), on="k").select(proj).count()  # full eval
        publish(snk, snk_dir, f=f)

    facts = "SELECT * FROM (VALUES {}) v(id,k,qty)".format
    T.merge_table(f_con, "f", f_con.sql(facts("(1,'A',10),(2,'A',5),(3,'B',7)")), ts(1), ("id",))
    publish(f_con, f_dir, f=ts(1))
    run(ts(1), NEVER)
    assert seen == {"merge": 3, "source": 3, "query": 3}  # ids 1,2,3

    T.merge_table(f_con, "f", f_con.sql(facts("(1,'A',10),(2,'A',8),(3,'B',7),(4,'B',2)")), ts(2), ("id",))
    publish(f_con, f_dir, f=ts(2))
    run(ts(2), ts(1))
    assert seen == {"merge": 4, "source": 4, "query": 4}  # +insert id4

    T.merge_table(f_con, "f", f_con.sql(facts("(1,'A',10),(3,'B',7),(4,'B',2)")), ts(3), ("id",))
    publish(f_con, f_dir, f=ts(3))
    run(ts(3), ts(2))
    assert seen == {"merge": 3, "source": 3, "query": 3}  # -delete id2 (net Z-set weight goes negative)
    assert seen["merge"] == len(rows(snk, snk_dir, "o", "id"))  # count == rows actually written
    snk.close()


def test_builder_count_after_aggregate_is_group_count(tmp_path):
    """``.aggregate(by).count()`` shortcuts to the number of groups (count distinct ``by``), without running
    the metric aggregations."""
    f_con, f_dir = _producer(tmp_path, "fact")
    # k has 2 distinct groups (A,B) across 3 rows.
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'A',10),(2,'A',5),(3,'B',7)) v(id,k,qty)"),
                  ts(1), ("id",))
    publish(f_con, f_dir, f=ts(1))
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"fact": 1}, f=ts(1), previous_f=NEVER)
    from duckstring import agg

    n_groups = p.trickle("fact.f").aggregate(by="k", qty=agg.sum("qty")).count()
    assert n_groups == 2  # groups A, B — not the 3 underlying rows
    snk.close()


def test_agg_count_metric_is_incremental(tmp_path):
    """``agg.count()`` is a maintained distributive metric (the per-group net Z-set weight ``_a_cnt``): the
    grouped row count updates incrementally across inserts and deletes."""
    from duckstring import agg

    f_con, f_dir = _producer(tmp_path, "fact")
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "o" / "m1" / "data"

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"fact": 1}, f=f, previous_f=pf)
        p.trickle("fact.f").aggregate(by="k", n=agg.count()).merge("by_k", pk="k")
        publish(snk, snk_dir, f=f)

    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'A'),(2,'A'),(3,'B')) v(id,k)"), ts(1), ("id",))
    publish(f_con, f_dir, f=ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "by_k", "k, n") == [("A", 2), ("B", 1)]

    # +insert two B rows, delete one A → A:1, B:3 (maintained from the delta, no rescan).
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'A'),(3,'B'),(4,'B'),(5,'B')) v(id,k)"),
                  ts(2), ("id",))
    publish(f_con, f_dir, f=ts(2))
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "by_k", "k, n") == [("A", 1), ("B", 3)]
    snk.close()


def test_builder_filter_applies_to_delta_and_crosses_boundary(tmp_path):
    """`.filter()` distributes over the Z-set delta: a dimension change that pushes a row across the filter
    boundary inserts/retracts it incrementally (the old image passes/fails the filter on its own side)."""
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "f" / "m1" / "data"

    def run(f, pf):
        pond = Pond("f", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0).alias("o")
             .join(pond.trickle("catalog.product", p=1.0).alias("p"), on="product_id")
             .filter("o.qty * p.price >= 10")
             .select("o.order_id, o.qty * p.price AS total")
             .merge("f", pk="order_id"))
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    run(ts(1), NEVER)   # totals 10, 9 → only order 10 passes (>= 10)
    assert rows(snk, snk_dir, "f", "order_id, total") == [(10, 10)]

    pr([("p1", 5), ("p2", 20)], ts(2))   # p2 reprice → order 11 total 20 crosses into the output
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "f", "order_id, total") == [(10, 10), (11, 20)]

    pr([("p1", 3), ("p2", 20)], ts(3))   # p1 reprice → order 10 total 6 drops out (retracted)
    run(ts(3), ts(2))
    assert rows(snk, snk_dir, "f", "order_id, total") == [(11, 20)]
    snk.close()


def test_builder_mutate_computed_column_and_pk(tmp_path):
    """`.mutate()` adds computed columns while preserving the others (the `*` default), supports parallel
    columns in one call and a chained mutate referencing an earlier one, and a (deterministic) mutated column
    can be the merge pk. Verified across a bootstrap and an incremental dimension change."""
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "m" / "data"

    def run(f, pf):
        pond = Pond("m", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line").alias("o")
             .join(pond.trickle("catalog.product").alias("p"), on="product_id")
             # parallel mutates (both see the input), then a chained mutate building on `total`
             .mutate(line_id="o.order_id || '-' || o.product_id", total="o.qty * p.price")
             .mutate(total_with_tax="total * 1.1")
             .merge("m", pk="line_id"))   # the synthetic id is the pk
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "m", "line_id, total, total_with_tax") == [
        ("10-p1", 10, Decimal("11.0")), ("11-p2", 9, Decimal("9.9"))]
    # the `*` kept the source columns alongside the mutated ones
    assert rows(snk, snk_dir, "m", "order_id, product_id, qty, price") == [
        (10, "p1", 2, 5), (11, "p2", 1, 9)]

    pr([("p1", 50), ("p2", 9)], ts(2))   # p1 reprice → only 10-p1 recomputes incrementally
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "m", "line_id, total, total_with_tax") == [
        ("10-p1", 100, Decimal("110.0")), ("11-p2", 9, Decimal("9.9"))]
    snk.close()


def test_builder_filter_on_mutated_column(tmp_path):
    """A `.filter()` placed after a `.mutate()` references the mutated column — the pipeline is call-ordered.
    The filter still distributes over the delta: a dim change that pushes a row across the boundary on the
    mutated value inserts/retracts it incrementally."""
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "fm" / "m1" / "data"

    def run(f, pf):
        pond = Pond("fm", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0).alias("o")
             .join(pond.trickle("catalog.product", p=1.0).alias("p"), on="product_id")
             .mutate(total="o.qty * p.price")
             .filter("total >= 10")              # ← references the mutated column
             .select("o.order_id, total")
             .merge("fm", pk="order_id"))
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    run(ts(1), NEVER)   # totals 10, 9 → only order 10 passes
    assert rows(snk, snk_dir, "fm", "order_id, total") == [(10, 10)]

    pr([("p1", 5), ("p2", 20)], ts(2))   # p2 reprice → order 11 total 20 crosses in
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "fm", "order_id, total") == [(10, 10), (11, 20)]

    pr([("p1", 3), ("p2", 20)], ts(3))   # p1 reprice → order 10 total 6 drops out (retracted)
    run(ts(3), ts(2))
    assert rows(snk, snk_dir, "fm", "order_id, total") == [(11, 20)]
    snk.close()


def test_builder_mutate_errors(tmp_path):
    snk = duckdb.connect()
    pond = Pond("e", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=ts(1))
    with pytest.raises(BuildError, match="at least one"):
        pond.trickle("sales.order_line").mutate()
    with pytest.raises(BuildError, match="reserved"):
        pond.trickle("sales.order_line").mutate(_duckstring_x="1")
    # a join operand can't carry a .mutate() — attach it to the result
    with pytest.raises(BuildError, match="operand"):
        dim = pond.trickle("catalog.product").mutate(x="price * 2")
        pond.trickle("sales.order_line").join(dim, on="product_id")
    snk.close()


def test_builder_matches_comprehensive_row_for_row(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    snk_con = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"
    _priced(tmp_path, ts(1), NEVER, snk_con, snk_dir)
    pr([("p1", 50), ("p2", 9)], ts(2))
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3), (13, "p2", 4)], ts(2))  # add order 13
    _priced(tmp_path, ts(2), ts(1), snk_con, snk_dir)

    truth_con = duckdb.connect()
    truth_con.execute("SET TimeZone='UTC'")
    ol_main = T._strip_system(truth_con.sql(ParquetDataPlane().read_select(tmp_path / "ponds/sales/m1/data", "order_line")))
    pr_main = T._strip_system(truth_con.sql(ParquetDataPlane().read_select(tmp_path / "ponds/catalog/m1/data", "product")))
    ol_main.create_view("ol", replace=True)
    pr_main.create_view("pr", replace=True)
    truth = sorted(truth_con.sql(
        "SELECT ol.order_id, ol.qty * pr.price FROM ol JOIN pr USING (product_id) ORDER BY 1"
    ).fetchall())
    assert rows(snk_con, snk_dir, "priced", "order_id, total") == truth
    snk_con.close()


def test_builder_joins_on_non_pk_business_code(tmp_path):
    """The headline capability: join on any column (a business code), not the dimension's PK — and a
    deletion still propagates (it is a full-row retraction, not a key-only tombstone)."""
    f_con, f_dir = _producer(tmp_path, "fact")
    d_con, d_dir = _producer(tmp_path, "dim")
    # fact joins to dim on `code`; dim's own PK is `sku`, NOT `code`.
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'X'),(2,'Y'),(3,'X')) v(id,code)"), ts(1), ("id",))
    T.merge_table(d_con, "d", d_con.sql("SELECT * FROM (VALUES ('s1','X','xan'),('s2','Y','yur')) v(sku,code,label)"),
                  ts(1), ("sku",))
    publish(f_con, f_dir, f=ts(1))
    publish(d_con, d_dir, f=ts(1))
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "o" / "m1" / "data"

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"fact": 1, "dim": 1}, f=f, previous_f=pf)
        (p.trickle("fact.f").join(p.trickle("dim.d"), on="code")
           .select("s0.id, s0.code, s1.label").merge("o", pk="id"))
        publish(snk, snk_dir, f=f)

    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "o", "id, label") == [(1, "xan"), (2, "yur"), (3, "xan")]
    # the dim row for code Y is deleted (by its sku PK) → output rows joining Y must drop.
    T.merge_table(d_con, "d", d_con.sql("SELECT * FROM (VALUES ('s1','X','xan')) v(sku,code,label)"), ts(2), ("sku",))
    publish(d_con, d_dir, f=ts(2))
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "o", "id, label") == [(1, "xan"), (3, "xan")]  # id 2 (code Y) dropped
    snk.close()


# ─── ripple (overwrite) inputs ───────────────────────────────────────────────────


def test_builder_unchanged_ripple_is_stable_history(tmp_path, monkeypatch):
    """An unchanged overwrite Ripple dimension (its published f hasn't advanced) is a free stable operand:
    the fact's delta flows incrementally, no comprehensive recompute."""
    f_con, f_dir = _producer(tmp_path, "fact")
    d_con, d_dir = _producer(tmp_path, "dim")
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'X'),(2,'Y')) v(id,code)"), ts(1), ("id",))
    d_con.execute("CREATE TABLE m AS SELECT * FROM (VALUES ('X','xan'),('Y','yur')) v(code,label)")
    publish(f_con, f_dir, f=ts(1))
    publish(d_con, d_dir, f=ts(1))  # plain overwrite dim
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "o" / "m1" / "data"
    paths = _spy_paths(monkeypatch)

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"fact": 1, "dim": 1}, f=f, previous_f=pf)
        (p.trickle("fact.f", p=1.0).join(p.trickle("dim.m", p=1.0), on="code")
           .select("s0.id, s0.code, s1.label").merge("o", pk="id"))
        publish(snk, snk_dir, f=f)

    run(ts(1), NEVER)
    paths.clear()
    # fact adds id 3; dim re-published at the SAME freshness → unchanged → incremental.
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'X'),(2,'Y'),(3,'X')) v(id,code)"), ts(2), ("id",))
    publish(f_con, f_dir, f=ts(2))
    publish(d_con, d_dir, f=ts(1))  # dim NOT advanced
    run(ts(2), ts(1))
    assert paths == ["incremental"]
    assert rows(snk, snk_dir, "o", "id, label") == [(1, "xan"), (2, "yur"), (3, "xan")]
    snk.close()


def test_builder_changed_ripple_forces_comprehensive(tmp_path, monkeypatch):
    f_con, f_dir = _producer(tmp_path, "fact")
    d_con, d_dir = _producer(tmp_path, "dim")
    T.merge_table(f_con, "f", f_con.sql("SELECT * FROM (VALUES (1,'X'),(2,'Y')) v(id,code)"), ts(1), ("id",))
    d_con.execute("CREATE TABLE m AS SELECT * FROM (VALUES ('X','xan'),('Y','yur')) v(code,label)")
    publish(f_con, f_dir, f=ts(1))
    publish(d_con, d_dir, f=ts(1))
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "o" / "m1" / "data"
    paths = _spy_paths(monkeypatch)

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"fact": 1, "dim": 1}, f=f, previous_f=pf)
        (p.trickle("fact.f", p=1.0).join(p.trickle("dim.m", p=1.0), on="code")
           .select("s0.id, s0.code, s1.label").merge("o", pk="id"))
        publish(snk, snk_dir, f=f)

    run(ts(1), NEVER)
    paths.clear()
    # dim ripple changes (relabel X, drop Y) AND advances its freshness → comprehensive recompute.
    d_con.execute("CREATE OR REPLACE TABLE m AS SELECT * FROM (VALUES ('X','XAN')) v(code,label)")
    publish(d_con, d_dir, f=ts(2))
    run(ts(2), ts(1))
    assert paths == ["comprehensive"]
    assert rows(snk, snk_dir, "o", "id, label") == [(1, "XAN")]  # id 2 (code Y) dropped, X relabelled
    snk.close()


# ─── change-fraction threshold ───────────────────────────────────────────────────


def _spy_paths(monkeypatch):
    """Record which path the builder took per run: 'comprehensive' (Pond.merge_table) vs 'incremental'
    (Pond.apply_zset)."""
    paths: list[str] = []
    orig_m, orig_z = Pond.merge_table, Pond.apply_zset

    def spy_m(self, *a, **k):
        paths.append("comprehensive")
        return orig_m(self, *a, **k)

    def spy_z(self, *a, **k):
        paths.append("incremental")
        return orig_z(self, *a, **k)

    monkeypatch.setattr(Pond, "merge_table", spy_m)
    monkeypatch.setattr(Pond, "apply_zset", spy_z)
    return paths


def test_builder_threshold_falls_back_to_comprehensive(tmp_path, monkeypatch):
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p3", 3)], ts(1))
    pr([("p1", 5), ("p2", 9), ("p3", 7)], ts(1))
    snk_con = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"
    _priced(tmp_path, ts(1), NEVER, snk_con, snk_dir)

    paths = _spy_paths(monkeypatch)
    pr([("p1", 50), ("p2", 90), ("p3", 70)], ts(2))  # 3 of 3 prices change → 100% > p=0.3
    _priced(tmp_path, ts(2), ts(1), snk_con, snk_dir)
    assert paths == ["comprehensive"]
    assert rows(snk_con, snk_dir, "priced", "order_id, total") == [(10, 100), (11, 90), (12, 210)]
    snk_con.close()


def test_builder_threshold_p1_forces_incremental(tmp_path, monkeypatch):
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p3", 3)], ts(1))
    pr([("p1", 5), ("p2", 9), ("p3", 7)], ts(1))
    snk_con = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"
    _priced(tmp_path, ts(1), NEVER, snk_con, snk_dir, dim_p=1.0)

    paths = _spy_paths(monkeypatch)
    pr([("p1", 50), ("p2", 90), ("p3", 70)], ts(2))  # 100% churn, but dim_p=1.0 → stays incremental
    _priced(tmp_path, ts(2), ts(1), snk_con, snk_dir, dim_p=1.0)
    assert paths == ["incremental"]
    assert rows(snk_con, snk_dir, "priced", "order_id, total") == [(10, 100), (11, 90), (12, 210)]
    snk_con.close()


# ─── ivm / key_filter strategy escapes ───────────────────────────────────────────


def test_builder_ivm_false_forces_comprehensive(tmp_path, monkeypatch):
    """``ivm=False`` ignores deltas: even a tiny dimension change (well under ``p``, so the default would go
    incremental) recomputes the whole output and diffs vs the stored main. Output stays correct."""
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p3", 3)], ts(1))
    pr([("p1", 5), ("p2", 9), ("p3", 7)], ts(1))
    snk_con = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"

    def build(f, pf, *, ivm=True):
        pond = Pond("priced", "1.0.0", snk_con, root=tmp_path,
                    source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line").join(pond.trickle("catalog.product"), on="product_id")
             .select("s0.order_id, s0.qty * s1.price AS total").merge("priced", pk="order_id", ivm=ivm))
        publish(snk_con, snk_dir, f=f)

    build(ts(1), NEVER)
    paths = _spy_paths(monkeypatch)
    pr([("p1", 50), ("p2", 9), ("p3", 7)], ts(2))   # one product reprices (1/3 < p=0.3 → default is incremental)
    build(ts(2), ts(1), ivm=False)
    assert paths == ["comprehensive"]   # but ivm=False forces the comprehensive recompute
    assert rows(snk_con, snk_dir, "priced", "order_id, total") == [(10, 100), (11, 9), (12, 21)]
    snk_con.close()


def test_builder_key_filter_false_skips_in_filter(tmp_path):
    """``key_filter=False`` keeps the incremental delta composition but drops the ``IN (SELECT k0 …)``
    pre-filter — the same result over full new/old states (correct, just unpruned)."""
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    snk_con = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"

    def build(f, pf, *, key_filter=True, capture=None):
        pond = Pond("priced", "1.0.0", snk_con, root=tmp_path,
                    source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        b = (pond.trickle("sales.order_line").join(pond.trickle("catalog.product", p=1.0), on="product_id")
                 .select("s0.order_id, s0.qty * s1.price AS total"))
        if capture is not None:
            orig = b._view
            b._view = lambda sql: capture.append(sql) or orig(sql)
        b.merge("priced", pk="order_id", key_filter=key_filter)
        publish(snk_con, snk_dir, f=f)

    build(ts(1), NEVER)   # bootstrap
    pr([("p1", 50), ("p2", 9)], ts(2))   # only the dimension changes
    seen: list[str] = []
    build(ts(2), ts(1), key_filter=False, capture=seen)
    assert not any("IN (SELECT k0" in s for s in seen)   # the pre-filter is skipped
    assert rows(snk_con, snk_dir, "priced", "order_id, total") == [(10, 100), (11, 9)]
    snk_con.close()


# ─── build-time errors ───────────────────────────────────────────────────────────


def test_builder_build_time_errors(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2)], ts(1))
    pr([("p1", 5)], ts(1))
    con = duckdb.connect()
    pond = Pond("priced", "1.0.0", con, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=ts(1))

    # A composed operand (a join DAG) is now a valid join input (the snowflake guard is lifted) — this
    # composes without error.
    composed = pond.trickle("catalog.product").alias("p1").join(
        pond.trickle("catalog.product").alias("p2"), on="product_id")
    pond.trickle("sales.order_line").join(composed, on="product_id")

    # But a join operand carrying its own .filter()/.select() is rejected (attach those to the result).
    with pytest.raises(BuildError, match="operand"):
        dim_with_select = pond.trickle("catalog.product").select("product_id")
        pond.trickle("sales.order_line").join(dim_with_select, on="product_id")

    # A joined graph with no .select(...) now resolves via the bare `*` (the join key product_id is
    # deduplicated), so this composes and runs.
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"
    (pond.trickle("sales.order_line").join(pond.trickle("catalog.product"), on="product_id")
         .merge("x", pk="order_id"))
    publish(con, snk_dir, f=ts(1))
    assert rows(con, snk_dir, "x", "order_id, price") == [(10, 5)]

    # But a `*` with a genuine (non-join-key) name collision raises: the snowflake p1⋈p2 both carry `price`.
    with pytest.raises(BuildError, match="ambiguous"):
        (pond.trickle("sales.order_line")
             .join(pond.trickle("catalog.product").alias("p1").join(
                 pond.trickle("catalog.product").alias("p2"), on="product_id"), on="product_id")
             .merge("y", pk="order_id"))

    # No pk passed to a non-aggregate .merge(...) → BuildError (pk only defaults after .aggregate()).
    with pytest.raises(BuildError, match="output key"):
        (pond.trickle("sales.order_line").join(pond.trickle("catalog.product"), on="product_id")
             .select("s0.order_id, s1.price").merge("x"))

    # An empty/None pk passed explicitly raises a friendly BuildError.
    with pytest.raises(BuildError, match="key"):
        (pond.trickle("sales.order_line").join(pond.trickle("catalog.product"), on="product_id")
             .select("s0.order_id, s1.price").merge("x", pk=()))

    # .select that omits the PK.
    with pytest.raises(BuildError, match="PK"):
        (pond.trickle("sales.order_line").join(pond.trickle("catalog.product"), on="product_id")
             .select("s1.price").merge("x", pk="order_id"))


# ─── chained merges: materialise an intermediate mid-chain in one ripple ─────────


def _chain_sources(tmp_path):
    """Three producers for a genuine *chain* a→b→c (c joins on a column produced by a⋈b, not on the spine —
    a shape a single star builder can't express, so the intermediate must be materialised)."""
    a_con, a_dir = _producer(tmp_path, "a")
    b_con, b_dir = _producer(tmp_path, "b")
    c_con, c_dir = _producer(tmp_path, "c")

    def a(state, f):  # order lines: order_id → product_id
        vals = ", ".join(f"({o}, '{p}', {q})" for o, p, q in state)
        T.merge_table(a_con, "ol", a_con.sql(f"SELECT * FROM (VALUES {vals}) v(order_id, product_id, qty)"),
                      f, ("order_id",))
        publish(a_con, a_dir, f=f)

    def b(state, f):  # product → category
        vals = ", ".join(f"('{p}', '{cat}')" for p, cat in state)
        T.merge_table(b_con, "prod", b_con.sql(f"SELECT * FROM (VALUES {vals}) v(product_id, category)"),
                      f, ("product_id",))
        publish(b_con, b_dir, f=f)

    def c(state, f):  # category → tax (the join key only exists after a⋈b)
        vals = ", ".join(f"('{cat}', {t})" for cat, t in state)
        T.merge_table(c_con, "tax", c_con.sql(f"SELECT * FROM (VALUES {vals}) v(category, tax)"), f, ("category",))
        publish(c_con, c_dir, f=f)

    return (a_con, b_con, c_con), a, b, c


def test_builder_chains_through_materialised_intermediate(tmp_path):
    _cons, a, b, c = _chain_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    out_dir = tmp_path / "ponds" / "o" / "m1" / "data"

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"a": 1, "b": 1, "c": 1}, f=f, previous_f=pf)
        ab = (p.trickle("a.ol").join(p.trickle("b.prod"), on="product_id")
                .select("s0.order_id, s0.product_id, s0.qty, s1.category").merge("ab", pk="order_id"))
        (ab.join(p.trickle("c.tax"), on="category")
            .select("s0.order_id, s0.qty, s0.category, s1.tax").merge("abc", pk="order_id"))
        publish(snk, out_dir, f=f)

    # bootstrap — the is_full of a/b cascades through ab into abc (comprehensive both hops).
    a([(10, "p1", 2), (11, "p2", 1)], ts(1))
    b([("p1", "books"), ("p2", "food")], ts(1))
    c([("books", 10), ("food", 5)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, out_dir, "abc", "order_id, category, tax") == [(10, "books", 10), (11, "food", 5)]
    assert rows(snk, out_dir, "ab", "order_id, category") == [(10, "books"), (11, "food")]  # intermediate stored

    # c-only change (books tax 10→20): ab is unchanged (not recomputed); only order 10 (books) flows to abc.
    c([("books", 20), ("food", 5)], ts(2))
    run(ts(2), ts(1))
    assert rows(snk, out_dir, "ab", "order_id, category") == [(10, "books"), (11, "food")]  # ab untouched
    assert rows(snk, out_dir, "abc", "order_id, category, tax") == [(10, "books", 20), (11, "food", 5)]

    # a-only change (order 11's product p2→p1, i.e. food→books): the change propagates through the stored ab.
    a([(10, "p1", 2), (11, "p1", 1)], ts(3))
    run(ts(3), ts(2))
    assert rows(snk, out_dir, "ab", "order_id, category") == [(10, "books"), (11, "books")]
    assert rows(snk, out_dir, "abc", "order_id, category, tax") == [(10, "books", 20), (11, "books", 20)]
    snk.close()


def test_builder_chain_matches_comprehensive_join(tmp_path):
    """A chained build equals the full 3-way join recomputed from scratch (correctness of the threaded
    in-run delta), across an incremental run."""
    _cons, a, b, c = _chain_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    out_dir = tmp_path / "ponds" / "o" / "m1" / "data"

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"a": 1, "b": 1, "c": 1}, f=f, previous_f=pf)
        ab = (p.trickle("a.ol").join(p.trickle("b.prod"), on="product_id")
                .select("s0.order_id, s0.product_id, s0.qty, s1.category").merge("ab", pk="order_id"))
        (ab.join(p.trickle("c.tax"), on="category")
            .select("s0.order_id, s0.qty, s0.category, s1.tax").merge("abc", pk="order_id"))
        publish(snk, out_dir, f=f)

    a([(10, "p1", 2), (11, "p2", 1), (12, "p3", 4)], ts(1))
    b([("p1", "books"), ("p2", "food"), ("p3", "books")], ts(1))
    c([("books", 10), ("food", 5)], ts(1))
    run(ts(1), NEVER)
    # An incremental run touching all three sources at once.
    a([(10, "p1", 2), (11, "p2", 1), (12, "p3", 9), (13, "p1", 1)], ts(2))  # 12 qty change, 13 new
    b([("p1", "media"), ("p2", "food"), ("p3", "books")], ts(2))            # p1 books→media
    c([("books", 10), ("food", 7), ("media", 3)], ts(2))                    # food tax change, media new
    run(ts(2), ts(1))

    got = rows(snk, out_dir, "abc", "order_id, category, tax")
    # The ground truth: recompute the whole 3-way join from the published source mains.
    ref = duckdb.connect()
    want = sorted(ref.sql(f"""
        SELECT ol.order_id, prod.category, tax.tax
        FROM ({ParquetDataPlane().read_select(tmp_path / 'ponds/a/m1/data', 'ol')}) ol
        JOIN ({ParquetDataPlane().read_select(tmp_path / 'ponds/b/m1/data', 'prod')}) prod USING (product_id)
        JOIN ({ParquetDataPlane().read_select(tmp_path / 'ponds/c/m1/data', 'tax')}) tax USING (category)
    """).fetchall())
    assert got == want
    snk.close()


# ─── builder .append() terminal (monotonic transforms + conflict handling) ───────


def _enriched(tmp_path, snk, snk_dir, f, pf, *, fail_on_conflict=True, log_drops=True, dim_p=0.3):
    """Append-enrich the order-line spine with the product dim (one output row per order_id). ``dim_p=1.0``
    forces the incremental path (skips the change-fraction threshold) so a tiny dim drives a Z-set delta."""
    pond = Pond("e", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
    (pond.trickle("sales.order_line").join(pond.trickle("catalog.product", p=dim_p), on="product_id")
         .select("s0.order_id, s0.product_id, s0.qty, s1.price")
         .append("enriched", pk="order_id", fail_on_conflict=fail_on_conflict, log_drops=log_drops))
    publish(snk, snk_dir, f=f)


def test_builder_append_monotonic_accumulates_history(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "e" / "m1" / "data"

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    _enriched(tmp_path, snk, snk_dir, ts(1), NEVER)               # bootstrap → append both
    assert rows(snk, snk_dir, "enriched", "order_id, price") == [(10, 5), (11, 9)]
    assert T.load_sidecar(snk_dir)["enriched"]["mode"] == "append"

    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3)], ts(2))     # a NEW order (monotonic spine growth)
    _enriched(tmp_path, snk, snk_dir, ts(2), ts(1))
    assert rows(snk, snk_dir, "enriched", "order_id, price") == [(10, 5), (11, 9), (12, 5)]
    snk.close()


def test_builder_append_fails_on_changed_past(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "e" / "m1" / "data"

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    _enriched(tmp_path, snk, snk_dir, ts(1), NEVER)

    pr([("p1", 50), ("p2", 9)], ts(2))   # p1 reprice retracts order 10's enriched row — not append-safe
    with pytest.raises(T.DeltaError, match="not append-safe|retraction|changed-past"):
        _enriched(tmp_path, snk, snk_dir, ts(2), ts(1))
    snk.close()


def test_builder_append_drops_conflicts_and_logs(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "e" / "m1" / "data"

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    _enriched(tmp_path, snk, snk_dir, ts(1), NEVER)

    # p1 reprice, forced down the incremental path (dim_p=1.0) so ΔO carries the retraction explicitly.
    pr([("p1", 50), ("p2", 9)], ts(2))
    _enriched(tmp_path, snk, snk_dir, ts(2), ts(1), fail_on_conflict=False, dim_p=1.0)
    # Order 10 stays frozen at its original enriched price (the past is not rewritten).
    assert rows(snk, snk_dir, "enriched", "order_id, price") == [(10, 5), (11, 9)]
    # Both dropped rows are logged to the __droplog companion: the retraction (−1, old price 5) and the
    # changed-image insert (+1, new price 50).
    drops = snk.sql('SELECT order_id, price, _duckstring_d FROM "enriched__droplog" ORDER BY _duckstring_d').fetchall()
    assert drops == [(10, 5, -1), (10, 50, 1)]
    # ...and the __droplog is published alongside the table (like __changelog) — readable, but not a Trickle
    # base, so it carries no sidecar entry.
    published = ParquetDataPlane().read_select(snk_dir, "enriched__droplog")
    assert sorted(snk.sql(f"SELECT order_id, price, _duckstring_d FROM ({published})").fetchall()) == [
        (10, 5, -1), (10, 50, 1),
    ]
    assert "enriched__droplog" not in T.load_sidecar(snk_dir)

    # A new order still flows through while the conflict is tolerated.
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p2", 4)], ts(3))
    _enriched(tmp_path, snk, snk_dir, ts(3), ts(2), fail_on_conflict=False)
    assert rows(snk, snk_dir, "enriched", "order_id, price") == [(10, 5), (11, 9), (12, 9)]
    snk.close()


def test_builder_append_log_drops_false_squashes_table(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "e" / "m1" / "data"

    ol([(10, "p1", 2)], ts(1))
    pr([("p1", 5)], ts(1))
    _enriched(tmp_path, snk, snk_dir, ts(1), NEVER, fail_on_conflict=False, log_drops=False)
    pr([("p1", 50)], ts(2))
    _enriched(tmp_path, snk, snk_dir, ts(2), ts(1), fail_on_conflict=False, log_drops=False)
    assert not _table_exists(snk, "enriched__droplog")
    snk.close()


def _table_exists(con, name):
    return con.execute("SELECT 1 FROM duckdb_tables() WHERE table_name = ?", [name]).fetchone() is not None


# ─── spine-PK append fast path (skip dim deltas when output is keyed by the spine) ───


def test_spine_pk_passthrough_detection():
    from duckstring.trickle_builder import TrickleBuilder

    def detect(projection, out_pk, spine_pk, spine_alias=None):
        b = TrickleBuilder(None, "spine")
        b._ops = [("select", projection)]
        b._alias = spine_alias
        return b._spine_pk_passthrough(out_pk, spine_pk)

    # Verbatim s0 pass-throughs (single, renamed, composite) → detected.
    assert detect("s0.order_id, s1.price", ("order_id",), ("order_id",)) == {"order_id": "order_id"}
    # ...and the same off a custom spine .alias() (the detector keys on the spine's effective alias).
    assert detect("o.order_id, p.price", ("order_id",), ("order_id",), spine_alias="o") == {"order_id": "order_id"}
    assert detect("s0.order_id, p.price", ("order_id",), ("order_id",), spine_alias="o") is None  # s0 isn't the alias
    assert detect("s0.oid AS order_id, s1.price", ("order_id",), ("oid",)) == {"order_id": "oid"}
    assert detect('s0."a", s0.b, s1.x', ("a", "b"), ("a", "b")) == {"a": "a", "b": "b"}
    # A comma inside a computed dim column doesn't break item splitting.
    assert detect("s0.id, round(s1.a, 2) AS r", ("id",), ("id",)) == {"id": "id"}

    # Conservative bails (→ None, falls back to the always-correct general path):
    assert detect("round(s0.x, 2) AS k, s1.y", ("k",), ("x",)) is None    # computed PK
    assert detect("s1.k, s0.v", ("k",), ("k",)) is None                    # PK from a dimension, not s0
    assert detect("s0.order_id, s0.region", ("order_id", "region"), ("order_id",)) is None  # out PK ⊋ spine PK
    assert detect("s0.order_id", ("order_id",), ()) is None                # spine has no declared PK


def test_builder_append_spine_pk_fast_path(tmp_path, monkeypatch):
    from duckstring.trickle_builder import TrickleBuilder

    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "e" / "m1" / "data"

    def run(f, pf):  # fail_on_conflict=False + log_drops=False + s0.order_id PK → fast path eligible
        _enriched(tmp_path, snk, snk_dir, f, pf, fail_on_conflict=False, log_drops=False)

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "enriched", "order_id, price") == [(10, 5), (11, 9)]

    # A dimension-only change touches no spine rows → nothing flows; the past stays frozen.
    pr([("p1", 50), ("p2", 9)], ts(2))
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "enriched", "order_id, price") == [(10, 5), (11, 9)]

    # A new spine row arriving the same run a dim reprices is enriched with the *current* dim value.
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3)], ts(3))
    pr([("p1", 70), ("p2", 9)], ts(3))
    run(ts(3), ts(2))
    assert rows(snk, snk_dir, "enriched", "order_id, price") == [(10, 5), (11, 9), (12, 70)]

    # Prove the fast path skipped the telescoping ΔO composition entirely: with _compute sabotaged, a
    # new-spine-row run still succeeds (it never calls it).
    def _boom(*a, **k):
        raise AssertionError("_compute should not run on the spine-PK fast path")

    monkeypatch.setattr(TrickleBuilder, "_compute", _boom)
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3), (13, "p2", 1)], ts(4))
    run(ts(4), ts(3))
    assert rows(snk, snk_dir, "enriched", "order_id, price") == [(10, 5), (11, 9), (12, 70), (13, 9)]
    snk.close()


# ─── .alias() / .sql() / .schema() (relational surface, Ibis-aligned) ────────────


def test_builder_alias_names_sources(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "o" / "m1" / "data"

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    pond = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=ts(1), previous_f=NEVER)
    (pond.trickle("sales.order_line").alias("ol")
         .join(pond.trickle("catalog.product").alias("pr"), on="product_id")
         .select("ol.order_id, ol.qty, pr.price, ol.qty * pr.price AS total")
         .merge("priced", pk="order_id"))
    publish(snk, snk_dir, f=ts(1))
    assert rows(snk, snk_dir, "priced", "order_id, total") == [(10, 10), (11, 9)]
    snk.close()


def test_builder_sql_aggregation_keeps_incremental_output(tmp_path):
    """The priced→revenue pattern in one ripple: incremental join cached with .merge(), aggregated with the
    comprehensive .sql() escape, final .merge() — and the delta OUT stays incremental (only changed groups)."""
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "rev" / "m1" / "data"

    def run(f, pf):
        pond = Pond("rev", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        priced = (
            pond.trickle("sales.order_line").alias("o")
                .join(pond.trickle("catalog.product").alias("p"), on="product_id")
                .select("o.order_id, o.product_id, o.qty * p.price AS revenue")
                .merge("priced_line", pk="order_id")
        )
        agg = priced.alias("pl").sql(
            "SELECT product_id, sum(revenue) AS total_revenue, count(*) AS n FROM pl GROUP BY product_id"
        )
        with pytest.raises(BuildError, match="after .sql"):
            agg.join(pond.trickle("catalog.product"), on="product_id")   # no incremental ops post-.sql
        agg.merge("revenue_by_product", pk="product_id")
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    run(ts(1), NEVER)
    # p1: 2*5 + 3*5 = 25 (n=2); p2: 1*9 = 9 (n=1).
    assert rows(snk, snk_dir, "revenue_by_product", "product_id, total_revenue, n") == [("p1", 25, 2), ("p2", 9, 1)]

    pr([("p1", 10), ("p2", 9)], ts(2))   # only p1 reprices
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "revenue_by_product", "product_id, total_revenue, n") == [("p1", 50, 2), ("p2", 9, 1)]
    # Incremental output: only p1 moved → only p1 in this run's changelog window (p2 untouched).
    latest = snk.sql('SELECT DISTINCT product_id FROM "revenue_by_product__changelog" '
                     'WHERE _duckstring_f = (SELECT max(_duckstring_f) FROM "revenue_by_product__changelog")').fetchall()
    assert latest == [("p1",)]
    snk.close()


def test_builder_sql_requires_alias_and_uses_star(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    ol([(10, "p1", 2)], ts(1))
    pr([("p1", 5)], ts(1))
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    pond = Pond("x", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=ts(1), previous_f=NEVER)
    # .sql() still needs an alias to name the table it runs over.
    with pytest.raises(BuildError, match="alias"):
        pond.trickle("sales.order_line").sql("SELECT 1")
    # A joined DAG with no .select() now resolves its columns via the bare `*` (the join key is deduplicated),
    # so .sql() can reference them by name directly — no explicit projection required first.
    snk_dir = tmp_path / "ponds" / "x" / "m1" / "data"
    (pond.trickle("sales.order_line").join(pond.trickle("catalog.product"), on="product_id").alias("j")
         .sql("SELECT order_id, qty * price AS total FROM j").merge("x", pk="order_id"))
    publish(snk, snk_dir, f=ts(1))
    assert rows(snk, snk_dir, "x", "order_id, total") == [(10, 10)]
    snk.close()


def test_duckdb_to_ibis_type_map():
    from duckstring.trickle_builder import _duckdb_to_ibis

    assert _duckdb_to_ibis("BIGINT") == "int64"
    assert _duckdb_to_ibis("VARCHAR") == "string"
    assert _duckdb_to_ibis("DOUBLE") == "float64"
    assert _duckdb_to_ibis("TIMESTAMP WITH TIME ZONE") == "timestamp('UTC')"
    assert _duckdb_to_ibis("DECIMAL(18,2)") == "decimal(18,2)"
    with pytest.raises(BuildError, match="no Ibis mapping"):
        _duckdb_to_ibis("INTEGER[]")


def test_builder_schema_and_to_ibis_schema(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    ol([(10, "p1", 2)], ts(1))
    pr([("p1", 5)], ts(1))
    pond = Pond("o", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=ts(1), previous_f=NEVER)
    priced = (
        pond.trickle("sales.order_line").alias("o")
            .join(pond.trickle("catalog.product").alias("p"), on="product_id")
            .select("CAST(o.order_id AS BIGINT) AS order_id, CAST(o.qty * p.price AS DOUBLE) AS revenue")
            .merge("priced", pk="order_id")
    )
    assert priced.schema() == {"order_id": "BIGINT", "revenue": "DOUBLE"}
    assert priced.to_ibis_schema() == {"order_id": "int64", "revenue": "float64"}
    snk.close()


def test_builder_sql_accepts_ibis_expression(tmp_path):
    ibis = pytest.importorskip("ibis")
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "rev" / "m1" / "data"
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    pond = Pond("rev", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=ts(1), previous_f=NEVER)
    priced = (
        pond.trickle("sales.order_line").alias("o")
            .join(pond.trickle("catalog.product").alias("p"), on="product_id")
            .select("o.order_id, o.product_id, o.qty * p.price AS revenue")
            .merge("priced_line", pk="order_id")
    )
    pl = ibis.table(priced.to_ibis_schema(), name="pl")
    agg = pl.group_by("product_id").aggregate(total_revenue=pl.revenue.sum())
    priced.alias("pl").sql(agg).merge("revenue_by_product", pk="product_id")
    publish(snk, snk_dir, f=ts(1))
    assert rows(snk, snk_dir, "revenue_by_product", "product_id, total_revenue") == [("p1", 25), ("p2", 9)]
    snk.close()


# ─── join types: left / semi / anti (incremental), right / full (comprehensive) ──


def _two_dim_sources(tmp_path):
    """A fact spine (order_id, product_id, store_id) + two dimensions on different spine keys."""
    f_con, f_dir = _producer(tmp_path, "fact")
    p_con, p_dir = _producer(tmp_path, "prod")
    s_con, s_dir = _producer(tmp_path, "store")

    def fact(state, f):
        vals = ", ".join(f"({o}, '{p}', '{s}')" for o, p, s in state)
        T.merge_table(f_con, "f", f_con.sql(f"SELECT * FROM (VALUES {vals}) v(order_id, product_id, store_id)"),
                      f, ("order_id",))
        publish(f_con, f_dir, f=f)

    def prod(state, f):
        vals = ", ".join(f"('{p}', {pr})" for p, pr in state)
        T.merge_table(p_con, "p", p_con.sql(f"SELECT * FROM (VALUES {vals}) v(product_id, price)"), f, ("product_id",))
        publish(p_con, p_dir, f=f)

    def store(state, f):
        vals = ", ".join(f"('{s}', '{r}')" for s, r in state) if state else None
        rel = s_con.sql(f"SELECT * FROM (VALUES {vals}) v(store_id, region)") if vals else \
            s_con.sql("SELECT NULL::VARCHAR AS store_id, NULL::VARCHAR AS region WHERE 1=0")
        T.merge_table(s_con, "s", rel, f, ("store_id",))
        publish(s_con, s_dir, f=f)

    return fact, prod, store


def test_builder_left_join_keeps_unmatched_spine(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "j" / "m1" / "data"

    def run(f, pf):  # p=1.0 → exercise the incremental spine-recompute (not the comprehensive fallback)
        pond = Pond("j", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0).alias("o")
             .join(pond.trickle("catalog.product", p=1.0).alias("p"), on="product_id", how="left")
             .select("o.order_id, p.price")
             .merge("j", pk="order_id"))
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "pX", 1)], ts(1))   # pX has no product row
    pr([("p1", 5), ("p2", 9)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "j", "order_id, price") == [(10, 5), (11, None)]   # 11 kept, NULL price
    pr([("p1", 5), ("p2", 9), ("pX", 7)], ts(2))     # pX appears → 11 flips matched
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "j", "order_id, price") == [(10, 5), (11, 7)]
    pr([("p1", 50), ("p2", 9), ("pX", 7)], ts(3))    # p1 reprice → 10 updates
    run(ts(3), ts(2))
    assert rows(snk, snk_dir, "j", "order_id, price") == [(10, 50), (11, 7)]
    ol([(10, "p1", 2), (11, "pX", 1), (12, "pZ", 1)], ts(4))   # new unmatched spine row
    run(ts(4), ts(3))
    assert rows(snk, snk_dir, "j", "order_id, price") == [(10, 50), (11, 7), (12, None)]
    snk.close()


def test_builder_semi_join_filters_spine(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "j" / "m1" / "data"

    def run(f, pf):
        pond = Pond("j", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0).alias("o")
             .join(pond.trickle("catalog.product", p=1.0).alias("p"), on="product_id", how="semi")
             .select("o.order_id, o.qty")
             .merge("j", pk="order_id"))
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "pX", 1)], ts(1))
    pr([("p1", 5)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "j", "order_id, qty") == [(10, 2)]      # 11 (pX, no match) filtered out
    pr([("p1", 5), ("pX", 7)], ts(2))                                  # pX appears → 11 enters
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "j", "order_id, qty") == [(10, 2), (11, 1)]
    pr([("p1", 5)], ts(3))                                             # pX removed → 11 leaves
    run(ts(3), ts(2))
    assert rows(snk, snk_dir, "j", "order_id, qty") == [(10, 2)]
    snk.close()


def test_builder_anti_join_keeps_unmatched(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "j" / "m1" / "data"

    def run(f, pf):
        pond = Pond("j", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0).alias("o")
             .join(pond.trickle("catalog.product", p=1.0).alias("p"), on="product_id", how="anti")
             .select("o.order_id, o.qty")
             .merge("j", pk="order_id"))
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "pX", 1)], ts(1))
    pr([("p1", 5), ("pX", 7)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "j", "order_id, qty") == []   # both match → anti empty
    pr([("p1", 5)], ts(2))                                  # pX removed → 11 has no match → enters anti
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "j", "order_id, qty") == [(11, 1)]
    snk.close()


def test_builder_right_and_full_outer_join_comprehensive(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    rdir = tmp_path / "ponds" / "r" / "m1" / "data"
    fdir = tmp_path / "ponds" / "fo" / "m1" / "data"
    ol([(10, "p1", 2)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))   # p2 has no order

    pr_pond = Pond("r", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=ts(1), previous_f=NEVER)
    (pr_pond.trickle("sales.order_line").alias("o")
            .join(pr_pond.trickle("catalog.product").alias("p"), on="product_id", how="right")
            .select("p.product_id, o.order_id").merge("r", pk="product_id"))
    publish(snk, rdir, f=ts(1))
    assert rows(snk, rdir, "r", "product_id, order_id") == [("p1", 10), ("p2", None)]   # p2 kept, NULL order

    fo_pond = Pond("fo", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=ts(1), previous_f=NEVER)
    ol([(10, "p1", 2), (11, "pX", 1)], ts(1))   # pX has no product; p2 has no order
    (fo_pond.trickle("sales.order_line").alias("o")
            .join(fo_pond.trickle("catalog.product").alias("p"), on="product_id", how="full")
            .select("coalesce(o.product_id, p.product_id) AS pid, o.order_id, p.price").merge("fo", pk="pid"))
    publish(snk, fdir, f=ts(1))
    assert rows(snk, fdir, "fo", "pid, order_id, price") == [("p1", 10, 5), ("p2", None, 9), ("pX", 11, None)]
    snk.close()


def test_builder_mixed_left_inner_star(tmp_path):
    fact, prod, store = _two_dim_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "m" / "m1" / "data"

    def run(f, pf):
        pond = Pond("m", "1.0.0", snk, root=tmp_path,
                    source_majors={"fact": 1, "prod": 1, "store": 1}, f=f, previous_f=pf)
        (pond.trickle("fact.f", p=1.0).alias("f")
             .join(pond.trickle("prod.p", p=1.0).alias("p"), on="product_id", how="left")
             .join(pond.trickle("store.s", p=1.0).alias("s"), on="store_id", how="inner")
             .select("f.order_id, p.price, s.region")
             .merge("m", pk="order_id"))
        publish(snk, snk_dir, f=f)

    fact([(10, "p1", "sA"), (11, "pX", "sA")], ts(1))   # pX unmatched in prod (left → NULL)
    prod([("p1", 5)], ts(1))
    store([("sA", "north")], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "m", "order_id, price, region") == [(10, 5, "north"), (11, None, "north")]
    store([("sA", "south")], ts(2))   # inner-dim change → both orders re-region
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "m", "order_id, price, region") == [(10, 5, "south"), (11, None, "south")]
    prod([("p1", 5), ("pX", 7)], ts(3))   # left-dim gains pX → order 11 now priced
    run(ts(3), ts(2))
    assert rows(snk, snk_dir, "m", "order_id, price, region") == [(10, 5, "south"), (11, 7, "south")]
    store([], ts(4))   # store emptied → inner join drops everything
    run(ts(4), ts(3))
    assert rows(snk, snk_dir, "m", "order_id, price, region") == []
    snk.close()


def test_builder_join_guards(tmp_path):
    snk = duckdb.connect()
    pond = Pond("x", "1.0.0", snk, root=tmp_path, source_majors={"a": 1, "b": 1, "c": 1}, f=ts(1))
    with pytest.raises(BuildError, match="how"):
        pond.trickle("a.t").join(pond.trickle("b.u"), on="k", how="cross")
    # A right/full outer join is no longer restricted to a solo join — it can sit anywhere in a DAG
    # (the v2 affected-key recompute maintains the incomparables per node). Composing it raises no error:
    pond.trickle("a.t").join(pond.trickle("b.u"), on="k", how="full").join(pond.trickle("c.v"), on="k")
    # A .sql()/.aggregate() result can't be a join operand (no incremental compute left).
    with pytest.raises(BuildError, match="operand"):
        from duckstring import agg
        pond.trickle("a.t").join(pond.trickle("b.u").aggregate(by="k", n=agg.count()), on="k")
    snk.close()


# ─── DAG composition: bushy trees + incremental outer joins (v2) ─────────────────


def _four_sources(tmp_path):
    """A 4-leaf DAG: orders(order_id,cust_id,prod_id), customers(cust_id,region), products(prod_id,cat_id),
    categories(cat_id,tax) — for a bushy (orders⋈customers)⋈(products⋈categories)."""
    a_con, a_dir = _producer(tmp_path, "a")
    b_con, b_dir = _producer(tmp_path, "b")
    c_con, c_dir = _producer(tmp_path, "c")
    d_con, d_dir = _producer(tmp_path, "d")

    def a(state, f):
        vals = ", ".join(f"({o},'{cu}','{pr}')" for o, cu, pr in state)
        T.merge_table(a_con, "orders", a_con.sql(f"SELECT * FROM (VALUES {vals}) v(order_id, cust_id, prod_id)"),
                      f, ("order_id",))
        publish(a_con, a_dir, f=f)

    def b(state, f):
        vals = ", ".join(f"('{cu}','{r}')" for cu, r in state)
        T.merge_table(b_con, "customers", b_con.sql(f"SELECT * FROM (VALUES {vals}) v(cust_id, region)"),
                      f, ("cust_id",))
        publish(b_con, b_dir, f=f)

    def c(state, f):
        vals = ", ".join(f"('{pr}','{ca}')" for pr, ca in state)
        T.merge_table(c_con, "products", c_con.sql(f"SELECT * FROM (VALUES {vals}) v(prod_id, cat_id)"),
                      f, ("prod_id",))
        publish(c_con, c_dir, f=f)

    def d(state, f):
        vals = ", ".join(f"('{ca}',{t})" for ca, t in state)
        T.merge_table(d_con, "categories", d_con.sql(f"SELECT * FROM (VALUES {vals}) v(cat_id, tax)"),
                      f, ("cat_id",))
        publish(d_con, d_dir, f=f)

    return a, b, c, d


def _bushy_truth(tmp_path):
    ref = duckdb.connect()
    pp = ParquetDataPlane()
    return sorted(ref.sql(f"""
        SELECT orders.order_id, customers.region, categories.tax
        FROM ({pp.read_select(tmp_path / 'ponds/a/m1/data', 'orders')}) orders
        JOIN ({pp.read_select(tmp_path / 'ponds/b/m1/data', 'customers')}) customers USING (cust_id)
        JOIN ({pp.read_select(tmp_path / 'ponds/c/m1/data', 'products')}) products USING (prod_id)
        JOIN ({pp.read_select(tmp_path / 'ponds/d/m1/data', 'categories')}) categories USING (cat_id)
    """).fetchall())


def test_builder_bushy_join_matches_comprehensive(tmp_path):
    """(A⋈B)⋈(C⋈D): a bushy DAG with no privileged spine — composed operands on both sides of the top
    join. The incremental result equals a full recompute across a run that changes a leaf in each subtree."""
    a, b, c, d = _four_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    out_dir = tmp_path / "ponds" / "o" / "m1" / "data"

    def run(f, pf):
        p = Pond("o", "1.0.0", snk, root=tmp_path,
                 source_majors={"a": 1, "b": 1, "c": 1, "d": 1}, f=f, previous_f=pf)
        ab = (p.trickle("a.orders", p=1.0).alias("o")
               .join(p.trickle("b.customers", p=1.0).alias("cu"), on="cust_id"))
        cd = (p.trickle("c.products", p=1.0).alias("pr")
               .join(p.trickle("d.categories", p=1.0).alias("g"), on="cat_id"))
        (ab.join(cd, on="prod_id").select("o.order_id, cu.region, g.tax").merge("res", pk="order_id"))
        publish(snk, out_dir, f=f)

    a([(1, "cA", "pX"), (2, "cB", "pY")], ts(1))
    b([("cA", "north"), ("cB", "south")], ts(1))
    c([("pX", "books"), ("pY", "food")], ts(1))
    d([("books", 10), ("food", 5)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, out_dir, "res", "order_id, region, tax") == [(1, "north", 10), (2, "south", 5)]

    # A change in each subtree at once: new order (A), a region update (B), a tax update (D).
    a([(1, "cA", "pX"), (2, "cB", "pY"), (3, "cA", "pY")], ts(2))
    b([("cA", "NORTH"), ("cB", "south")], ts(2))
    d([("books", 12), ("food", 5)], ts(2))
    run(ts(2), ts(1))
    assert rows(snk, out_dir, "res", "order_id, region, tax") == _bushy_truth(tmp_path)
    assert rows(snk, out_dir, "res", "order_id, region, tax") == [
        (1, "NORTH", 12), (2, "south", 5), (3, "NORTH", 5)]
    snk.close()


def test_builder_right_join_incremental(tmp_path):
    """A ``right`` join maintained incrementally (no longer solo + comprehensive): B-side incomparables are
    kept and their first-match / last-match / new-unmatched transitions tracked across runs."""
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    rdir = tmp_path / "ponds" / "r" / "m1" / "data"

    def run(f, pf):
        p = Pond("r", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (p.trickle("sales.order_line", p=1.0).alias("o")
          .join(p.trickle("catalog.product", p=1.0).alias("p"), on="product_id", how="right")
          .select("p.product_id, o.order_id").merge("r", pk="product_id"))
        publish(snk, rdir, f=f)

    ol([(10, "p1", 2)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))   # p2 unmatched (B-side incomparable)
    run(ts(1), NEVER)
    assert rows(snk, rdir, "r", "product_id, order_id") == [("p1", 10), ("p2", None)]

    ol([(10, "p1", 2), (12, "p2", 1)], ts(2))   # p2 gains its first order → incomparable retracted
    pr([("p1", 5), ("p2", 9), ("p3", 7)], ts(2))  # p3 appears unmatched → new incomparable
    run(ts(2), ts(1))
    assert rows(snk, rdir, "r", "product_id, order_id") == [("p1", 10), ("p2", 12), ("p3", None)]

    ol([(10, "p1", 2)], ts(3))   # p2's order removed → p2 back to incomparable (last match dropped)
    run(ts(3), ts(2))
    assert rows(snk, rdir, "r", "product_id, order_id") == [("p1", 10), ("p2", None), ("p3", None)]
    snk.close()


def test_builder_full_join_incremental_matches_comprehensive(tmp_path):
    """A ``full`` join maintained incrementally — both sides' incomparables — equals a comprehensive full
    outer join recompute across an incremental run."""
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    fdir = tmp_path / "ponds" / "fo" / "m1" / "data"

    def run(f, pf):
        p = Pond("fo", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (p.trickle("sales.order_line", p=1.0).alias("o")
          .join(p.trickle("catalog.product", p=1.0).alias("p"), on="product_id", how="full")
          .select("coalesce(o.product_id, p.product_id) AS pid, o.order_id, p.price").merge("fo", pk="pid"))
        publish(snk, fdir, f=f)

    def truth():
        ref = duckdb.connect()
        pp = ParquetDataPlane()
        return sorted(ref.sql(f"""
            SELECT coalesce(o.product_id, p.product_id) AS pid, o.order_id, p.price
            FROM ({pp.read_select(tmp_path / 'ponds/sales/m1/data', 'order_line')}) o
            FULL JOIN ({pp.read_select(tmp_path / 'ponds/catalog/m1/data', 'product')}) p USING (product_id)
        """).fetchall())

    ol([(10, "p1", 2), (11, "pX", 1)], ts(1))   # pX unmatched (A-side incomparable)
    pr([("p1", 5), ("p2", 9)], ts(1))           # p2 unmatched (B-side incomparable)
    run(ts(1), NEVER)
    assert rows(snk, fdir, "fo", "pid, order_id, price") == truth()

    # pX gains a product (A-incomparable → matched); p2 gains an order (B-incomparable → matched); p1
    # product dropped (order 10 keeps p1 as an A-side incomparable).
    ol([(10, "p1", 2), (11, "pX", 1), (12, "p2", 4)], ts(2))
    pr([("p2", 9), ("pX", 7)], ts(2))
    run(ts(2), ts(1))
    assert rows(snk, fdir, "fo", "pid, order_id, price") == truth()
    snk.close()


# ─── incremental aggregation (distributive: count / sum / mean) ──────────────────


def test_builder_aggregate_distributive_incremental(tmp_path):
    from duckstring import agg

    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "rev" / "m1" / "data"

    def run(f, pf):
        pond = Pond("rev", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0).alias("o")
             .join(pond.trickle("catalog.product", p=1.0).alias("p"), on="product_id")
             .select("o.product_id, o.qty * p.price AS revenue, o.qty AS units")
             .aggregate(by="product_id",
                        total_revenue=agg.sum("revenue"),
                        units_sold=agg.sum("units"),
                        n=agg.count(),
                        avg_revenue=agg.mean("revenue"))
             .merge("revenue_by_product"))   # pk defaults to product_id
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    run(ts(1), NEVER)   # bootstrap (comprehensive)
    # p1: orders 10,12 → revenue 10+15=25, units 5, n 2, avg 12.5 ; p2: revenue 9, units 1, n 1, avg 9
    assert rows(snk, snk_dir, "revenue_by_product",
                "product_id, total_revenue, units_sold, n, avg_revenue") == [
        ("p1", 25, 5, 2, 12.5), ("p2", 9, 1, 1, 9.0)]
    assert T.load_sidecar(snk_dir)["revenue_by_product"]["mode"] == "merge"

    # A NEW order for p2 (spine delta) — only p2's group recomputes, incrementally.
    ol([(10, "p1", 2), (11, "p2", 1), (12, "p1", 3), (13, "p2", 4)], ts(2))
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "revenue_by_product",
                "product_id, total_revenue, units_sold, n, avg_revenue") == [
        ("p1", 25, 5, 2, 12.5), ("p2", 45, 5, 2, 22.5)]   # p2: rev 9+36=45, units 5, n 2, avg 22.5
    # Incremental output: only p2 moved → only p2 in this run's changelog window.
    latest = snk.sql('SELECT DISTINCT product_id FROM "revenue_by_product__changelog" '
                     'WHERE _duckstring_f = (SELECT max(_duckstring_f) FROM "revenue_by_product__changelog")').fetchall()
    assert latest == [("p2",)]

    # A dimension change (p1 reprice) — only p1's group recomputes.
    pr([("p1", 10), ("p2", 9)], ts(3))
    run(ts(3), ts(2))
    assert rows(snk, snk_dir, "revenue_by_product", "product_id, total_revenue") == [("p1", 50), ("p2", 45)]
    snk.close()


def test_builder_aggregate_group_emptied(tmp_path):
    from duckstring import agg

    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "rev" / "m1" / "data"

    def run(f, pf):
        pond = Pond("rev", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0).alias("o")
             .join(pond.trickle("catalog.product", p=1.0).alias("p"), on="product_id")
             .select("o.product_id, o.qty * p.price AS revenue")
             .group_by("product_id").aggregate(total=agg.sum("revenue"), n=agg.count())
             .merge("rev"))
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "p2", 1)], ts(1))
    pr([("p1", 5), ("p2", 9)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "rev", "product_id, total, n") == [("p1", 10, 1), ("p2", 9, 1)]
    # Remove p2's only order → its group empties out and is retracted.
    ol([(10, "p1", 2)], ts(2))
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "rev", "product_id, total, n") == [("p1", 10, 1)]
    snk.close()


def test_builder_aggregate_min_max_extend_and_rescan(tmp_path):
    from duckstring import agg

    _cons, ol, _pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "ext" / "m1" / "data"

    def run(f, pf):  # bare-source aggregate (no join) → min/max of qty per product
        pond = Pond("ext", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0)
             .aggregate(by="product_id", max_qty=agg.max("qty"), min_qty=agg.min("qty"), n=agg.count())
             .merge("ext"))
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "p1", 5), (12, "p2", 3)], ts(1))
    run(ts(1), NEVER)
    assert rows(snk, snk_dir, "ext", "product_id, max_qty, min_qty, n") == [("p1", 5, 2, 2), ("p2", 3, 3, 1)]

    # Pure insert → extend in place (no rescan): p1 gains a qty=1, min drops to 1.
    ol([(10, "p1", 2), (11, "p1", 5), (12, "p2", 3), (13, "p1", 1)], ts(2))
    run(ts(2), ts(1))
    assert rows(snk, snk_dir, "ext", "product_id, max_qty, min_qty, n") == [("p1", 5, 1, 3), ("p2", 3, 3, 1)]

    # Retraction of the supporting max (order 11 qty 5→3) → p1 rescans: max falls to 3.
    ol([(10, "p1", 2), (11, "p1", 3), (12, "p2", 3), (13, "p1", 1)], ts(3))
    run(ts(3), ts(2))
    assert rows(snk, snk_dir, "ext", "product_id, max_qty, min_qty, n") == [("p1", 3, 1, 3), ("p2", 3, 3, 1)]
    snk.close()


def test_builder_aggregate_var_stddev(tmp_path):
    from duckstring import agg

    _cons, ol, _pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "stat" / "m1" / "data"

    def run(f, pf):
        pond = Pond("stat", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0)
             .aggregate(by="product_id",
                        v=agg.var("qty"), sd=agg.stddev("qty"), vp=agg.var("qty", how="pop"))
             .merge("stat"))
        publish(snk, snk_dir, f=f)

    ol([(10, "p1", 2), (11, "p1", 4), (12, "p1", 6)], ts(1))
    run(ts(1), NEVER)   # qtys 2,4,6: sample var 4, stddev 2, pop var 8/3
    got = snk.sql(f"SELECT v, sd, vp FROM ({ParquetDataPlane().read_select(snk_dir, 'stat')})").fetchone()
    assert got[0] == pytest.approx(4.0) and got[1] == pytest.approx(2.0) and got[2] == pytest.approx(8 / 3)

    # Incremental insert (centred-moment merge-in): qtys 2,4,6,8 → sample var 20/3.
    ol([(10, "p1", 2), (11, "p1", 4), (12, "p1", 6), (13, "p1", 8)], ts(2))
    run(ts(2), ts(1))
    assert snk.sql(f"SELECT v FROM ({ParquetDataPlane().read_select(snk_dir, 'stat')})").fetchone()[0] \
        == pytest.approx(20 / 3)
    snk.close()


def test_builder_aggregate_variance_numerical_stability(tmp_path):
    """The centred-moment (Chan/Pébay) maintenance stays accurate where the naive ``Σx² − (Σx)²/n`` form
    catastrophically cancels: a large offset with a tiny spread, grown incrementally across inserts **and**
    a retraction (a merge that changes a value = −old +new). The incremental result must match DuckDB's own
    stable ``var_samp`` over the reconstructed full set."""
    from duckstring import agg

    _cons, ol, _pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "stat" / "m1" / "data"
    base = 1_000_000_000  # a billion-ish offset: Σx² dwarfs the variance signal

    def run(f, pf):
        pond = Pond("stat", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1}, f=f, previous_f=pf)
        (pond.trickle("sales.order_line", p=1.0)   # force the incremental merge path on every non-bootstrap run
             .aggregate(by="product_id", v=agg.var("qty"))
             .merge("stat"))
        publish(snk, snk_dir, f=f)

    def expected(rows):
        vals = ", ".join(f"({q})" for _id, _p, q in rows)
        return snk.sql(f"SELECT var_samp(q) FROM (VALUES {vals}) t(q)").fetchone()[0]

    def got():
        return snk.sql(f"SELECT v FROM ({ParquetDataPlane().read_select(snk_dir, 'stat')})").fetchone()[0]

    r1 = [(i, "p1", base + d) for i, d in enumerate([1, 2, 3, 4, 5])]
    ol(r1, ts(1))
    run(ts(1), NEVER)
    assert got() == pytest.approx(expected(r1), rel=1e-6)

    # Incremental inserts — same tiny spread, more rows: the running M2 must not have lost precision.
    r2 = r1 + [(i, "p1", base + d) for i, d in enumerate([6, 7, 8, 9, 10], start=5)]
    ol(r2, ts(2))
    run(ts(2), ts(1))
    assert got() == pytest.approx(expected(r2), rel=1e-6)

    # A retraction: change one value (merge diffs to −old +new) — exercises the merge-out branch.
    r3 = [(0, "p1", base + 100)] + r2[1:]
    ol(r3, ts(3))
    run(ts(3), ts(2))
    assert got() == pytest.approx(expected(r3), rel=1e-6)
    snk.close()


def test_builder_aggregate_weighted_and_comoment(tmp_path):
    """Phase-1 breadth: the weighted family (weight_total / weighted_sum / weighted_average) and the paired
    co-moments (covariance / correlation / ols_slope / ols_intercept), maintained incrementally across a
    bootstrap, an insert, and a retraction — checked against DuckDB's native aggregates over the full set."""
    from duckstring import agg

    fc = duckdb.connect(str(tmp_path / "facts.duckdb"))
    fc_dir = tmp_path / "ponds" / "facts" / "m1" / "data"
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "stat" / "m1" / "data"

    def fct(rows, f):
        vals = ", ".join(f"({i}, '{g}', {x}, {y}, {w})" for i, g, x, y, w in rows)
        T.merge_table(fc, "f", fc.sql(f"SELECT * FROM (VALUES {vals}) v(id, g, x, y, w)"), f, ("id",))
        publish(fc, fc_dir, f=f)

    def run(f, pf):
        pond = Pond("stat", "1.0.0", snk, root=tmp_path, source_majors={"facts": 1}, f=f, previous_f=pf)
        (pond.trickle("facts.f", p=1.0)   # p=1.0 → force the incremental merge path on every non-bootstrap run
             .aggregate(by="g",
                        wt=agg.weight_total("w"), ws=agg.weighted_sum("x", "w"), wa=agg.weighted_average("x", "w"),
                        cov=agg.covariance("x", "y"), cor=agg.pearson_correlation("x", "y"),
                        slope=agg.ols_slope("x", "y"), icpt=agg.ols_intercept("x", "y"))
             .merge("stat"))
        publish(snk, snk_dir, f=f)

    def expected(rows, g):
        vals = ", ".join(f"({x}, {y}, {w})" for _i, gg, x, y, w in rows if gg == g)
        return snk.sql(
            f"SELECT sum(w), sum(w*x), sum(w*x)/sum(w), covar_samp(y, x), corr(y, x), "
            f"regr_slope(y, x), regr_intercept(y, x) FROM (VALUES {vals}) t(x, y, w)"
        ).fetchone()

    def got(g):
        return snk.sql(
            f"SELECT wt, ws, wa, cov, cor, slope, icpt "
            f"FROM ({ParquetDataPlane().read_select(snk_dir, 'stat')}) WHERE g = '{g}'"
        ).fetchone()

    r1 = [(1, "a", 1.0, 2.0, 1.0), (2, "a", 2.0, 4.0, 2.0), (3, "a", 3.0, 5.0, 1.0),
          (4, "b", 10.0, 1.0, 3.0), (5, "b", 20.0, 3.0, 2.0), (6, "b", 30.0, 8.0, 1.0)]
    fct(r1, ts(1))
    run(ts(1), NEVER)
    assert got("a") == pytest.approx(expected(r1, "a"))

    # Incremental insert into group 'a'.
    r2 = r1 + [(7, "a", 4.0, 9.0, 2.0)]
    fct(r2, ts(2))
    run(ts(2), ts(1))
    assert got("a") == pytest.approx(expected(r2, "a"))

    # A retraction: change row 2's y and w (merge diffs to −old +new) — the merge-out branch.
    r3 = [(2, "a", 2.0, 4.5, 3.0) if row[0] == 2 else row for row in r2]
    fct(r3, ts(3))
    run(ts(3), ts(2))
    assert got("a") == pytest.approx(expected(r3, "a"))
    assert got("b") == pytest.approx(expected(r3, "b"))
    snk.close()


def test_builder_aggregate_argmax_and_semigroup(tmp_path):
    """Phase-2: payload extremes (argmin/argmax) and the boolean/bitwise semigroups, maintained via the
    rescan-on-retraction path. Includes a retraction of the *supporting* extreme (forcing a group rescan),
    checked against DuckDB's native arg_max / arg_min / bool_or / bit_or over the full set."""
    from duckstring import agg

    fc = duckdb.connect(str(tmp_path / "facts.duckdb"))
    fc_dir = tmp_path / "ponds" / "facts" / "m1" / "data"
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "stat" / "m1" / "data"

    def fct(rows, f):
        vals = ", ".join(f"({i}, '{g}', {k}, '{lbl}', {flag}, {bits})" for i, g, k, lbl, flag, bits in rows)
        T.merge_table(fc, "f", fc.sql(f"SELECT * FROM (VALUES {vals}) v(id, g, k, lbl, flag, bits)"), f, ("id",))
        publish(fc, fc_dir, f=f)

    def run(f, pf):
        pond = Pond("stat", "1.0.0", snk, root=tmp_path, source_majors={"facts": 1}, f=f, previous_f=pf)
        (pond.trickle("facts.f", p=1.0)
             .aggregate(by="g",
                        top=agg.argmax("lbl", "k"), bot=agg.argmin("lbl", "k"),
                        peak=agg.max("k"), anyflag=agg.bool_or("flag"), allflag=agg.bool_and("flag"),
                        orbits=agg.bit_or("bits"))
             .merge("stat"))
        publish(snk, snk_dir, f=f)

    def expected(rows, g):
        vals = ", ".join(f"({k}, '{lbl}', {flag}, {bits})" for _i, gg, k, lbl, flag, bits in rows if gg == g)
        return snk.sql(
            f"SELECT arg_max(lbl, k), arg_min(lbl, k), max(k), bool_or(flag), bool_and(flag), bit_or(bits) "
            f"FROM (VALUES {vals}) t(k, lbl, flag, bits)"
        ).fetchone()

    def got(g):
        return snk.sql(
            f"SELECT top, bot, peak, anyflag, allflag, orbits "
            f"FROM ({ParquetDataPlane().read_select(snk_dir, 'stat')}) WHERE g = '{g}'"
        ).fetchone()

    r1 = [(1, "a", 10, "lo", False, 1), (2, "a", 30, "hi", True, 2), (3, "a", 20, "mid", False, 4)]
    fct(r1, ts(1))
    run(ts(1), NEVER)
    assert got("a") == expected(r1, "a")   # top='hi' (k=30), bot='lo' (k=10), peak=30, any=T, all=F, bit_or=7

    # Insert a new max — extends the extreme in place (no rescan).
    r2 = r1 + [(4, "a", 40, "top", True, 8)]
    fct(r2, ts(2))
    run(ts(2), ts(1))
    assert got("a") == expected(r2, "a")

    # Retract the supporting max (drop row 4) → the argmax/max/bit_or must rescan the group.
    r3 = r2[:-1]
    fct(r3, ts(3))
    run(ts(3), ts(2))
    assert got("a") == expected(r3, "a")
    snk.close()


def test_builder_accumulate_scan(tmp_path):
    """Phase-3: order-dependent scans via .along(...).accumulate(...).append(...). The running values
    (sum / count / max / first / ema / tema) continue from carried fold-state across an incremental run that
    appends new tail rows. Linear metrics checked vs DuckDB window functions; ema vs an independent Python
    fold."""
    import math

    from duckstring import acc

    ev = duckdb.connect(str(tmp_path / "stream.duckdb"))
    ev_dir = tmp_path / "ponds" / "stream" / "m1" / "data"
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "scored" / "m1" / "data"

    def emit(rows, f):   # append only the NEW rows (the tail) each run
        vals = ", ".join(f"({i}, '{g}', {t}, {x})" for i, g, t, x in rows)
        T.append_table(ev, "ev", ev.sql(f"SELECT * FROM (VALUES {vals}) v(id, g, t, x)"), f, ("id",))
        publish(ev, ev_dir, f=f)

    def run(f, pf):
        pond = Pond("scored", "1.0.0", snk, root=tmp_path, source_majors={"stream": 1}, f=f, previous_f=pf)
        (pond.trickle("stream.ev")
             .along("t")
             .accumulate(by="g",
                         cs=acc.sum("x"), rc=acc.count(), rmax=acc.max("x"), f0=acc.first("x"),
                         e=acc.ema("x", 0.5), td=acc.tema("x", lam=0.1))
             .append("scored", pk="id"))
        publish(snk, snk_dir, f=f)

    def scored():
        return {r[0]: r for r in snk.sql(
            f"SELECT id, g, t, x, cs, rc, rmax, f0, e, td "
            f"FROM ({ParquetDataPlane().read_select(snk_dir, 'scored')})"
        ).fetchall()}

    def ema_ref(all_rows):   # independent per-group folds for ema / tema, keyed by id
        out = {}
        st = {}
        for i, g, t, x in sorted(all_rows, key=lambda r: (r[1], r[2])):
            e_prev, td_prev, t_prev = st.get(g, (None, None, None))
            e = x if e_prev is None else 0.5 * x + 0.5 * e_prev
            if td_prev is None:
                td = x
            else:
                a = 1 - math.exp(-0.1 * (t - t_prev))
                td = a * x + (1 - a) * td_prev
            st[g] = (e, td, t)
            out[i] = (e, td)
        return out

    all_rows = [(1, "a", 1, 10), (2, "a", 2, 20), (3, "a", 3, 30), (4, "b", 1, 5), (5, "b", 2, 15)]
    emit(all_rows, ts(1))
    run(ts(1), NEVER)
    got = scored()
    assert len(got) == 5
    # group a ordered by t: sum 10/30/60, count 1/2/3, max 10/20/30, first=10 throughout
    assert got[1][4:8] == (10, 1, 10, 10) and got[3][4:8] == (60, 3, 30, 10)
    assert got[4][4:8] == (5, 1, 5, 5) and got[5][4:8] == (20, 2, 15, 5)   # b sum 5→20, max 5→15, first=5
    ref = ema_ref(all_rows)
    for i, (e, td) in ref.items():
        assert got[i][8] == pytest.approx(e) and got[i][9] == pytest.approx(td)

    # Incremental run: append new tail rows; the scan must continue from carried state, not restart.
    new = [(6, "a", 4, 40), (7, "b", 3, 25)]
    emit(new, ts(2))
    run(ts(2), ts(1))
    got = scored()
    assert len(got) == 7
    assert got[6][4:8] == (100, 4, 40, 10)   # sum 60+40, count 4, max 40, first still 10 — continued
    assert got[7][4:8] == (45, 3, 25, 5)
    ref = ema_ref(all_rows + new)
    for i in (6, 7):
        assert got[i][8] == pytest.approx(ref[i][0]) and got[i][9] == pytest.approx(ref[i][1])
    # earlier rows are unchanged (append history is frozen)
    assert got[1][4:8] == (10, 1, 10, 10)
    snk.close()


def test_builder_aggregate_product(tmp_path):
    """`agg.product` via the retractable log-sum-exp accumulators (count / n_zero / n_neg / Σ log|x|) — checked
    vs DuckDB's native `product`, across a bootstrap, an insert, and a retraction including a sign flip."""
    from duckstring import agg

    _cons, ol, _pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "stat" / "m1" / "data"

    def run(f, pf):
        pond = Pond("stat", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1}, f=f, previous_f=pf)
        pond.trickle("sales.order_line", p=1.0).aggregate(by="product_id", p=agg.product("qty")).merge("stat")
        publish(snk, snk_dir, f=f)

    def expected(rows, g):
        vals = ", ".join(str(q) for _o, gg, q in rows if gg == g)
        return snk.sql(f"SELECT product(v) FROM (VALUES ({'), ('.join(vals.split(', '))})) t(v)").fetchone()[0]

    def got(g):
        return snk.sql(
            f"SELECT p FROM ({ParquetDataPlane().read_select(snk_dir, 'stat')}) WHERE product_id = '{g}'"
        ).fetchone()[0]

    r1 = [(1, "a", 2), (2, "a", 3), (3, "a", -4)]   # product -24
    ol(r1, ts(1))
    run(ts(1), NEVER)
    assert got("a") == pytest.approx(expected(r1, "a"))

    r2 = r1 + [(4, "a", 5)]   # product -120
    ol(r2, ts(2))
    run(ts(2), ts(1))
    assert got("a") == pytest.approx(expected(r2, "a"))

    # Retract the negative (change row 3 to +4): sign flips, product 2·3·4·5 = 120.
    r3 = [(3, "a", 4) if o == 3 else (o, g, q) for o, g, q in r2]
    ol(r3, ts(3))
    run(ts(3), ts(2))
    assert got("a") == pytest.approx(expected(r3, "a"))
    snk.close()


def test_builder_accumulate_product_and_scan(tmp_path):
    """`acc.product` (running product) and `acc.scan` (a custom fold with JSON-persisted state) across a
    bootstrap and an incremental tail run."""
    from duckstring import acc

    ev = duckdb.connect(str(tmp_path / "stream.duckdb"))
    ev_dir = tmp_path / "ponds" / "stream" / "m1" / "data"
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "scored" / "m1" / "data"

    def emit(rows, f):
        vals = ", ".join(f"({i}, '{g}', {t}, {x})" for i, g, t, x in rows)
        T.append_table(ev, "ev", ev.sql(f"SELECT * FROM (VALUES {vals}) v(id, g, t, x)"), f, ("id",))
        publish(ev, ev_dir, f=f)

    # a custom fold: running max-so-far minus running min-so-far (the running range), state = [min, max].
    def range_fold(state, row):
        x = row["x"]
        lo = x if state[0] is None else min(state[0], x)
        hi = x if state[1] is None else max(state[1], x)
        return [lo, hi], hi - lo

    def run(f, pf):
        pond = Pond("scored", "1.0.0", snk, root=tmp_path, source_majors={"stream": 1}, f=f, previous_f=pf)
        (pond.trickle("stream.ev")
             .along("t")
             .accumulate(by="g", prod=acc.product("x"),
                         rng=acc.scan(range_fold, [None, None], dtype="BIGINT"))
             .append("scored", pk="id"))
        publish(snk, snk_dir, f=f)

    def scored():
        return {r[0]: (r[1], r[2]) for r in snk.sql(
            f"SELECT id, prod, rng FROM ({ParquetDataPlane().read_select(snk_dir, 'scored')})"
        ).fetchall()}

    emit([(1, "a", 1, 2), (2, "a", 2, 5), (3, "a", 3, 3)], ts(1))
    run(ts(1), NEVER)
    got = scored()
    assert got[1] == (2, 0) and got[2] == (10, 3) and got[3] == (30, 3)   # prod 2/10/30; range 0/3/3

    emit([(4, "a", 4, 4)], ts(2))   # continues: prod 30·4=120, range max5-min2=3
    run(ts(2), ts(1))
    got = scored()
    assert got[4] == (120, 3)
    snk.close()


def test_builder_accumulate_fast_path(tmp_path):
    """The SQL-window fast path (all metrics scalar-seed window aggregates): the result must match a
    from-scratch DuckDB window over the full set, and the incremental run must continue from the carried seed
    (not restart) — proving the fast path is correct both at bootstrap and incrementally."""
    from duckstring import acc

    ev = duckdb.connect(str(tmp_path / "stream.duckdb"))
    ev_dir = tmp_path / "ponds" / "stream" / "m1" / "data"
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "scored" / "m1" / "data"

    def emit(rows, f):
        vals = ", ".join(f"({i}, '{g}', {t}, {x})" for i, g, t, x in rows)
        T.append_table(ev, "ev", ev.sql(f"SELECT * FROM (VALUES {vals}) v(id, g, t, x)"), f, ("id",))
        publish(ev, ev_dir, f=f)

    def run(f, pf):
        pond = Pond("scored", "1.0.0", snk, root=tmp_path, source_majors={"stream": 1}, f=f, previous_f=pf)
        (pond.trickle("stream.ev")
             .along("t")
             .accumulate(by="g", cs=acc.sum("x"), rc=acc.count(), mn=acc.min("x"),
                         mx=acc.max("x"), f0=acc.first("x"))
             .append("scored", pk="id"))
        publish(snk, snk_dir, f=f)

    def scored():
        return {r[0]: r[1:] for r in snk.sql(
            f"SELECT id, cs, rc, mn, mx, f0 FROM ({ParquetDataPlane().read_select(snk_dir, 'scored')})"
        ).fetchall()}

    def expected(all_rows):
        vals = ", ".join(f"({i}, '{g}', {t}, {x})" for i, g, t, x in all_rows)
        return {r[0]: r[1:] for r in snk.sql(
            f"SELECT id, sum(x) OVER w, CAST(count(*) OVER w AS BIGINT), min(x) OVER w, max(x) OVER w, "
            f"first_value(x) OVER w FROM (VALUES {vals}) v(id, g, t, x) "
            f"WINDOW w AS (PARTITION BY g ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        ).fetchall()}

    r1 = [(1, "a", 1, 10), (2, "a", 2, 5), (3, "a", 3, 20), (4, "b", 1, 3), (5, "b", 2, 8)]
    emit(r1, ts(1))
    run(ts(1), NEVER)
    assert scored() == expected(r1)

    new = [(6, "a", 4, 2), (7, "b", 3, 1)]
    emit(new, ts(2))
    run(ts(2), ts(1))
    got = scored()
    assert got == expected(r1 + new)
    assert got[6] == (37, 4, 2, 20, 10)   # continued from the seed (sum 35+2, min 2, max 20, first 10)
    snk.close()


def test_builder_accumulate_lag_and_convolution(tmp_path):
    """`acc.prev` / `acc.lag(n)` (FIFO-buffer lag, reaching across the run boundary) and `acc.convolution`
    (FIR over the last K values), checked vs DuckDB's native LAG and a hand-computed convolution."""
    from duckstring import acc

    ev = duckdb.connect(str(tmp_path / "stream.duckdb"))
    ev_dir = tmp_path / "ponds" / "stream" / "m1" / "data"
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "scored" / "m1" / "data"

    def emit(rows, f):
        vals = ", ".join(f"({i}, '{g}', {t}, {x})" for i, g, t, x in rows)
        T.append_table(ev, "ev", ev.sql(f"SELECT * FROM (VALUES {vals}) v(id, g, t, x)"), f, ("id",))
        publish(ev, ev_dir, f=f)

    def run(f, pf):
        pond = Pond("scored", "1.0.0", snk, root=tmp_path, source_majors={"stream": 1}, f=f, previous_f=pf)
        (pond.trickle("stream.ev")
             .along("t")
             .accumulate(by="g", p1=acc.prev("x"), p2=acc.lag("x", 2),
                         smooth=acc.convolution("x", [1, 1, 1]))   # rolling sum-of-3
             .append("scored", pk="id"))
        publish(snk, snk_dir, f=f)

    def scored():
        return {r[0]: (r[1], r[2], r[3]) for r in snk.sql(
            f"SELECT id, p1, p2, smooth FROM ({ParquetDataPlane().read_select(snk_dir, 'scored')})"
        ).fetchall()}

    emit([(1, "a", 1, 10), (2, "a", 2, 20), (3, "a", 3, 30)], ts(1))
    run(ts(1), NEVER)
    got = scored()
    assert got[1] == (None, None, None)        # nothing behind row 1
    assert got[2] == (10, None, None)          # prev=10, lag2 not yet, conv needs 3
    assert got[3] == (20, 10, 60)              # prev=20, lag2=10, conv=10+20+30

    # Incremental tail rows: lag/conv must reach back into run 1's buffer, not restart.
    emit([(4, "a", 4, 40), (5, "a", 5, 50)], ts(2))
    run(ts(2), ts(1))
    got = scored()
    assert got[4] == (30, 20, 90)              # prev=30, lag2=20, conv=20+30+40
    assert got[5] == (40, 30, 120)             # prev=40, lag2=30, conv=30+40+50
    snk.close()


def test_builder_accumulate_merge_retraction(tmp_path):
    """Merge-mode ordered scan (`.accumulate(...).merge(...)`): an edit to a row in a group's *past* re-folds
    that group's suffix (running values from the edit onward all change), and a deletion drops + re-folds —
    neither of which append-mode allows. Checked against a from-scratch fold over the current membership."""
    from duckstring import acc

    ev = duckdb.connect(str(tmp_path / "stream.duckdb"))
    ev_dir = tmp_path / "ponds" / "stream" / "m1" / "data"
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "scored" / "m1" / "data"

    def emit(rows, f):   # a MERGE source — editing/removing a row produces retractions in its delta
        vals = ", ".join(f"({i}, '{g}', {t}, {x})" for i, g, t, x in rows)
        T.merge_table(ev, "ev", ev.sql(f"SELECT * FROM (VALUES {vals}) v(id, g, t, x)"), f, ("id",))
        publish(ev, ev_dir, f=f)

    def run(f, pf):
        pond = Pond("scored", "1.0.0", snk, root=tmp_path, source_majors={"stream": 1}, f=f, previous_f=pf)
        (pond.trickle("stream.ev")
             .along("t").accumulate(by="g", cs=acc.sum("x")).merge("scored", pk="id"))
        publish(snk, snk_dir, f=f)

    def scored():
        return {r[0]: r[1] for r in snk.sql(
            f"SELECT id, cs FROM ({ParquetDataPlane().read_select(snk_dir, 'scored')})"
        ).fetchall()}

    emit([(1, "a", 1, 10), (2, "a", 2, 20), (3, "a", 3, 30)], ts(1))
    run(ts(1), NEVER)
    assert scored() == {1: 10, 2: 30, 3: 60}

    # Tail append (row 4, beyond the high-water): the *fast path* — resume from carried state, O(new),
    # emitting only the new row; rows 1-3 are untouched.
    emit([(1, "a", 1, 10), (2, "a", 2, 20), (3, "a", 3, 30), (4, "a", 4, 40)], ts(2))
    run(ts(2), ts(1))
    assert scored() == {1: 10, 2: 30, 3: 60, 4: 100}

    # Edit row 2 (a past row): 20 -> 25. The past path re-folds the group from t=2 on: 10, 35, 65, 105.
    emit([(1, "a", 1, 10), (2, "a", 2, 25), (3, "a", 3, 30), (4, "a", 4, 40)], ts(3))
    run(ts(3), ts(2))
    assert scored() == {1: 10, 2: 35, 3: 65, 4: 105}

    # Delete row 3: membership {1,2,4}; re-folds to 10, 35, 75 and row 3 is gone.
    emit([(1, "a", 1, 10), (2, "a", 2, 25), (4, "a", 4, 40)], ts(4))
    run(ts(4), ts(3))
    assert scored() == {1: 10, 2: 35, 4: 75}
    snk.close()


def test_builder_agg_reduce_ordered(tmp_path):
    """`agg.reduce` — a custom order-dependent reduction (one value per group, the final fold in `.along`
    order). Here `net = last − first`, so an edit to the *first* row, or a deletion, re-folds the group and
    changes the reduced value (order-independent reductions couldn't express this)."""
    from duckstring import agg

    ev = duckdb.connect(str(tmp_path / "stream.duckdb"))
    ev_dir = tmp_path / "ponds" / "stream" / "m1" / "data"
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "red" / "m1" / "data"

    def emit(rows, f):
        vals = ", ".join(f"({i}, '{g}', {t}, {x})" for i, g, t, x in rows)
        T.merge_table(ev, "ev", ev.sql(f"SELECT * FROM (VALUES {vals}) v(id, g, t, x)"), f, ("id",))
        publish(ev, ev_dir, f=f)

    def net(state, row):   # state = [first-seen value]; output = current − first
        first = state[0] if state[0] is not None else row["x"]
        return [first], row["x"] - first

    def run(f, pf):
        pond = Pond("red", "1.0.0", snk, root=tmp_path, source_majors={"stream": 1}, f=f, previous_f=pf)
        (pond.trickle("stream.ev")
             .along("t").aggregate(by="g", chg=agg.reduce(net, [None])).merge("red"))
        publish(snk, snk_dir, f=f)

    def reduced():
        return {r[0]: r[1] for r in snk.sql(
            f"SELECT g, chg FROM ({ParquetDataPlane().read_select(snk_dir, 'red')})"
        ).fetchall()}

    emit([(1, "a", 1, 10), (2, "a", 2, 20), (3, "a", 3, 30), (6, "b", 1, 5), (7, "b", 2, 8)], ts(1))
    run(ts(1), NEVER)
    assert reduced() == {"a": 20, "b": 3}      # a: 30−10, b: 8−5

    # Tail append a row 4 (t=4, x=50): the tail path resumes from carried state → net 50 − 10 = 40.
    emit([(1, "a", 1, 10), (2, "a", 2, 20), (3, "a", 3, 30), (4, "a", 4, 50), (6, "b", 1, 5), (7, "b", 2, 8)], ts(2))
    run(ts(2), ts(1))
    assert reduced() == {"a": 40, "b": 3}

    # Edit the first row of a (10 -> 4): the past path re-folds → net 50 − 4 = 46; b untouched.
    emit([(1, "a", 1, 4), (2, "a", 2, 20), (3, "a", 3, 30), (4, "a", 4, 50), (6, "b", 1, 5), (7, "b", 2, 8)], ts(3))
    run(ts(3), ts(2))
    assert reduced() == {"a": 46, "b": 3}

    # Delete the last row of a (row 4): membership {1:4, 2:20, 3:30} → 30 − 4 = 26.
    emit([(1, "a", 1, 4), (2, "a", 2, 20), (3, "a", 3, 30), (6, "b", 1, 5), (7, "b", 2, 8)], ts(4))
    run(ts(4), ts(3))
    assert reduced() == {"a": 26, "b": 3}
    snk.close()


def test_builder_accumulate_guards(tmp_path):
    from duckstring import acc

    snk = duckdb.connect()
    pond = Pond("x", "1.0.0", snk, root=tmp_path, source_majors={"a": 1}, f=ts(1))
    with pytest.raises(BuildError, match="order axis|along"):
        pond.trickle("a.t").accumulate(by="g", cs=acc.sum("x"))
    with pytest.raises(BuildError, match="pk"):   # merge mode needs an output pk
        pond.trickle("a.t").along("t").accumulate(by="g", cs=acc.sum("x")).merge("o")
    with pytest.raises(BuildError, match="acc"):
        pond.trickle("a.t").along("t").accumulate(by="g", bad="nope")
    snk.close()


def test_builder_aggregate_guards(tmp_path):
    from duckstring import agg

    snk = duckdb.connect()
    pond = Pond("x", "1.0.0", snk, root=tmp_path, source_majors={"a": 1}, f=ts(1))
    with pytest.raises(BuildError, match="metric"):
        pond.trickle("a.t").aggregate(by="k", bad="not a metric")
    with pytest.raises(BuildError, match="group key|by"):
        pond.trickle("a.t").aggregate(total=agg.sum("x"))
    with pytest.raises(BuildError, match="aggregate"):
        pond.trickle("a.t").aggregate(by="k", n=agg.count()).append("out")
    with pytest.raises(BuildError, match="aggregate"):
        pond.trickle("a.t").aggregate(by="k", n=agg.count()).select("k")
    snk.close()


# ─── coverage-miss absorption (the retention-lag delete bug) ─────────────────────


def test_builder_absorbs_coverage_miss_comprehensively(tmp_path):
    ol_con, ol_dir = _producer(tmp_path, "sales")
    pr_con, pr_dir = _producer(tmp_path, "catalog")

    def order_lines(state, f):
        vals = ", ".join(f"({o}, '{p}')" for o, p in state)
        T.merge_table(ol_con, "order_line",
                      ol_con.sql(f"SELECT * FROM (VALUES {vals}) v(order_id, product_id)"), f, ("order_id",), retain_n=1)
        publish(ol_con, ol_dir, f=f)

    T.merge_table(pr_con, "product", pr_con.sql("SELECT * FROM (VALUES ('p1', 5), ('p2', 9)) v(product_id, price)"),
                  ts(1), ("product_id",))
    publish(pr_con, pr_dir, f=ts(1))
    order_lines([(10, "p1"), (11, "p2")], ts(1))

    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "priced" / "m1" / "data"

    def build(f, previous_f):
        pond = Pond("priced", "1.0.0", snk, root=tmp_path,
                    source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=previous_f)
        (pond.trickle("sales.order_line")
             .join(pond.trickle("catalog.product"), on="product_id")
             .select("s0.order_id, s0.product_id, s1.price").merge("priced", pk="order_id"))
        publish(snk, snk_dir, f=f)

    build(ts(1), NEVER)
    assert rows(snk, snk_dir, "priced", "order_id") == [(10,), (11,)]
    order_lines([(10, "p1")], ts(2))   # 11 removed
    order_lines([(10, "p1")], ts(3))   # advance again so the changelog floor passes ts(1)
    build(ts(3), ts(1))
    assert rows(snk, snk_dir, "priced", "order_id") == [(10,)]  # 11 dropped, not stale
    snk.close()


# ─── retention (lag SLA; correctness never depends on it) ────────────────────────


def test_retention_retain_n_keeps_newest_runs(reg):
    for h, v in [(1, "a"), (2, "b"), (3, "c"), (4, "d")]:
        T.merge_table(reg, "dim", _state(reg, [(1, v)]), ts(h), ("id",), retain_n=2)
    kept = [r[0] for r in reg.sql(f"SELECT DISTINCT {T.F_COL} FROM dim__changelog ORDER BY 1").fetchall()]
    assert kept == [ts(3), ts(4)]


def test_retention_retain_t_drops_old(reg):
    T.merge_table(reg, "dim", _state(reg, [(1, "a")]), ts(1), ("id",), retain_t=timedelta(hours=2))
    T.merge_table(reg, "dim", _state(reg, [(1, "b")]), ts(4), ("id",), retain_t=timedelta(hours=2))
    kept = [r[0] for r in reg.sql(f"SELECT DISTINCT {T.F_COL} FROM dim__changelog ORDER BY 1").fetchall()]
    assert kept == [ts(4)]


def test_append_retention_and_window(reg):
    for h in (1, 2, 3):
        T.append_table(reg, "event", reg.sql(f"SELECT {h} AS id"), ts(h), ("id",), retain_n=2)
    assert sorted(r[0] for r in reg.sql("SELECT id FROM event").fetchall()) == [2, 3]


def test_floor_anchors_at_bootstrap_and_retention_advances_it(reg, tmp_path):
    T.merge_table(reg, "dim", _state(reg, [(1, "a")]), ts(1), ("id",))
    assert T.load_sidecar(publish(reg, tmp_path / "d1", f=ts(1)))["dim"]["floor"] == ts(1).isoformat()
    T.merge_table(reg, "dim", _state(reg, [(1, "b")]), ts(2), ("id",))
    assert T.load_sidecar(publish(reg, tmp_path / "d2", f=ts(2)))["dim"]["floor"] == ts(1).isoformat()
    T.merge_table(reg, "dim", _state(reg, [(1, "c")]), ts(3), ("id",), retain_n=1)
    assert T.load_sidecar(publish(reg, tmp_path / "d3", f=ts(3)))["dim"]["floor"] == ts(3).isoformat()


# ─── a merge Ripple threading through the local runner ───────────────────────────


def _trickle_project(tmp_path: Path) -> Path:
    (tmp_path / "pond.toml").write_text(
        '[pond]\nname = "loud"\nversion = "0.1.0"\n[sources]\nsrc = "1.0.0"\n'
    )
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "pond.py").write_text(textwrap.dedent("""
        from duckstring import ripple

        @ripple
        def loud(pond):
            pond.read_table("src.dim")          # registers the Source as the view `dim`
            pond.merge_table("loud", pond.con.sql("SELECT id, upper(v) AS v FROM dim"), pk="id")
    """))
    (tmp_path / "src" / "puddles.py").write_text(textwrap.dedent("""
        from duckstring import puddle

        @puddle("src.dim")
        def src_dim(p):
            return p.con.sql("SELECT 1 AS id, 'a' AS v UNION ALL SELECT 2 AS id, 'b' AS v")
    """))
    return tmp_path


def test_merge_ripple_via_local_runner(tmp_path):
    project = load_project(_trickle_project(tmp_path))
    hydrate(project)
    result = run_pond(project)
    assert result.ok, [r.error for r in result.ripples if r.status != "ok"]

    out = project.out_dir
    _sc = T.load_sidecar(out)["loud"]
    assert _sc["mode"] == "merge" and _sc["pk"] == ["id"] and _sc["floor"] is not None
    con = duckdb.connect()
    assert rows(con, out, "loud", "id, v") == [(1, "A"), (2, "B")]
    # The main is log-structured: the bootstrap state is the changelog (no base yet, no checkpoint).
    assert con.sql(
        f"SELECT count(*) FROM ({ParquetDataPlane().read_select(out, 'loud__changelog')})"
    ).fetchone()[0] == 2  # the two initial rows, as +1 changelog entries


# ─── incremental draw (cross-Catchment transfer window) ──────────────────────────


def test_incremental_draw_window_roundtrip(tmp_path):
    """A merge changelog publishes as per-run parts (`dim__changelog/{f}.parquet`); a draw ships only the
    parts newer than what the consumer has landed, and the consumer drops them into its parts directory."""
    import shutil

    prod, cons = tmp_path / "prod", tmp_path / "cons"
    con = duckdb.connect()

    def producer_run(pairs, hour):
        vals = ", ".join(f"({i}, '{v}')" for i, v in pairs)
        T.merge_table(con, "dim", con.sql(f"SELECT * FROM (VALUES {vals}) t(id, v)"), ts(hour), ("id",))
        ParquetDataPlane().export(con, prod, f=ts(hour))

    producer_run([(1, "a"), (2, "b")], 1)
    producer_run([(1, "A"), (3, "c")], 2)

    # Initial (wholesale) draw: the consumer copies everything the producer has published.
    shutil.copytree(prod, cons)
    assert T.landed_after(cons) == ts(2).isoformat()

    producer_run([(1, "A"), (3, "C"), (4, "d")], 3)

    # Incremental draw: the merge main is log-structured (no base file until a checkpoint), so only the new
    # changelog parts move — here just the ts(3) part.
    after = datetime.fromisoformat(T.landed_after(cons))
    shipped = [n for n in T.table_parts(prod, "dim__changelog") if T.part_f(n) > after]
    assert [T.part_f(n) for n in shipped] == [ts(3)]
    for n in shipped:
        shutil.copy(prod / "dim__changelog" / n, cons / "dim__changelog" / n)

    rcon = duckdb.connect()
    rcon.execute("SET TimeZone='UTC'")
    d = T.read_delta(rcon, cons, "dim", previous_f=ts(2), f=ts(3), dp=ParquetDataPlane())
    assert sorted(d.upserts.fetchall()) == [(3, "C"), (4, "d")]
    assert d.deletes.fetchall() == []
    # The reconstructed current state on the consumer reflects the whole history landed via parts.
    assert rows(rcon, cons, "dim", "id, v") == [(1, "A"), (3, "C"), (4, "d")]


def test_landed_after_bootstrap_is_none(tmp_path):
    assert T.landed_after(tmp_path) is None


# ─── No-change signal: write return values + builder.was_changed() (no-change-skip.md) ─────


def test_write_return_values_signal_change(reg):
    # append_table: new rows → True; empty relation → False; same-f replay → False.
    assert T.append_table(reg, "ev", reg.sql("SELECT 1 AS id"), ts(1), ("id",)) is True
    empty = reg.sql("SELECT CAST(NULL AS INTEGER) AS id WHERE 1=0")
    assert T.append_table(reg, "ev", empty, ts(2), ("id",)) is False
    assert T.append_table(reg, "ev", reg.sql("SELECT 1 AS id"), ts(1), ("id",)) is False  # replay

    # merge_table: a real change → True; re-merging the identical state → False.
    assert T.merge_table(reg, "dim", _state(reg, [(1, "a"), (2, "b")]), ts(1), ("id",)) is True
    assert T.merge_table(reg, "dim", _state(reg, [(1, "a"), (2, "b")]), ts(2), ("id",)) is False
    assert T.merge_table(reg, "dim", _state(reg, [(1, "A"), (2, "b")]), ts(3), ("id",)) is True


def test_builder_was_changed_tracks_output(tmp_path):
    _cons, ol, pr = _star_sources(tmp_path)
    snk = duckdb.connect(str(tmp_path / "snk.duckdb"))
    snk_dir = tmp_path / "ponds" / "f" / "m1" / "data"

    def run(f, pf):
        pond = Pond("f", "1.0.0", snk, root=tmp_path, source_majors={"sales": 1, "catalog": 1}, f=f, previous_f=pf)
        out = (pond.trickle("sales.order_line", p=1.0)
                   .join(pond.trickle("catalog.product", p=1.0), on="product_id")
                   .select("s0.order_id, s0.qty * s1.price AS total")
                   .merge("f", pk="order_id"))
        publish(snk, snk_dir, f=f)
        return out.was_changed()

    ol([(10, "p1", 2)], ts(1))
    pr([("p1", 5)], ts(1))
    assert run(ts(1), NEVER) is True            # bootstrap writes output
    assert run(ts(2), ts(1)) is False           # nothing upstream changed → empty ΔO, no change
    pr([("p1", 9)], ts(3))                       # reprice → output total changes
    assert run(ts(3), ts(2)) is True
    snk.close()
