from __future__ import annotations

import duckstring
from duckstring import ripple
from duckstring.core import Catchment, Pond


def test_ripple_exported():
    assert hasattr(duckstring, "ripple")


def test_ripple_plain_decorator():
    @ripple
    def load(pond):
        pass

    assert load is not None
    assert callable(load)


def test_ripple_plain_decorator_returns_function():
    def original(pond):
        return 42

    wrapped = ripple(original)
    assert wrapped is original


def test_ripple_with_args():
    @ripple
    def load(pond):
        pass

    @ripple(parents=[load])
    def clean(pond):
        pass

    assert callable(clean)


def test_ripple_with_name_arg():
    @ripple(name="custom", parents=[])
    def _fn(pond):
        pass

    assert callable(_fn)


def test_stub_classes_importable():
    assert Catchment is not None
    assert Pond is not None


def test_previous_f_defaults_to_never():
    """pond.previous_f defaults to NEVER (the bracket (previous_f, f] reads everything on a first run);
    an explicit value is stored verbatim."""
    import duckdb

    from duckstring.core import Pond
    from duckstring.engine.core import NEVER

    con = duckdb.connect()
    assert Pond("p", "1.0.0", con, root=".").previous_f == NEVER
    from datetime import datetime, timezone
    prev = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert Pond("p", "1.0.0", con, root=".", previous_f=prev).previous_f == prev
    con.close()


def test_read_table_registers_source_view(tmp_path):
    """A foreign read_table registers the Source table as a view under its own name, so SQL can
    say `FROM table` directly — no Python frame scanning (unreliable in the threaded executor)."""
    import duckdb

    from duckstring.core import Pond

    data_dir = tmp_path / "ponds" / "src" / "m1" / "data"
    data_dir.mkdir(parents=True)
    duckdb.sql("SELECT 1 AS id, 'a' AS val").write_parquet(str(data_dir / "event.parquet"))

    con = duckdb.connect()
    pond = Pond(name="snk", version="1.0.0", con=con, root=tmp_path, source_majors={"src": 1})
    rel = pond.read_table("src.event")
    assert rel.fetchall() == [(1, "a")]
    # The registered view — referenced by table name, not by the Python variable.
    assert con.sql("SELECT val FROM event WHERE id = 1").fetchall() == [("a",)]
    con.close()


def test_read_table_view_yields_to_own_table(tmp_path):
    """If the Pond already owns a table with the Source table's name, the view registration is
    skipped (loudly clashing names can't silently shadow own data) — the relation still works."""
    import duckdb

    from duckstring.core import Pond

    data_dir = tmp_path / "ponds" / "src" / "m1" / "data"
    data_dir.mkdir(parents=True)
    duckdb.sql("SELECT 99 AS id").write_parquet(str(data_dir / "event.parquet"))

    con = duckdb.connect()
    con.execute("CREATE TABLE event (id INT)")
    con.execute("INSERT INTO event VALUES (1)")
    pond = Pond(name="snk", version="1.0.0", con=con, root=tmp_path, source_majors={"src": 1})
    rel = pond.read_table("src.event")
    assert rel.fetchall() == [(99,)]  # the relation reads the Source
    assert con.sql("SELECT id FROM event").fetchall() == [(1,)]  # own table untouched
    con.close()
