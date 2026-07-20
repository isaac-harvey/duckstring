"""The serving core (plans/data-serving.md) — one warm, resident, sandboxed, read-only DuckDB executor
over the published data. Every front door (HTTP/CLI, Postgres wire, Flight SQL) runs through this.

Catalog convention: ``catalog = catchment``, ``schema = pond``, ``table = table``. The bare ``{pond}``
schema resolves to the **served major**; ``{pond}_v{major}`` is every deployed major (explicit pinning).

**The sandbox is the teeth.** Serviceable data is external (S3/local Parquet), and arbitrary DuckDB SQL
could escape an allowlist via ``read_parquet``/``read_csv``. So a read connection **materialises** the
serviceable tables into local DuckDB first (with external access on — this is also the low-latency
"resident" win), *then* sets ``enable_external_access = false`` + ``lock_configuration = true``, so the
user's SQL can only touch the materialised serviceable tables — nothing else on disk or S3. A full
(operator) connection skips the sandbox and exposes every output table as views (ops/debug).

Increment 1 builds a fresh connection per call; the warm/cached/refresh-on-freshness lifecycle is a
follow-up. Snapshot consistency rides the data plane's ``f`` stamp (reads are of the latest committed run).
"""

from __future__ import annotations

from pathlib import Path

DEFAULT_MEMORY_LIMIT = "1GB"
DEFAULT_STATEMENT_TIMEOUT_S = 30.0


def _qi(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _served_ponds(driver) -> list[tuple[str, int, list[int]]]:
    """(name, served_major, deployed_majors) for every non-spout/non-draw pond."""
    names = sorted({m["name"] for m in driver.meta.values()
                    if not m.get("is_draw") and not m.get("is_spout")})
    out = []
    for name in names:
        majors = driver.deployed_majors(name)
        if majors:
            out.append((name, driver.served_major(name) or majors[0], majors))
    return out


def build_serving_con(driver, *, sandboxed: bool):
    """A DuckDB connection presenting the Catchment's serving surface. ``sandboxed`` (read users):
    only serviceable tables, materialised local, external access locked. Not sandboxed (full/ops):
    every output table, as views, external access open."""
    import duckdb

    from ..dataplane import get_data_plane
    from ..keys import pond_key
    from .registry import pond_data_dir

    con = duckdb.connect()
    dp = get_data_plane()
    dp.prepare(con)
    root = Path(driver.root)

    for name, served, majors in _served_ponds(driver):
        for major in majors:
            key = pond_key(name, major)
            pv_id = driver._pond_ids(key)[1]
            tables = driver.serviceable(key) if sandboxed else driver._output_tables(pv_id)
            if not tables:
                continue
            data_dir = pond_data_dir(root, name, major, driver.data_root)
            data_dir.duckdb_setup(con)  # object store → httpfs + creds (before we lock external access)
            schema = f"{name}_v{major}"
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(schema)}")
            for t in sorted(tables):
                try:
                    sel = dp.read_select(data_dir, t)
                except FileNotFoundError:
                    continue  # table declared/deployed but not yet published (pond never ran)
                verb = "TABLE" if sandboxed else "VIEW"  # materialise for the sandbox; view for ops
                con.execute(f"CREATE {verb} {_qi(schema)}.{_qi(t)} AS {sel}")
            # The bare `{pond}` schema aliases the SERVED major (views onto the local _vN objects — no
            # external access, so they survive the lockdown).
            if major == served:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(name)}")
                for t in sorted(tables):
                    if _relation_exists(con, schema, t):
                        con.execute(f"CREATE VIEW {_qi(name)}.{_qi(t)} AS "
                                    f"SELECT * FROM {_qi(schema)}.{_qi(t)}")

    if sandboxed:
        con.execute(f"SET memory_limit='{DEFAULT_MEMORY_LIMIT}'")
        con.execute("SET enable_external_access=false")  # block read_parquet/read_csv/httpfs
        con.execute("SET lock_configuration=true")        # freeze it LAST so the user can't re-enable
    return con


def _relation_exists(con, schema: str, table: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table]).fetchone() is not None


def serving_query(driver, sql: str, *, sandboxed: bool = True) -> dict:
    """Run a read-only query against the serving surface. Returns {columns, rows}. Read users get the
    sandboxed connection (serviceable-only, no external access); full users get the ops connection."""
    con = build_serving_con(driver, sandboxed=sandboxed)
    try:
        cur = con.execute(sql)
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return {"columns": columns, "rows": rows}
    finally:
        con.close()
