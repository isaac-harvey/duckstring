"""The Athena dispatch backend (cloud E1, v0) — comprehensive builder recomputes on Athena.

The model (duckstring-cloud ``plans/athena.md``): work that fits the Duck runs in the Duck;
a ``pond.trickle(...)`` terminal whose anticipated size exceeds the Duck's envelope runs its
**comprehensive recompute** on Athena — the heavy read side (scan/join/filter/project) as one
Trino query — and the Duck keeps the merge/diff/publish semantics locally (``merge_table`` /
``append_zset`` on the returned relation, the same comprehensive path the local engine takes).

**v0 scope (demo-grade, fail-loud):** left-deep chains of equi-joins over source leaves
(``inner``/``left``/``right``/``full``), any number of ``.filter``s, at most one ``.select``,
no ``.mutate``/``.aggregate``/``.accumulate``/``.sql``. Anything else refuses (→ the local
engine, exactly as before). Sources are **staged**: each leaf's current state is written to
the scratch prefix as parquet and registered as a temporary Glue external table — production
(E2) registers the published plane instead; staging is the local-dev shim that works against
any data root. Expressions pass through un-transpiled (keep projections/filters to the
DuckDB∩Trino surface for now); the full compile story is E1-proper.

Config is environment-only; **absent config ⇒ this module does nothing** (pure OSS path):

- ``DUCKSTRING_ATHENA_WORKGROUP`` — the workgroup queries run in (the enablement gate).
- ``DUCKSTRING_GLUE_DATABASE``   — the Glue database for temporary external tables.
- ``DUCKSTRING_ATHENA_SCRATCH``  — s3:// prefix for staged sources + UNLOAD results
  (lifecycle-expired; nothing durable lives here).
- ``DUCKSTRING_ATHENA_REGION``   — AWS region (else the boto3 default chain).
- ``DUCKSTRING_DUCK_SIZE``       — envelope preset (s/m/l/xl → row thresholds below).
- ``DUCKSTRING_ATHENA_MIN_ROWS`` — explicit envelope override (rows summed over leaves).
- ``DUCKSTRING_ATHENA_FORCE=1``  — dispatch every eligible terminal (testing).

Failure posture: any Athena-side problem logs loudly and returns ``None`` — the caller runs
the local path. Classic is the oracle; the pond is never down because the offload engine had
a bad day. Requires ``boto3`` (lazily imported) and DuckDB httpfs.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass

log = logging.getLogger("duckstring.athena")

# Duck preset → comprehensive-recompute row envelope (sum over source leaves). Deliberately
# conservative demo numbers; E1-proper estimates bytes from sidecar stats.
_SIZE_ROWS = {"s": 1_000_000, "m": 4_000_000, "l": 16_000_000, "xl": 64_000_000}

_JOIN_SQL = {"inner": "INNER JOIN", "left": "LEFT JOIN", "right": "RIGHT JOIN", "full": "FULL JOIN"}

# DuckDB type name → Athena/Glue DDL type (parquet-compatible surface only).
_TYPES = {
    "BIGINT": "bigint", "INTEGER": "int", "SMALLINT": "smallint", "TINYINT": "tinyint",
    "HUGEINT": "decimal(38,0)", "DOUBLE": "double", "FLOAT": "float", "BOOLEAN": "boolean",
    "VARCHAR": "string", "DATE": "date", "BLOB": "binary",
    "TIMESTAMP": "timestamp", "TIMESTAMP WITH TIME ZONE": "timestamp",
}


@dataclass
class Config:
    workgroup: str | None
    database: str | None
    scratch: str | None
    region: str | None
    min_rows: int
    force: bool

    @classmethod
    def from_env(cls, env=None) -> "Config":
        e = env if env is not None else os.environ
        size = e.get("DUCKSTRING_DUCK_SIZE", "s").lower()
        min_rows = int(e.get("DUCKSTRING_ATHENA_MIN_ROWS", _SIZE_ROWS.get(size, _SIZE_ROWS["s"])))
        return cls(
            workgroup=e.get("DUCKSTRING_ATHENA_WORKGROUP") or None,
            database=e.get("DUCKSTRING_GLUE_DATABASE") or None,
            scratch=(e.get("DUCKSTRING_ATHENA_SCRATCH") or "").rstrip("/") or None,
            region=e.get("DUCKSTRING_ATHENA_REGION") or None,
            min_rows=min_rows,
            force=e.get("DUCKSTRING_ATHENA_FORCE", "0") == "1",
        )

    @property
    def enabled(self) -> bool:
        return bool(self.workgroup and self.database and self.scratch)


# ─── eligibility & estimate ──────────────────────────────────────────────────────


def _shape_refusal(builder) -> str | None:
    """None when the v0 compiler can express this terminal; else the human-readable reason."""
    from .trickle.builder import _Join, _Source

    if builder._materialised is not None or builder._agg is not None or builder._acc is not None:
        return ".sql()/.aggregate()/.accumulate() terminals are v0-local"
    if any(kind == "mutate" for kind, _ in builder._ops):
        return ".mutate() is v0-local"
    if sum(1 for kind, _ in builder._ops if kind == "select") > 1:
        return "multiple .select()s are v0-local"

    def left_deep(node) -> bool:
        if isinstance(node, _Source):
            return True
        if isinstance(node, _Join):
            return node.how in _JOIN_SQL and isinstance(node.right, _Source) and left_deep(node.left)
        return False

    if not left_deep(builder._root):
        return "non-left-deep or semi/anti join shapes are v0-local"
    return None


def wants(builder) -> bool:
    """Cheap gate for the terminal hook: config present and the shape is expressible."""
    cfg = Config.from_env()
    if not cfg.enabled:
        return False
    reason = _shape_refusal(builder)
    if reason is not None:
        log.info("athena: not dispatching (%s)", reason)
        return False
    return True


def _estimate_rows(builder) -> int:
    total = 0
    for leaf in builder._leaves():
        rel = builder.ctx.read_table(leaf.ref)
        total += rel.count("*").fetchone()[0]
    return total


# ─── SQL compile (v0) ────────────────────────────────────────────────────────────


def _compile_select(builder, cfg: Config, src_tables: dict[int, str]) -> str:
    """One Trino SELECT: left-deep FROM/JOIN over the staged tables (aliased with the
    builder's own leaf aliases), filters ANDed into WHERE, the .select projection verbatim."""
    from .trickle.builder import _Join, _Source

    builder._prepare_leaves()
    leaves = builder._leaves()
    alias = {id(l): builder._alias_for(l) for l in leaves}

    def leaf_owning(col: str, upto: int):
        """The leftmost leaf (index < upto) whose bare columns contain ``col``."""
        for i in range(upto):
            if col in builder._bare_cols(leaves[i]):
                return leaves[i]
        raise ValueError(f"join key '{col}' not found in any left-side source")

    def qualify(side: str, col: str, right_leaf, right_index: int) -> str:
        if "." in col:  # already alias-qualified by the user
            return col
        if side == "right":
            return f"{alias[id(right_leaf)]}.{col}"
        return f"{alias[id(leaf_owning(col, right_index))]}.{col}"

    # FROM clause, walking the left-deep spine outward.
    def from_clause(node) -> tuple[str, int]:
        if isinstance(node, _Source):
            return f'{src_tables[id(node)]} AS {alias[id(node)]}', 1
        assert isinstance(node, _Join)
        left_sql, left_n = from_clause(node.left)
        right = node.right  # a _Source by the left-deep guard
        on = " AND ".join(
            f"{qualify('left', l, right, left_n)} = {qualify('right', r, right, left_n)}"
            for l, r in node.on_pairs
        )
        return f"{left_sql} {_JOIN_SQL[node.how]} {src_tables[id(right)]} AS {alias[id(right)]} ON {on}", left_n + 1

    from_sql, _ = from_clause(builder._root)
    selects = [p for kind, p in builder._ops if kind == "select"]
    filters = [p for kind, p in builder._ops if kind == "filter"]
    projection = selects[0] if selects else "*"
    where = f" WHERE {' AND '.join(f'({f})' for f in filters)}" if filters else ""
    return f"SELECT {projection} FROM {from_sql}{where}"


# ─── the dispatch ────────────────────────────────────────────────────────────────


def comprehensive(builder, out_pk) -> "object | None":
    """Run this terminal's comprehensive recompute on Athena. Returns a DuckDB relation over
    the result (hand it to ``merge_table``/``append_zset``), or ``None`` on refusal/failure
    (caller runs the local path). Never raises for an Athena-side problem."""
    cfg = Config.from_env()
    try:
        if not cfg.force:
            rows = _estimate_rows(builder)
            if rows < cfg.min_rows:
                log.info("athena: %d source rows < envelope %d — local", rows, cfg.min_rows)
                return None
        return _run(builder, cfg)
    except Exception as exc:  # noqa: BLE001 — the fallback contract
        log.warning("athena: dispatch failed (%s: %s) — running local", type(exc).__name__, str(exc)[:300])
        return None


def _run(builder, cfg: Config):
    import boto3

    ctx = builder.ctx
    con = ctx.con
    job = uuid.uuid4().hex[:12]
    session = boto3.Session(region_name=cfg.region)
    athena = session.client("athena")
    s3 = session.client("s3")
    _duckdb_s3_secret(con, session)

    t0 = time.monotonic()
    # 1. Stage each leaf's current state as parquet + a temporary Glue external table.
    builder._prepare_leaves()
    src_tables: dict[int, str] = {}
    ddl_names: list[str] = []
    for i, leaf in enumerate(builder._leaves()):
        rel = ctx.read_table(leaf.ref)
        prefix = f"{cfg.scratch}/jobs/{job}/src{i}"
        con.execute(f"COPY ({rel.sql_query()}) TO '{prefix}/data.parquet' (FORMAT PARQUET)")
        tbl = f"dsjob_{job}_src{i}"
        cols = ", ".join(f"`{c}` {_athena_type(str(t))}" for c, t in zip(rel.columns, rel.types))
        _query(athena, cfg, f"CREATE EXTERNAL TABLE {cfg.database}.{tbl} ({cols}) "
                            f"STORED AS PARQUET LOCATION '{prefix}/'")
        src_tables[id(leaf)] = f"{cfg.database}.{tbl}"
        ddl_names.append(tbl)

    try:
        # 2. The heavy read side, UNLOADed to parquet.
        select = _compile_select(builder, cfg, src_tables)
        out = f"{cfg.scratch}/jobs/{job}/out"
        qid, stats = _query(
            athena, cfg, f"UNLOAD ({select}) TO '{out}/' WITH (format = 'PARQUET')",
            token=f"ds-{job}",
        )
        # 3. The dispatch record — the metering + audit artifact (billing.md).
        bucket, key_prefix = cfg.scratch.removeprefix("s3://").split("/", 1)
        record = {
            "query_id": qid, "job": job, "f": str(getattr(ctx, "f", "")),
            "select": select, "bytes_scanned": stats.get("DataScannedInBytes", 0),
            "athena_ms": stats.get("TotalExecutionTimeInMillis", 0),
            "wall_ms": round((time.monotonic() - t0) * 1000),
        }
        s3.put_object(Bucket=bucket, Key=f"{key_prefix}/athena/jobs/{qid}.json",
                      Body=json.dumps(record).encode())
        log.warning("athena: dispatched %s — %.1f MB scanned, %d ms (job %s)",
                    qid, record["bytes_scanned"] / 1e6, record["athena_ms"], job)
        # 4. The result, as a relation the terminal's own machinery consumes.
        return con.read_parquet(f"{out}/*")
    finally:
        for tbl in ddl_names:  # best-effort; scratch lifecycle is the backstop
            try:
                _query(athena, cfg, f"DROP TABLE IF EXISTS {cfg.database}.{tbl}")
            except Exception:  # noqa: BLE001
                pass


# ─── helpers ─────────────────────────────────────────────────────────────────────


def _athena_type(duck_type: str) -> str:
    t = duck_type.upper()
    if t.startswith("DECIMAL"):
        return t.lower()
    if t not in _TYPES:
        raise ValueError(f"no Athena mapping for DuckDB type {duck_type}")
    return _TYPES[t]


def _duckdb_s3_secret(con, session) -> None:
    """Load httpfs + feed the boto3 credential chain's keys to DuckDB for s3:// I/O."""
    creds = session.get_credentials()
    if creds is None:
        raise RuntimeError("no AWS credentials in the boto3 chain")
    frozen = creds.get_frozen_credentials()
    con.execute("INSTALL httpfs; LOAD httpfs")
    region = session.region_name or "us-east-1"
    token = f", SESSION_TOKEN '{frozen.token}'" if frozen.token else ""
    con.execute(
        "CREATE OR REPLACE SECRET ds_athena ("
        f"TYPE S3, KEY_ID '{frozen.access_key}', SECRET '{frozen.secret_key}'{token}, "
        f"REGION '{region}')"
    )


def _query(athena, cfg: Config, sql: str, token: str | None = None):
    """Run one Athena statement to completion. Returns ``(query_id, statistics)``."""
    kwargs = {
        "QueryString": sql,
        "WorkGroup": cfg.workgroup,
        "QueryExecutionContext": {"Database": cfg.database},
    }
    if token:
        kwargs["ClientRequestToken"] = token.ljust(32, "x")  # Athena wants ≥32 chars
    qid = athena.start_query_execution(**kwargs)["QueryExecutionId"]
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        ex = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = ex["Status"]["State"]
        if state == "SUCCEEDED":
            return qid, ex.get("Statistics", {})
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"athena query {state}: {ex['Status'].get('StateChangeReason', '')[:300]}")
        time.sleep(0.5)
    raise TimeoutError(f"athena query {qid} did not finish in 300s")
