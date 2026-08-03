"""The Athena Flock engine (v0) — comprehensive builder recomputes on Amazon Athena.

The default engine behind the Flock tier (``duckstring/flock``). A ``pond.trickle(...)``
terminal's comprehensive recompute runs its heavy read side (scan/join/filter/project) as one
Trino query; the Duck keeps the merge/diff/publish semantics locally.

**v0 scope (fail-loud):** left-deep chains of equi-joins over source leaves
(``inner``/``left``/``right``/``full``), any number of ``.filter``s, at most one ``.select``,
no ``.mutate``/``.aggregate``/``.accumulate``/``.sql``. Anything else refuses (→ the local
engine). Sources are **staged**: each leaf's current state is written to the scratch prefix as
parquet and registered as a temporary Glue external table — the cloud registers the published
Iceberg plane instead; staging is the local-dev shim that works against any data root.

**No metering.** OSS dispatches and runs; the cloud accounts for usage out of band (per-tenant
workgroup → CloudWatch bytes-scanned). The one durable side effect is the query's result under
the (lifecycle-expired) scratch prefix; the ``flock: dispatched …`` log line is observability,
not an artifact.

Config (env; **all three required to enable**):

- ``DUCKSTRING_FLOCK_ATHENA_WORKGROUP`` — the workgroup queries run in.
- ``DUCKSTRING_FLOCK_ATHENA_DATABASE``  — the Glue database for temporary external tables.
- ``DUCKSTRING_FLOCK_ATHENA_SCRATCH``   — s3:// prefix for staged sources + UNLOAD results.
- ``DUCKSTRING_FLOCK_ATHENA_REGION``    — AWS region (else the boto3 default chain).

Requires ``boto3`` (lazily imported) and DuckDB httpfs.
"""

from __future__ import annotations

import logging
import time
import uuid

from .. import equivalence

log = logging.getLogger("duckstring.flock.athena")

_JOIN_SQL = {"inner": "INNER JOIN", "left": "LEFT JOIN", "right": "RIGHT JOIN", "full": "FULL JOIN"}

# DuckDB type name → Athena/Glue DDL type (parquet-compatible surface only).
_TYPES = {
    "BIGINT": "bigint", "INTEGER": "int", "SMALLINT": "smallint", "TINYINT": "tinyint",
    "HUGEINT": "decimal(38,0)", "DOUBLE": "double", "FLOAT": "float", "BOOLEAN": "boolean",
    "VARCHAR": "string", "DATE": "date", "BLOB": "binary",
    "TIMESTAMP": "timestamp", "TIMESTAMP WITH TIME ZONE": "timestamp",
}



# ── What Athena (Trino) may be handed unchanged ────────────────────────────────
#
# Scoped to THIS engine: equivalence is a property of the expression and the engine, not of SQL. Each
# entry is here because Trino's documented semantics match DuckDB's measured behaviour; the excluded
# ones are excluded for a specific reason, not caution in general.
#
# ALLOWED
#   arithmetic + - *  — Trino and DuckDB share the decimal result-type rule (p1+p2, s1+s2) and decimal
#                       arithmetic is exact, so no rounding can creep in.
#   round()           — both round half away from zero (DuckDB measured: round(2.5)=3, round(3.5)=4).
#   abs/floor/ceil    — exact, no half-way case.
#   coalesce/nullif   — null semantics agree.
#
# EXCLUDED, each with a concrete divergence
#   division (/)      — DuckDB `7 / 2` is TRUE division → 3.5 DOUBLE; Trino's is integer division → 3.
#                       This one is dangerous rather than merely different: `conform` would cast Trino's
#                       3 to DOUBLE and publish 3.0 — the right type carrying the wrong value.
#   div by zero       — DuckDB yields inf; Trino throws.
#   CAST              — at the .5 boundary DuckDB casts to even (CAST(2.5 AS INTEGER) = 2) while Trino
#                       rounds half up = 3. Note DuckDB's own cast and round() disagree there.
#   string functions  — trimming, collation and CHAR padding are not verified.
#   SUM/AVG on FLOAT  — IEEE addition is not associative, so a parallel engine's partitioning shows up
#                       in the last bits. Not a correctness cliff (DuckDB is not bit-stable across
#                       thread counts either) but unverified, so it waits for a measurement.
#
# The list grows from evidence: tests/test_flock_athena_conformance.py runs each candidate on BOTH
# engines and compares. Nothing joins it on reasoning alone.
_ALLOWED_NODES = equivalence.BASE_NODES | frozenset({"Add", "Sub", "Mul", "Neg", "Coalesce", "If", "Case"})
_ALLOWED_FUNCS = frozenset({"round", "abs", "floor", "ceil", "ceiling", "coalesce", "nullif",
                            "least", "greatest"})


class AthenaEngine:
    """A :class:`duckstring.flock.FlockEngine`. Holds the env config; one per resolve."""

    def __init__(self, env):
        self.workgroup = env.get("DUCKSTRING_FLOCK_ATHENA_WORKGROUP") or None
        self.database = env.get("DUCKSTRING_FLOCK_ATHENA_DATABASE") or None
        self.scratch = (env.get("DUCKSTRING_FLOCK_ATHENA_SCRATCH") or "").rstrip("/") or None
        self.region = env.get("DUCKSTRING_FLOCK_ATHENA_REGION") or None
        self.last_error: str | None = None  # last dispatch failure (read by flock's dispatch counters)

    # ── FlockEngine protocol ──────────────────────────────────────────────────

    def enabled(self) -> bool:
        return bool(self.workgroup and self.database and self.scratch)

    def eligible(self, builder) -> str | None:
        """None when this terminal can be dispatched to Athena; else the reason (→ local compute).

        Two questions, both this engine's: can the v0 compiler EXPRESS the shape, and are its expressions
        ones Trino is known to evaluate the way DuckDB does (see the lists above)."""
        from ...trickle.builder import _Join, _Source

        unproven = equivalence.unproven(builder, _ALLOWED_NODES, _ALLOWED_FUNCS)
        if unproven is not None:
            return unproven

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

    def estimate_rows(self, builder) -> int:
        total = 0
        for leaf in builder._leaves():
            total += builder.ctx.read_table(leaf.ref).count("*").fetchone()[0]
        return total

    def dispatch(self, builder, out_pk):
        """Run the comprehensive recompute on Athena; return a DuckDB relation over the result,
        or ``None`` on any Athena-side failure (the caller runs local). Never raises for an
        engine problem — classic is the oracle."""
        try:
            result = self._run(builder)
            self.last_error = None
            return result
        except Exception as exc:  # noqa: BLE001 — the fallback contract
            self.last_error = f"{type(exc).__name__}: {str(exc)[:300]}"
            log.warning("flock/athena: dispatch failed (%s: %s) — running local",
                        type(exc).__name__, str(exc)[:300])
            return None

    # ── execution ─────────────────────────────────────────────────────────────

    def _run(self, builder):
        import boto3

        ctx = builder.ctx
        con = ctx.con
        job = uuid.uuid4().hex[:12]
        session = boto3.Session(region_name=self.region)
        athena = session.client("athena")
        _duckdb_s3_secret(con, session)

        # 1. Stage each leaf's current state as parquet + a temporary Glue external table.
        builder._prepare_leaves()
        src_tables: dict[int, str] = {}
        ddl_names: list[str] = []
        for i, leaf in enumerate(builder._leaves()):
            rel = ctx.read_table(leaf.ref)
            prefix = f"{self.scratch}/stage/{job}/src{i}"
            con.execute(f"COPY ({rel.sql_query()}) TO '{prefix}/data.parquet' (FORMAT PARQUET)")
            tbl = f"dsjob_{job}_src{i}"
            cols = ", ".join(f"`{c}` {_athena_type(str(t))}" for c, t in zip(rel.columns, rel.types, strict=True))
            self._query(athena, f"CREATE EXTERNAL TABLE {self.database}.{tbl} ({cols}) "
                                f"STORED AS PARQUET LOCATION '{prefix}/'")
            src_tables[id(leaf)] = f"{self.database}.{tbl}"
            ddl_names.append(tbl)

        try:
            # 2. The heavy read side, UNLOADed to parquet.
            select = _compile_select(builder, src_tables)
            out = f"{self.scratch}/stage/{job}/out"
            qid, stats = self._query(
                athena, f"UNLOAD ({select}) TO '{out}/' WITH (format = 'PARQUET')", token=f"ds-{job}"
            )
            log.warning("flock: dispatched via athena %s — %.1f MB scanned, %d ms",
                        qid, stats.get("DataScannedInBytes", 0) / 1e6,
                        stats.get("TotalExecutionTimeInMillis", 0))
            # 3. The result, as a relation the terminal's own machinery consumes. (No metering
            #    artifact — the cloud accounts for usage out of band; see duckstring.flock.)
            return con.read_parquet(f"{out}/*")
        finally:
            for tbl in ddl_names:  # best-effort; scratch lifecycle is the backstop
                try:
                    self._query(athena, f"DROP TABLE IF EXISTS {self.database}.{tbl}")
                except Exception:  # noqa: BLE001
                    pass

    def _query(self, athena, sql: str, token: str | None = None):
        """Run one Athena statement to completion. Returns ``(query_id, statistics)``."""
        kwargs = {
            "QueryString": sql,
            "WorkGroup": self.workgroup,
            "QueryExecutionContext": {"Database": self.database},
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


# ─── SQL compile (v0) ────────────────────────────────────────────────────────────


def _compile_select(builder, src_tables: dict[int, str]) -> str:
    """One Trino SELECT: left-deep FROM/JOIN over the staged tables (aliased with the builder's
    own leaf aliases), filters ANDed into WHERE, the .select projection verbatim."""
    from ...trickle.builder import _Join, _Source

    builder._prepare_leaves()
    leaves = builder._leaves()
    alias = {id(lf): builder._alias_for(lf) for lf in leaves}

    def leaf_owning(col: str, upto: int):
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

    def from_clause(node) -> tuple[str, int]:
        if isinstance(node, _Source):
            return f'{src_tables[id(node)]} AS {alias[id(node)]}', 1
        assert isinstance(node, _Join)
        left_sql, left_n = from_clause(node.left)
        right = node.right  # a _Source by the left-deep guard
        on = " AND ".join(
            f"{qualify('left', lc, right, left_n)} = {qualify('right', rc, right, left_n)}"
            for lc, rc in node.on_pairs
        )
        return f"{left_sql} {_JOIN_SQL[node.how]} {src_tables[id(right)]} AS {alias[id(right)]} ON {on}", left_n + 1

    from_sql, _ = from_clause(builder._root)
    selects = [p for kind, p in builder._ops if kind == "select"]
    filters = [p for kind, p in builder._ops if kind == "filter"]
    projection = selects[0] if selects else "*"
    where = f" WHERE {' AND '.join(f'({f})' for f in filters)}" if filters else ""
    return f"SELECT {projection} FROM {from_sql}{where}"


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
        "CREATE OR REPLACE SECRET ds_flock_athena ("
        f"TYPE S3, KEY_ID '{frozen.access_key}', SECRET '{frozen.secret_key}'{token}, "
        f"REGION '{region}')"
    )
