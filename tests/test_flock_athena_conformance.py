"""Differential conformance: Athena (Trino) vs DuckDB, measured rather than assumed.

Everything in the Athena engine's allow-list is there because Trino's *documented* semantics match
DuckDB's *measured* behaviour. Documentation is not evidence about the engine you are actually running,
so this runs each candidate expression on BOTH and compares the answers.

It is the gate for changing that list: an expression joins the allow-list when this passes for it, and
leaves the list when this fails. Nothing is admitted on reasoning alone.

Needs a real Athena (workgroup + Glue database + S3 scratch). Skipped otherwise — so it is inert in CI
and meaningful on a cloud box:

    DUCKSTRING_FLOCK_ATHENA_WORKGROUP=duckstring-flock \\
    DUCKSTRING_FLOCK_ATHENA_DATABASE=duckstring_flock \\
    DUCKSTRING_FLOCK_ATHENA_SCRATCH=s3://…/flock-scratch \\
    DUCKSTRING_FLOCK_ATHENA_REGION=ap-southeast-2 \\
    pytest tests/test_flock_athena_conformance.py -v
"""

from __future__ import annotations

import os

import duckdb
import pytest

pytestmark = pytest.mark.timeout(600)

_REQUIRED = ("DUCKSTRING_FLOCK_ATHENA_WORKGROUP", "DUCKSTRING_FLOCK_ATHENA_DATABASE",
             "DUCKSTRING_FLOCK_ATHENA_SCRATCH")

pytest.importorskip("boto3")
if not all(os.environ.get(k) for k in _REQUIRED):
    pytest.skip("Athena is not configured — set DUCKSTRING_FLOCK_ATHENA_* to measure", allow_module_level=True)

# (label, expression, on the allow-list today). The expression is evaluated over a fixed literal row so
# the comparison is about SEMANTICS, not about data plumbing.
_CASES = [
    # Allowed today — these must agree, or the list is wrong.
    ("mult_decimal",   "CAST(7 AS DECIMAL(10,2)) * CAST(3.5 AS DECIMAL(8,3))", True),
    ("add_decimal",    "CAST(1.05 AS DECIMAL(10,2)) + CAST(2.10 AS DECIMAL(10,2))", True),
    ("round_half_up",  "round(CAST(2.5 AS DECIMAL(10,1)))", True),
    ("round_half_up2", "round(CAST(3.5 AS DECIMAL(10,1)))", True),
    ("round_2dp",      "round(CAST(2.345 AS DECIMAL(10,3)), 2)", True),
    ("round_neg",      "round(CAST(-2.5 AS DECIMAL(10,1)))", True),
    ("abs_neg",        "abs(CAST(-7.25 AS DECIMAL(10,2)))", True),
    ("floor_ceil",     "floor(CAST(2.7 AS DOUBLE)) + ceil(CAST(2.1 AS DOUBLE))", True),
    ("coalesce_null",  "coalesce(CAST(NULL AS INTEGER), 5)", True),
    ("compare_null",   "CASE WHEN CAST(NULL AS INTEGER) > 1 THEN 'y' ELSE 'n' END", True),
    # Excluded today — measured so the exclusion is evidence-based, and so we learn if it can be lifted.
    ("int_division",   "7 / 2", False),
    ("cast_half",      "CAST(CAST(2.5 AS DOUBLE) AS INTEGER)", False),
    ("cast_trunc",     "CAST(CAST(2.9 AS DOUBLE) AS INTEGER)", False),
    ("upper_trim",     "upper(trim('  ab  '))", False),
    ("concat_null",    "CASE WHEN ('a' || CAST(NULL AS VARCHAR)) IS NULL THEN 'null' ELSE 'notnull' END", False),
]


def _athena_values(exprs: list[str], cast_to: list[str] | None = None) -> list[str]:
    """Evaluate each expression on Athena, as strings.

    ``cast_to`` mirrors what ``flock.conform`` does in production: cast the engine's result to the type
    DuckDB produced. Comparing raw output would flag differences that never reach your data — Athena's
    ``round(x, 2)`` keeps scale 3 (``2.350``) where DuckDB rescales to 2 (``2.35``), and conform casts
    that back. What matters is whether the values agree AFTER that, so that is what we compare."""
    import boto3

    region = os.environ.get("DUCKSTRING_FLOCK_ATHENA_REGION")
    athena = boto3.client("athena", **({"region_name": region} if region else {}))
    if cast_to:
        select = ", ".join(f"CAST(CAST(({e}) AS {t}) AS VARCHAR) AS c{i}"
                           for i, (e, t) in enumerate(zip(exprs, cast_to, strict=True)))
    else:
        select = ", ".join(f"CAST(({e}) AS VARCHAR) AS c{i}" for i, e in enumerate(exprs))
    qid = athena.start_query_execution(
        QueryString=f"SELECT {select}",
        WorkGroup=os.environ["DUCKSTRING_FLOCK_ATHENA_WORKGROUP"],
        QueryExecutionContext={"Database": os.environ["DUCKSTRING_FLOCK_ATHENA_DATABASE"]},
        ResultConfiguration={"OutputLocation": os.environ["DUCKSTRING_FLOCK_ATHENA_SCRATCH"].rstrip("/") + "/conformance/"},
    )["QueryExecutionId"]
    import time
    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if info["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    assert info["State"] == "SUCCEEDED", f"Athena query failed: {info.get('StateChangeReason')}"
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [c.get("VarCharValue") for c in rows[1]["Data"]]  # row 0 is the header


def _duckdb_values(exprs: list[str]) -> tuple[list[str], list[str]]:
    """The oracle: each expression's value AND its DuckDB type (the type conform casts the engine to)."""
    con = duckdb.connect()
    types = [str(con.sql(f"SELECT ({e}) AS v").types[0]) for e in exprs]
    select = ", ".join(f"CAST(({e}) AS VARCHAR) AS c{i}" for i, e in enumerate(exprs))
    values = [None if v is None else str(v) for v in con.sql(f"SELECT {select}").fetchone()]
    return values, types


def test_athena_matches_duckdb_on_the_allow_list():
    """The list is a claim about this engine; this is the measurement behind it. Failures print the whole
    matrix, including the expressions we already exclude — so an exclusion that turns out to be
    unnecessary is visible and can be lifted, and one that turns out to be necessary is documented."""
    exprs = [e for _l, e, _a in _CASES]
    duck, duck_types = _duckdb_values(exprs)
    athena = _athena_values(exprs, cast_to=duck_types)  # post-conform, i.e. what would actually ship

    report, wrong_allow, unnecessary_exclusion = [], [], []
    for (label, expr, allowed), d, a in zip(_CASES, duck, athena, strict=True):
        same = d == a
        report.append(f"  {'=' if same else '!'} {label:15s} allow={str(allowed):5s} duckdb={d!r:12s} athena={a!r:12s}  {expr}")
        if allowed and not same:
            wrong_allow.append(label)
        if not allowed and same:
            unnecessary_exclusion.append(label)

    print("\nAthena vs DuckDB:\n" + "\n".join(report))
    if unnecessary_exclusion:
        print(f"\nSafe to consider admitting (they agreed): {', '.join(unnecessary_exclusion)}")
    assert not wrong_allow, (
        f"allow-listed expressions disagree between engines: {', '.join(wrong_allow)}\n"
        + "\n".join(report)
    )
