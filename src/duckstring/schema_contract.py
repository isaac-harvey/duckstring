"""The version contract: a Pond's output schema, and the additive-compatibility check at a major line.

A Pond's output schema is a *runtime* result (it's whatever its Ripples produced), so it can't be
vetted before the run. Instead the Duck checks it **at publish time, before overwriting the live
tables** — the staging area is just its own registry, and publishing is the promotion. If the output
isn't a superset of the contract the Catchment handed it, the publish is aborted (last-good data is
left untouched) and the run is failed; otherwise it publishes and reports the schema so the Catchment
can freeze/extend the contract.

The contract is **forward-only**: a Pond version that advances its major line must keep everything the
line already published (additive — new tables/columns are fine, and so is a **lossless widening** of a
column's type; drops, removed tables, and narrowing type changes are violations). A deliberate
rollback to an already-accepted version skips this check — it's governed by ``min_version`` instead
(see ``routes/deploy.py``). The sanctioned way to make a breaking
change is a **major bump**: the new line is a fresh contract and Sinks opt in by re-pinning, while the
old line keeps running for the Sinks that haven't.
"""

from __future__ import annotations

# A schema is ``{table: {column: type}}`` — the DuckDB type string. A change is judged by whether the new
# type can hold everything the old one could (see ``is_widening``), not by string equality.
Schema = dict[str, dict[str, str]]

# Integer families, narrowest first. A value of any type is representable in every type to its right, so
# a move rightwards is lossless. Kept as explicit chains rather than a size calculation because the
# signed/unsigned split is not a total order.
_INT_CHAINS = (
    ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"),
    ("UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"),
    # An unsigned type also fits any signed type with strictly more bits.
    ("UTINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"),
    ("USMALLINT", "INTEGER", "BIGINT", "HUGEINT"),
    ("UINTEGER", "BIGINT", "HUGEINT"),
    ("UBIGINT", "HUGEINT"),
)
# Temporal precision, coarsest first: a coarser instant is exactly representable at a finer precision.
_TS_CHAIN = ("TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP", "TIMESTAMP_NS")
_FLOAT_CHAIN = ("FLOAT", "DOUBLE")


def _decimal_parts(type_: str) -> tuple[int, int] | None:
    """``(precision, scale)`` for a DECIMAL/NUMERIC type string, else None."""
    import re

    m = re.fullmatch(r"\s*(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*", type_.upper())
    return (int(m.group(1)), int(m.group(2))) if m else None


def _in_chain(old: str, new: str, *chains) -> bool:
    return any(old in c and new in c and c.index(new) > c.index(old) for c in chains)


def is_widening(old: str, new: str) -> bool:
    """Whether every value of ``old`` is representable in ``new`` — a **lossless** widening.

    This is the type-level reading of the same "additive" rule the contract applies to tables and
    columns: growing the domain keeps every existing value readable, so a Sink pinned to this major line
    stays safe. Shrinking it does not, and stays a violation.

    Motivating case (found in production): a Ripple whose SQL takes a data-dependent branch — a
    ``COALESCE`` over a drift table, say — can emit ``DECIMAL(24,2)`` on one run and ``DECIMAL(25,2)`` on
    the next, with identical data semantics. Comparing type strings verbatim called that a breaking
    change and **permanently wedged the Pond**, since the contract is forward-only. It is not a breaking
    change: every value still fits.

    Deliberately conservative — INTEGER → DOUBLE is *not* accepted (a large BIGINT loses precision as a
    double), and nested types must match exactly."""
    old, new = old.strip().upper(), new.strip().upper()
    if old == new:
        return True
    od, nd = _decimal_parts(old), _decimal_parts(new)
    if od and nd:
        # Neither the fractional digits nor the integer digits may shrink.
        return nd[1] >= od[1] and (nd[0] - nd[1]) >= (od[0] - od[1])
    if od or nd:
        return False  # decimal ↔ non-decimal: not a lossless widening we are willing to assert
    return _in_chain(old, new, *_INT_CHAINS, _TS_CHAIN, _FLOAT_CHAIN)


class ContractViolation(Exception):
    """A Pond's published output broke its major line's additive contract."""


# The stable message prefix a contract failure carries — the machine-readable sub-reason the status API
# (`failure_kind`) and the UI derive a "Contract violation" from, restart-proof (it lives in the stored
# error message, not in ephemeral state).
CONTRACT_PREFIX = "version contract violation: "


def extract_schema(con) -> Schema:
    """The output schema of every published table in a Pond's registry connection
    (``{table: {column: type}}``).

    Trickle companions (``__changelog``/``__droplog``/``__band``/``__base``) are framework-internal, and
    ``_duckstring_*`` system columns are framework-owned — all excluded so the contract captures only the
    user-facing output schema.

    A **merge main** is log-structured: its physical base table (same name) is materialised only at the
    first checkpoint (compaction), so before then the registry holds only the ``__changelog``. The main's
    user-facing schema is nonetheless well-defined from run 1 — it is exactly the changelog's user columns
    — so a changelog is taken as evidence of its merge main and described from it. Without this, an
    un-compacted merge main is invisible to the contract, the catalog, and column lineage."""
    from .dataplane import RESERVED_PREFIX, registry_tables
    from .trickle_io import BASE_SUFFIX, CHANGELOG_SUFFIX, DROPLOG_SUFFIX, WARM_SUFFIX, base_table_name

    def _cols(table: str) -> dict[str, str]:
        return {
            row[0]: row[1]
            for row in con.execute(f'DESCRIBE "{table}"').fetchall()
            if not str(row[0]).startswith(RESERVED_PREFIX)
        }

    out: Schema = {}
    for table in registry_tables(con):
        if table.endswith(CHANGELOG_SUFFIX):
            # The merge main — its user schema is the changelog's user columns (available before the
            # base is materialised). A physical base (below) overrides with the same columns.
            out.setdefault(base_table_name(table), _cols(table))
        elif table.endswith((DROPLOG_SUFFIX, WARM_SUFFIX, BASE_SUFFIX)):
            continue  # framework-internal Trickle companions — never an output on their own
        else:
            out[table] = _cols(table)  # a plain table, an append history, or a checkpointed merge base
    return out


def contract_violations(output: Schema, contract: Schema | None) -> list[str]:
    """How ``output`` breaks ``contract`` (empty list = compatible). Additive changes are fine — new
    tables, new columns, and a **lossless widening** of a column's type (see :func:`is_widening`); a
    removed table, a dropped column, or a narrowing type change is a violation. A ``None``/empty contract
    (a first run, or a rollback the Catchment chose not to gate) never fails."""
    if not contract:
        return []
    out: list[str] = []
    for table, columns in contract.items():
        if table not in output:
            out.append(f"table '{table}' is no longer produced")
            continue
        have = output[table]
        for column, type_ in columns.items():
            if column not in have:
                out.append(f"column '{table}.{column}' was dropped")
            elif not is_widening(type_, have[column]):
                out.append(f"column '{table}.{column}' changed type {type_} → {have[column]}")
    return out
