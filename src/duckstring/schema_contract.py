"""The version contract: a Pond's output schema, and the additive-compatibility check at a major line.

A Pond's output schema is a *runtime* result (it's whatever its Ripples produced), so it can't be
vetted before the run. Instead the Duck checks it **at publish time, before overwriting the live
tables** — the staging area is just its own registry, and publishing is the promotion. If the output
isn't a superset of the contract the Catchment handed it, the publish is aborted (last-good data is
left untouched) and the run is failed; otherwise it publishes and reports the schema so the Catchment
can freeze/extend the contract.

The contract is **forward-only**: a Pond version that advances its major line must keep everything the
line already published (additive — new tables/columns are fine; drops, removed tables, and type
changes are violations). A deliberate rollback to an already-accepted version skips this check — it's
governed by ``min_version`` instead (see ``routes/deploy.py``). The sanctioned way to make a breaking
change is a **major bump**: the new line is a fresh contract and Sinks opt in by re-pinning, while the
old line keeps running for the Sinks that haven't.
"""

from __future__ import annotations

# A schema is ``{table: {column: type}}`` — type is the DuckDB type string (compared verbatim; a type
# change is conservatively a violation, no widening rules in Phase 2).
Schema = dict[str, dict[str, str]]


class ContractViolation(Exception):
    """A Pond's published output broke its major line's additive contract."""


# The stable message prefix a contract failure carries — the machine-readable sub-reason the status API
# (`failure_kind`) and the UI derive a "Contract violation" from, restart-proof (it lives in the stored
# error message, not in ephemeral state).
CONTRACT_PREFIX = "version contract violation: "


def extract_schema(con) -> Schema:
    """The output schema of every published table in a Pond's registry connection
    (``{table: {column: type}}``).

    A Trickle's ``__changelog``/``__droplog`` companions are framework-internal (CDC / dropped-row
    diagnostic), and ``_duckstring_*`` system columns are framework-owned — all excluded so the contract
    captures only the user-facing output schema."""
    from .dataplane import RESERVED_PREFIX, registry_tables
    from .trickle_io import CHANGELOG_SUFFIX, DROPLOG_SUFFIX

    return {
        table: {
            row[0]: row[1]
            for row in con.execute(f'DESCRIBE "{table}"').fetchall()
            if not str(row[0]).startswith(RESERVED_PREFIX)
        }
        for table in registry_tables(con)
        if not table.endswith((CHANGELOG_SUFFIX, DROPLOG_SUFFIX))
    }


def contract_violations(output: Schema, contract: Schema | None) -> list[str]:
    """How ``output`` breaks ``contract`` (empty list = compatible). Additive changes — new tables,
    new columns — are fine; a removed table, a dropped column, or a changed type is a violation. A
    ``None``/empty contract (a first run, or a rollback the Catchment chose not to gate) never fails."""
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
            elif have[column] != type_:
                out.append(f"column '{table}.{column}' changed type {type_} → {have[column]}")
    return out
