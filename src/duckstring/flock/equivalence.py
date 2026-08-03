"""Expression-level equivalence gating: which SQL an engine may be handed unchanged.

DuckDB is the authority for what a terminal produces, so a dispatch is only permitted for expressions
whose meaning we know the engine reproduces. That knowledge is **per engine** — "provably equivalent" is
not a property of an expression, it is a property of an expression *and* the engine that will run it —
so the allow-lists live with each engine and this module only provides the machinery.

The deliberate non-goal: rewriting user SQL into an engine's dialect. We pass expressions through
unchanged and refuse the ones we cannot vouch for; a rewrite layer would be a second implementation of
DuckDB's semantics, which is exactly the thing that would silently drift.
"""

from __future__ import annotations

import logging
import re

log = logging.getLogger("duckstring.flock")

# Structural nodes every engine agrees on: references, literals, comparisons, boolean connectives.
BASE_NODES = frozenset({
    "Column", "Identifier", "Star", "Alias", "Literal", "Boolean", "Null", "Paren",
    "EQ", "NEQ", "GT", "GTE", "LT", "LTE", "And", "Or", "Not", "Is", "In", "Between",
})

_BARE_COLUMN = re.compile(r"\s*[\w.\"]+(\s+AS\s+[\w\"]+)?\s*", re.IGNORECASE)


def expressions_of(builder) -> list[str]:
    """Every user-written SQL fragment this terminal would hand to an engine."""
    out: list[str] = []
    for kind, payload in builder._ops:
        if kind in ("filter", "select"):
            out.append(str(payload))
        elif kind == "mutate":
            out.extend(str(v) for v in (payload or {}).values())
    return out


def unproven(builder, allowed_nodes: frozenset[str], allowed_funcs: frozenset[str]) -> str | None:
    """``None`` when every expression is built from ``allowed_nodes`` (and, for function calls, from
    ``allowed_funcs``); else the reason this terminal must run locally.

    Judged with a real parser (sqlglot, the ``duckstring[lineage]`` extra). Without one we cannot read an
    expression at all, so only bare column lists pass — unparsed SQL is never assumed safe."""
    exprs = expressions_of(builder)
    if not exprs:
        return None
    try:
        import sqlglot
        import sqlglot.expressions as sge
    except ImportError:
        if all(all(_BARE_COLUMN.fullmatch(part) for part in e.split(",")) for e in exprs):
            return None
        return ("no SQL parser available to check the expressions — install duckstring[lineage], or use "
                "bare column projections")
    for expr in exprs:
        try:
            trees = sqlglot.parse(f"SELECT {expr}", read="duckdb")
        except Exception:  # noqa: BLE001 — unparseable means uncheckable
            return f"could not parse {expr!r}"
        for tree in trees:
            for node in tree.walk():
                name = type(node).__name__
                if name in ("Select", "Expression") or name in allowed_nodes:
                    continue
                if isinstance(node, sge.Anonymous) or name == "Anonymous":
                    fn = str(getattr(node, "this", "")).lower()
                    if fn in allowed_funcs:
                        continue
                    return f"function {fn}() is not on this engine's verified list (in {expr!r})"
                if name.lower() in allowed_funcs:
                    continue  # sqlglot models many builtins as their own node type (Round, Abs, …)
                return f"{name} is not on this engine's verified list (in {expr!r})"
    return None
