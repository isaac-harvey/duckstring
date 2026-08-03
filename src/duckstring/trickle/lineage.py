"""Static column lineage walked from the captured plan IR (plans/lineage.md Phase 2).

``column_lineage(body, schemas=…)`` maps every output column of a captured Trickle plan
(:mod:`duckstring.trickle.capture`) to the **source columns it derives from** — a walk over structures
the builder already defines (the operator DAG, the ordered ``filter/mutate/select`` pipeline, the
aggregate/accumulate specs), using the same namespace rules the compiler applies (``alias.col`` refs,
bare mutated names, bare leaf columns unique across the subtree, equi-join keys unified).

**Exact or absent — never inferred.** A column whose derivation the walk can prove is a set of
``(ref, column)`` pairs (``ref`` a catalog source ref; own-output refs are resolved transitively, so
chained statements bottom out at external sources). A constant (``1 AS one``) is an **empty** set —
exactly nothing. Anything the walk can't prove — a ``kind: sql`` statement's output (Phase 3 upgrades
those with a real parser), a star output over a source with no known schema — is ``None`` (*opaque*,
we know we don't know). Nothing here executes; like capture, this module depends only on trickle/.
"""

from __future__ import annotations

import re

from .builder import _LEFT_ONLY, _select_items

# Provenance of one column: a set of (ref, column) pairs; None = opaque (unprovable).
Prov = set | None

_IDENT = re.compile(r"[A-Za-z_]\w*")
_COLPART = re.compile(r'"[^"]*"|[A-Za-z_]\w*')


class _Namespace:
    """The columns visible at a point in one statement's pipeline: leaf columns keyed ``(alias, col)``
    and derived (mutated) columns keyed by bare name — the two namespaces ``_qualify`` keeps apart."""

    def __init__(self) -> None:
        self.leaf: dict[tuple[str, str], Prov] = {}   # (alias, col) → provenance
        self.derived: dict[str, Prov] = {}            # mutated name → provenance
        self.aliases: set[str] = set()
        self.complete = True  # False when some leaf's column set is unknown (no schema)
        # For a leaf whose column set is unknown but whose ref is EXTERNAL, an explicit ``alias.col``
        # is still provable — it names exactly (ref, col). alias → ref; None = an opaque own output.
        self.unknown_ref: dict[str, str | None] = {}

    def bare_leaf(self, name: str):
        """The unique leaf column with bare name ``name``, or None (absent/ambiguous). Unified join keys
        share provenance, so any of their copies is equally correct."""
        hits = [k for k in self.leaf if k[1] == name]
        return hits[0] if len(hits) == 1 else None


def _union(*provs: Prov) -> Prov:
    """Union of provenances; any opaque operand makes the result opaque."""
    out: set = set()
    for p in provs:
        if p is None:
            return None
        out |= p
    return out


def _expr_refs(expr: str, ns: _Namespace) -> Prov:
    """The provenance an expression's column references contribute — scanning for ``alias.col`` /
    ``alias."col"`` and bare names that resolve to a derived or unique leaf column, skipping string
    literals (the same token rules ``_qualify`` applies). Unresolved identifiers are SQL functions /
    keywords by the builder's namespace contract, so the union over resolved refs is complete —
    *unless* a leaf's column set is unknown, in which case a bare name we can't place makes the
    result opaque rather than silently under-reported."""
    provs: list[Prov] = []
    i, n = 0, len(expr)
    while i < n:
        ch = expr[i]
        if ch == "'":  # string literal — skip (incl. doubled '' escapes)
            j = i + 1
            while j < n:
                if expr[j] == "'":
                    if j + 1 < n and expr[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            i = j
            continue
        m = _IDENT.match(expr, i)
        if not m:
            i += 1
            continue
        word = m.group(0)
        k = m.end()
        if word in ns.aliases and k < n and expr[k] == ".":
            cm = _COLPART.match(expr, k + 1)
            if cm:
                col = cm.group(0)
                bare = col[1:-1] if col.startswith('"') else col
                if (word, bare) in ns.leaf:
                    provs.append(ns.leaf[(word, bare)])
                elif word in ns.unknown_ref:  # explicit ref over an unknown-schema leaf: still exact
                    ref = ns.unknown_ref[word]
                    provs.append({(ref, bare)} if ref is not None else None)
                elif not ns.complete:
                    provs.append(None)
                i = cm.end()
                continue
        if word in ns.derived:
            provs.append(ns.derived[word])
        else:
            hit = ns.bare_leaf(word)
            if hit is not None:
                provs.append(ns.leaf[hit])
            elif not ns.complete and word.isidentifier() and not (k < n and expr[k] == "("):
                # an unknown bare identifier over an incomplete namespace *might* be a column — opaque
                provs.append(None)
        i = m.end()
    return _union(*provs)


def _leaf_columns(ref: str, schemas: dict, own: dict) -> dict | None:
    """``{col: provenance}`` for a source leaf: an own (chained) output resolves transitively; an
    external ref takes its catalog/sidecar schema's column list (each col ← itself). None = unknown."""
    if ref in own:
        cols = own[ref]
        return None if cols is None else dict(cols)
    schema = (schemas or {}).get(ref)
    if schema is None:
        return None
    return {col: {(ref, col)} for col in schema}


def _walk_dag(node: dict, schemas: dict, own: dict) -> _Namespace:
    """Compose the DAG's namespace: leaves populate ``(alias, col)``; a join concatenates namespaces
    (left-only for semi/anti) and unifies its equi-key columns' provenance."""
    if node["node"] == "source":
        ns = _Namespace()
        ns.aliases = {node["alias"]}
        cols = _leaf_columns(node["ref"], schemas, own)
        if cols is None:
            ns.complete = False
            # an external ref stays addressable by explicit alias.col; an opaque own output does not
            ns.unknown_ref[node["alias"]] = node["ref"] if node["ref"] not in own else None
        else:
            ns.leaf = {(node["alias"], c): p for c, p in cols.items()}
        return ns
    left = _walk_dag(node["left"], schemas, own)
    right = _walk_dag(node["right"], schemas, own)
    ns = _Namespace()
    ns.aliases = left.aliases | right.aliases
    ns.complete = left.complete and (node["how"] in _LEFT_ONLY or right.complete)
    ns.unknown_ref = {**left.unknown_ref, **({} if node["how"] in _LEFT_ONLY else right.unknown_ref)}
    ns.leaf = dict(left.leaf)
    if node["how"] not in _LEFT_ONLY:
        ns.leaf.update(right.leaf)
    for pair in node.get("on", []):
        lk = _resolve_key(pair["left"], left)
        rk = _resolve_key(pair["right"], right)
        if lk is None or rk is None:
            continue  # unresolvable over an incomplete namespace — the key columns stay one-sided
        merged = _union(left.leaf.get(lk, set()), right.leaf.get(rk, set()))
        ns.leaf[lk] = merged
        if node["how"] not in _LEFT_ONLY:
            ns.leaf[rk] = merged  # an equi-key column carries values from both sides
    return ns


def _resolve_key(name: str, side: _Namespace):
    """Resolve a join-key name within one side — ``alias.col`` exact, else the unique bare leaf col."""
    if "." in name:
        alias, col = name.split(".", 1)
        return (alias, col) if (alias, col) in side.leaf else None
    return side.bare_leaf(name)


def _apply_pipeline(ns: _Namespace, pipeline: list) -> None:
    """Apply the ordered filter/mutate/select ops to the namespace's derived map, in call order.
    A filter adds no column edges (column lineage tracks derivation, not row selection)."""
    for op in pipeline or []:
        if op["op"] == "mutate":
            pre = {name: _expr_refs(expr, ns) for name, expr in op["columns"].items()}
            ns.derived.update(pre)  # siblings computed against the pre-mutate namespace


def _output_columns(stmt: dict, ns: _Namespace) -> dict | None:
    """The statement's output ``{col: provenance}`` after the pipeline — the last ``select``'s items,
    or the bare-``*`` (leaf cols with unified keys collapsed to one copy, plus mutated cols)."""
    selects = [op for op in stmt.get("pipeline") or [] if op["op"] == "select"]
    if selects:
        out: dict = {}
        for item in _select_items(selects[-1]["projection"]):
            m = re.match(r"^(.*?)\s+as\s+(\"?)([A-Za-z_]\w*)\2\s*$", item, re.IGNORECASE)
            expr, name = (m.group(1), m.group(3)) if m else (item, None)
            if name is None:  # bare `alias.col` / `col` — the output name is the column's own
                cm = re.match(r'^(?:[A-Za-z_]\w*\.)?("?)([A-Za-z_]\w*)\1$', item.strip())
                if cm is None:
                    return None  # a computed item with no AS — the builder wouldn't name it either
                name = cm.group(2)
            out[name] = _expr_refs(expr, ns)
        return out
    if not ns.complete:
        return None  # a star output over an unknown column set is unenumerable — opaque table
    out = {}
    seen_prov: list = []
    for (_alias, col), prov in ns.leaf.items():  # insertion order = left-to-right leaves
        if col in out:
            if prov is not None and prov in seen_prov:
                continue  # the unified copy of an equi-join key — deduplicated, same provenance
            return None  # a genuine bare-name collision — the builder raises here; we go opaque
        out[col] = prov
        seen_prov.append(prov)
    out.update(ns.derived)  # a mutated column replaces a same-named leaf column
    return out


def _sql_statement_lineage(stmt: dict, schemas: dict, own: dict) -> dict | None:
    """Column lineage through a ``kind: sql`` statement (the ``.sql()`` escape hatch), via **sqlglot**
    when installed (the ``duckstring[lineage]`` extra) — else ``None`` (opaque, exactly as before; the
    parser only ever *upgrades* precision, absence never degrades below honest-opaque).

    The composed input relation is exposed to the query under the statement's ``alias``; its column
    provenance is the input DAG's star output (pipeline applied). The query is parsed (DuckDB dialect),
    qualified against that one-table schema so ``SELECT *`` expands and bare columns bind, and each
    projection's column references map through the input provenance. A parse/qualify failure → opaque."""
    try:
        import sqlglot
        from sqlglot import exp
        from sqlglot.optimizer.qualify import qualify
    except ImportError:
        return None

    ns = _walk_dag(stmt["input_dag"], schemas, own)
    _apply_pipeline(ns, stmt.get("pipeline"))
    input_cols = _output_columns({"pipeline": [op for op in stmt.get("pipeline") or []
                                               if op["op"] == "select"]}, ns)
    if input_cols is None:
        return None  # the input star is unenumerable — the query's bare refs can't bind
    alias = stmt["alias"]
    try:
        tree = sqlglot.parse_one(stmt["query"], dialect="duckdb")
        tree = qualify(tree, schema={alias: {c: "VARCHAR" for c in input_cols}}, dialect="duckdb")
    except Exception:
        return None
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        return None
    out: dict = {}
    for proj in select.expressions:
        name = proj.alias_or_name
        refs = [c for c in proj.find_all(exp.Column)]
        provs = []
        for c in refs:
            col = c.name
            if col in input_cols:
                provs.append(input_cols[col])
            else:
                provs.append(None)  # a ref we can't place (a subquery's own alias, etc.) — opaque col
        out[name] = _union(*provs) if provs else set()  # no column refs = a constant / count(*)
    return out


def _statement_lineage(stmt: dict, schemas: dict, own: dict) -> dict | None:
    if stmt.get("kind") == "sql":
        return _sql_statement_lineage(stmt, schemas, own)  # sqlglot-backed (Phase 3); opaque without it
    if stmt.get("kind") != "trickle":
        return None
    ns = _walk_dag(stmt["dag"], schemas, own)
    _apply_pipeline(ns, stmt.get("pipeline"))

    if stmt.get("aggregate"):
        agg = stmt["aggregate"]
        out: dict = {}
        for by in agg["by"]:
            out[by] = ns.derived.get(by) if by in ns.derived else (
                ns.leaf[ns.bare_leaf(by)] if ns.bare_leaf(by) else (set() if ns.complete else None))
        for name, m in agg["metrics"].items():
            cols = [c for c in (m.get("col"), m.get("col2")) if c]
            out[name] = _union(*(_expr_refs(c, ns) for c in cols)) if cols else set()
        return out

    cols = _output_columns(stmt, ns)
    if stmt.get("accumulate"):
        if cols is None:
            return None
        acc = stmt["accumulate"]
        along = stmt.get("along")
        for name, m in acc["metrics"].items():
            base = [c for c in (m.get("col"), along) if c]
            cols[name] = _union(*(_expr_refs(c, ns) for c in base)) if base else set()
    return cols


def column_lineage(body: dict, schemas: dict | None = None) -> dict:
    """Column lineage for a captured plan body (``{"catalog", "statements"}`` — or just statements plus
    an explicit ``schemas`` map ``{ref: [col, …]}``; catalog entries carrying an Extension-2 ``schema``
    contribute automatically). Returns ``{output_table: {column: {(ref, column), …} | None}}`` per
    statement output — own-output refs in chained statements resolve transitively to external sources.
    An opaque *table* (a ``kind: sql`` statement, an unenumerable star) maps to ``None`` wholesale."""
    schemas = dict(schemas or {})
    for ref, entry in (body.get("catalog") or {}).items():
        if isinstance(entry, dict) and entry.get("schema") and ref not in schemas:
            schemas[ref] = list(entry["schema"])
    own: dict = {}
    result: dict = {}
    for stmt in body.get("statements", []):
        name = stmt["output"]["name"] if isinstance(stmt.get("output"), dict) else stmt.get("output")
        lin = _statement_lineage(stmt, schemas, own)
        result[name] = lin
        own[name] = lin  # readable back by later statements (chained merges)
    return result
