"""Plan capture — serialise a Trickle ripple into the DuckFlock **plan IR** (``duckflock_plan: 1``).

A ripple author writes normal ``pond.trickle(...)`` code; capture runs that code against a *recording*
handle whose ``.trickle()`` returns a :class:`TrickleBuilder` **minus execution** — every
``.merge()``/``.append()`` records a statement instead of computing, and ``read``s register catalog
entries. The result is the logical plan a Duckstring Cloud runtime submits to DuckFlock (the normative
schema is duckflock's ``plans/plan-ir.md``; this module mirrors the ``TrickleBuilder`` structure that doc
mirrors, so capture is mechanical and semantics can't drift in translation).

**Additive & opt-in.** Nothing here touches the executing path. A ripple the recorder can't express — a
raw ``con`` access, a callable metric (``agg.reduce`` / ``acc.scan``), an Ibis-expression ``.sql()`` —
raises :class:`NonCapturable`; the caller falls back to classic execution. Capture fails loud, never
silently wrong.

**Byte-sensitive serialisation.** DuckFlock deserialises with ``preserve_order`` — mutate-column and
metric order is semantic — so every dict here is emitted in declaration order; the key order within each
statement/entry is fixed to the schema. Timestamps and the epoch envelope are the caller's concern (they
are storage/run facts, not builder facts); this module emits only the builder-derived ``catalog`` +
``statements`` body, plus an optional envelope wrapper.
"""

from __future__ import annotations

from .builder import BuildError, TrickleBuilder, _Join, _Source
from .io import normalize_pk

# The plan-IR envelope version. Additive fields only within a version (duckflock deserialises forward).
DUCKFLOCK_PLAN = 1


class NonCapturable(Exception):
    """The ripple used a construct capture can't express (raw ``con`` access, a callable metric, a
    non-string ``.sql()`` query). The caller marks the ripple non-capturable and runs it classically."""


# ─── metric serialisation ──────────────────────────────────────────────────────

# acc metric internal-kind → (IR kind, param IR key). ``lag`` is special-cased (n==1 → "prev"), ``conv``
# carries its kernel in ``init`` not ``param``.
_ACC_PARAM_KEY = {"ema": "alpha", "tema": "lam"}


def _agg_metric(m) -> dict:
    """Serialise an :class:`duckstring.agg.Metric`. Key order (schema-fixed): kind, col, col2, how."""
    if m.kind == "reduce" or m.fn is not None:
        raise NonCapturable("agg.reduce carries a Python callable — not expressible in the plan IR")
    out = {"kind": m.kind}
    if m.col is not None:
        out["col"] = m.col
    if m.col2 is not None:
        out["col2"] = m.col2
    if m.how is not None:
        out["how"] = m.how
    return out


def _acc_metric(m) -> dict:
    """Serialise an :class:`duckstring.acc.AccMetric`. Key order: kind, col, then the kind's scalar param
    (``alpha``/``lam``/``n``/``kernel``). ``acc.prev`` and ``acc.lag(1)`` share the internal ``lag`` kind;
    the canonical IR for ``n==1`` is ``prev`` (no param). ``acc.convolution`` is internal ``conv``."""
    if m.kind == "scan" or m.fn is not None:
        raise NonCapturable("acc.scan carries a Python callable — not expressible in the plan IR")
    kind = m.kind
    extra: dict = {}
    if kind == "lag":
        if m.param == 1:
            kind = "prev"
        else:
            extra["n"] = m.param
    elif kind == "conv":
        kind = "convolution"
        extra["kernel"] = list(m.init)
    elif kind in _ACC_PARAM_KEY:
        extra[_ACC_PARAM_KEY[kind]] = m.param
    out = {"kind": kind}
    if m.col is not None:
        out["col"] = m.col
    out.update(extra)
    return out


# ─── DAG + pipeline serialisation ───────────────────────────────────────────────


def _dag(node, alias_of: dict) -> dict:
    """Serialise a builder operator node. Source: {node, ref, alias, p}; join: {node, how, on, left,
    right}. ``alias`` is the *effective* alias (explicit ``.alias()`` else the positional ``s{i}`` the
    compiler assigns) so DuckFlock never re-derives positionals."""
    if isinstance(node, _Source):
        return {"node": "source", "ref": node.ref, "alias": alias_of[id(node)], "p": node.p}
    if isinstance(node, _Join):
        return {
            "node": "join",
            "how": node.how,
            "on": [{"left": lft, "right": rgt} for lft, rgt in node.on_pairs],
            "left": _dag(node.left, alias_of),
            "right": _dag(node.right, alias_of),
        }
    raise NonCapturable(f"unrecognised DAG node {type(node).__name__}")


def _pipeline(ops) -> list:
    """Serialise the ordered filter/mutate/select pipeline (call order is semantic)."""
    out = []
    for kind, arg in ops:
        if kind == "filter":
            out.append({"op": "filter", "predicate": arg})
        elif kind == "mutate":
            out.append({"op": "mutate", "columns": dict(arg)})  # insertion order preserved
        elif kind == "select":
            out.append({"op": "select", "projection": arg})
        else:  # defensive — the builder only records these three
            raise NonCapturable(f"unrecognised pipeline op {kind!r}")
    return out


def _leaf_refs(node) -> list:
    """The source refs under a DAG node, left-to-right (first-appearance order for the catalog)."""
    if isinstance(node, _Source):
        return [node.ref]
    if isinstance(node, _Join):
        return _leaf_refs(node.left) + _leaf_refs(node.right)
    return []


def _merge_options(ivm, key_filter, retain_t, retain_n) -> dict:
    """Sparse ``options`` for a merge terminal — only non-default values (order: ivm, key_filter,
    retain_t_s, retain_n). DuckFlock fills defaults on deserialise."""
    opts: dict = {}
    if not ivm:
        opts["ivm"] = False
    if not key_filter:
        opts["key_filter"] = False
    _retention_options(opts, retain_t, retain_n)
    return opts


def _append_options(ivm, key_filter, fail_on_conflict, log_drops, retain_t, retain_n) -> dict:
    """Sparse ``options`` for an append terminal. ``fail_on_conflict``/``log_drops`` are emitted only when
    conflicts are **waived** (``fail_on_conflict=False``) — with conflicts raising, ``log_drops`` has no
    effect, so it is omitted. Order: ivm, key_filter, fail_on_conflict, log_drops, retain_t_s, retain_n."""
    opts: dict = {}
    if not ivm:
        opts["ivm"] = False
    if not key_filter:
        opts["key_filter"] = False
    if not fail_on_conflict:
        opts["fail_on_conflict"] = False
        opts["log_drops"] = bool(log_drops)
    _retention_options(opts, retain_t, retain_n)
    return opts


def _retention_options(opts: dict, retain_t, retain_n) -> None:
    if retain_t is not None:
        opts["retain_t_s"] = retain_t.total_seconds() if hasattr(retain_t, "total_seconds") else retain_t
    if retain_n is not None:
        opts["retain_n"] = retain_n


# ─── the recorder + the capturing builder/handle ────────────────────────────────


class PlanRecorder:
    """Accumulates captured ``statements`` and assembles the source/own ``catalog``.

    ``source_catalog(ref) -> {location, mode, pk, floor, f}`` supplies a source's storage facts (read from
    its published sidecar + a location the caller resolves — an ``__SRC__`` token under conformance, an S3
    dir in production). ``own_location`` is the location string for this pond's own outputs (readable back
    for chained statements). The recorder owns the catalog *structure* (sources in first-appearance order,
    then own outputs in statement order) and the own-output entries; the resolver supplies only per-source
    facts, mirroring "the catalog entries are the sidecar contents plus a location"."""

    def __init__(self, source_catalog, own_location: str) -> None:
        self.source_catalog = source_catalog
        self.own_location = own_location
        self.statements: list = []
        self._owns: dict[str, dict] = {}
        self._own_order: list[str] = []

    def record(self, stmt: dict, name: str, terminal: str, eff_pk) -> None:
        self.statements.append(stmt)
        self._owns[name] = {"mode": terminal, "pk": list(eff_pk)}  # own mode == the terminal (merge|append)
        self._own_order.append(name)

    def catalog(self) -> dict:
        sources: list[str] = []
        seen: set[str] = set()
        for stmt in self.statements:
            dag = stmt.get("dag") if stmt["kind"] == "trickle" else stmt.get("input_dag")
            for ref in _leaf_refs_from_ir(dag):
                if ref in self._owns or ref in seen:  # an own output read back by a later statement
                    continue
                seen.add(ref)
                sources.append(ref)
        cat: dict = {}
        for ref in sources:
            entry = self.source_catalog(ref)
            if entry is None:
                raise NonCapturable(f"no catalog entry for source ref {ref!r}")
            cat[ref] = entry
        for name in self._own_order:
            o = self._owns[name]
            cat[name] = {"location": self.own_location, "mode": o["mode"], "pk": o["pk"]}
        return cat


def _leaf_refs_from_ir(dag: dict) -> list:
    """Leaf refs of an already-serialised DAG dict, left-to-right."""
    if dag is None:
        return []
    if dag["node"] == "source":
        return [dag["ref"]]
    return _leaf_refs_from_ir(dag["left"]) + _leaf_refs_from_ir(dag["right"])


class CaptureBuilder(TrickleBuilder):
    """A :class:`TrickleBuilder` whose terminals **record** rather than execute. The fluent surface
    (``join``/``filter``/``mutate``/``select``/``along``/``aggregate``/``accumulate``/``alias``) is the
    real builder's — pure recording already — reused verbatim; only ``sql`` and the terminals are
    overridden so nothing runs on the connection."""

    def __init__(self, rec: PlanRecorder, spine_ref: str, *, p: float = 0.3, _capture_sql=None) -> None:
        super().__init__(rec, spine_ref, p=p)
        self.rec = rec
        self._capture_sql = _capture_sql

    def sql(self, query) -> "CaptureBuilder":
        if self._agg is not None:
            raise BuildError(".sql() can't follow .aggregate() — aggregate is terminal-bound to .merge()")
        if self._alias is None:
            raise BuildError(".sql() needs a table name to reference — call .alias('t') first, then '… FROM t'")
        if not isinstance(query, str):
            raise NonCapturable(".sql() with a non-string (Ibis) query isn't captured — pass a SQL string")
        self._prepare_leaves()
        snap = {
            "alias": self._alias,
            "input_dag": _dag(self._root, self._alias_of),
            "pipeline": _pipeline(self._ops),
            "query": query,
        }
        out = CaptureBuilder(self.rec, self._alias, _capture_sql=snap)
        out._materialised = True  # mark comprehensive so a later .join()/.filter() raises like the base
        return out

    def merge(self, name: str, *, pk=None, ivm: bool = True, key_filter: bool = True,
              retain_t=None, retain_n=None) -> "CaptureBuilder":
        options = _merge_options(ivm, key_filter, retain_t, retain_n)
        eff_pk = self._effective_pk(pk)
        self._emit(name, "merge", pk, eff_pk, options)
        return self._chain(name, eff_pk)

    def append(self, name: str, *, pk=None, fail_on_conflict: bool = True, log_drops: bool = True,
               ivm: bool = True, key_filter: bool = True, retain_t=None, retain_n=None) -> "CaptureBuilder":
        if self._agg is not None:
            raise BuildError(".append() can't follow .aggregate() — an aggregate updates groups; use .merge()")
        options = _append_options(ivm, key_filter, fail_on_conflict, log_drops, retain_t, retain_n)
        eff_pk = normalize_pk(pk)
        self._emit(name, "append", pk, eff_pk, options)
        return self._chain(name, eff_pk)

    # ─── recording internals ─────────────────────────────────────────────────────

    def _effective_pk(self, pk):
        """The output identity actually used: the ``by`` group key for a keyless aggregate merge, else the
        declared pk. (The statement's ``output.pk`` records the *literal* argument — ``None`` here — while
        the own catalog entry uses this effective key, matching the hand-declared IR.)"""
        if self._agg is not None and pk is None:
            return self._agg["by"]
        return normalize_pk(pk)

    def _emit(self, name: str, terminal: str, literal_pk, eff_pk, options: dict) -> None:
        output = {"name": name, "terminal": terminal, "pk": list(normalize_pk(literal_pk)) or None,
                  "options": options}
        if self._capture_sql is not None:
            stmt = {
                "kind": "sql",
                "alias": self._capture_sql["alias"],
                "input_dag": self._capture_sql["input_dag"],
                "pipeline": self._capture_sql["pipeline"],
                "query": self._capture_sql["query"],
                "output": output,
            }
        else:
            self._prepare_leaves()
            stmt = {
                "kind": "trickle",
                "output": output,
                "dag": _dag(self._root, self._alias_of),
                "pipeline": _pipeline(self._ops),
            }
            if self._agg is not None:
                stmt["aggregate"] = {
                    "by": list(self._agg["by"]),
                    "metrics": {out: _agg_metric(m) for out, m in self._agg["metrics"].items()},
                }
            elif self._acc is not None:
                stmt["along"] = self._along
                stmt["accumulate"] = {
                    "by": list(self._acc["by"]),
                    "metrics": {out: _acc_metric(m) for out, m in self._acc["metrics"].items()},
                }
        self.rec.record(stmt, name, terminal, eff_pk)

    def _chain(self, name: str, eff_pk) -> "CaptureBuilder":  # type: ignore[override]
        """A chained handle rooted at the just-written output. An own-output operand is an in-run
        materialised delta, so its change-fraction threshold is moot — ``p=1.0`` (disabled), matching the
        hand-declared IR for chained sources."""
        return CaptureBuilder(self.rec, name, p=1.0)


class CapturePond:
    """The recording *host* a ripple function runs against. ``.trickle()`` returns a
    :class:`CaptureBuilder`; direct data access (``read_table``/``read_delta``/``merge_table``/…) is
    **non-capturable** and fails loud so the caller falls back to classic execution."""

    def __init__(self, rec: PlanRecorder) -> None:
        self.rec = rec

    def trickle(self, spine_ref: str, *, p: float = 0.3) -> CaptureBuilder:
        return CaptureBuilder(self.rec, spine_ref, p=p)

    def _no(self, what: str):
        raise NonCapturable(
            f"{what} is a raw data access — a ripple that reaches past .trickle(...) isn't captured; "
            f"it runs on the classic host path"
        )

    def read_table(self, ref):  # noqa: D401
        self._no("read_table")

    def read_delta(self, ref):
        self._no("read_delta")

    def merge_table(self, *a, **k):
        self._no("merge_table")

    def append_table(self, *a, **k):
        self._no("append_table")

    def apply_zset(self, *a, **k):
        self._no("apply_zset")

    def skip(self, *a, **k):
        self._no("pond.skip()")


def capture_plan(run_python, *, source_catalog, own_location: str = "__OWN__") -> dict:
    """Capture a ripple function into the plan-IR **body** ``{"catalog", "statements"}``.

    ``run_python(host)`` is the user's ripple: it composes ``host.trickle(...)`` chains and terminates them
    with ``.merge()``/``.append()``. ``source_catalog(ref) -> {location, mode, pk, floor, f}`` supplies each
    source's storage facts; ``own_location`` is this pond's own-output location string. Raises
    :class:`NonCapturable` on anything the recorder can't express (the opt-in, fail-loud contract). The
    caller wraps the body in the ``duckflock_plan``/``job_id``/``pond``/``epoch``/``config`` envelope
    (:func:`envelope`), which carries run/storage facts the builder doesn't hold."""
    rec = PlanRecorder(source_catalog, own_location)
    run_python(CapturePond(rec))
    return {"catalog": rec.catalog(), "statements": rec.statements}


def envelope(body: dict, *, job_id: str, tenant: str, pond: dict, epoch: dict, config: dict) -> dict:
    """Wrap a captured body in the versioned plan-IR envelope. ``epoch`` is ``{"f", "previous_f"}`` as
    ``isoformat()`` strings (``NEVER`` = ``0001-01-01T00:00:00+00:00``); ``pond`` is
    ``{"name", "major", "version"}``. Additive fields only within ``duckflock_plan: 1``."""
    return {
        "duckflock_plan": DUCKFLOCK_PLAN,
        "job_id": job_id,
        "tenant": tenant,
        "pond": pond,
        "epoch": epoch,
        "catalog": body["catalog"],
        "statements": body["statements"],
        "config": config,
    }
