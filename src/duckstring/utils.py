from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

# Optional ibis integration (recommended)
try:
    import ibis  # type: ignore

    _HAVE_IBIS = True
except Exception:  # pragma: no cover
    ibis = None  # type: ignore
    _HAVE_IBIS = False

# Optional duckdb (only needed for execution)
try:
    import duckdb  # type: ignore

    _HAVE_DUCKDB = True
except Exception:  # pragma: no cover
    duckdb = None  # type: ignore
    _HAVE_DUCKDB = False


def expr_schema(expr: Any) -> Optional[Dict[str, str]]:
    if expr is None:
        return None
    if _HAVE_IBIS:
        try:
            sch = expr.schema()
            return {k: str(v) for k, v in sch.items()}
        except Exception:
            return None
    return None


def physical_table_name(pond_name: str, table_name: str) -> str:
    if not pond_name or not table_name:
        raise ValueError("pond_name and table_name must be non-empty")
    return f"ds__{pond_name}__{table_name}"


def ibis_placeholder_table(pond_name: str, table_name: str, schema: Dict[str, str]) -> Any:
    if not _HAVE_IBIS:
        raise RuntimeError(
            "Ibis is not available. Install 'ibis-framework' to use contract-backed preflight checks."
        )
    ibis_schema = ibis.schema({k: ibis.dtype(v) for k, v in schema.items()})
    return ibis.table(ibis_schema, name=physical_table_name(pond_name, table_name))


def select_and_alias(table_expr: Any, mapping: Mapping[str, str]) -> Any:
    if not _HAVE_IBIS:
        raise RuntimeError("Ibis is not available. Install 'ibis-framework' to use duckstring Pond.get().")

    projections = []
    for out_col, in_col in mapping.items():
        projections.append(table_expr[in_col].name(out_col))
    return table_expr.select(projections)


def toposort(edges: Mapping[str, Set[str]]) -> List[str]:
    nodes = set(edges.keys())
    indeg: Dict[str, int] = {n: 0 for n in nodes}
    downstreams: Dict[str, Set[str]] = {n: set() for n in nodes}

    for n, ups in edges.items():
        for u in ups:
            if u not in nodes:
                continue
            indeg[n] += 1
            downstreams[u].add(n)

    q = sorted([n for n in nodes if indeg[n] == 0])
    out: List[str] = []
    while q:
        n = q.pop(0)
        out.append(n)
        for d in sorted(downstreams.get(n, set())):
            indeg[d] -= 1
            if indeg[d] == 0:
                q.append(d)

    if len(out) != len(nodes):
        raise ValueError("Cycle detected in pond dependency graph.")
    return out


def layered_toposort(edges: Mapping[str, Set[str]]) -> List[List[str]]:
    """
    Return stages as list[list[node]] such that all nodes in a stage can run in parallel.
    Deterministic ordering: nodes sorted within each stage.
    """
    nodes = set(edges.keys())
    indeg = {n: 0 for n in nodes}
    downstream = {n: set() for n in nodes}

    for n, ups in edges.items():
        for u in ups:
            if u not in nodes:
                continue
            indeg[n] += 1
            downstream[u].add(n)

    stages: List[List[str]] = []
    ready = sorted([n for n in nodes if indeg[n] == 0])

    processed = set()
    while ready:
        stage = list(ready)
        stages.append(stage)
        ready = []
        for n in stage:
            processed.add(n)
            for d in downstream.get(n, set()):
                indeg[d] -= 1
                if indeg[d] == 0:
                    ready.append(d)
        ready = sorted(ready)

    if len(processed) != len(nodes):
        raise ValueError("Cycle detected in pond dependency graph.")
    return stages


def parse_semver_major(version: str) -> int:
    # semver-ish: "X.Y.Z"
    try:
        return int(version.split(".", 1)[0])
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"Invalid version string: {version!r}") from exc


def parse_semver(version: str) -> Tuple[int, int, int]:
    parts = version.split(".")
    if len(parts) != 3:
        raise ValueError(f"Invalid version string: {version!r}")
    try:
        return int(parts[0]), int(parts[1]), int(parts[2])
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"Invalid version string: {version!r}") from exc


def split_pond_ref(pond_ref: str) -> Tuple[str, Optional[str]]:
    if "@" not in pond_ref:
        if not pond_ref:
            raise ValueError("Pond reference must be non-empty.")
        return pond_ref, None
    name, version = pond_ref.split("@", 1)
    if not name or not version:
        raise ValueError(f"Invalid pond reference: {pond_ref!r}")
    return name, version


def load_module_from_file(name: str, path: Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod
