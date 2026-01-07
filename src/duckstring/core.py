from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
)

# Optional ibis integration (recommended)
try:
    import ibis  # type: ignore
    from ibis.expr.types import Table as IbisTable  # type: ignore
    from ibis.expr.schema import Schema as IbisSchema  # type: ignore

    _HAVE_IBIS = True
except Exception:  # pragma: no cover
    ibis = None  # type: ignore
    IbisTable = Any  # type: ignore
    IbisSchema = Any  # type: ignore
    _HAVE_IBIS = False


# ----------------------------
# Contracts / manifests
# ----------------------------

ColumnType = str
SchemaSpec = Dict[str, ColumnType]


@dataclass(frozen=True)
class TableContract:
    name: str
    schema: SchemaSpec
    description: Optional[str] = None


@dataclass(frozen=True)
class PondContract:
    name: str
    version: str
    description: Optional[str] = None
    # exported tables only
    tables: Dict[str, TableContract] = field(default_factory=dict)

    def require_table(self, table_name: str) -> TableContract:
        if table_name not in self.tables:
            available = ", ".join(sorted(self.tables.keys())) or "<none>"
            raise KeyError(
                f"Upstream pond '{self.name}@{self.version}' does not export table "
                f"'{table_name}'. Available: {available}"
            )
        return self.tables[table_name]


@dataclass(frozen=True)
class SourceSpec:
    pond: str
    constraint: str  # semver range or pinned version/tag, as a string


@dataclass
class FlowStage:
    """
    A stage boundary in the Pond graph.

    Semantics:
      - A stage groups table materializations that have no ordering requirement relative
        to each other (i.e., can be executed in parallel).
      - Stage ordering is defined by the order of stage creation.
    """

    index: int
    parallelizable: bool
    outputs: List[str] = field(default_factory=list)
    notes: Optional[str] = None


@dataclass(frozen=True)
class PondManifest:
    """
    Output of pond.build(): a pure description of the pond.
    Intended to be serialized to JSON/YAML by the CLI.
    """

    name: str
    version: str
    description: Optional[str]
    sources: Dict[str, str]  # pond_name -> constraint
    stages: List[FlowStage]
    exported_tables: Dict[str, TableContract]
    private_tables: Dict[str, TableContract]


# ----------------------------
# Basin interface (resolver)
# ----------------------------

class Basin(Protocol):
    """
    Minimal interface Pond needs for preflight resolution.

    A Basin is responsible for resolving upstream pond contracts (from git/registry/etc).
    """

    def resolve_contract(self, pond_name: str, constraint: str) -> PondContract: ...


# ----------------------------
# Internal table definitions
# ----------------------------

@dataclass
class _TableDef:
    name: str
    expr: Optional[Any]  # ibis table expression, ideally
    exported: bool
    schema: Optional[SchemaSpec] = None
    description: Optional[str] = None


def _expr_schema(expr: Any) -> Optional[SchemaSpec]:
    """
    Best-effort schema extraction.
    Prefer ibis because it gives stable, explicit column names.
    """
    if expr is None:
        return None

    if _HAVE_IBIS:
        try:
            sch = expr.schema()
            # ibis schema -> dict[str, str]
            return {k: str(v) for k, v in sch.items()}
        except Exception:
            return None

    # Non-ibis objects: cannot infer safely
    return None


def _ibis_placeholder_table(name: str, schema: SchemaSpec) -> Any:
    """
    Create an ibis table expression from a contract schema (no backend binding).
    """
    if not _HAVE_IBIS:
        raise RuntimeError(
            "Ibis is not available. Install 'ibis-framework' to use contract-backed preflight checks."
        )

    # ibis.schema expects mapping col->dtype; dtype strings are accepted by ibis.dtype
    ibis_schema = ibis.schema({k: ibis.dtype(v) for k, v in schema.items()})
    return ibis.table(ibis_schema, name=name)


def _select_and_alias(table_expr: Any, mapping: Mapping[str, str]) -> Any:
    """
    mapping: output_col -> input_col
    """
    if not _HAVE_IBIS:
        raise RuntimeError(
            "Ibis is not available. Install 'ibis-framework' to use duckstring Pond.get()."
        )

    projections = []
    for out_col, in_col in mapping.items():
        # ibis: t[in_col].name(out_col)
        projections.append(table_expr[in_col].name(out_col))
    return table_expr.select(projections)


# ----------------------------
# Upstream accessors
# ----------------------------

class _UpstreamPond:
    def __init__(self, pond: "Pond", name: str, constraint: str):
        self._pond = pond
        self.name = name
        self.constraint = constraint

    @property
    def contract(self) -> Optional[PondContract]:
        return self._pond._resolved_contracts.get(self.name)

    def get(self, table_name: str, mapping: Mapping[str, str]) -> Any:
        """
        Get an ibis table expression for an upstream exported table, selecting + aliasing
        according to mapping (output_col -> upstream_col).

        Preflight validation:
          - requires upstream contract resolved (via pond.attach_basin() or pond.build()).
          - validates requested columns exist in upstream contract.
        """
        contract = self.contract
        if contract is None:
            raise RuntimeError(
                f"Upstream contract for '{self.name}' is not resolved. "
                f"Call pond.attach_basin(basin) or pond.build(basin) before upstream.get()."
            )

        t_contract = contract.require_table(table_name)

        # Validate input columns exist
        missing = sorted({src_col for src_col in mapping.values()} - set(t_contract.schema.keys()))
        if missing:
            available = ", ".join(sorted(t_contract.schema.keys()))
            raise KeyError(
                f"Upstream get({self.name}.{table_name}) requested missing columns: {missing}. "
                f"Available: {available}"
            )

        # Create an ibis placeholder from contract and select/alias requested columns
        upstream_expr = _ibis_placeholder_table(name=f"{self.name}.{table_name}", schema=t_contract.schema)
        return _select_and_alias(upstream_expr, mapping)


class _UpstreamRegistry(Mapping[str, _UpstreamPond]):
    def __init__(self, pond: "Pond"):
        self._pond = pond

    def __getitem__(self, key: str) -> _UpstreamPond:
        if key not in self._pond._sources:
            available = ", ".join(sorted(self._pond._sources.keys())) or "<none>"
            raise KeyError(f"Unknown upstream pond '{key}'. Declared sources: {available}")
        return _UpstreamPond(self._pond, key, self._pond._sources[key])

    def __iter__(self):
        return iter(self._pond._sources.keys())

    def __len__(self):
        return len(self._pond._sources)


# ----------------------------
# Pond
# ----------------------------

class Pond:
    """
    Declarative pond definition:
      - metadata (name/version/description)
      - sources (dependency graph edges)
      - local tables (private/exported)
      - stage boundaries (flow)

    Execution and storage are intentionally not modeled here.
    """

    def __init__(self, name: str, description: Optional[str], version: str):
        if not name:
            raise ValueError("Pond.name must be non-empty.")
        if not version:
            raise ValueError("Pond.version must be non-empty.")

        self.name = name
        self.description = description
        self.version = version

        # pond_name -> version constraint
        self._sources: Dict[str, str] = {}

        # resolved pond_name -> PondContract (exported surface only)
        self._resolved_contracts: Dict[str, PondContract] = {}

        # table_name -> _TableDef (includes private + exported)
        self._tables: Dict[str, _TableDef] = {}

        # stage boundaries
        self._stages: List[FlowStage] = []
        self._pending_outputs: List[str] = []

        # upstream access
        self.upstream: Mapping[str, _UpstreamPond] = _UpstreamRegistry(self)

        # optional basin attached for preflight
        self._basin: Optional[Basin] = None

        if not _HAVE_IBIS:
            # You can still build manifests without ibis; but get()/schema checks require ibis.
            pass

    # -------- Sources --------

    def source(self, sources: Mapping[str, str]) -> None:
        """
        Declare upstream pond dependencies as pond_name -> version constraint.
        Example: {"customer": "^1.0.0", "order": "~1.2.3"}
        """
        for pond_name, constraint in sources.items():
            if pond_name == self.name:
                raise ValueError("A pond cannot declare itself as an upstream source.")
            if not pond_name or not constraint:
                raise ValueError(f"Invalid source declaration: {pond_name!r}: {constraint!r}")
            self._sources[pond_name] = constraint

    def attach_basin(self, basin: Basin) -> None:
        """
        Attach a basin so upstream.get() can resolve contracts immediately.
        """
        self._basin = basin
        self._resolve_upstream_contracts()

    # -------- Flow / stage boundaries --------

    def flow(self, actions: Optional[Sequence[Any]] = None, *, notes: Optional[str] = None) -> None:
        """
        Create a stage boundary.

        Intended use:
          - Call pond.flow([...]) after defining a batch of transformations/sinks to indicate
            they belong to the same stage and can be executed in parallel.

        Implementation detail:
          - This records the names of tables sunk since the previous stage boundary.
          - 'actions' is accepted for ergonomic parity with user code, but Pond does not
            interpret or execute actions; execution belongs to Basin.

        parallelizable rule:
          - If 'actions' is a list/sequence, stage is marked parallelizable=True.
          - If actions is None, parallelizable defaults to True (safe default).
        """
        parallelizable = True
        if actions is not None:
            parallelizable = True  # stage-level hint: "no ordering requirements inside the stage"

        stage = FlowStage(
            index=len(self._stages),
            parallelizable=parallelizable,
            outputs=list(self._pending_outputs),
            notes=notes,
        )
        self._stages.append(stage)
        self._pending_outputs.clear()

    # -------- Local get / sinks --------

    def get(self, table_name: str, mapping: Mapping[str, str]) -> Any:
        """
        Local get: fetch a table already added to the pond context (private or exported),
        then select+alias according to mapping.

        Preflight validation:
          - validates the requested columns exist if schema is known (from ibis expr or
            previously inferred schema).
        """
        if table_name not in self._tables:
            available = ", ".join(sorted(self._tables.keys())) or "<none>"
            raise KeyError(f"Local table '{table_name}' not found in pond context. Available: {available}")

        tdef = self._tables[table_name]

        # Ensure we have an ibis expression to build transformations
        if tdef.expr is None:
            # If schema known, create placeholder; else cannot proceed
            if tdef.schema is None:
                raise RuntimeError(
                    f"Table '{table_name}' has no expression and unknown schema; cannot build ibis expression."
                )
            tdef.expr = _ibis_placeholder_table(name=f"{self.name}.{table_name}", schema=tdef.schema)

        # Validate input cols if we have schema
        schema = tdef.schema or _expr_schema(tdef.expr)
        if schema is not None:
            missing = sorted({src_col for src_col in mapping.values()} - set(schema.keys()))
            if missing:
                available = ", ".join(sorted(schema.keys()))
                raise KeyError(
                    f"Local get({table_name}) requested missing columns: {missing}. Available: {available}"
                )

        return _select_and_alias(tdef.expr, mapping)

    def sink(self, tables: Mapping[str, Any], *, description: Optional[str] = None) -> None:
        """
        Exported materializations (public surface of the pond contract).
        sink() only registers tables in the pond context; it does not execute.
        """
        self._register_tables(tables=tables, exported=True, description=description)

    def sink_private(self, tables: Mapping[str, Any], *, description: Optional[str] = None) -> None:
        """
        Internal materializations (not part of exported contract surface).
        """
        self._register_tables(tables=tables, exported=False, description=description)

    def _register_tables(self, tables: Mapping[str, Any], exported: bool, description: Optional[str]) -> None:
        for name, expr in tables.items():
            if not name:
                raise ValueError("Table name must be non-empty.")
            if name in self._tables:
                raise ValueError(f"Table '{name}' is already defined in this pond.")

            schema = _expr_schema(expr)
            self._tables[name] = _TableDef(
                name=name,
                expr=expr,
                exported=exported,
                schema=schema,
                description=description,
            )
            self._pending_outputs.append(name)

    # -------- Build (manifest + preflight) --------

    def build(self, basin: Optional[Basin] = None) -> PondManifest:
        """
        Finalize and return a manifest for this pond.

        If a basin is supplied (or was attached), upstream contracts are resolved and
        preflight checks are enabled for upstream.get() calls.

        Notes:
          - build() does not execute transformations.
          - if there are pending outputs not yet captured by a stage boundary, build()
            will create an implicit final stage.
        """
        if basin is not None:
            self.attach_basin(basin)
        elif self._basin is not None:
            self._resolve_upstream_contracts()

        # Create implicit final stage if the user forgot to call pond.flow() after last sinks
        if self._pending_outputs:
            self.flow(notes="implicit final stage")

        exported: Dict[str, TableContract] = {}
        private: Dict[str, TableContract] = {}

        for tname, tdef in self._tables.items():
            schema = tdef.schema or (_expr_schema(tdef.expr) if tdef.expr is not None else None) or {}
            t_contract = TableContract(name=tname, schema=schema, description=tdef.description)
            if tdef.exported:
                exported[tname] = t_contract
            else:
                private[tname] = t_contract

        return PondManifest(
            name=self.name,
            version=self.version,
            description=self.description,
            sources=dict(self._sources),
            stages=list(self._stages),
            exported_tables=exported,
            private_tables=private,
        )

    def _resolve_upstream_contracts(self) -> None:
        """
        Resolve upstream contracts via attached basin.
        """
        if self._basin is None:
            return

        for pond_name, constraint in self._sources.items():
            if pond_name in self._resolved_contracts:
                continue
            contract = self._basin.resolve_contract(pond_name, constraint)
            self._resolved_contracts[pond_name] = contract
