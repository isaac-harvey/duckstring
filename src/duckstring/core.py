from __future__ import annotations

import json
import os
import shutil
import sqlite3
import time
import warnings
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
)

from .utils import (
    _HAVE_DUCKDB,
    _HAVE_IBIS,
    duckdb,
    ibis,
)
from .utils import (
    expr_schema as _expr_schema,
)
from .utils import (
    ibis_placeholder_table as _ibis_placeholder_table,
)
from .utils import (
    layered_toposort as _layered_toposort,
)
from .utils import (
    load_module_from_file as _load_module_from_file,
)
from .utils import (
    parse_semver as _parse_semver,
)
from .utils import (
    parse_semver_major as _parse_semver_major,
)
from .utils import (
    physical_table_name as _physical_table_name,
)
from .utils import (
    select_and_alias as _select_and_alias,
)
from .utils import (
    split_pond_ref as _split_pond_ref,
)
from .utils import (
    toposort as _toposort,
)

# ----------------------------
# Resolver context
# ----------------------------

_ACTIVE_RESOLVER: ContextVar[Optional["ContractResolver"]] = ContextVar(
    "duckstring_active_resolver",
    default=None,
)


def _get_active_resolver() -> Optional["ContractResolver"]:
    return _ACTIVE_RESOLVER.get()


@contextmanager
def _resolver_context(resolver: Optional["ContractResolver"]):
    token = _ACTIVE_RESOLVER.set(resolver)
    try:
        yield
    finally:
        _ACTIVE_RESOLVER.reset(token)

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

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "schema": dict(self.schema), "description": self.description}

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "TableContract":
        return TableContract(
            name=str(d["name"]),
            schema=dict(d.get("schema", {})),
            description=d.get("description"),
        )


@dataclass(frozen=True)
class PondContract:
    name: str
    version: str
    description: Optional[str] = None
    tables: Dict[str, TableContract] = field(default_factory=dict)

    def require_table(self, table_name: str) -> TableContract:
        if table_name not in self.tables:
            available = ", ".join(sorted(self.tables.keys())) or "<none>"
            raise KeyError(
                f"Upstream pond '{self.name}@{self.version}' does not export table "
                f"'{table_name}'. Available: {available}"
            )
        return self.tables[table_name]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "tables": {k: v.to_dict() for k, v in self.tables.items()},
        }

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "PondContract":
        tables = {k: TableContract.from_dict(v) for k, v in dict(d.get("tables", {})).items()}
        return PondContract(
            name=str(d["name"]),
            version=str(d["version"]),
            description=d.get("description"),
            tables=tables,
        )


@dataclass
class FlowStage:
    index: int
    parallelizable: bool
    outputs: List[str] = field(default_factory=list)
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "parallelizable": self.parallelizable,
            "outputs": list(self.outputs),
            "notes": self.notes,
        }

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "FlowStage":
        return FlowStage(
            index=int(d["index"]),
            parallelizable=bool(d.get("parallelizable", True)),
            outputs=list(d.get("outputs", [])),
            notes=d.get("notes"),
        )


@dataclass(frozen=True)
class PondManifest:
    name: str
    version: str
    description: Optional[str]
    sources: Dict[str, str]
    stages: List[FlowStage]
    exported_tables: Dict[str, TableContract]
    private_tables: Dict[str, TableContract]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "sources": dict(self.sources),
            "stages": [s.to_dict() for s in self.stages],
            "exported_tables": {k: v.to_dict() for k, v in self.exported_tables.items()},
            "private_tables": {k: v.to_dict() for k, v in self.private_tables.items()},
        }

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "PondManifest":
        return PondManifest(
            name=str(d["name"]),
            version=str(d["version"]),
            description=d.get("description"),
            sources=dict(d.get("sources", {})),
            stages=[FlowStage.from_dict(x) for x in d.get("stages", [])],
            exported_tables={
                k: TableContract.from_dict(v) for k, v in dict(d.get("exported_tables", {})).items()
            },
            private_tables={
                k: TableContract.from_dict(v) for k, v in dict(d.get("private_tables", {})).items()
            },
        )


# ----------------------------
# Resolver interface (what Pond needs)
# ----------------------------

class ContractResolver(Protocol):
    def resolve_contract(self, pond_name: str, constraint: str) -> PondContract: ...


# ----------------------------
# Internal table definitions
# ----------------------------

@dataclass
class _TableDef:
    name: str
    expr: Optional[Any]
    exported: bool
    schema: Optional[SchemaSpec] = None
    description: Optional[str] = None


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
        contract = self.contract
        if contract is None:
            if not _HAVE_IBIS:
                raise RuntimeError(
                    f"Upstream contract for '{self.name}' is not resolved, and ibis is unavailable."
                )
            warnings.warn(
                f"Upstream contract for '{self.name}' is not resolved. "
                "Using a placeholder schema for development-time access.",
                RuntimeWarning,
                stacklevel=2,
            )
            fallback_cols = sorted({str(c) for c in mapping.values() if c})
            schema = {col: "float64" for col in fallback_cols}
            self._pond._record_upstream_select(self.name, table_name, mapping.values())
            upstream_expr = _ibis_placeholder_table(self.name, table_name, schema)
            return _select_and_alias(upstream_expr, mapping)

        t_contract = contract.require_table(table_name)

        missing = sorted({src_col for src_col in mapping.values()} - set(t_contract.schema.keys()))
        if missing:
            available = ", ".join(sorted(t_contract.schema.keys()))
            raise KeyError(
                f"Upstream get({self.name}.{table_name}) requested missing columns: {missing}. "
                f"Available: {available}"
            )

        self._pond._record_upstream_select(self.name, table_name, mapping.values())

        upstream_expr = _ibis_placeholder_table(self.name, table_name, t_contract.schema)
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
    def __init__(self, name: str, description: Optional[str], version: str):
        if not name:
            raise ValueError("Pond.name must be non-empty.")
        if not version:
            raise ValueError("Pond.version must be non-empty.")

        self.name = name
        self.description = description
        self.version = version

        self._sources: Dict[str, str] = {}
        self._resolved_contracts: Dict[str, PondContract] = {}
        self._upstream_selects: Dict[str, Dict[str, Set[str]]] = {}
        self._tables: Dict[str, _TableDef] = {}
        self._stages: List[FlowStage] = []
        self._pending_outputs: List[str] = []
        self.upstream: Mapping[str, _UpstreamPond] = _UpstreamRegistry(self)

        self._resolver: Optional[ContractResolver] = _get_active_resolver()

    def source(self, sources: Mapping[str, str]) -> None:
        for pond_name, constraint in sources.items():
            if pond_name == self.name:
                raise ValueError("A pond cannot declare itself as an upstream source.")
            if not pond_name or not constraint:
                raise ValueError(f"Invalid source declaration: {pond_name!r}: {constraint!r}")
            self._sources[pond_name] = constraint
        self._resolve_upstream_contracts()

    def attach_resolver(self, resolver: ContractResolver) -> None:
        self._resolver = resolver
        self._resolve_upstream_contracts()

    def flow(self, actions: Optional[Sequence[Any]] = None, *, notes: Optional[str] = None) -> None:
        _ = actions
        stage = FlowStage(
            index=len(self._stages),
            parallelizable=True,
            outputs=list(self._pending_outputs),
            notes=notes,
        )
        self._stages.append(stage)
        self._pending_outputs.clear()

    def get(self, table_name: str, mapping: Mapping[str, str]) -> Any:
        if table_name not in self._tables:
            available = ", ".join(sorted(self._tables.keys())) or "<none>"
            raise KeyError(f"Local table '{table_name}' not found in pond context. Available: {available}")

        tdef = self._tables[table_name]

        if tdef.expr is None:
            if tdef.schema is None:
                raise RuntimeError(
                    f"Table '{table_name}' has no expression and unknown schema; cannot build ibis expression."
                )
            tdef.expr = _ibis_placeholder_table(self.name, table_name, tdef.schema)

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
        self._register_tables(tables=tables, exported=True, description=description)

    def sink_private(self, tables: Mapping[str, Any], *, description: Optional[str] = None) -> None:
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

    def build(self, resolver: Optional[ContractResolver] = None) -> PondManifest:
        if resolver is not None:
            self.attach_resolver(resolver)
        elif self._resolver is not None:
            self._resolve_upstream_contracts()

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
        if self._resolver is None:
            return

        for pond_name, constraint in self._sources.items():
            if pond_name in self._resolved_contracts:
                continue
            contract = self._resolver.resolve_contract(pond_name, constraint)
            self._resolved_contracts[pond_name] = contract

    def _record_upstream_select(self, pond_name: str, table_name: str, columns: Sequence[str]) -> None:
        pond_tables = self._upstream_selects.setdefault(pond_name, {})
        current = pond_tables.setdefault(table_name, set())
        for col in columns:
            if col:
                current.add(str(col))

    def upstream_select(self, pond_name: str, table_name: str) -> Optional[Set[str]]:
        pond_tables = self._upstream_selects.get(pond_name)
        if pond_tables is None:
            return None
        cols = pond_tables.get(table_name)
        if cols is None:
            return None
        return set(cols)


# ----------------------------
# Compute: Species and Duck
# ----------------------------

@dataclass(frozen=True)
class Species:
    kind: str = "local"
    engine: str = "duckdb"
    options: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if self.kind != "local":
            raise ValueError(f"Only Species(kind='local') is supported for now. Got: {self.kind!r}")
        if self.engine != "duckdb":
            raise ValueError(f"Only Species(engine='duckdb') is supported for now. Got: {self.engine!r}")


@dataclass
class Duck:
    species: Species
    duckdb_path: Path

    def validate(self) -> None:
        self.species.validate()
        if not self.duckdb_path:
            raise ValueError("Duck.duckdb_path must be set")

    def connect_duckdb(self):
        if not _HAVE_DUCKDB:
            raise RuntimeError("duckdb is required for local execution. Install 'duckdb'.")
        return duckdb.connect(str(self.duckdb_path))

    def connect_ibis(self):
        if not _HAVE_IBIS:
            raise RuntimeError("ibis is required for execution planning/compilation. Install 'ibis-framework'.")
        return ibis.duckdb.connect(database=str(self.duckdb_path))


# ----------------------------
# State store (SQLite default)
# ----------------------------

class SQLiteStateStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.path), timeout=30)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA foreign_keys=ON;")
        return con

    def _init_db(self) -> None:
        con = self._connect()
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS locks (
                    node_id TEXT PRIMARY KEY,
                    holder  TEXT NOT NULL,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS pond_success (
                    pond_name TEXT PRIMARY KEY,
                    last_success_at REAL NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS pond_versions (
                    pond_name TEXT NOT NULL,
                    major INTEGER NOT NULL,
                    version TEXT NOT NULL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (pond_name, major)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS table_success (
                    pond_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    last_success_at REAL NOT NULL,
                    PRIMARY KEY (pond_name, table_name)
                )
                """
            )
            con.commit()
        finally:
            con.close()

    def acquire_lock(self, *, node_id: str, holder: str, ttl_secs: int) -> bool:
        import time

        def _format_duration(seconds: float) -> str:
            if seconds < 1.0:
                value = seconds * 1000.0
                unit = "ms"
            elif seconds < 60.0:
                value = seconds
                unit = "s"
            elif seconds < 3600.0:
                value = seconds / 60.0
                unit = "min"
            elif seconds < 86400.0:
                value = seconds / 3600.0
                unit = "h"
            else:
                value = seconds / 86400.0
                unit = "d"
            return f"{value:.3g}{unit}"

        now = float(time.time())
        expires = now + float(ttl_secs)

        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE;")
            row = con.execute(
                "SELECT holder, expires_at FROM locks WHERE node_id = ?",
                (node_id,),
            ).fetchone()

            if row is None:
                con.execute(
                    "INSERT INTO locks(node_id, holder, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
                    (node_id, holder, now, expires),
                )
                con.commit()
                return True

            _, current_expires = row
            if float(current_expires) <= now:
                con.execute(
                    "UPDATE locks SET holder = ?, acquired_at = ?, expires_at = ? WHERE node_id = ?",
                    (holder, now, expires, node_id),
                )
                con.commit()
                return True

            con.rollback()
            return False
        finally:
            con.close()

    def release_lock(self, *, node_id: str, holder: str) -> None:
        con = self._connect()
        try:
            con.execute("DELETE FROM locks WHERE node_id = ? AND holder = ?", (node_id, holder))
            con.commit()
        finally:
            con.close()

    def set_pond_success(self, *, pond_name: str, ts: float) -> None:
        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO pond_success(pond_name, last_success_at)
                VALUES (?, ?)
                ON CONFLICT(pond_name) DO UPDATE SET last_success_at=excluded.last_success_at
                """,
                (pond_name, ts),
            )
            con.commit()
        finally:
            con.close()

    def get_pond_version(self, *, pond_name: str, major: int) -> Optional[str]:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT version FROM pond_versions WHERE pond_name = ? AND major = ?",
                (pond_name, int(major)),
            ).fetchone()
            if row is None:
                return None
            return str(row[0])
        finally:
            con.close()

    def set_pond_version(self, *, pond_name: str, version: str, ts: float) -> None:
        major = _parse_semver_major(version)
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE;")
            row = con.execute(
                "SELECT version FROM pond_versions WHERE pond_name = ? AND major = ?",
                (pond_name, int(major)),
            ).fetchone()
            if row is None:
                con.execute(
                    """
                    INSERT INTO pond_versions(pond_name, major, version, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (pond_name, int(major), version, ts),
                )
                con.commit()
                return

            existing = str(row[0])
            if _parse_semver(version) >= _parse_semver(existing):
                con.execute(
                    """
                    UPDATE pond_versions
                    SET version = ?, updated_at = ?
                    WHERE pond_name = ? AND major = ?
                    """,
                    (version, ts, pond_name, int(major)),
                )
                con.commit()
            else:
                con.rollback()
        finally:
            con.close()

    def set_table_success(self, *, pond_name: str, table_name: str, ts: float) -> None:
        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO table_success(pond_name, table_name, last_success_at)
                VALUES (?, ?, ?)
                ON CONFLICT(pond_name, table_name) DO UPDATE SET last_success_at=excluded.last_success_at
                """,
                (pond_name, table_name, ts),
            )
            con.commit()
        finally:
            con.close()


# ----------------------------
# Catchment and Basin (v1: local + pulse)
# ----------------------------

@dataclass(frozen=True)
class PulsePlan:
    ponds_topo: Tuple[str, ...]
    outlets: Dict[str, str]
    manifests: Dict[str, PondManifest]


@dataclass(frozen=True)
class PulseResult:
    plan: PulsePlan
    run_id: str
    started_at: float
    ended_at: float
    success: bool

    @property
    def duration(self) -> float:
        return self.ended_at - self.started_at

    @property
    def ponds_topo(self) -> Tuple[str, ...]:
        return self.plan.ponds_topo

    @property
    def outlets(self) -> Dict[str, str]:
        return self.plan.outlets

    @property
    def manifests(self) -> Dict[str, PondManifest]:
        return self.plan.manifests

    def __str__(self) -> str:
        return self.run_id


class Catchment:
    SPEC_VERSION = 1
    DEFAULT_MANIFEST_NAME = "duckstring.manifest.json"
    DEFAULT_POND_ENTRYPOINT = "pond.py"
    DEFAULT_POND_FACTORY_NAME = "pond"

    def __init__(self, *, root_dir: str = ".duckstring"):
        self.root_dir = root_dir
        self.ponds: Dict[str, Any] = {}

        self.species: Dict[str, Species] = {}
        self.default_species: Optional[str] = None
        self.pond_species: Dict[str, str] = {}

        self.modes: Dict[str, Dict[str, Any]] = {"pulse": {"type": "pulse"}}

        self._loaded_from: Optional[str] = None
        self._state: Optional[SQLiteStateStore] = None

    @property
    def state(self) -> SQLiteStateStore:
        if self._state is None:
            state_path = Path(self.root_dir) / "state" / "duckstring_state.sqlite"
            self._state = SQLiteStateStore(state_path)
        return self._state

    @staticmethod
    def load(path: str | os.PathLike[str]) -> "Catchment":
        p = Path(path)
        data = json.loads(p.read_text(encoding="utf-8"))
        c = Catchment.from_dict(data)
        c._loaded_from = str(p)
        return c

    def save(self, path: Optional[str | os.PathLike[str]] = None) -> None:
        out = Path(path) if path is not None else (Path(self._loaded_from) if self._loaded_from else None)
        if out is None:
            raise ValueError("No save path provided and Catchment was not loaded from a file.")
        out.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        self._loaded_from = str(out)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "spec_version": self.SPEC_VERSION,
            "root_dir": self.root_dir,
            "ponds": dict(self.ponds),
            "species": {k: asdict(v) for k, v in self.species.items()},
            "default_species": self.default_species,
            "pond_species": dict(self.pond_species),
            "modes": dict(self.modes),
        }

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "Catchment":
        spec_version = int(d.get("spec_version", 1))
        if spec_version != Catchment.SPEC_VERSION:
            raise ValueError(f"Unsupported catchment spec_version={spec_version}. Expected {Catchment.SPEC_VERSION}.")

        c = Catchment(root_dir=str(d.get("root_dir", "catchment")))
        c.ponds = dict(d.get("ponds", {}))
        c.species = {k: Species(**v) for k, v in dict(d.get("species", {})).items()}
        c.default_species = d.get("default_species")
        c.pond_species = dict(d.get("pond_species", {}))
        c.modes = dict(d.get("modes", {"pulse": {"type": "pulse"}}))
        return c

    def set_root_dir(self, root_dir: str) -> None:
        self.root_dir = root_dir

    def _resolve_local_path(self, path: str) -> Path:
        p = Path(path)
        if p.is_absolute():
            return p
        if self._loaded_from:
            return (Path(self._loaded_from).parent / p).resolve()
        return p.resolve()

    def _set_pond_path(
        self,
        pond_name: str,
        version: Optional[str],
        path: str,
        *,
        overwrite: bool,
    ) -> None:
        def _paths_match(a: str, b: str) -> bool:
            try:
                return self._resolve_local_path(a) == self._resolve_local_path(b)
            except Exception:
                return a == b

        existing = self.ponds.get(pond_name)

        if version is None:
            if existing is None or overwrite:
                self.ponds[pond_name] = path
                return
            if isinstance(existing, str):
                if not _paths_match(existing, path):
                    raise ValueError(f"Conflict while loading ponds: {pond_name!r} already set to a different path.")
                return
            if isinstance(existing, dict):
                if len(existing) == 1 and _paths_match(next(iter(existing.values())), path):
                    return
                raise ValueError(f"Conflict while loading ponds: {pond_name!r} already set to versioned paths.")
            raise ValueError(f"Unsupported pond catalog entry for {pond_name!r}.")

        if existing is None or isinstance(existing, str):
            if isinstance(existing, str) and not overwrite and not _paths_match(existing, path):
                raise ValueError(f"Conflict while loading ponds: {pond_name!r} already set to a different path.")
            self.ponds[pond_name] = {version: path}
            return

        if isinstance(existing, dict):
            if version in existing and not overwrite and not _paths_match(existing[version], path):
                raise ValueError(
                    f"Conflict while loading ponds: {pond_name!r}@{version} already set to a different path."
                )
            existing[version] = path
            return

        raise ValueError(f"Unsupported pond catalog entry for {pond_name!r}.")

    def get_pond_path(self, pond_name: str, version: Optional[str] = None) -> Path:
        if pond_name not in self.ponds:
            raise KeyError(f"Unknown pond {pond_name!r}. Available: {', '.join(sorted(self.ponds.keys()))}")
        entry = self.ponds[pond_name]
        if isinstance(entry, str):
            return self._resolve_local_path(entry)
        if isinstance(entry, dict):
            if version is None:
                if len(entry) == 1:
                    version = next(iter(entry.keys()))
                else:
                    raise KeyError(f"Pond {pond_name!r} requires a version to be specified.")
            if version not in entry:
                available = ", ".join(sorted(entry.keys()))
                raise KeyError(f"Pond {pond_name!r} has no version {version!r}. Available: {available}")
            return self._resolve_local_path(str(entry[version]))
        raise ValueError(f"Unsupported pond catalog entry for {pond_name!r}.")

    def load_ponds(self, ponds_json_path: str | os.PathLike[str], *, overwrite: bool = False) -> None:
        p = Path(ponds_json_path)
        ponds = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(ponds, dict):
            raise ValueError("ponds.json must be a JSON object")

        data_entries = ponds.get("data")
        if isinstance(data_entries, list):
            for entry in data_entries:
                if not isinstance(entry, dict):
                    raise ValueError("ponds.json data entries must be objects")
                entry_ponds = entry.get("ponds") or {}
                if not isinstance(entry_ponds, dict):
                    raise ValueError("ponds.json data entry ponds must be a mapping")
                ref_type = entry.get("reference_type", "local")
                version_by = entry.get("version_by") or {"type": "directory", "template": "{pond}/{version}"}
                self.set_ponds(
                    reference_type=str(ref_type),
                    version_by=dict(version_by),
                    ponds=entry_ponds,
                    overwrite=overwrite,
                )
            return

        ponds_map = ponds.get("ponds") if isinstance(ponds.get("ponds"), dict) else ponds
        if not isinstance(ponds_map, dict):
            raise ValueError("ponds.json must be a mapping of pond_name -> local_path")

        for name, path in ponds_map.items():
            if not isinstance(name, str) or not isinstance(path, str):
                raise ValueError("ponds.json keys/values must be strings")
            pond_name, version = _split_pond_ref(name)
            self._set_pond_path(pond_name, version, path, overwrite=overwrite)

    def set_ponds(
        self,
        *,
        reference_type: str,
        version_by: Mapping[str, Any],
        ponds: Mapping[str, str],
        overwrite: bool = False,
    ) -> None:
        reference_type = str(reference_type)
        if reference_type != "local":
            raise ValueError(f"Only reference_type='local' is supported right now. Got: {reference_type!r}")

        version_by = dict(version_by or {})
        version_type = str(version_by.get("type", "directory"))
        template = str(version_by.get("template", "{pond}/{version}"))
        if version_type != "directory":
            raise ValueError(f"Only version_by.type='directory' is supported right now. Got: {version_type!r}")
        if "{version}" not in template:
            raise ValueError("version_by.template must include '{version}' for directory-based versioning.")

        for ref, path in ponds.items():
            if not isinstance(ref, str) or not isinstance(path, str):
                raise ValueError("ponds keys/values must be strings")
            pond_name, version = _split_pond_ref(ref)
            base = Path(path)
            if version is not None:
                self._set_pond_path(pond_name, version, path, overwrite=overwrite)
                continue

            if not base.exists():
                raise FileNotFoundError(f"Pond path not found for {pond_name!r}: {base}")
            if not base.is_dir():
                raise ValueError(f"Pond path for {pond_name!r} must be a directory: {base}")

            versions = sorted([p.name for p in base.iterdir() if p.is_dir()])
            if not versions:
                raise ValueError(
                    f"No versions found for {pond_name!r} in {base}. "
                    "Expected subdirectories named by version."
                )
            for ver in versions:
                resolved = str(base / ver)
                self._set_pond_path(pond_name, ver, resolved, overwrite=overwrite)

    def set_species(self, species: Mapping[str, Species], *, overwrite: bool = False) -> None:
        for name, sp in species.items():
            if name in self.species and not overwrite and self.species[name] != sp:
                raise ValueError(f"Conflict while setting species: {name!r} already exists with a different value.")
            sp.validate()
            self.species[name] = sp

    def set_default_species(self, name: str) -> None:
        if name not in self.species:
            raise KeyError(f"Unknown species {name!r}. Available: {', '.join(sorted(self.species.keys()))}")
        self.default_species = name

    def set_pond_species(self, mapping: Mapping[str, str], *, overwrite: bool = False) -> None:
        for pond_name, sp_name in mapping.items():
            if sp_name not in self.species:
                raise KeyError(f"Unknown species {sp_name!r} for pond {pond_name!r}")
            if not overwrite and pond_name in self.pond_species and self.pond_species[pond_name] != sp_name:
                raise ValueError(f"Conflict: pond {pond_name!r} already assigned to a different species.")
            self.pond_species[pond_name] = sp_name

    def set_modes(self, modes: Mapping[str, Mapping[str, Any]], *, overwrite: bool = False) -> None:
        for name, spec in modes.items():
            spec = dict(spec or {})
            t = str(spec.get("type", "pulse"))
            if t != "pulse":
                raise ValueError(f"Only mode type='pulse' is supported for now. Got: {t!r}")
            if name in self.modes and not overwrite and self.modes[name] != spec:
                raise ValueError(f"Conflict while setting modes: {name!r} already exists with a different value.")
            self.modes[name] = spec

    def basin(
        self,
        *,
        outlets: Optional[Mapping[str, str]] = None,
        mode: str = "pulse",
        pond_species: Optional[Mapping[str, str]] = None,
        name: Optional[str] = None,
    ) -> "Basin":
        return Basin(
            catchment=self,
            outlets=dict(outlets or {}),
            mode=mode,
            pond_species=dict(pond_species or {}),
            name=name,
        )

    def validate(self) -> None:
        if not self.ponds:
            warnings.warn("Catchment has an empty pond catalog.", RuntimeWarning, stacklevel=2)

        for sp in self.species.values():
            sp.validate()

        if self.default_species is None:
            raise ValueError("Catchment.default_species is not set. Call set_default_species(...).")

        if self.default_species not in self.species:
            raise ValueError(f"default_species {self.default_species!r} is not present in species registry.")

        for name, spec in self.modes.items():
            t = str(dict(spec).get("type", "pulse"))
            if t != "pulse":
                raise ValueError(f"Only mode type='pulse' is supported for now. Got: {t!r} (mode {name!r})")


class Basin(ContractResolver):
    SPEC_VERSION = 1

    def __init__(
        self,
        *,
        catchment: Optional[Catchment] = None,
        outlets: Optional[Dict[str, str]] = None,
        mode: str = "pulse",
        pond_species: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
    ):
        self.catchment = catchment
        self.name = name
        self.outlets = outlets or {}
        self.mode = mode
        self.pond_species = pond_species or {}
        self.ducks: Dict[str, Any] = {"instances": {}, "default": None, "ponds": {}}
        self.hydrated: Dict[str, Any] = {}

        self._loaded_from: Optional[str] = None

        self._resolved: bool = False
        self._manifests: Dict[str, PondManifest] = {}
        self._contracts: Dict[str, PondContract] = {}
        self._edges: Dict[str, Set[str]] = {}
        self._topo: List[str] = []
        self._constraints: Dict[str, str] = {}
        self._pinned: Set[str] = set()
        self._resolving: bool = False
        self._auto_upgrade: bool = True
        self._resolving_stack: Set[str] = set()

    def _reset_resolution(self) -> None:
        self._resolved = False
        self._manifests.clear()
        self._contracts.clear()
        self._edges.clear()
        self._topo.clear()
        self._constraints.clear()
        self._pinned.clear()
        self._resolving = False
        self._auto_upgrade = True
        self._resolving_stack.clear()

    def set_catchment(self, catchment: Catchment) -> None:
        self.catchment = catchment
        self._reset_resolution()

    def set_outlets(self, outlets: Mapping[str, str]) -> None:
        outlets = dict(outlets)
        if self.catchment is not None:
            missing = [name for name in outlets.keys() if name not in self.catchment.ponds]
            if missing:
                raise KeyError(
                    f"Outlet pond(s) not present in catchment pond catalog: {', '.join(sorted(missing))}"
                )
        self.outlets = outlets
        self._reset_resolution()

    def set_ducks(self, instances: Mapping[str, Mapping[str, Any]]) -> None:
        if self.catchment is not None:
            for name, inst in instances.items():
                sp = inst.get("species")
                if sp is None:
                    raise ValueError(f"Duck instance {name!r} is missing required 'species'.")
                if sp not in self.catchment.species:
                    raise KeyError(f"Duck instance {name!r} references unknown species {sp!r}.")
        self.ducks["instances"] = {k: dict(v) for k, v in instances.items()}

    def set_default_duck(self, name: str) -> None:
        self.ducks["default"] = name

    def set_pond_ducks(self, mapping: Mapping[str, str]) -> None:
        self.ducks["ponds"] = dict(mapping)

    def set_mode(self, mode: str) -> None:
        self.mode = mode
        self._reset_resolution()

    def _ensure_catchment(self) -> Catchment:
        if self.catchment is None:
            raise ValueError("Basin has no catchment attached.")
        return self.catchment

    def _ensure_ducks(self) -> None:
        instances = dict(self.ducks.get("instances") or {})
        default_duck = self.ducks.get("default")
        pond_ducks = dict(self.ducks.get("ponds") or {})

        if not default_duck:
            if self.catchment is not None and self.catchment.default_species is not None:
                default_name = "default"
                if default_name not in instances:
                    instances[default_name] = {"species": self.catchment.default_species}
                default_duck = default_name
                self.ducks["instances"] = instances
                self.ducks["default"] = default_duck
            else:
                raise ValueError("basin.ducks.default must be set before hydration.")

        if default_duck not in instances:
            raise ValueError(f"basin.ducks.default={default_duck!r} not found in basin.ducks.instances")

        for pond_name, duck_name in pond_ducks.items():
            if duck_name not in instances:
                raise ValueError(
                    f"basin.ducks.ponds[{pond_name!r}] refers to unknown duck instance {duck_name!r}"
                )

    def to_dict(self) -> Dict[str, Any]:
        catchment_path = None
        if self.catchment is not None and self.catchment._loaded_from:
            catchment_path = self.catchment._loaded_from
        return {
            "spec_version": self.SPEC_VERSION,
            "name": self.name,
            "mode": self.mode,
            "outlets": dict(self.outlets),
            "ducks": dict(self.ducks),
            "hydrated": dict(self.hydrated),
            "catchment": {"path": catchment_path} if catchment_path else None,
        }

    @staticmethod
    def from_dict(d: Mapping[str, Any]) -> "Basin":
        spec_version = int(d.get("spec_version", 1))
        if spec_version != Basin.SPEC_VERSION:
            raise ValueError(f"Unsupported basin spec_version={spec_version}. Expected {Basin.SPEC_VERSION}.")

        catchment = None
        catchment_info = d.get("catchment")
        catchment_path = None
        if isinstance(catchment_info, dict):
            catchment_path = catchment_info.get("path")
        elif isinstance(catchment_info, str):
            catchment_path = catchment_info
        if isinstance(catchment_path, str) and catchment_path:
            catchment = Catchment.load(catchment_path)

        b = Basin(
            catchment=catchment,
            outlets=dict(d.get("outlets", {})),
            mode=str(d.get("mode", "pulse")),
            pond_species={},
            name=d.get("name"),
        )
        b.ducks = dict(d.get("ducks") or {"instances": {}, "default": None, "ponds": {}})
        b.hydrated = dict(d.get("hydrated") or {})
        return b

    @staticmethod
    def load(path: str | os.PathLike[str]) -> "Basin":
        p = Path(path)
        data = json.loads(p.read_text(encoding="utf-8"))
        b = Basin.from_dict(data)
        b._loaded_from = str(p)
        return b

    def save(self, path: Optional[str | os.PathLike[str]] = None) -> None:
        out = Path(path) if path is not None else (Path(self._loaded_from) if self._loaded_from else None)
        if out is None:
            raise ValueError("No save path provided and Basin was not loaded from a file.")
        out.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
        self._loaded_from = str(out)

    def resolve_contract(self, pond_name: str, constraint: str) -> PondContract:
        if self._resolving:
            self._require_version(pond_name, constraint, pinned=False)
            self._build_manifest(pond_name, constraint)
        else:
            self.resolve(auto_upgrade=True)
        if pond_name not in self._contracts:
            raise KeyError(f"No contract resolved for pond {pond_name!r}. Is it in the dependency graph?")
        expected = self._constraints.get(pond_name)
        if expected is not None and expected != constraint:
            expected_semver = _parse_semver(expected)
            constraint_semver = _parse_semver(constraint)
            if expected_semver[0] != constraint_semver[0] or expected_semver < constraint_semver:
                raise ValueError(
                    f"Constraint mismatch for pond {pond_name!r}: basin expected {expected!r}, got {constraint!r}"
                )
        return self._contracts[pond_name]

    def _resolve_dependency_version(self, pond_name: str, constraint: str) -> str:
        if not self._auto_upgrade:
            return constraint
        required_semver = _parse_semver(constraint)
        major = required_semver[0]
        catchment = self._ensure_catchment()
        prev = catchment.state.get_pond_version(pond_name=pond_name, major=major)
        if prev is None:
            return constraint

        prev_semver = _parse_semver(prev)
        if prev_semver[0] != major:
            return constraint

        if prev_semver >= required_semver:
            try:
                _ = catchment.get_pond_path(pond_name, prev)
            except Exception as exc:
                raise FileNotFoundError(
                    f"Previously executed version {prev!r} for pond {pond_name!r} "
                    f"(major {major}) is not available in the catchment catalog."
                ) from exc
            return prev

        return constraint

    def _require_version(self, pond_name: str, constraint: str, *, pinned: bool) -> str:
        prev = self._constraints.get(pond_name)
        if pinned:
            if prev is not None and prev != constraint:
                raise ValueError(
                    f"Constraint conflict for pond {pond_name!r}: previously {prev!r}, now {constraint!r}"
                )
            self._constraints[pond_name] = constraint
            self._pinned.add(pond_name)
            return constraint

        resolved = self._resolve_dependency_version(pond_name, constraint)
        if prev is None:
            self._constraints[pond_name] = resolved
            return resolved

        if pond_name in self._pinned:
            if prev != resolved:
                raise ValueError(
                    f"Constraint conflict for pond {pond_name!r}: pinned to {prev!r}, got {resolved!r}"
                )
            return prev

        prev_semver = _parse_semver(prev)
        resolved_semver = _parse_semver(resolved)
        if prev_semver[0] != resolved_semver[0]:
            raise ValueError(
                f"Constraint conflict for pond {pond_name!r}: {prev!r} vs {resolved!r} (different majors)"
            )

        if resolved_semver > prev_semver:
            self._constraints[pond_name] = resolved
            return resolved

        return prev

    def _build_manifest(self, pond_name: str, constraint: str) -> PondManifest:
        if pond_name in self._manifests:
            return self._manifests[pond_name]

        if pond_name in self._resolving_stack:
            raise ValueError(f"Cycle detected while resolving pond {pond_name!r}.")

        _ = self._ensure_catchment().get_pond_path(pond_name, constraint)

        self._resolving_stack.add(pond_name)
        try:
            pond_obj = self._load_pond_object(pond_name, override_version=constraint)
            mf = pond_obj.build(self)
        finally:
            self._resolving_stack.remove(pond_name)

        if mf.version != constraint:
            raise ValueError(
                f"Pond {pond_name!r} manifest version {mf.version!r} does not match required "
                f"{constraint!r}. v1 requires exact versions; ensure the local repo is checked out "
                "at the correct version."
            )

        self._manifests[pond_name] = mf
        self._edges[pond_name] = set(mf.sources.keys())
        self._contracts[pond_name] = PondContract(
            name=mf.name,
            version=mf.version,
            description=mf.description,
            tables=dict(mf.exported_tables),
        )
        return mf

    def resolve(self, *, auto_upgrade: bool = True) -> None:
        if self._resolved:
            return

        catchment = self._ensure_catchment()

        if not self.outlets:
            raise ValueError("Basin.outlets is empty. Call basin.set_outlets(...) first.")

        catchment.validate()

        if self.mode not in catchment.modes:
            raise KeyError(
                f"Unknown mode {self.mode!r}. Available: {', '.join(sorted(catchment.modes.keys()))}"
            )
        if str(catchment.modes[self.mode].get("type", "pulse")) != "pulse":
            raise ValueError("v1 only supports pulse mode.")

        for pond_name in self.outlets.keys():
            if pond_name not in catchment.ponds:
                raise KeyError(f"Outlet pond {pond_name!r} is not present in catchment pond catalog.")

        self._auto_upgrade = auto_upgrade
        self._resolving = True

        def visit(pond_name: str, constraint: str, *, pinned: bool) -> None:
            resolved_constraint = self._require_version(pond_name, constraint, pinned=pinned)
            mf = self._build_manifest(pond_name, resolved_constraint)

            for up_name, up_constraint in mf.sources.items():
                if up_name not in catchment.ponds:
                    raise KeyError(
                        f"Pond {pond_name!r} depends on {up_name!r}, but it is not in the catchment pond catalog."
                    )
                visit(up_name, up_constraint, pinned=False)

        try:
            for out_name, out_constraint in self.outlets.items():
                visit(out_name, out_constraint, pinned=True)

            reachable = set(self._constraints.keys())
            edges_sub = {k: {u for u in v if u in reachable} for k, v in self._edges.items() if k in reachable}
            self._topo = _toposort(edges_sub)

            self._resolved = True
        finally:
            self._resolving = False

    def hydrate(self, *, auto_upgrade: bool = False) -> None:
        catchment = self._ensure_catchment()
        self.resolve(auto_upgrade=auto_upgrade)
        self._ensure_ducks()

        root = Path(catchment.root_dir)
        ponds_root = root / "ponds"
        ponds_root.mkdir(parents=True, exist_ok=True)

        for pond_name, version in sorted(self._constraints.items()):
            src = catchment.get_pond_path(pond_name, version)
            dest = ponds_root / pond_name / version
            if not dest.exists():
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(src, dest)
            mf = self._manifests.get(pond_name)
            if mf is not None:
                mf_path = dest / catchment.DEFAULT_MANIFEST_NAME
                mf_path.write_text(json.dumps(mf.to_dict(), indent=2), encoding="utf-8")

        _ = dict(self.ducks.get("instances") or {})
        default_duck = self.ducks.get("default")
        pond_ducks = dict(self.ducks.get("ponds") or {})

        hydrated_ponds: Dict[str, dict] = {}
        for pond_name in sorted(self._edges.keys()):
            version = self._constraints[pond_name]
            deps = sorted(self._edges[pond_name])
            duck_name = pond_ducks.get(pond_name, default_duck)

            hydrated_ponds[pond_name] = {
                "version": version,
                "major": _parse_semver_major(version),
                "path": str((ponds_root / pond_name / version).resolve()),
                "dependencies": deps,
                "run_if": "all_succeeded",
                "duck": duck_name,
            }

        self.hydrated = {
            "ponds": hydrated_ponds,
            "stages": _layered_toposort(self._edges),
        }

    def plan(self) -> PulsePlan:
        self.resolve()
        return PulsePlan(
            ponds_topo=tuple(self._topo),
            outlets=dict(self.outlets),
            manifests=dict(self._manifests),
        )

    def pulse(self, *, verbose: bool = False) -> PulseResult:
        catchment = self._ensure_catchment()
        hydrated = dict(self.hydrated.get("ponds") or {})
        if not hydrated:
            raise RuntimeError("Basin is not hydrated. Run basin.hydrate() first.")

        def _build_hydrated_catalog(root: Path) -> Dict[str, Dict[str, str]]:
            ponds_root = root / "ponds"
            if not ponds_root.exists():
                raise RuntimeError("Basin is not hydrated. Missing root ponds directory.")
            catalog: Dict[str, Dict[str, str]] = {}
            for pond_dir in ponds_root.iterdir():
                if not pond_dir.is_dir():
                    continue
                versions = {
                    sub.name: str(sub.resolve())
                    for sub in pond_dir.iterdir()
                    if sub.is_dir()
                }
                if versions:
                    catalog[pond_dir.name] = versions
            return catalog

        original_ponds = catchment.ponds
        root = Path(catchment.root_dir)
        hydrated_catalog = _build_hydrated_catalog(root)
        for pond_name, info in hydrated.items():
            version = info.get("version")
            if not version:
                raise RuntimeError(f"Hydrated pond {pond_name!r} is missing a version.")
            if pond_name not in hydrated_catalog or version not in hydrated_catalog[pond_name]:
                raise RuntimeError(
                    f"Hydrated pond {pond_name!r}@{version!r} not found in {root / 'ponds'}."
                )
        catchment.ponds = hydrated_catalog

        self._reset_resolution()
        self.resolve(auto_upgrade=True)

        if not _HAVE_DUCKDB:
            raise RuntimeError("duckdb is required for Basin.pulse(). Install 'duckdb'.")
        if not _HAVE_IBIS:
            raise RuntimeError("ibis is required for Basin.pulse(). Install 'ibis-framework'.")

        (root / "data").mkdir(parents=True, exist_ok=True)
        (root / "state").mkdir(parents=True, exist_ok=True)

        run_id = f"pulse:{self.name or 'basin'}:{os.getpid()}"

        duckdb_path = root / "state" / "duckstring.duckdb"

        sp_name = catchment.default_species
        if sp_name is None:
            raise ValueError("Catchment.default_species not set.")
        sp = catchment.species[sp_name]
        duck = Duck(species=sp, duckdb_path=duckdb_path)
        duck.validate()

        con = duck.connect_duckdb()

        # Prefer a single shared DuckDB session for both raw SQL and Ibis execution.
        # This avoids issues where compiled SQL references temporary in-memory tables
        # (e.g. ibis.memtable) that only exist inside the Ibis session.
        ibis_con = None
        if _HAVE_IBIS:
            try:
                backend_cls = getattr(getattr(getattr(ibis, "backends", None), "duckdb", None), "Backend", None)
                if backend_cls is not None and hasattr(backend_cls, "from_connection"):
                    ibis_con = backend_cls.from_connection(con)
            except Exception:
                ibis_con = None
        if ibis_con is None:
            ibis_con = duck.connect_ibis()

        import time

        def _format_duration(seconds: float) -> str:
            if seconds < 1.0:
                value = seconds * 1000.0
                unit = "ms"
            elif seconds < 60.0:
                value = seconds
                unit = "s"
            elif seconds < 3600.0:
                value = seconds / 60.0
                unit = "min"
            elif seconds < 86400.0:
                value = seconds / 3600.0
                unit = "h"
            else:
                value = seconds / 86400.0
                unit = "d"
            return f"{value:.3g}{unit}"

        started_at = float(time.time())
        success = False
        pond_started_at: Dict[str, float] = {}
        ibis_default_backend = None
        ibis_backend_set = False

        if _HAVE_IBIS:
            try:
                # Ensure ibis.read_parquet and similar helpers register temp views
                # against the same DuckDB session used for materialization.
                ibis_default_backend = ibis.options.default_backend
                ibis.set_backend(ibis_con)
                ibis_backend_set = True
            except Exception:
                ibis_backend_set = False

        try:
            for pond_name in self._topo:
                if not catchment.state.acquire_lock(node_id=f"pond:{pond_name}", holder=run_id, ttl_secs=3600):
                    raise RuntimeError(f"Could not acquire lock for pond {pond_name!r}. Another run may be active.")

                try:
                    if verbose:
                        version = self._constraints.get(pond_name, "<unknown>")
                        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                        print(f"[{ts}] {pond_name}@{version} > Started")
                    pond_started_at[pond_name] = float(time.time())
                    pond_obj = self._load_pond_object(pond_name)
                    _ = pond_obj.build(self)

                    for stage in pond_obj._stages:
                        for table_name in stage.outputs:
                            tdef = pond_obj._tables.get(table_name)
                            if tdef is None or tdef.expr is None:
                                raise RuntimeError(
                                    f"Pond {pond_name!r} did not register an expression for table {table_name!r}."
                                )

                            sql = ibis_con.compile(tdef.expr)

                            physical = _physical_table_name(pond_name, table_name)

                            # Use the backend to materialize: this ensures in-memory sources
                            # like ibis.memtable are registered correctly before execution.
                            if hasattr(ibis_con, "create_table"):
                                ibis_con.create_table(physical, obj=tdef.expr, overwrite=True)
                            else:  # pragma: no cover
                                sql = ibis_con.compile(tdef.expr)
                                con.execute(f'CREATE OR REPLACE TABLE "{physical}" AS {sql}')

                            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{pond_name}"')
                            con.execute(
                                f'CREATE OR REPLACE VIEW "{pond_name}"."{table_name}" AS SELECT * FROM "{physical}"'
                            )

                            version = self._constraints.get(pond_name)
                            if not version:
                                raise RuntimeError(f"Missing version constraint for pond {pond_name!r}.")
                            out_dir = root / "data" / pond_name / version
                            out_dir.mkdir(parents=True, exist_ok=True)
                            out_path = out_dir / f"{table_name}.parquet"
                            if out_path.exists():
                                out_path.unlink()
                            con.execute(
                                f"COPY (SELECT * FROM \"{physical}\") TO '{out_path.as_posix()}' (FORMAT PARQUET)"
                            )

                            ts = float(time.time())
                            catchment.state.set_table_success(pond_name=pond_name, table_name=table_name, ts=ts)

                    ts = float(time.time())
                    version = self._constraints.get(pond_name)
                    if version is None:
                        raise RuntimeError(f"Missing version constraint for pond {pond_name!r}.")
                    catchment.state.set_pond_version(pond_name=pond_name, version=version, ts=ts)
                    catchment.state.set_pond_success(pond_name=pond_name, ts=ts)
                    if verbose:
                        version = self._constraints.get(pond_name, "<unknown>")
                        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
                        elapsed = ts - pond_started_at.get(pond_name, ts)
                        print(f"[{ts_str}] {pond_name}@{version} > Completed in {_format_duration(elapsed)}")

                finally:
                    catchment.state.release_lock(node_id=f"pond:{pond_name}", holder=run_id)

            success = True
        finally:
            if _HAVE_IBIS and ibis_backend_set:
                try:
                    if ibis_default_backend is None:
                        ibis.options.default_backend = None
                    else:
                        ibis.set_backend(ibis_default_backend)
                except Exception:
                    pass
            con.close()
            catchment.ponds = original_ponds

        ended_at = float(time.time())
        return PulseResult(
            plan=self.plan(),
            run_id=run_id,
            started_at=started_at,
            ended_at=ended_at,
            success=success,
        )

    def _load_pond_object(self, pond_name: str, *, override_version: Optional[str] = None) -> Pond:
        catchment = self._ensure_catchment()
        version = override_version or self._constraints.get(pond_name)
        repo = catchment.get_pond_path(pond_name, version)
        entry = repo / catchment.DEFAULT_POND_ENTRYPOINT
        if not entry.exists():
            raise FileNotFoundError(
                f"Missing pond entrypoint for {pond_name!r}: expected {entry} with function "
                f"{catchment.DEFAULT_POND_FACTORY_NAME}()."
            )

        version_tag = (version or "unknown").replace(".", "_")
        module_name = f"duckstring_pond_{pond_name}_{version_tag}"
        mod = _load_module_from_file(module_name, entry)

        factory_name = catchment.DEFAULT_POND_FACTORY_NAME
        if not hasattr(mod, factory_name):
            raise AttributeError(f"Entrypoint {entry} does not define function {factory_name}().")
        factory = getattr(mod, factory_name)
        if not callable(factory):
            raise TypeError(f"{factory_name} in {entry} is not callable.")

        with _resolver_context(self):
            try:
                pond_obj = factory()
            except TypeError:
                try:
                    pond_obj = factory(basin=self)
                except TypeError:
                    pond_obj = factory(resolver=self)
                    warnings.warn(
                        f"Pond factory for {pond_name!r} requires resolver=...; "
                        "prefer running under Basin/Snapshot context instead.",
                        RuntimeWarning,
                        stacklevel=2,
                    )

        return pond_obj


# ----------------------------
# Snapshot (dev/offline UX)
# ----------------------------

@dataclass(frozen=True)
class _SnapshotTableRef:
    kind: str
    pond_name: str
    pond_version: str
    table_name: str


class _SnapshotUpstreamPond:
    def __init__(self, snapshot: "Snapshot", name: str, constraint: str):
        self._snapshot = snapshot
        self.name = name
        self.constraint = constraint

    def get(
        self,
        table_name: str,
        *,
        infer_select: bool = False,
        select: Optional[Sequence[str]] = None,
    ) -> Any:
        pond = self._snapshot.pond
        if infer_select:
            columns = pond.upstream_select(self.name, table_name)
            if columns is None:
                raise KeyError(
                    f"No select-map metadata available for {self.name}.{table_name}. "
                    "Ensure the pond uses upstream.get(..., mapping=...) for this table."
                )
            select_cols = sorted(columns)
        elif select is not None:
            select_cols = [str(c) for c in select if c]
        else:
            select_cols = []

        table = self._snapshot._read_source_table(self.name, self.constraint, table_name)
        if select_cols:
            table = table.select(select_cols)

        ref = _SnapshotTableRef(
            kind="upstream",
            pond_name=self.name,
            pond_version=self.constraint,
            table_name=table_name,
        )
        table._duckstring_snapshot_ref = ref
        return table


class _SnapshotDownstreamRegistry:
    def __init__(self, snapshot: "Snapshot"):
        self._snapshot = snapshot

    def get(self, table_name: str) -> Any:
        table = self._snapshot._read_downstream_table(table_name)
        ref = _SnapshotTableRef(
            kind="downstream",
            pond_name=self._snapshot.pond.name,
            pond_version=self._snapshot.pond.version,
            table_name=table_name,
        )
        table._duckstring_snapshot_ref = ref
        return table


class _SnapshotUpstreamRegistry(Mapping[str, _SnapshotUpstreamPond]):
    def __init__(self, snapshot: "Snapshot"):
        self._snapshot = snapshot

    def __getitem__(self, key: str) -> _SnapshotUpstreamPond:
        pond = self._snapshot.pond
        if key not in pond._sources:
            available = ", ".join(sorted(pond._sources.keys())) or "<none>"
            raise KeyError(f"Unknown upstream pond '{key}'. Declared sources: {available}")
        return _SnapshotUpstreamPond(self._snapshot, key, pond._sources[key])

    def __iter__(self):
        return iter(self._snapshot.pond._sources.keys())

    def __len__(self):
        return len(self._snapshot.pond._sources)


class Snapshot(ContractResolver):
    def __init__(
        self,
        *,
        name: str,
        description: Optional[str],
        pond: Pond | Any,
        source_catchment: Catchment,
        sink_catchment: Catchment,
    ):
        self.name = name
        self.description = description
        self.source_catchment = source_catchment
        self.sink_catchment = sink_catchment
        self._pond_factory = pond if callable(pond) and not isinstance(pond, Pond) else None
        self._pond: Optional[Pond] = pond if isinstance(pond, Pond) else None
        self._sinks: List[Any] = []

        self.upstream: Mapping[str, _SnapshotUpstreamPond] = _SnapshotUpstreamRegistry(self)
        self.downstream = _SnapshotDownstreamRegistry(self)

    @property
    def pond(self) -> Pond:
        if self._pond is None:
            factory = self._pond_factory
            if factory is None:
                raise ValueError("Snapshot has no pond factory or pond instance attached.")
            with _resolver_context(self):
                try:
                    self._pond = factory()
                except TypeError:
                    try:
                        self._pond = factory(snapshot=self)
                    except TypeError:
                        self._pond = factory(resolver=self)
        return self._pond

    def _snapshot_root(self) -> Path:
        return (
            Path(self.sink_catchment.root_dir)
            / "snapshots"
            / self.pond.name
            / self.pond.version
        )

    def _data_table_path(self, catchment: Catchment, pond_name: str, version: str, table_name: str) -> Path:
        root = Path(catchment.root_dir)
        new_path = root / "data" / pond_name / version / f"{table_name}.parquet"
        if new_path.exists():
            return new_path
        legacy = root / "data" / f"{pond_name}@{version}" / f"{table_name}.parquet"
        return legacy

    def _read_parquet_schema(self, path: Path) -> Dict[str, str]:
        if _HAVE_DUCKDB:
            con = duckdb.connect()
            try:
                rows = con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')"
                ).fetchall()
                return {str(name): str(dtype) for name, dtype, *_ in rows}
            finally:
                con.close()
        if _HAVE_IBIS:
            t = ibis.read_parquet(str(path))
            return {k: str(v) for k, v in t.schema().items()}
        raise RuntimeError("duckdb or ibis is required to read parquet schemas.")

    def _read_source_table(self, pond_name: str, version: str, table_name: str) -> Any:
        if not _HAVE_IBIS:
            raise RuntimeError("ibis is required for Snapshot upstream access.")
        path = self._data_table_path(self.source_catchment, pond_name, version, table_name)
        if not path.exists():
            raise FileNotFoundError(f"Source table not found: {path}")
        return ibis.read_parquet(str(path))

    def _read_downstream_table(self, table_name: str) -> Any:
        if not _HAVE_IBIS:
            raise RuntimeError("ibis is required for Snapshot downstream access.")
        base = self._snapshot_root()
        output_path = base / "output" / f"{table_name}.parquet"
        downstream_path = base / "downstream" / f"{table_name}.parquet"
        path = output_path if output_path.exists() else downstream_path
        if not path.exists():
            raise FileNotFoundError(f"Snapshot downstream table not found: {path}")
        return ibis.read_parquet(str(path))

    def resolve_contract(self, pond_name: str, constraint: str) -> PondContract:
        base = self._snapshot_root() / "upstream" / pond_name / constraint
        if not base.exists():
            source_dir = (
                Path(self.source_catchment.root_dir)
                / "data"
                / pond_name
                / constraint
            )
            if source_dir.exists():
                base = source_dir
            else:
                legacy = Path(self.source_catchment.root_dir) / "data" / f"{pond_name}@{constraint}"
                if legacy.exists():
                    base = legacy
                else:
                    raise FileNotFoundError(
                        f"Snapshot upstream data not found for {pond_name!r}@{constraint!r}: {base}"
                    )

        tables: Dict[str, TableContract] = {}
        for path in sorted(base.glob("*.parquet")):
            schema = self._read_parquet_schema(path)
            tables[path.stem] = TableContract(
                name=path.stem,
                schema=schema,
                description=None,
            )

        if not tables:
            raise FileNotFoundError(f"No upstream tables found for {pond_name!r}@{constraint!r} in {base}")

        return PondContract(
            name=pond_name,
            version=constraint,
            description=None,
            tables=tables,
        )

    def sink(self, expr: Any) -> None:
        ref = getattr(expr, "_duckstring_snapshot_ref", None)
        if not isinstance(ref, _SnapshotTableRef):
            raise ValueError("Snapshot.sink(expr) requires expr from snap.upstream or snap.downstream.")
        self._sinks.append(expr)

    def _ensure_sink_catchment(self) -> None:
        root = Path(self.sink_catchment.root_dir)
        root.mkdir(parents=True, exist_ok=True)
        catchment_path = root / "catchment.json"
        if not catchment_path.exists():
            self.sink_catchment.save(catchment_path)

    def materialize(self, *, registry_path: str, activate: bool = True, verbose: bool = False) -> None:
        if not _HAVE_DUCKDB or not _HAVE_IBIS:
            raise RuntimeError("duckdb and ibis are required for Snapshot.materialize().")

        self._ensure_sink_catchment()
        base = self._snapshot_root()
        base.mkdir(parents=True, exist_ok=True)

        db_path = Path(self.sink_catchment.root_dir) / "state" / "duckstring_snapshot.duckdb"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))
        ibis_con = None
        if _HAVE_IBIS:
            try:
                backend_cls = getattr(getattr(getattr(ibis, "backends", None), "duckdb", None), "Backend", None)
                if backend_cls is not None and hasattr(backend_cls, "from_connection"):
                    ibis_con = backend_cls.from_connection(con)
            except Exception:
                ibis_con = None
        if ibis_con is None:
            ibis_con = ibis.duckdb.connect(database=str(db_path))

        ibis_default_backend = None
        ibis_backend_set = False
        try:
            ibis_default_backend = ibis.options.default_backend
            ibis.set_backend(ibis_con)
            ibis_backend_set = True
        except Exception:
            ibis_backend_set = False

        try:
            for idx, expr in enumerate(self._sinks):
                ref = expr._duckstring_snapshot_ref
                if ref.kind == "upstream":
                    out_dir = base / "upstream" / ref.pond_name / ref.pond_version
                else:
                    out_dir = base / "downstream"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{ref.table_name}.parquet"

                tmp_name = f"snapshot_tmp_{idx}"
                if hasattr(ibis_con, "create_table"):
                    ibis_con.create_table(tmp_name, obj=expr, overwrite=True)
                    con.execute(f"COPY (SELECT * FROM \"{tmp_name}\") TO '{out_path.as_posix()}' (FORMAT PARQUET)")
                else:  # pragma: no cover
                    sql = ibis_con.compile(expr)
                    con.execute(f"COPY ({sql}) TO '{out_path.as_posix()}' (FORMAT PARQUET)")

                if verbose:
                    print(f"[snapshot] wrote {out_path}")
        finally:
            if _HAVE_IBIS and ibis_backend_set:
                try:
                    if ibis_default_backend is None:
                        ibis.options.default_backend = None
                    else:
                        ibis.set_backend(ibis_default_backend)
                except Exception:
                    pass
            con.close()

        registry = {}
        reg_path = Path(registry_path)
        if reg_path.exists():
            registry = json.loads(reg_path.read_text(encoding="utf-8"))
        registry = dict(registry or {})
        snapshots = dict(registry.get("snapshots") or {})
        entry = {
            "name": self.name,
            "description": self.description,
            "pond": {"name": self.pond.name, "version": self.pond.version},
            "source_catchment": {"root_dir": self.source_catchment.root_dir},
            "sink_catchment": {"root_dir": self.sink_catchment.root_dir},
            "materialized_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        snapshots[self.name] = entry
        registry["snapshots"] = snapshots
        if activate:
            registry["active"] = self.name
        reg_path.write_text(json.dumps(registry, indent=2, sort_keys=True), encoding="utf-8")

    def flow(self, *, in_place: bool = True, duck: Optional[str] = None, verbose: bool = False) -> None:
        if not _HAVE_DUCKDB or not _HAVE_IBIS:
            raise RuntimeError("duckdb and ibis are required for Snapshot.flow().")

        _ = duck
        pond = self.pond
        base = self._snapshot_root()
        upstream_root = base / "upstream"
        output_root = base / "output"
        output_root.mkdir(parents=True, exist_ok=True)

        db_path = Path(self.sink_catchment.root_dir) / "state" / "duckstring_snapshot.duckdb"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))
        ibis_con = None
        if _HAVE_IBIS:
            try:
                backend_cls = getattr(getattr(getattr(ibis, "backends", None), "duckdb", None), "Backend", None)
                if backend_cls is not None and hasattr(backend_cls, "from_connection"):
                    ibis_con = backend_cls.from_connection(con)
            except Exception:
                ibis_con = None
        if ibis_con is None:
            ibis_con = ibis.duckdb.connect(database=str(db_path))

        ibis_default_backend = None
        ibis_backend_set = False
        try:
            ibis_default_backend = ibis.options.default_backend
            ibis.set_backend(ibis_con)
            ibis_backend_set = True
        except Exception:
            ibis_backend_set = False

        try:
            # Register upstream tables
            for up_name, up_version in pond._sources.items():
                up_dir = upstream_root / up_name / up_version
                if not up_dir.exists():
                    raise FileNotFoundError(
                        f"Snapshot upstream data not found for {up_name!r}@{up_version!r}: {up_dir}"
                    )
                for path in sorted(up_dir.glob("*.parquet")):
                    physical = _physical_table_name(up_name, path.stem)
                    con.execute(
                        f"CREATE OR REPLACE VIEW \"{physical}\" AS SELECT * FROM read_parquet('{path.as_posix()}')"
                    )

            # Register downstream tables (for in-place runs)
            if in_place:
                downstream_dir = base / "output"
                if not downstream_dir.exists():
                    downstream_dir = base / "downstream"
                if downstream_dir.exists():
                    for path in sorted(downstream_dir.glob("*.parquet")):
                        physical = _physical_table_name(pond.name, path.stem)
                        con.execute(
                            f"CREATE OR REPLACE VIEW \"{physical}\" AS SELECT * FROM read_parquet('{path.as_posix()}')"
                        )

            for stage in pond._stages:
                for table_name in stage.outputs:
                    tdef = pond._tables.get(table_name)
                    if tdef is None or tdef.expr is None:
                        raise RuntimeError(f"Snapshot pond has no expression for table {table_name!r}.")

                    physical = _physical_table_name(pond.name, table_name)
                    if hasattr(ibis_con, "create_table"):
                        ibis_con.create_table(physical, obj=tdef.expr, overwrite=True)
                    else:  # pragma: no cover
                        sql = ibis_con.compile(tdef.expr)
                        con.execute(f'CREATE OR REPLACE TABLE "{physical}" AS {sql}')

                    out_path = output_root / f"{table_name}.parquet"
                    if out_path.exists():
                        out_path.unlink()
                    con.execute(
                        f"COPY (SELECT * FROM \"{physical}\") TO '{out_path.as_posix()}' (FORMAT PARQUET)"
                    )
                    if verbose:
                        print(f"[snapshot] wrote {out_path}")
        finally:
            if _HAVE_IBIS and ibis_backend_set:
                try:
                    if ibis_default_backend is None:
                        ibis.options.default_backend = None
                    else:
                        ibis.set_backend(ibis_default_backend)
                except Exception:
                    pass
            con.close()

    def get(self, table_name: str) -> Any:
        if not _HAVE_IBIS:
            raise RuntimeError("ibis is required for Snapshot.get().")
        output_path = self._snapshot_root() / "output" / f"{table_name}.parquet"
        if not output_path.exists():
            raise FileNotFoundError(f"Snapshot output table not found: {output_path}")
        return ibis.read_parquet(str(output_path))

    @classmethod
    def load_active(cls, registry_path: str) -> "Snapshot":
        reg_path = Path(registry_path)
        data = json.loads(reg_path.read_text(encoding="utf-8"))
        active = data.get("active")
        if not active:
            raise KeyError("snapshot registry has no active snapshot.")
        entry = dict((data.get("snapshots") or {}).get(active) or {})
        if not entry:
            raise KeyError(f"snapshot {active!r} not found in registry.")

        def _load_catchment(info: Mapping[str, Any]) -> Catchment:
            info = dict(info or {})
            path = info.get("path")
            if isinstance(path, str) and path:
                return Catchment.load(path)
            root_dir = info.get("root_dir")
            if isinstance(root_dir, str) and root_dir:
                catchment_path = Path(root_dir) / "catchment.json"
                if catchment_path.exists():
                    return Catchment.load(catchment_path)
                return Catchment(root_dir=root_dir)
            return Catchment()

        source = _load_catchment(entry.get("source_catchment") or {})
        sink = _load_catchment(entry.get("sink_catchment") or {})

        pond_path = reg_path.parent / "pond.py"
        mod = _load_module_from_file("duckstring_snapshot_pond", pond_path)
        if not hasattr(mod, "pond"):
            raise AttributeError(f"Snapshot pond file {pond_path} does not define pond().")
        pond_factory = mod.pond

        return cls(
            name=active,
            description=entry.get("description"),
            pond=pond_factory,
            source_catchment=source,
            sink_catchment=sink,
        )
