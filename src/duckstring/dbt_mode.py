"""dbt-mode Ponds: deploy a dbt project as a Pond with zero ``@ripple`` code (see ``plans/dbt.md``).

A dbt project already has an implicit pipeline — ``ref()`` gives the model DAG the same way Duckstring's
ripple edges give the intra-Pond DAG. A **dbt-mode Pond** re-expresses that graph as Ripples for
freshness/failure/retry tracking, without asking the user to transcribe anything: ``pond.toml`` points at
the dbt project, and each dbt **model becomes a Ripple**.

This module is the seam. It **lazy-imports dbt** everywhere so the rest of duckstring runs without it
(dbt is the optional ``duckstring[dbt]`` extra). Two entry points:

- **deploy** (``manifest_to_ripples``) — parse the project, translate models → the same ripple-row shape
  ``@ripple`` discovery produces (so ``routes/deploy._register`` stores them unchanged).
- **runtime** (``run_model`` + ``materialize_sources``) — the Duck's dbt executor invokes dbt per model
  against the Pond's own DuckDB registry, after materialising the cross-Pond Sources it reads.

**Cross-Pond source convention:** a dbt ``source('X', 'tbl')`` maps to Duckstring Source Pond ``X``'s
published table ``tbl`` — ``X`` must be declared in ``pond.toml [sources]``. Before a model runs, each such
Source is materialised into the registry under the exact relation dbt resolves it to (read from the parsed
manifest — ``{schema}.{identifier}``). A dbt source whose name is *not* a declared Duckstring Source is left
to dbt's own resolution (an unmanaged warehouse table).
"""

from __future__ import annotations

from pathlib import Path

DBT_PROFILE = "duckstring"  # the profile name a dbt-mode project must declare in dbt_project.yml


class DbtError(RuntimeError):
    """A dbt invocation (parse/run) failed. Message carries dbt's own error text."""


def dbt_project_subpath(info: dict) -> str | None:
    """The ``[pond] dbt_project`` subpath from a parsed ``pond.toml``, or ``None`` for a normal (Python)
    Pond. Presence of this key is what makes a Pond dbt-mode."""
    return info.get("pond", {}).get("dbt_project")


def project_dir(source_dir: Path, info: dict) -> Path:
    """The absolute dbt project directory for a deployed Pond source."""
    sub = dbt_project_subpath(info) or "."
    return Path(source_dir) / sub


def _require_dbt():
    try:
        from dbt.cli.main import dbtRunner
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise DbtError(
            "dbt-mode Ponds need the dbt extra — install with: pip install 'duckstring[dbt]'"
        ) from exc
    return dbtRunner


def write_profile(profiles_dir: Path, registry_path: str) -> Path:
    """Generate the dbt ``profiles.yml`` wiring the ``duckstring`` profile to the Pond's own DuckDB
    registry via the ``dbt-duckdb`` adapter. ``registry_path`` is the DuckDB file (or ``:memory:`` for a
    parse-only invocation). Returns the profiles dir."""
    profiles_dir = Path(profiles_dir)
    profiles_dir.mkdir(parents=True, exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(
        f"{DBT_PROFILE}:\n"
        "  target: prod\n"
        "  outputs:\n"
        "    prod:\n"
        "      type: duckdb\n"
        f"      path: '{registry_path}'\n"
        "      schema: main\n",
        encoding="utf-8",
    )
    return profiles_dir


def _invoke(args: list[str], *, cwd_note: str) -> object:
    dbtRunner = _require_dbt()
    res = dbtRunner().invoke([*args, "--quiet"])
    if not res.success:
        detail = str(getattr(res, "exception", None) or "see dbt output")
        raise DbtError(f"dbt {args[0]} failed for {cwd_note}: {detail}")
    return res.result


def parse_manifest(proj_dir: Path, profiles_dir: Path):
    """Parse the dbt project and return its Manifest (no SQL executed). Used at deploy (topology) and Duck
    startup (topology). The profile can point at ``:memory:`` — parse doesn't touch the registry."""
    return _invoke(
        ["parse", "--project-dir", str(proj_dir), "--profiles-dir", str(profiles_dir), "--target", "prod"],
        cwd_note=str(proj_dir),
    )


def _models(manifest) -> list:
    return [n for n in manifest.nodes.values() if n.resource_type == "model"]


def manifest_to_ripples(manifest) -> list[dict]:
    """Translate the dbt model graph into the ripple-row shape ``routes/deploy._register`` expects:
    ``{"func": name, "name": name, "parents": [parent_names], "always_run": False}``. ``func`` is the model
    name (a stable hashable identity — the deploy code only uses it as a dict key to resolve parents to
    names, never calls it), so no schema change is needed."""
    model_uids = {n.unique_id for n in _models(manifest)}
    uid_to_name = {n.unique_id: n.name for n in _models(manifest)}
    ripples = []
    for n in _models(manifest):
        parents = [uid_to_name[d] for d in n.depends_on.nodes if d in model_uids]
        ripples.append({"func": n.name, "name": n.name, "parents": parents, "always_run": False})
    return ripples


def manifest_topology(manifest) -> dict[str, list[str]]:
    """The intra-Pond ``{model_name: [parent_model_names]}`` graph — the runtime counterpart of
    :func:`manifest_to_ripples`, for the Duck's ``load_topology``."""
    return {r["name"]: r["parents"] for r in manifest_to_ripples(manifest)}


def model_sources(manifest, model_name: str) -> list:
    """The dbt source nodes a given model depends on (each carries ``source_name``/``name``/``schema``/
    ``identifier`` — everything needed to materialise it as the relation dbt will read)."""
    node = next((n for n in _models(manifest) if n.name == model_name), None)
    if node is None:
        return []
    return [manifest.sources[d] for d in node.depends_on.nodes if d in manifest.sources]


def materialize_sources(pond, manifest, model_name: str, declared_sources: set[str]) -> None:
    """Materialise every cross-Pond Source ``model_name`` reads into the Pond's registry, under the exact
    ``{schema}.{identifier}`` relation dbt resolves the source to. ``pond`` is a :class:`~duckstring.core.Pond`
    bound to the registry cursor; it does the data-plane read (honouring the major pin + as-of ``f``, and
    raising :class:`~duckstring.core.MissingSourceAsset` for an unpublished Source — which the Duck parks).
    A dbt source whose name isn't a declared Duckstring Source is skipped (left to dbt)."""
    for src in model_sources(manifest, model_name):
        if src.source_name not in declared_sources:
            continue  # not a Duckstring Source — dbt resolves it itself
        rel = pond.read_table(f"{src.source_name}.{src.name}")  # data-plane read, system cols stripped
        pond.con.execute(f'CREATE SCHEMA IF NOT EXISTS "{src.schema}"')
        pond.con.execute(f'DROP TABLE IF EXISTS "{src.schema}"."{src.identifier}"')
        rel.create(f'"{src.schema}"."{src.identifier}"')


def run_model(proj_dir: Path, profiles_dir: Path, model_name: str, target_path: Path) -> None:
    """Run a single dbt model against the Pond's registry (the profile points there). ``--select`` scopes
    it to the one model — its upstream models already exist as registry tables (the Duck runs ripples in
    DAG order). ``target_path`` isolates dbt's compiled artifacts per major line."""
    _invoke(
        ["run", "--select", model_name, "--project-dir", str(proj_dir),
         "--profiles-dir", str(profiles_dir), "--target", "prod", "--target-path", str(target_path)],
        cwd_note=f"model '{model_name}'",
    )
