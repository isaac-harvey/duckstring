"""``duckstring serve`` — the Catalog / data-service surface (plans/data-serving.md): list what's
serviceable, run read-only SQL through the sandboxed serving core, promote the served major, and toggle
per-table exposure. Read users get only serviceable tables; full users see + manage everything."""

from __future__ import annotations

from typing import Optional

import typer

app = typer.Typer(help="The Catalog: query, expose, and promote served data.", no_args_is_help=True)

_CATCHMENT = typer.Option(None, "--catchment", "-c", help="Catchment (uses default if omitted).")


@app.command()
def status(catchment: Optional[str] = _CATCHMENT) -> None:
    """List the serviceable surface: each pond, its served major, and its exposed tables."""
    from . import _http
    from .config import resolve_catchment
    _, cfg = resolve_catchment(catchment)
    cat = _http.get(f"{cfg['url']}/api/serve", auth=cfg).json()
    ponds = cat.get("ponds", [])
    if not ponds:
        typer.echo("Nothing served. Declare `[serve] tables` in pond.toml, or toggle with `serve expose`.")
        return
    for p in ponds:
        majors = ",".join(str(m) for m in p["majors"])
        typer.echo(f"{p['name']}  (served v{p['served_major']}; majors {majors})")
        for t in p["tables"]:
            eye = "○" if not t.get("exposed") else "●"
            src = f" [{t['source']}]" if t.get("source") else ""
            typer.echo(f"    {eye} v{t['major']}.{t['table']}{src}")


@app.command()
def query(
    sql: str = typer.Argument(..., help="Read-only SQL over the catalog (pond=schema; served or _vN)."),
    catchment: Optional[str] = _CATCHMENT,
    limit: int = typer.Option(1000, "--limit", "-n", help="Row cap for the printed result."),
) -> None:
    """Run a query against the serving surface (sandboxed for read users)."""
    from rich.console import Console
    from rich.table import Table

    from . import _http
    from .config import resolve_catchment
    _, cfg = resolve_catchment(catchment)
    res = _http.post(f"{cfg['url']}/api/serve/query", auth=cfg, json={"sql": sql, "limit": limit}).json()
    table = Table(show_header=True, header_style="bold")
    for col in res["columns"]:
        table.add_column(str(col))
    for row in res["rows"]:
        table.add_row(*("" if v is None else str(v) for v in row))
    Console().print(table)


@app.command()
def promote(
    pond: str = typer.Argument(..., help="Pond name."),
    major: int = typer.Option(..., "--major", "-m", help="Major to make the served default."),
    catchment: Optional[str] = _CATCHMENT,
) -> None:
    """Promote a major to the served default (blue-green flip). Refused if it doesn't publish the
    currently-served tables."""
    from . import _http
    from .config import resolve_catchment
    _, cfg = resolve_catchment(catchment)
    _http.post(f"{cfg['url']}/api/ponds/{pond}/serve/promote", auth=cfg, json={"major": major})
    typer.echo(f"Promoted {pond} → served v{major}.")


@app.command()
def expose(
    pond: str = typer.Argument(..., help="Pond name."),
    table: str = typer.Argument(..., help="Table to expose/hide."),
    catchment: Optional[str] = _CATCHMENT,
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major line (default: latest)."),
    on: bool = typer.Option(False, "--on", help="Expose the table."),
    off: bool = typer.Option(False, "--off", help="Hide the table."),
    default: bool = typer.Option(False, "--default", help="Clear the override (revert to pond.toml)."),
) -> None:
    """Toggle a table's operational exposure (the eye). One of --on / --off / --default."""
    from . import _http
    from .config import resolve_catchment
    if sum((on, off, default)) != 1:
        typer.echo("Error: pass exactly one of --on / --off / --default.", err=True)
        raise typer.Exit(1)
    exposed = True if on else (False if off else None)
    _, cfg = resolve_catchment(catchment)
    _http.post(f"{cfg['url']}/api/ponds/{pond}/serve/expose", auth=cfg,
               params=_http.pond_params(major, None), json={"table": table, "exposed": exposed})
    state = "exposed" if on else ("hidden" if off else "reverted to pond.toml")
    typer.echo(f"{pond}.{table}: {state}.")
