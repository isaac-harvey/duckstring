"""`duckstring lineage [pond] [--table T]` — the observed table-level lineage (plans/lineage.md).

Prints the tables each Ripple actually read and wrote on its latest recorded run — recorded by the Pond
handle at the moment of the call, so it is exact (never inferred) and per-run rather than static::

    duckstring lineage                 # every pond with recorded lineage
    duckstring lineage sales           # one pond's ripples (reads → writes)
    duckstring lineage sales -t order  # only the ripples touching table 'order'
"""

from __future__ import annotations

from typing import Optional

import typer


def show(
    pond: Optional[str] = typer.Argument(None, help="Pond name (default: every pond with recorded lineage)."),
    table: Optional[str] = typer.Option(None, "--table", "-t", help="Only the Ripples touching this table."),
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major line (default: highest deployed)."),
    columns: bool = typer.Option(False, "--columns", help="Also show the deploy-captured column lineage "
                                                          "(which source columns each output column derives from)."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (default if omitted)."),
) -> None:
    """Show the observed table-level lineage — what each Ripple actually read and wrote."""
    from rich.console import Console
    from rich.tree import Tree

    from . import _http
    from .config import resolve_catchment

    _, cfg = resolve_catchment(catchment)
    params = [("pond", pond), ("major", major), ("table", table), ("columns", "true" if columns else None)]
    query = "&".join(f"{k}={v}" for k, v in params if v is not None)
    data = _http.get(f"{cfg['url']}/api/lineage{'?' + query if query else ''}", auth=cfg).json()

    ponds = data.get("ponds", [])
    if not ponds or all(not p["ripples"] and not p.get("columns") for p in ponds):
        typer.echo("No recorded lineage yet — lineage is observed as Ripples run.")
        return
    console = Console()
    for p in ponds:
        if not p["ripples"] and not p.get("columns"):
            continue
        tree = Tree(f"[bold]{p['name']}[/bold]@{p['major']} [dim]{p['version']}[/dim]")
        for r in p["ripples"]:
            node = tree.add(f"[cyan]{r['ripple']}[/cyan] [dim](run {r['f']})[/dim]")
            for rd in r["reads"]:
                label = f"{rd['source']}.{rd['table']}" if rd["source"] else f"[dim]own[/dim] {rd['table']}"
                node.add(f"← {label}")
            for w in r["writes"]:
                node.add(f"[green]→ {w}[/green]")
        for tname, cols in (p.get("columns") or {}).items():
            cnode = tree.add(f"[magenta]{tname}[/magenta] [dim]columns[/dim]")
            if cols == "opaque":
                cnode.add("[dim]opaque (a .sql() output — column derivations unprovable)[/dim]")
                continue
            for col, prov in cols.items():
                if prov == "constant":
                    cnode.add(f"{col} [dim]← (constant)[/dim]")
                elif prov == "opaque":
                    cnode.add(f"{col} [dim]← opaque[/dim]")
                else:
                    srcs = ", ".join(f"{s['ref']}.{s['column']}" for s in prov)
                    cnode.add(f"{col} ← {srcs}")
        console.print(tree)
