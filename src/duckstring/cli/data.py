from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Optional

import typer


def get(
    outlet: str = typer.Argument(..., help="Pond name."),
    ripple: str = typer.Argument(..., help="Ripple name within the Pond."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted)."),
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major version to read from (default: latest)."),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Specific semver whose major line to read."),
    path: Optional[Path] = typer.Option(None, "--path", help="Output directory (default: ./ponds/{outlet}/{ripple})."),
) -> None:
    """Download a Ripple's output directory from a Catchment."""
    from rich.console import Console

    from . import _http
    from .config import resolve_catchment

    _, cfg = resolve_catchment(catchment)
    url = cfg["url"]

    out_dir = path or (Path("ponds") / outlet / ripple)
    out_dir = Path(out_dir)

    console = Console()
    console.print(f"Fetching [bold]{outlet}.{ripple}[/bold]...")
    resp = _http.get(
        f"{url}/api/ponds/{outlet}/ripples/{ripple}", auth=cfg,
        params=_http.pond_params(major, version),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(out_dir)
    except zipfile.BadZipFile:
        (out_dir / "output.bin").write_bytes(resp.content)

    console.print(f"[green]Written to[/green] {out_dir}")


def objects(
    outlet: str = typer.Argument(..., help="Pond name."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted)."),
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major version to read from (default: latest)."),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Specific semver whose major line to read."),
) -> None:
    """List a Pond's published non-tabular Objects (models, blobs)."""
    from rich.console import Console
    from rich.table import Table

    from . import _http
    from .config import resolve_catchment

    _, cfg = resolve_catchment(catchment)
    resp = _http.get(
        f"{cfg['url']}/api/ponds/{outlet}/objects", auth=cfg, params=_http.pond_params(major, version),
    )
    items = resp.json().get("objects", [])
    console = Console()
    if not items:
        console.print("[dim]No objects.[/dim]")
        return

    def _size(n) -> str:
        if n is None:
            return ""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if n < 1024 or unit == "TB":
                return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
            n /= 1024
        return f"{n} B"

    table = Table(show_header=True, header_style="bold dim")
    for col in ("name", "kind", "size", "freshness"):
        table.add_column(col)
    for o in items:
        kind = "directory" if o.get("is_dir") else f"file{(' · ' + o['ext']) if o.get('ext') else ''}"
        table.add_row(o["name"], kind, _size(o.get("size")), (o.get("f") or "")[:19].replace("T", " "))
    console.print(table)


def get_object(
    outlet: str = typer.Argument(..., help="Pond name."),
    name: str = typer.Argument(..., help="Object name."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted)."),
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major version to read from (default: latest)."),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Specific semver whose major line to read."),
    out: Optional[Path] = typer.Option(None, "--out", "-o", help="Output path (default ./{name}; a dir Object unzips here)."),
) -> None:
    """Download a Pond's published Object (a single file, or a directory Object unzipped into a folder)."""
    from rich.console import Console

    from . import _http
    from .config import resolve_catchment

    _, cfg = resolve_catchment(catchment)
    console = Console()
    console.print(f"Fetching [bold]{outlet}.{name}[/bold]...")
    resp = _http.get(
        f"{cfg['url']}/api/ponds/{outlet}/objects/{name}", auth=cfg, params=_http.pond_params(major, version),
    )
    # A directory Object comes back as a zip; a single file as raw bytes. Detect the zip by content.
    is_zip = resp.headers.get("content-type", "").startswith("application/zip")
    if is_zip:
        dest = Path(out) if out else Path(name)
        dest.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(dest)
        console.print(f"[green]Written to[/green] {dest}/")
    else:
        dest = Path(out) if out else Path(name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(resp.content)
        console.print(f"[green]Written to[/green] {dest}")


def delete_table(
    outlet: str = typer.Argument(..., help="Pond name."),
    table: str = typer.Argument(..., help="Table name to delete."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted)."),
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major version (default: latest)."),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Specific semver whose major line."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    """Delete a table from a Pond (its data + registry state) now. It stays gone until the Pond next runs,
    which recreates it only if the code still produces it. Requires the Pond to be idle."""
    from duckstring.trickle_io import base_table_name

    from . import _http
    from .config import resolve_catchment

    _, cfg = resolve_catchment(catchment)
    params = _http.pond_params(major, version)
    # A companion (changelog/band/droplog/base) resolves to its base table — the whole collection is deleted.
    base = base_table_name(table)
    try:
        tables = _http.get(f"{cfg['url']}/api/ponds/{outlet}/tables", auth=cfg, params=params).json().get("tables", [])
        mode = next((t.get("trickle") for t in tables if t["name"] == base), None)
    except Exception:
        mode = None
    if base != table:
        typer.echo(f"Note: '{table}' is part of table '{base}' — deleting it removes the whole table.")
    if mode == "merge":
        typer.echo(f"Note: deleting '{base}' removes its changelog too.")
    elif mode == "append":
        typer.echo(f"Warning: '{base}' is an append Trickle — deleting removes its droplog and drops its accumulated history.")
    if not yes:
        typer.confirm(f"Delete table '{outlet}.{base}' (data + state)?", abort=True)
    _http.delete(f"{cfg['url']}/api/ponds/{outlet}/tables/{table}", auth=cfg, params=params)
    typer.echo(f"Deleted '{outlet}.{base}'. It returns only if the Pond recreates it on a future run.")


def delete_object(
    outlet: str = typer.Argument(..., help="Pond name."),
    name: str = typer.Argument(..., help="Object name to delete."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted)."),
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major version (default: latest)."),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Specific semver whose major line."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    """Delete a non-tabular Object from a Pond. It returns only if a Ripple writes it again."""
    from . import _http
    from .config import resolve_catchment

    _, cfg = resolve_catchment(catchment)
    if not yes:
        typer.confirm(f"Delete object '{outlet}.{name}'?", abort=True)
    _http.delete(
        f"{cfg['url']}/api/ponds/{outlet}/objects/{name}", auth=cfg, params=_http.pond_params(major, version),
    )
    typer.echo(f"Deleted object '{outlet}.{name}'.")


def query(
    outlet: str = typer.Argument(..., help="Pond name."),
    ripple: Optional[str] = typer.Argument(None, help="Ripple name (default query: SELECT * LIMIT 10)."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted)."),
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major version to query (default: latest)."),
    version: Optional[str] = typer.Option(None, "--version", "-v", help="Specific semver whose major line to query."),
    sql: Optional[str] = typer.Option(None, "--sql", help="SQL statement, or @path/to/file.sql to read from a file."),
    csv_out: Optional[str] = typer.Option(None, "--csv", metavar="FILENAME", help="Write result as CSV."),
    json_out: Optional[str] = typer.Option(None, "--json", metavar="FILENAME", help="Write result as JSON records."),
    parquet_out: Optional[str] = typer.Option(None, "--parquet", metavar="FILENAME", help="Write result as Parquet."),
    path: Optional[Path] = typer.Option(None, "--path", help="Output directory for file output (overrides default location)."),
) -> None:
    """Run a SQL query against a Pond's tables and print or save the result."""
    from rich.console import Console
    from rich.table import Table

    from . import _http
    from .config import resolve_catchment

    _, cfg = resolve_catchment(catchment)
    url = cfg["url"]
    console = Console()

    sql_stmt = sql
    if sql_stmt and sql_stmt.startswith("@"):
        sql_file = Path(sql_stmt[1:])
        if not sql_file.exists():
            typer.echo(f"Error: SQL file not found: {sql_file}", err=True)
            raise typer.Exit(1)
        sql_stmt = sql_file.read_text(encoding="utf-8")
    elif not sql_stmt and ripple:
        sql_stmt = f"SELECT * FROM {outlet}.{ripple} LIMIT 10"

    payload: dict = {"pond": outlet}
    if major is not None:
        payload["major"] = major
    if version is not None:
        payload["version"] = version
    if ripple:
        payload["ripple"] = ripple
    if sql_stmt:
        payload["sql"] = sql_stmt

    output_filename = csv_out or json_out or parquet_out
    if output_filename:
        if csv_out:
            payload["format"] = "csv"
        elif json_out:
            payload["format"] = "json"
        else:
            payload["format"] = "parquet"

    resp = _http.post(f"{url}/api/query", auth=cfg, json=payload)

    if not output_filename:
        try:
            rows = resp.json()
        except Exception:
            typer.echo(resp.text)
            return

        if not rows:
            console.print("[dim]No results.[/dim]")
            return

        if isinstance(rows, list) and rows:
            table = Table(show_header=True, header_style="bold dim")
            for col in rows[0].keys():
                table.add_column(str(col))
            for row in rows:
                table.add_row(*[str(v) if v is not None else "" for v in row.values()])
            console.print(table)
        else:
            import json
            console.print(json.dumps(rows, indent=2))
    else:
        if path:
            out_path = Path(path) / output_filename
        elif ripple:
            out_path = Path("ponds") / outlet / ripple / output_filename
        else:
            out_path = Path("ponds") / outlet / output_filename

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(resp.content)
        console.print(f"[green]Written to[/green] {out_path}")
