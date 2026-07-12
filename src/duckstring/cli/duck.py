"""``duckstring duck`` — per-Pond Duck config (prereqs D5): the worker's preset size and whether it
may escalate work to the Flock (a co-resident DuckFlock driver's distribution). Operational config,
operator-owned — never ``pond.toml``. Inert on a stock local Catchment (the subprocess Duck is
whatever the host is); sizing for remote launchers.
"""

from __future__ import annotations

from typing import Optional

import typer

app = typer.Typer(help="Per-Pond Duck config: preset size + Flock escalation.", no_args_is_help=True)

_CATCHMENT = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted).")
_MAJOR = typer.Option(None, "--major", "-m", help="Major version to target (default: latest).")

_SIZES = ("s", "m", "l", "xl")


def _fmt(cfg: dict) -> str:
    o = cfg.get("override", {})
    marks = []
    marks.append(f"size: {cfg['size']}{'' if o.get('size') else ' (default)'}")
    flock = "on" if cfg["flock"] else "off"
    marks.append(f"flock: {flock}{'' if o.get('flock') is not None else ' (default)'}")
    return "   ".join(marks)


@app.command("show")
def show(
    pond: Optional[str] = typer.Argument(None, help="Pond to show (omit for every Pond)."),
    catchment: Optional[str] = _CATCHMENT,
    major: Optional[int] = _MAJOR,
) -> None:
    """Show a Pond's effective Duck config (or every Pond's, plus the Catchment defaults)."""
    from . import _http
    from .config import resolve_catchment
    _, cfg = resolve_catchment(catchment)
    if pond is not None:
        got = _http.get(f"{cfg['url']}/api/ponds/{pond}/duck", auth=cfg,
                        params=_http.pond_params(major, None)).json()
        typer.echo(f"{pond}: {_fmt(got)}")
        return
    status = _http.get(f"{cfg['url']}/api/status", auth=cfg).json()
    defaults = None
    for p in status.get("ponds", []):
        if p.get("duck") is None:  # Draws/Spouts are not run by a Duck
            continue
        defaults = p["duck"].get("defaults") or defaults
        typer.echo(f"{p['id']}: {_fmt(p['duck'])}")
    if defaults:
        typer.echo(f"defaults: size {defaults['size']}, flock {'on' if defaults['flock'] else 'off'}")


@app.command("set")
def set_(
    pond: str = typer.Argument(..., help="Pond whose Duck config to set."),
    catchment: Optional[str] = _CATCHMENT,
    major: Optional[int] = _MAJOR,
    size: Optional[str] = typer.Option(None, "--size", "-s", help="Preset size: s | m | l | xl."),
    flock: Optional[str] = typer.Option(None, "--flock", help="Flock escalation: on | off."),
    clear: bool = typer.Option(False, "--clear", help="Drop the override (revert to Catchment defaults)."),
) -> None:
    """Set (or --clear) a Pond's Duck config override."""
    from . import _http
    from .config import resolve_catchment
    if not clear and size is None and flock is None:
        typer.echo("Error: nothing to set — pass --size, --flock, or --clear.", err=True)
        raise typer.Exit(1)
    if size is not None and size.lower() not in _SIZES:
        typer.echo(f"Error: size must be one of {', '.join(_SIZES)}.", err=True)
        raise typer.Exit(1)
    flock_val: Optional[bool] = None
    if flock is not None:
        if flock.lower() not in ("on", "off"):
            typer.echo("Error: --flock takes on|off.", err=True)
            raise typer.Exit(1)
        flock_val = flock.lower() == "on"
    _, cfg = resolve_catchment(catchment)
    params = _http.pond_params(major, None)
    _http.post(
        f"{cfg['url']}/api/ponds/{pond}/duck", auth=cfg, params=params,
        json={"size": size.lower() if size else None, "flock": flock_val, "clear": clear},
    )
    if clear:
        typer.echo(f"Cleared '{pond}' — back to Catchment defaults.")
    else:
        got = _http.get(f"{cfg['url']}/api/ponds/{pond}/duck", auth=cfg, params=params).json()
        typer.echo(f"{pond}: {_fmt(got)}")
