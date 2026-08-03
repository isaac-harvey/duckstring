"""``duckstring duck`` — per-Pond compute config (plans/cloud-config.md): the Duck target (where the
worker runs) + preset size, and the Pond's Flock posture (over-envelope offload). These may be
DECLARED in ``pond.toml`` (``[pond] duck``, ``[flock] mode/engine/oom_policy``) and re-read on every
redeploy; an operator override set here coalesces over the declaration and survives redeploys.

Inert on a stock local Catchment (the subprocess Duck is whatever the host is, the Flock is off with
no engine configured); the Duck target + sizing matter for remote launchers.
"""

from __future__ import annotations

from typing import Optional

import typer

app = typer.Typer(help="Per-Pond compute config: Duck target/size + Flock posture.", no_args_is_help=True)
pool_app = typer.Typer(help="Duck Pools: Catchment-level named remote compute.", no_args_is_help=True)
app.add_typer(pool_app, name="pool")

_CATCHMENT = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted).")
_MAJOR = typer.Option(None, "--major", "-m", help="Major version to target (default: latest).")

_MODES = ("off", "upgrade", "always")
_POLICIES = ("fail_up", "fail")


def _src(cfg: dict, field: str) -> str:
    """Where the effective value came from — for the ``(override)``/``(declared)``/``(default)`` tag."""
    if cfg.get("override", {}).get(field) is not None:
        return "override"
    if cfg.get("declared", {}).get(field) is not None:
        return "declared"
    return "default"


def _fmt(cfg: dict) -> str:
    parts = [
        f"duck: {cfg['duck_target']} ({_src(cfg, 'duck_target')})",
        f"flock: {cfg['flock_mode']} ({_src(cfg, 'flock_mode')})",
    ]
    if cfg["flock_mode"] != "off":
        parts.append(f"engine: {cfg.get('flock_engine') or 'default'}")
        parts.append(f"oom: {cfg['oom_policy']}")
    if cfg["duck_target"] == "dedicated" and cfg.get("dedicated_instance_type"):
        auto = " auto-stop" if cfg.get("dedicated_auto_stop") else ""
        parts.append(f"instance: {cfg['dedicated_instance_type']}{auto}")
    return "   ".join(parts)


@app.command("show")
def show(
    pond: Optional[str] = typer.Argument(None, help="Pond to show (omit for every Pond)."),
    catchment: Optional[str] = _CATCHMENT,
    major: Optional[int] = _MAJOR,
) -> None:
    """Show a Pond's effective compute config (or every Pond's, plus the Catchment defaults)."""
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
        typer.echo(f"defaults: duck {defaults['duck_target']}, flock {defaults['flock_mode']}")


@app.command("set")
def set_(
    pond: str = typer.Argument(..., help="Pond whose compute config to set."),
    catchment: Optional[str] = _CATCHMENT,
    major: Optional[int] = _MAJOR,
    duck: Optional[str] = typer.Option(None, "--duck", help="Duck target: 'catchment' | a pool name | 'dedicated'."),
    flock: Optional[str] = typer.Option(None, "--flock", help="Flock posture: off | upgrade | always."),
    engine: Optional[str] = typer.Option(None, "--engine", help="Flock engine (e.g. athena)."),
    oom: Optional[str] = typer.Option(None, "--oom", help="OOM policy: fail_up | fail."),
    instance_type: Optional[str] = typer.Option(None, "--instance-type", help="Instance type for --duck dedicated."),
    auto_stop: Optional[bool] = typer.Option(None, "--auto-stop/--no-auto-stop",
                                             help="Stop a dedicated Pond Duck on run completion."),
    clear: bool = typer.Option(False, "--clear", help="Drop the override (revert to pond.toml / defaults)."),
) -> None:
    """Set (or --clear) a Pond's compute override. Only the flags you pass change; the override
    coalesces over the pond.toml-declared config and survives redeploys."""
    from . import _http
    from .config import resolve_catchment
    fields = {"duck_target": duck, "flock_mode": flock, "flock_engine": engine,
              "oom_policy": oom, "dedicated_instance_type": instance_type, "dedicated_auto_stop": auto_stop}
    if not clear and all(v is None for v in fields.values()):
        typer.echo("Error: nothing to set — pass a flag, or --clear.", err=True)
        raise typer.Exit(1)
    for name_, val, allowed in (("--flock", flock, _MODES), ("--oom", oom, _POLICIES)):
        if val is not None and val.lower() not in allowed:
            typer.echo(f"Error: {name_} must be one of {', '.join(allowed)}.", err=True)
            raise typer.Exit(1)
    _, cfg = resolve_catchment(catchment)
    params = _http.pond_params(major, None)
    body = {k: (v.lower() if isinstance(v, str) and k in ("flock_mode", "oom_policy") else v)
            for k, v in fields.items()}
    body["clear"] = clear
    _http.post(f"{cfg['url']}/api/ponds/{pond}/duck", auth=cfg, params=params, json=body)
    if clear:
        typer.echo(f"Cleared '{pond}' — back to declared / Catchment defaults.")
    else:
        got = _http.get(f"{cfg['url']}/api/ponds/{pond}/duck", auth=cfg, params=params).json()
        typer.echo(f"{pond}: {_fmt(got)}")


# ─── Duck Pools ──────────────────────────────────────────────────────────────────


def _fmt_pool(p: dict) -> str:
    provider = p.get("provider") or "fargate"
    bits = [f"{p['name']}", f"[{provider}]"]
    if provider == "fargate" and (p.get("cpu") or p.get("memory")):
        bits.append(f"{p.get('cpu') or '?'}cpu / {p.get('memory') or '?'}MiB")
    elif p.get("instance_type"):
        bits.append(p["instance_type"])
    if p.get("keep_warm"):
        bits.append(f"keep-warm {p['keep_warm']}")
    if p.get("idle_timeout"):
        bits.append(f"idle {p['idle_timeout']}s")
    if p.get("region"):
        bits.append(p["region"])
    if p.get("managed"):
        bits.append("(preset)")
    return "   ".join(bits)


@pool_app.command("ls")
def pool_ls(catchment: Optional[str] = _CATCHMENT) -> None:
    """List the defined Duck Pools."""
    from . import _http
    from .config import resolve_catchment
    _, cfg = resolve_catchment(catchment)
    pools = _http.get(f"{cfg['url']}/api/catchment/duck-pools", auth=cfg).json().get("pools", [])
    for p in pools:
        typer.echo(_fmt_pool(p))


@pool_app.command("add")
def pool_add(
    name: str = typer.Argument(..., help="Pool name (referenced by pond.toml `duck = \"<name>\"`)."),
    catchment: Optional[str] = _CATCHMENT,
    provider: Optional[str] = typer.Option(None, "--provider", help="fargate (default) | ec2."),
    cpu: Optional[int] = typer.Option(None, "--cpu", help="Fargate task cpu units (256 = 0.25 vCPU)."),
    memory: Optional[int] = typer.Option(None, "--memory", help="Fargate task memory (MiB)."),
    instance_type: Optional[str] = typer.Option(None, "--instance-type", "-t", help="EC2 instance type."),
    min_instances: Optional[int] = typer.Option(None, "--min", help="Floor / keep-warm baseline."),
    max_instances: Optional[int] = typer.Option(None, "--max", help="Ceiling."),
    keep_warm: Optional[int] = typer.Option(None, "--keep-warm", help="Spare capacity beyond current load."),
    idle_timeout: Optional[int] = typer.Option(None, "--idle-timeout", help="Seconds before scale-down."),
    region: Optional[str] = typer.Option(None, "--region", help="AWS region (defaults to the Catchment's)."),
) -> None:
    """Create or update a named Duck Pool (provider defaults to Fargate). Inert until the remote
    launcher is configured. The built-in S/M/L/XL presets need no add."""
    from . import _http
    from .config import resolve_catchment
    _, cfg = resolve_catchment(catchment)
    body = {"name": name, "provider": provider, "cpu": cpu, "memory": memory,
            "instance_type": instance_type, "min_instances": min_instances,
            "max_instances": max_instances, "keep_warm": keep_warm, "idle_timeout": idle_timeout,
            "region": region}
    p = _http.post(f"{cfg['url']}/api/catchment/duck-pools", auth=cfg, json=body).json()
    typer.echo(_fmt_pool(p))


@pool_app.command("rm")
def pool_rm(
    name: str = typer.Argument(..., help="Pool to remove."),
    catchment: Optional[str] = _CATCHMENT,
) -> None:
    """Remove a Duck Pool. Ponds pinned to it fall back to the Catchment Duck (never stranded)."""
    from . import _http
    from .config import resolve_catchment
    _, cfg = resolve_catchment(catchment)
    _http.delete(f"{cfg['url']}/api/catchment/duck-pools/{name}", auth=cfg)
    typer.echo(f"Removed pool '{name}'.")
