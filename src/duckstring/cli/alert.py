"""`duckstring alert add|ls|rm|test|log` — manage failure & freshness notification channels.

An alert channel delivers Pond/Spout failures, contract violations, recoveries, and freshness-SLA breaches
to an external destination (a webhook / Slack, or e-mail). It is operational config (persisted, survives
redeploys), not declared in pond.toml. Credentials go in the destination URI as ``${env:NAME}`` /
``${secret:NAME}`` references, resolved only at send time. See plans/alerts.md::

    duckstring alert add --to 'https://hooks.slack.com/services/${secret:SLACK_HOOK}'
    duckstring alert add ops --to 'mailto:oncall@x.com?smtp=smtp.x.com:587' --pond sales --stale 1h
    duckstring alert ls
    duckstring alert test ops
    duckstring alert log
"""

from __future__ import annotations

from typing import Optional

import typer

app = typer.Typer(help="Manage failure & freshness notification channels.", no_args_is_help=True)


def _resolve(catchment: Optional[str]) -> tuple[str, dict]:
    from .config import resolve_catchment
    _, cfg = resolve_catchment(catchment)
    return cfg["url"], cfg


@app.command("add")
def add(
    to: str = typer.Option(..., "--to", "-t", help="Destination URI (https://…, http://…, mailto:…); "
                                                   "credentials as ${env:NAME}/${secret:NAME}."),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="Channel name (default: derived from the scheme)."),
    pond: Optional[str] = typer.Option(None, "--pond", "-p", help="Scope to one Pond by name "
                                                                  "(default: catchment-wide)."),
    major: Optional[int] = typer.Option(None, "--major", "-m", help="Major of --pond to scope to "
                                                                    "(default: its highest deployed major)."),
    on: str = typer.Option("all", "--on", help="Event kinds, comma-separated: "
                                              "failure,contract,spout,recovery,freshness (or 'all')."),
    stale: Optional[str] = typer.Option(None, "--stale", help="Freshness-SLA bound, e.g. 1h, 30m — alert when a "
                                                             "scoped Pond is stale longer than this."),
    renotify: Optional[str] = typer.Option(None, "--renotify", help="Re-notify interval, e.g. 6h — repeat the alert "
                                                                    "while a failure/staleness episode persists "
                                                                    "(default: once per episode)."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (default if omitted)."),
) -> None:
    """Add a notification channel."""
    from urllib.parse import urlparse

    from . import _http
    from .window import _parse_duration

    stale_ms = _parse_duration(stale) * 1000 if stale else None
    renotify_ms = _parse_duration(renotify) * 1000 if renotify else None
    final = name or urlparse(to).scheme.lower() or "alert"
    scope = f"{pond}@{major}" if pond and major is not None else pond  # "name@major" | "name" | None
    url, cfg = _resolve(catchment)
    resp = _http.post(
        f"{url}/api/alerts", auth=cfg,
        json={"name": final, "destination": to, "scope": scope, "events": on, "stale_ms": stale_ms,
              "renotify_ms": renotify_ms},
    ).json()
    label = f" on '{scope}'" if scope else " (catchment-wide)"
    typer.echo(f"Alert channel '{resp['name']}'{label} → {to}")


@app.command("ls")
def ls(
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (default if omitted)."),
) -> None:
    """List notification channels."""
    from rich.console import Console
    from rich.table import Table

    from . import _http

    url, cfg = _resolve(catchment)
    channels = _http.get(f"{url}/api/alerts", auth=cfg).json().get("channels", [])
    if not channels:
        typer.echo("No alert channels.")
        return
    table = Table(show_header=True, header_style="bold dim", box=None, padding=(0, 1))
    for col in ("Name", "Scope", "Events", "Freshness SLA", "Re-notify", "Destination", "Enabled"):
        table.add_column(col)
    for c in channels:
        sla = f"{int(c['stale_ms'] / 1000)}s" if c.get("stale_ms") else "[dim]—[/dim]"
        ren = f"{int(c['renotify_ms'] / 1000)}s" if c.get("renotify_ms") else "[dim]—[/dim]"
        table.add_row(
            c["name"], c.get("scope") or "[dim]all[/dim]", c["events"], sla, ren, c["destination"],
            "[green]yes[/green]" if c["enabled"] else "[red]no[/red]",
        )
    Console().print(table)


@app.command("rm")
def rm(
    name: str = typer.Argument(..., help="The channel to remove (see `alert ls`)."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (default if omitted)."),
) -> None:
    """Remove a notification channel."""
    from . import _http

    url, cfg = _resolve(catchment)
    _http.delete(f"{url}/api/alerts/{name}", auth=cfg)
    typer.echo(f"Alert channel '{name}' removed.")


@app.command("test")
def test(
    name: str = typer.Argument(..., help="The channel to test (see `alert ls`)."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (default if omitted)."),
) -> None:
    """Send a test notification through the channel (validates connectivity + credentials)."""
    from . import _http

    url, cfg = _resolve(catchment)
    resp = _http.post(f"{url}/api/alerts/{name}/test", auth=cfg, json={}).json()
    if resp.get("ok"):
        typer.echo(f"Test notification sent through '{name}'.")
    else:
        typer.secho(f"Channel '{name}' test failed: {resp.get('error')}", fg=typer.colors.RED)
        raise typer.Exit(1)


@app.command("log")
def log(
    limit: int = typer.Option(50, "--limit", "-l", help="How many recent deliveries to show."),
    catchment: Optional[str] = typer.Option(None, "--catchment", "-c", help="Catchment to use (default if omitted)."),
) -> None:
    """Show recent alert deliveries (the audit log)."""
    from rich.console import Console
    from rich.table import Table

    from . import _http

    url, cfg = _resolve(catchment)
    rows = _http.get(f"{url}/api/alerts/deliveries", auth=cfg, params={"limit": limit}).json().get("deliveries", [])
    if not rows:
        typer.echo("No deliveries yet.")
        return
    table = Table(show_header=True, header_style="bold dim", box=None, padding=(0, 1))
    for col in ("When", "Channel", "Kind", "Pond", "Status", "Error"):
        table.add_column(col)
    colour = {"sent": "green", "pending": "yellow", "failed": "red"}
    for r in rows:
        status = f"[{colour.get(r['status'], 'white')}]{r['status']}[/]"
        table.add_row(
            r.get("created_at") or "", r["channel"], r["kind"], r.get("pond") or "[dim]—[/dim]",
            status, (r.get("error") or "")[:60],
        )
    Console().print(table)
