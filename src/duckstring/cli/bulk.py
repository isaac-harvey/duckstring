from __future__ import annotations

from typing import Optional

import typer

_CATCHMENT = typer.Option(None, "--catchment", "-c", help="Catchment to use (uses default if omitted).")

# Precedence order (see plans/selector_ui.md) and the irreversible subset that needs confirmation.
_OPS = ("kill", "sleep", "reset", "wipe", "remove", "clear", "repair", "refresh")
_IRREVERSIBLE = {"reset", "wipe", "remove"}


def _live_ponds(status: dict) -> dict[str, dict]:
    """The real, targetable Ponds keyed by id ("name@major") — Spouts/Draws excluded from bulk ops."""
    return {p["id"]: p for p in status["ponds"] if not p.get("is_spout") and not p.get("is_draw")}


def _resolve_seed(token: str, live: dict[str, dict]) -> str:
    """Resolve a `name` or `name@major` token to a live Pond id (name → its highest deployed major)."""
    if "@" in token:
        if token not in live:
            raise typer.BadParameter(f"no such Pond '{token}'")
        return token
    majors = sorted(int(p["major"]) for p in live.values() if p["name"] == token)
    if not majors:
        raise typer.BadParameter(f"no such Pond '{token}'")
    return f"{token}@{majors[-1]}"


def do(
    ponds: Optional[list[str]] = typer.Argument(None, help="Ponds to target (name or name@major)."),
    all_: bool = typer.Option(False, "--all", help="Select every Pond in the Catchment."),
    tree: bool = typer.Option(False, "--tree", help="Extend the selection to its whole connected component(s)."),
    between: bool = typer.Option(False, "--between", help="Extend to Ponds on paths between the selected ones."),
    downstream: bool = typer.Option(False, "--downstream", help="Extend the selection to all descendants."),
    kill: bool = typer.Option(False, "--kill", help="Terminate the Duck, park killed."),
    sleep: bool = typer.Option(False, "--sleep", help="Clear demand (started runs finish)."),
    reset: bool = typer.Option(False, "--reset", help="Delete all tables/objects + rewind freshness."),
    wipe: bool = typer.Option(False, "--wipe", help="Clear run history (no data scrub)."),
    remove: bool = typer.Option(False, "--remove", help="Retire the line (data + config + attachments)."),
    clear: bool = typer.Option(False, "--clear", help="Clear failure / unblock."),
    repair: bool = typer.Option(False, "--repair", help="Force-rebuild the connected set now."),
    refresh: bool = typer.Option(False, "--refresh", help="Flag the next run as a cold rebuild."),
    confirm: Optional[str] = typer.Option(None, "--confirm", help="Catchment name, to authorise irreversible ops."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the interactive confirmation prompt."),
    catchment: Optional[str] = _CATCHMENT,
) -> None:
    """Apply one or more operations to a set of Ponds, in precedence order — the CLI twin of the UI
    Selector. Select Ponds by name, or expand the selection with --all / --tree / --between / --downstream.
    Operations: --kill --sleep --reset --wipe --remove --clear --repair --refresh (applied in the order
    kill > sleep > reset > wipe > remove > clear > repair > refresh). Repair can't combine with
    remove/reset; remove implies (and subsumes) reset. Any of reset/wipe/remove need the catchment name
    (--confirm, or you'll be prompted). See plans/selector_ui.md."""
    from . import _http
    from .config import resolve_catchment

    ops = [name for name, on in (
        ("kill", kill), ("sleep", sleep), ("reset", reset), ("wipe", wipe),
        ("remove", remove), ("clear", clear), ("repair", repair), ("refresh", refresh),
    ) if on]
    if not ops:
        raise typer.BadParameter("select at least one operation (e.g. --sleep, --reset, --repair).")
    if "repair" in ops and ("remove" in ops or "reset" in ops):
        raise typer.BadParameter("repair cannot be combined with remove or reset.")

    _, cfg = resolve_catchment(catchment)
    status = _http.get(f"{cfg['url']}/api/status", auth=cfg).json()
    live = _live_ponds(status)
    if not live:
        typer.echo("No Ponds deployed.")
        raise typer.Exit()

    # Build the selected id set. --all ignores the explicit list; otherwise seed from the arguments and
    # expand with the relational flags (mirrors the UI Selector's helpers).
    if all_:
        scope = set(live)
    else:
        if not ponds:
            raise typer.BadParameter("name at least one Pond, or pass --all.")
        scope = {_resolve_seed(t, live) for t in ponds}
        parents = {pid: set() for pid in live}
        children = {pid: set() for pid in live}
        for src, sink in status["edges"]:
            if src in live and sink in live:
                children[src].add(sink)
                parents[sink].add(src)
        if downstream:
            scope |= _closure(scope, children)
        if between:
            desc = scope | _closure(scope, children)
            anc = scope | _closure(scope, parents)
            scope |= desc & anc
        if tree:
            undirected = {pid: children[pid] | parents[pid] for pid in live}
            scope |= _closure(scope, undirected)

    if not scope:
        typer.echo("Nothing selected.")
        raise typer.Exit()

    if any(o in _IRREVERSIBLE for o in ops) and confirm is None and not yes:
        name = status["catchment"].get("name") or "confirm"
        typer.echo(f"About to {', '.join(ops)} on {len(scope)} Pond(s) — this includes irreversible actions.")
        confirm = typer.prompt(f"Type the catchment name ('{name}') to confirm")

    body = {
        "ponds": [{"name": pid.rpartition("@")[0], "major": int(pid.rpartition("@")[2])} for pid in sorted(scope)],
        "operations": ops,
        "confirm": confirm,
    }
    resp = _http.post(f"{cfg['url']}/api/ponds/batch", auth=cfg, json=body).json()
    applied = ", ".join(resp.get("operations", ops))
    typer.echo(f"Applied [{applied}] to {len(body['ponds'])} Pond(s).")
    if resp.get("removed"):
        typer.echo(f"Removed: {', '.join(resp['removed'])}")
    for err in resp.get("errors", []):
        typer.echo(f"  ! {err['op']} on {err['pond']}: {err['error']}", err=True)


def _closure(seeds: set[str], adj: dict[str, set[str]]) -> set[str]:
    """Reachable set from ``seeds`` over ``adj`` (excluding the seeds themselves are still included by the
    caller via union)."""
    out: set[str] = set()
    stack = list(seeds)
    while stack:
        for nxt in adj.get(stack.pop(), ()):
            if nxt not in out and nxt not in seeds:
                out.add(nxt)
                stack.append(nxt)
    return out
