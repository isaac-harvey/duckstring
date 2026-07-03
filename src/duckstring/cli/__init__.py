from __future__ import annotations

import typer

from . import alert as alert_cmd
from . import catchment as catchment_cmd
from . import duct as duct_cmd
from . import pond as pond_cmd
from . import puddle as puddle_cmd
from . import secret as secret_cmd
from . import spout as spout_cmd
from .control import clear, failure_budget, force, kill, refresh, repair, reset, sleep, wake
from .data import delete_object, delete_table, get, get_object, objects, query
from .deploy import deploy
from .deploy import remove as remove_pond
from .status import status
from .trigger import pulse, remove, tap, tide, wave
from .window import app as window_app

app = typer.Typer(help="Duckstring CLI", no_args_is_help=True, add_completion=True)


def _version_callback(value: bool) -> None:
    if value:
        from duckstring import __version__

        typer.echo(f"duckstring {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    version: bool = typer.Option(
        False, "--version", callback=_version_callback, is_eager=True,
        help="Show the duckstring version and exit.",
    ),
) -> None:
    pass

trigger_app = typer.Typer(
    help="Send execution signals to Outlet Ponds.",
    no_args_is_help=True,
)
trigger_app.command("tap")(tap)
trigger_app.command("pulse")(pulse)
trigger_app.command("wave")(wave)
trigger_app.command("tide")(tide)
trigger_app.command("remove")(remove)
trigger_app.add_typer(window_app, name="window")

control_app = typer.Typer(
    help="Manage a Pond's execution & health: wake, sleep, force, kill, clear a failure, set budgets.",
    no_args_is_help=True,
)
control_app.command("force")(force)
control_app.command("refresh")(refresh)
control_app.command("repair")(repair)
control_app.command("reset")(reset)
control_app.command("wake")(wake)
control_app.command("sleep")(sleep)
control_app.command("kill")(kill)
control_app.command("clear")(clear)
control_app.command("failure-budget")(failure_budget)

catchment_cmd.app.command("open")(duct_cmd.open_pond)
catchment_cmd.app.command("close")(duct_cmd.close_pond)
catchment_cmd.app.add_typer(duct_cmd.app, name="duct")

app.add_typer(catchment_cmd.app, name="catchment")
app.add_typer(pond_cmd.app, name="pond")
app.add_typer(puddle_cmd.app, name="puddle")
app.add_typer(trigger_app, name="trigger")
app.add_typer(control_app, name="control")
app.add_typer(spout_cmd.app, name="spout")
app.add_typer(secret_cmd.app, name="secret")
app.add_typer(alert_cmd.app, name="alert")

pond_cmd.app.command("deploy")(deploy)
pond_cmd.app.command("remove")(remove_pond)
app.command("status")(status)
app.command("get")(get)
app.command("query")(query)
app.command("objects")(objects)
app.command("get-object")(get_object)
app.command("delete-table")(delete_table)
app.command("delete-object")(delete_object)


def main() -> None:
    app()


__all__ = ["app", "main"]
