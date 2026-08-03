from __future__ import annotations

import re

from duckstring.cli import app
from duckstring.cli.config import get_default_catchment, list_catchments


def _strip_ansi(s: str) -> str:
    return re.sub(r'\x1b\[[0-9;]*m', '', s)


def test_connect_registers(runner):
    result = runner.invoke(app, ["catchment", "connect", "--name", "prod", "--path", "https://example.com", "--yes"])
    assert result.exit_code == 0
    items = dict(list_catchments())
    assert "prod" in items
    assert items["prod"]["url"] == "https://example.com"
    assert items["prod"]["type"] == "remote"


def test_connect_requires_name(runner):
    result = runner.invoke(app, ["catchment", "connect", "--path", "https://example.com"])
    assert result.exit_code != 0


def test_connect_requires_path(runner):
    result = runner.invoke(app, ["catchment", "connect", "--name", "dev"])
    assert result.exit_code != 0


def test_connect_yes_sets_default(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    assert get_default_catchment() == "dev"


def test_connect_prompts_for_default(runner):
    result = runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474"], input="y\n")
    assert result.exit_code == 0
    assert get_default_catchment() == "dev"


def test_connect_decline_default(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    runner.invoke(app, ["catchment", "connect", "--name", "prod", "--path", "https://prod.example.com"], input="n\n")
    assert get_default_catchment() == "dev"  # unchanged


def test_connect_skips_prompt_when_already_default(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    # Second connect with same name: already default, prompt should not appear
    result = runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474"])
    assert result.exit_code == 0
    assert "Set" not in result.output


# ── auto-default with single catchment ───────────────────────────────────────


def test_single_catchment_is_implicit_default(runner, tmp_path, monkeypatch):
    """When only one catchment is registered with no explicit default, it is used automatically."""
    from duckstring.cli import app as cli_app
    from duckstring.cli.config import register_catchment

    register_catchment("solo", url="http://localhost:7474", kind="local")
    # No set_default_catchment call — solo should still resolve
    result = runner.invoke(cli_app, ["status", "--once"])
    # Will fail (no server), but must NOT fail with "no catchment" error
    assert "no catchment" not in result.output.lower()


def test_single_catchment_marker_in_list(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "solo", "--path", "http://localhost:7474"], input="n\n")
    result = runner.invoke(app, ["catchment", "list"])
    assert result.exit_code == 0
    assert "●" in result.output  # implicit default marked


# ── list ─────────────────────────────────────────────────────────────────────


def test_list_empty(runner):
    result = runner.invoke(app, ["catchment", "list"])
    assert result.exit_code == 0
    assert "No catchments" in result.output


def test_list_shows_registered(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    result = runner.invoke(app, ["catchment", "list"])
    assert result.exit_code == 0
    assert "dev" in result.output
    assert "http://localhost:7474" in result.output


def test_list_shows_multiple(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    runner.invoke(app, ["catchment", "connect", "--name", "prod", "--path", "https://prod.example.com"], input="n\n")
    result = runner.invoke(app, ["catchment", "list"])
    assert result.exit_code == 0
    assert "dev" in result.output
    assert "prod" in result.output


def test_list_marks_default(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474"], input="n\n")
    runner.invoke(app, ["catchment", "connect", "--name", "prod", "--path", "https://prod.example.com"], input="n\n")
    runner.invoke(app, ["catchment", "set-default", "prod"])
    result = runner.invoke(app, ["catchment", "list"])
    assert result.exit_code == 0
    assert "●" in result.output


# ── init ─────────────────────────────────────────────────────────────────────


def test_init_help(runner):
    result = runner.invoke(app, ["catchment", "init", "--help"])
    assert result.exit_code == 0
    out = _strip_ansi(result.output)
    assert "--name" in out
    assert "--port" in out
    assert "--root" in out


def test_init_registers_catchment(runner, tmp_path, mock_uvicorn):
    result = runner.invoke(app, ["catchment", "init", "--name", "local", "--root", str(tmp_path), "--yes"])
    assert result.exit_code == 0
    items = dict(list_catchments())
    assert "local" in items
    assert items["local"]["type"] == "local"
    assert items["local"]["root"] == str(tmp_path)


def test_init_already_registered_same_root(runner, tmp_path, mock_uvicorn):
    runner.invoke(app, ["catchment", "init", "--name", "dev", "--root", str(tmp_path), "--yes"])
    result = runner.invoke(app, ["catchment", "init", "--name", "dev", "--root", str(tmp_path)])
    assert result.exit_code == 0
    assert "already registered" in result.output.lower()


def test_init_already_registered_different_root_confirm(runner, tmp_path, mock_uvicorn):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    runner.invoke(app, ["catchment", "init", "--name", "dev", "--root", str(root_a), "--yes"])
    result = runner.invoke(app, ["catchment", "init", "--name", "dev", "--root", str(root_b)], input="y\n")
    assert result.exit_code == 0
    assert dict(list_catchments())["dev"]["root"] == str(root_b)


def test_init_already_registered_different_root_decline(runner, tmp_path):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    from duckstring.cli.config import register_catchment
    register_catchment("dev", url="http://localhost:7474", kind="local", root=str(root_a))
    runner.invoke(app, ["catchment", "init", "--name", "dev", "--root", str(root_b)], input="n\n")
    assert dict(list_catchments())["dev"]["root"] == str(root_a)  # unchanged


def test_init_root_conflict_with_other_catchment(runner, tmp_path, mock_uvicorn):
    runner.invoke(app, ["catchment", "init", "--name", "dev", "--root", str(tmp_path), "--yes"])
    result = runner.invoke(app, ["catchment", "init", "--name", "staging", "--root", str(tmp_path)], input="n\n")
    assert result.exit_code != 0
    assert "already registered" in result.output.lower() or "already registered" in (result.stderr or "").lower()


def test_local_catchments_can_share_port(runner, tmp_path, mock_uvicorn):
    """Two local catchments with the same port are allowed (they won't run simultaneously)."""
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    root_a.mkdir()
    root_b.mkdir()
    runner.invoke(app, ["catchment", "init", "--name", "dev", "--port", "7474", "--root", str(root_a), "--yes"])
    result = runner.invoke(
        app, ["catchment", "init", "--name", "staging", "--port", "7474", "--root", str(root_b)], input="n\n"
    )
    assert result.exit_code == 0


# ── start ─────────────────────────────────────────────────────────────────────


def test_start_help(runner):
    result = runner.invoke(app, ["catchment", "start", "--help"])
    assert result.exit_code == 0
    # Typer renders an argument's metavar differently across versions (NAME, then {name} from 0.22), so
    # match case-insensitively — the point of the test is that the command documents its name argument,
    # not how the current Typer chooses to draw it.
    assert "name" in result.output.lower()


def test_start_unknown_catchment_exits(runner):
    result = runner.invoke(app, ["catchment", "start", "nonexistent"])
    assert result.exit_code != 0


def test_start_remote_catchment_exits(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "prod", "--path", "https://example.com", "--yes"])
    result = runner.invoke(app, ["catchment", "start", "prod"])
    assert result.exit_code != 0


# ── disconnect ────────────────────────────────────────────────────────────────


def test_disconnect_removes_catchment(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    result = runner.invoke(app, ["catchment", "disconnect", "dev"])
    assert result.exit_code == 0
    items = dict(list_catchments())
    assert "dev" not in items


def test_disconnect_clears_default(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    result = runner.invoke(app, ["catchment", "disconnect", "dev"])
    assert result.exit_code == 0
    assert get_default_catchment() is None


def test_disconnect_unknown_exits(runner):
    result = runner.invoke(app, ["catchment", "disconnect", "nonexistent"])
    assert result.exit_code != 0


def test_disconnect_prompts_for_purge(runner, tmp_path):
    from duckstring.cli.config import register_catchment
    root = tmp_path / "data"
    root.mkdir()
    register_catchment("local", url="http://localhost:7474", kind="local", root=str(root))
    result = runner.invoke(app, ["catchment", "disconnect", "local"], input="n\n")
    assert result.exit_code == 0
    assert root.exists()
    assert "retained" in result.output


def test_disconnect_purge_via_prompt(runner, tmp_path):
    from duckstring.cli.config import register_catchment
    root = tmp_path / "data"
    root.mkdir()
    register_catchment("local", url="http://localhost:7474", kind="local", root=str(root))
    result = runner.invoke(app, ["catchment", "disconnect", "local"], input="y\n")
    assert result.exit_code == 0
    assert not root.exists()


def test_disconnect_purge_flag_deletes_without_prompt(runner, tmp_path):
    from duckstring.cli.config import register_catchment
    root = tmp_path / "data"
    root.mkdir()
    register_catchment("local", url="http://localhost:7474", kind="local", root=str(root))
    result = runner.invoke(app, ["catchment", "disconnect", "local", "--purge"])
    assert result.exit_code == 0
    assert not root.exists()


def test_disconnect_purge_missing_dir_is_ok(runner, tmp_path):
    from duckstring.cli.config import register_catchment
    root = tmp_path / "nonexistent"
    register_catchment("local", url="http://localhost:7474", kind="local", root=str(root))
    result = runner.invoke(app, ["catchment", "disconnect", "local", "--purge"])
    assert result.exit_code == 0


def test_disconnect_remote_no_purge_prompt(runner):
    """Remote catchments have no data directory — no purge prompt should appear."""
    runner.invoke(app, ["catchment", "connect", "--name", "prod", "--path", "https://example.com", "--yes"])
    result = runner.invoke(app, ["catchment", "disconnect", "prod"])
    assert result.exit_code == 0
    assert "retained" not in result.output
    assert "Delete data" not in result.output


# ── URL / path conflict detection ─────────────────────────────────────────────


def test_connect_rejects_duplicate_url(runner):
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    result = runner.invoke(app, ["catchment", "connect", "--name", "dev2", "--path", "http://localhost:7474"])
    assert result.exit_code != 0
    assert "already registered" in result.output.lower() or "already registered" in (result.stderr or "").lower()


def test_connect_allows_same_name_update(runner):
    """Re-connecting the same name with same URL is an update, not a conflict."""
    runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474", "--yes"])
    result = runner.invoke(app, ["catchment", "connect", "--name", "dev", "--path", "http://localhost:7474"], input="n\n")
    assert result.exit_code == 0
