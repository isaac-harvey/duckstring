"""Pond launcher: brings a Duck process to life and tears it down.

Local-subprocess by default (one Duck per executing Pond — that is, per ``name@major`` line). The
Duck dials back to the Catchment, so remote is just a different launcher with the same interface:
set ``DUCKSTRING_DUCK_LAUNCHER=module:Class`` (prereqs D6) to swap the implementation — the class is
constructed with the same ``(root, base_url, token=…, data_root=…)`` and receives each Pond's Duck
config (size/flock, prereqs D5) on ``ensure``. Nothing else changes.
"""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path

from ..keys import split_pond_key


def load_launcher_class(spec: str):
    """Resolve a ``module:Class`` launcher spec (``DUCKSTRING_DUCK_LAUNCHER``). Raises ImportError /
    AttributeError loudly — a Catchment configured for a custom launcher must not silently fall back
    to spawning local subprocesses."""
    module_name, _, class_name = spec.partition(":")
    if not module_name or not class_name:
        raise ValueError(f"DUCKSTRING_DUCK_LAUNCHER must be 'module:Class', got {spec!r}")
    return getattr(importlib.import_module(module_name), class_name)


class SubprocessLauncher:
    manages_processes = True  # owns real Duck processes, so liveness can be checked via proc.poll()

    def __init__(self, root: Path, base_url: str | None, token: str = "", data_root: str | None = None):
        self.root = root
        # The address Ducks dial back to. None = not yet known (a platform like Posit Connect picks
        # the bind address; it's learned from the first request) — spawns are deferred until then.
        self.base_url = base_url
        self.token = token
        # The data-plane root passed through to each Duck (object store / Volume / path); None = local default.
        self.data_root = data_root
        self._procs: dict[str, subprocess.Popen] = {}  # pond key (name@major) → process
        self._pending: dict[str, tuple] = {}  # spawns deferred until base_url is known

    def set_base_url(self, url: str) -> None:
        """Set the dial-back address and spawn any Ducks that were waiting on it. Their queued jobs
        are untouched — a Duck collects them on its first poll."""
        self.base_url = url
        pending, self._pending = self._pending, {}
        for pond_key, (version, source_path, duck) in pending.items():
            self.ensure(pond_key, version, source_path, duck=duck)

    def is_running(self, pond_key: str) -> bool:
        if pond_key in self._pending:
            return True  # queued to spawn — the launcher still owns it (don't let liveness fail it)
        proc = self._procs.get(pond_key)
        return proc is not None and proc.poll() is None

    def ensure(self, pond_key: str, version: str, source_path: str, duck: dict | None = None) -> None:
        if self.base_url is None:
            self._pending[pond_key] = (version, source_path, duck)
            return
        if self.is_running(pond_key):
            return
        name, major = split_pond_key(pond_key)
        # The Duck config rides as env: size is advisory locally (the subprocess is whatever the host
        # is); DUCKSTRING_DUCK_FLOCK is the pond's over-envelope offload posture (the Flock seam,
        # engine-pluggable and off locally — no engine configured).
        env = dict(os.environ)
        if duck:
            env["DUCKSTRING_DUCK_SIZE"] = duck.get("size") or "s"
            env["DUCKSTRING_DUCK_FLOCK"] = "on" if duck.get("flock", True) else "off"
        self._procs[pond_key] = subprocess.Popen(
            [
                sys.executable, "-m", "duckstring.duck",
                "--pond", name,
                "--major", str(major),
                "--version", version,
                "--catchment", self.base_url,
                # `--token=` (joined) so a urlsafe token starting with '-' isn't read as a flag.
                f"--token={self.token}",
                "--root", str(self.root),
                "--source-path", source_path,
                f"--data-root={self.data_root or ''}",
            ],
            env=env,
        )

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        self._pending.pop(pond_key, None)
        proc = self._procs.pop(pond_key, None)
        if proc is not None and proc.poll() is None:
            proc.terminate()
            if wait:  # block until the process exits (releases its registry.duckdb handle) — a caller that
                try:  # is about to open the registry directly needs the file free (e.g. delete_table).
                    proc.wait(timeout=10)
                except Exception:
                    proc.kill()

    def shutdown_all(self) -> None:
        self._pending.clear()
        for key in list(self._procs):
            self.terminate(key)


class NoopLauncher:
    """A launcher that never spawns anything — for tests/contexts that exercise the engine and
    persistence without running real Duck processes. Accepts the standard launcher constructor
    args so it is itself a valid ``DUCKSTRING_DUCK_LAUNCHER`` target."""

    manages_processes = False  # nothing to watch — liveness checking is skipped

    def __init__(self, root: Path | None = None, base_url: str | None = None,
                 token: str = "", data_root: str | None = None):
        pass

    def set_base_url(self, url: str) -> None:
        pass

    def is_running(self, pond_key: str) -> bool:
        return False

    def ensure(self, pond_key: str, version: str, source_path: str, duck: dict | None = None) -> None:
        pass

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        pass

    def shutdown_all(self) -> None:
        pass
