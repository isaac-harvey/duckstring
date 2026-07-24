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

    def set_data_root(self, data_root: str | None) -> None:
        """Re-point where future Duck spawns publish/read (a live data-root switch)."""
        self.data_root = data_root

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
        # The Duck's effective compute config rides as env (plans/cloud-config.md): the Flock trio is
        # the Pond's over-envelope offload posture (mode/engine/oom_policy), inert locally with no engine
        # configured. duck_target (catchment/pool/dedicated) is read by a remote launcher, not here (the
        # local subprocess is whatever the host is — no abstract size to set).
        env = dict(os.environ)
        if duck:
            env["DUCKSTRING_FLOCK_MODE"] = duck.get("flock_mode") or "off"
            if duck.get("flock_engine"):
                env["DUCKSTRING_FLOCK_ENGINE"] = duck["flock_engine"]
            env["DUCKSTRING_FLOCK_OOM_POLICY"] = duck.get("oom_policy") or "fail_up"
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
                # A Catchment-Pool Duck publishes LOCALLY (the fast handoff co-located Sinks + the viewer
                # read) and async-mirrors to the data root — the Persist (plans/persist.md). Remote
                # launchers do NOT pass persist-root: a remote Duck's local disk is unreachable to
                # everyone else, so it publishes straight to the data root as before.
                f"--persist-root={self.data_root or ''}",
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


class DispatchingLauncher:
    """Routes each Pond's Duck to a backend by its ``duck_target`` + provider (plans/cloud-config.md):
    the Catchment's own box (``local`` — the subprocess backend), or a remote backend chosen by the
    target pool's provider (Fargate by default, EC2 for the escape-hatch pools). This is what makes "a
    local Catchment running cloud Ducks" the same code path as a hosted one — ``catchment``-targeted
    Ponds always run on the Catchment's box, everything else remote. With no remote backend (cloud not
    enabled), remote targets degrade to local, so a ``duck = "M"`` pond.toml still runs anywhere.

    ``remotes`` is a ``{provider: backend}`` map (a single backend is accepted for back-compat and takes
    every remote spawn). ``dialback`` is the shared reachable-URL/relay holder torn down once here."""

    manages_processes = True  # liveness routes through is_running() → the owning backend

    def __init__(self, local, remotes=None, *, dialback=None, default_provider: str = "fargate"):
        self.local = local
        if remotes is None:
            self.remotes: dict = {}
        elif isinstance(remotes, dict):
            self.remotes = remotes
        else:
            self.remotes = {"_any": remotes}  # a single remote backend takes every remote spawn
        self.dialback = dialback
        self.default_provider = default_provider
        self.data_root = getattr(local, "data_root", None)  # mirrors the backends' value (see set_data_root)
        self._owner: dict[str, object] = {}  # pond_key → the backend that spawned it
        self.pools: dict[str, object] = {}  # named Pool → its PoolLauncher (one shared machine each)

    @property
    def base_url(self):
        return self.local.base_url

    def _remote_for(self, duck: dict | None):
        if not self.remotes:
            return None
        if "_any" in self.remotes:
            return self.remotes["_any"]
        provider = ((duck or {}).get("pool") or {}).get("provider") or self.default_provider
        return self.remotes.get(provider) or self.remotes.get(self.default_provider)

    def pool_identity(self, duck: dict | None) -> str | None:
        """The named Pool this spawn belongs to, or ``None`` for per-machine routing. A named,
        non-preset pool is ONE shared machine (plans/pool-agent.md; presets/dedicated stay isolated
        task-per-Pond by the settled taxonomy). Routable only when the durable plane exists (cross-Pool
        reads go through it) and the pool's provider has a machine to offer: ``local`` needs only the
        data root; ``fargate``/``ec2`` need their remote backend attached (cloud enabled)."""
        if not duck or not duck.get("remote") or duck.get("preset"):
            return None
        target = duck.get("duck_target")
        if not target or target == "dedicated":
            return None
        if self.data_root is None:
            return None  # no durable plane — nothing cross-Pool could read; degrade to per-Pond routing
        provider = ((duck.get("pool") or {}).get("provider")) or self.default_provider
        if provider == "local":
            return target
        if self._remote_for(duck) is not None:
            return target
        return None

    def pool_launcher(self, name: str):
        """The PoolLauncher for a named pool (the routes' lookup) — ``None`` if none is live."""
        return self.pools.get(name)

    def _pool_backend(self, name: str, duck: dict):
        from .pool_launcher import (
            Ec2PoolMachine,
            FargatePoolMachine,
            LocalPoolMachine,
            PoolLauncher,
        )

        pl = self.pools.get(name)
        if pl is not None:
            return pl
        spec = duck.get("pool") or {}
        provider = spec.get("provider") or self.default_provider
        if provider == "local":
            machine = LocalPoolMachine(
                name, Path(self.local.root) / "pools" / name, self.local.base_url,
                getattr(self.local, "token", ""), self.data_root, self.data_root)
        elif provider == "ec2":
            machine = Ec2PoolMachine(name, spec, self._remote_for(duck))
        else:
            machine = FargatePoolMachine(name, spec, self._remote_for(duck))
        pl = self.pools[name] = PoolLauncher(name, machine)
        return pl

    def _backend_for(self, duck: dict | None):
        if duck and duck.get("remote"):
            pool_name = self.pool_identity(duck)
            if pool_name is not None:
                return self._pool_backend(pool_name, duck)
            remote = self._remote_for(duck)
            if remote is not None:
                return remote
        return self.local

    def set_data_root(self, data_root: str | None) -> None:
        """Propagate a live data-root switch to every backend (the dispatcher itself reads none — its
        local/remote backends carry the value into each Duck spawn). Named-Pool machines captured the
        old root at start, so they are stopped and rebuilt lazily on the next spawn."""
        self.data_root = data_root
        self.local.data_root = data_root
        for remote in self.remotes.values():
            remote.data_root = data_root
        for pl in self.pools.values():
            pl.shutdown_all()
        self.pools.clear()

    def attach_remotes(self, remotes, dialback=None) -> None:
        """Install remote backends onto a live launcher — runtime cloud-enable (creds / the data root
        were added after boot). Resolves the shared dial-back to the currently-known bind address so the
        newly-attached backends aren't stuck deferred (the boot middleware fires only once)."""
        self.remotes = remotes if isinstance(remotes, dict) else {"_any": remotes}
        self.dialback = dialback
        url = self.local.base_url
        if url is not None:
            if dialback is not None:
                dialback.set_base_url(url)
            else:
                for remote in self.remotes.values():
                    remote.set_base_url(url)

    def set_base_url(self, url: str) -> None:
        self.local.set_base_url(url)
        if self.dialback is not None:
            self.dialback.set_base_url(url)  # resolves the shared dial-back → drains the remote backends
        else:
            for remote in self.remotes.values():
                remote.set_base_url(url)

    def launch_error(self, pond_key: str) -> str | None:
        """The owning backend's spawn-failure reason (remote backends only), for the driver to attribute a
        Pond failure to a real cause (missing config / an AWS error) rather than a generic crash."""
        owner = self._owner.get(pond_key)
        if owner is not None and hasattr(owner, "launch_error"):
            return owner.launch_error(pond_key)
        return None

    def is_running(self, pond_key: str) -> bool:
        owner = self._owner.get(pond_key)
        if owner is not None:
            return owner.is_running(pond_key)
        # Not yet routed (a pending spawn before base_url is known) — ask everyone; any owner counts.
        if self.local.is_running(pond_key):
            return True
        return any(r.is_running(pond_key) for r in self.remotes.values())

    def ensure(self, pond_key: str, version: str, source_path: str, duck: dict | None = None) -> None:
        backend = self._backend_for(duck)
        # If a Pond's target changed lines between runs, tear down the stale backend's Duck first.
        stale = self._owner.get(pond_key)
        if stale is not None and stale is not backend:
            stale.terminate(pond_key)
        self._owner[pond_key] = backend
        backend.ensure(pond_key, version, source_path, duck=duck)

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        owner = self._owner.pop(pond_key, None)
        if owner is not None:
            owner.terminate(pond_key, wait=wait)
        else:  # unknown ownership (restart) — terminate everywhere, harmless if absent
            self.local.terminate(pond_key, wait=wait)
            for remote in self.remotes.values():
                remote.terminate(pond_key, wait=wait)
            for pl in self.pools.values():
                pl.terminate(pond_key, wait=wait)

    def shutdown_all(self) -> None:
        self.local.shutdown_all()
        for remote in self.remotes.values():
            remote.shutdown_all()
        for pl in self.pools.values():
            pl.shutdown_all()
        self.pools.clear()
        if self.dialback is not None:
            self.dialback.stop()


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

    def set_data_root(self, data_root: str | None) -> None:
        pass

    def is_running(self, pond_key: str) -> bool:
        return False

    def ensure(self, pond_key: str, version: str, source_path: str, duck: dict | None = None) -> None:
        pass

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        pass

    def shutdown_all(self) -> None:
        pass
