"""Building — and live-refreshing — the remote Duck backends from the cloud gate.

The remote launchers (Fargate + EC2) and their shared dial-back are derived from the data root + AWS
creds. Extracted from ``app`` so they can be (re)built both at boot AND when the credentials or the data
root change at runtime: adding or rotating creds must make remote compute usable — and pick up the new
creds — without a Catchment restart (plans/cloud-config.md).
"""

from __future__ import annotations

import os
from pathlib import Path


def build_remote_backends(root: Path, base_url: str | None, token: str, data_root: str | None,
                          secret_store) -> tuple[dict, object | None]:
    """Return ``(remotes, dialback)`` — the ``{provider: backend}`` map + the shared dial-back — when
    cloud is enabled (a remote data root + AWS creds), else ``({}, None)`` (remote targets degrade to
    local). The backends are inert until a matching pool spawns; their boto3 clients are lazy."""
    from . import cloud

    if not (cloud.is_remote(data_root) and cloud.aws_configured(secret_store)):
        return {}, None

    from .dialback import RemoteDialback
    from .ec2_launcher import Ec2Launcher
    from .fargate_launcher import FargateLauncher
    from .relay import RelayManager, needs_relay

    # A local Catchment (a laptop behind NAT) can't be dialed back — the auto-relay bridges it. Built
    # only when needed + configured; a public DUCKSTRING_CATCHMENT_PUBLIC_URL wins and skips the relay.
    relay = None
    has_public = os.environ.get("DUCKSTRING_CATCHMENT_PUBLIC_URL")
    relay_off = os.environ.get("DUCKSTRING_RELAY", "").lower() in ("off", "0", "false")
    if not has_public and not relay_off and needs_relay(base_url):
        candidate = RelayManager(bind_url=base_url)
        if candidate.configured():
            relay = candidate
    dialback = RemoteDialback(base_url, relay=relay, public_url=has_public)
    kw = dict(token=token, data_root=data_root, dialback=dialback)
    remotes = {
        "fargate": FargateLauncher(root, base_url, **kw),
        "ec2": Ec2Launcher(root, base_url, **kw),
    }
    return remotes, dialback


def refresh_cloud_backends(app) -> bool:
    """Re-evaluate the cloud gate against the live launcher after credentials / the data root changed —
    so runtime enable + credential rotation don't need a restart. Returns whether remotes are now present.

    - Not enabled, or not a ``DispatchingLauncher`` (a custom/Noop launcher) → no-op.
    - Enabled, remotes already built → drop their cached boto3 clients so new/rotated creds take effect.
    - Enabled, no remotes yet → build them from the current gate and attach (runtime enable).
    """
    from . import cloud
    from .launcher import DispatchingLauncher

    launcher = getattr(app.state, "launcher", None)
    if not isinstance(launcher, DispatchingLauncher):
        return False
    data_root = getattr(app.state, "data_root", None)
    secret_store = getattr(app.state, "secret_store", None)
    if not (cloud.is_remote(data_root) and cloud.aws_configured(secret_store)):
        return False

    if launcher.remotes:
        for backend in launcher.remotes.values():
            reset = getattr(backend, "reset_client", None)
            if callable(reset):
                reset()
        return True

    remotes, dialback = build_remote_backends(
        app.state.root, launcher.base_url, app.state.duck_token, data_root, secret_store)
    if remotes:
        launcher.attach_remotes(remotes, dialback)
    return bool(remotes)
