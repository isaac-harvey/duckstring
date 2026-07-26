"""Building — and live-refreshing — the remote Duck backends from the cloud gate.

The remote launchers (Fargate + EC2) and their shared dial-back are derived from the data root + AWS
creds. Extracted from ``app`` so they can be (re)built both at boot AND when the credentials or the data
root change at runtime: adding or rotating creds must make remote compute usable — and pick up the new
creds — without a Catchment restart (plans/cloud-config.md).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path

_log = logging.getLogger("duckstring.catchment")
_CRED_TTL = 300.0        # seconds — re-validate credentials at most this often (in the background)
_cred_lock = threading.Lock()  # guards kicking a single background validation at a time


def _validated_public_url(url: str | None) -> str | None:
    """Fail FAST on a malformed ``DUCKSTRING_CATCHMENT_PUBLIC_URL``. Every remote Duck dials this back,
    so a host-less value (the classic: an un-tokened IMDSv2 metadata read yielding ``http://:7474``)
    doesn't error here or at launch — it surfaces a minute later as a silent-Duck heartbeat failure on
    some unrelated Pond. Reject it at boot with the actual problem named."""
    if not url:
        return None
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(
            f"DUCKSTRING_CATCHMENT_PUBLIC_URL is not a dialable URL: {url!r} — it needs a scheme and a "
            "host (e.g. http://10.0.1.5:7474). A missing host usually means the value was built from an "
            "empty variable (e.g. an EC2 metadata lookup without an IMDSv2 token)."
        )
    return url


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
    has_public = _validated_public_url(os.environ.get("DUCKSTRING_CATCHMENT_PUBLIC_URL"))
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


# ─── Credential validity (the persistent-warning source) ─────────────────────────

def _gate_enabled(app_state) -> bool:
    from . import cloud
    return (cloud.is_remote(getattr(app_state, "data_root", None))
            and cloud.aws_configured(getattr(app_state, "secret_store", None)))


def _validate_and_store(app_state) -> None:
    """Run the live STS check and cache the result on ``app_state.cloud_creds``; log a warning when the
    gate is enabled but the creds are rejected (so a rejected key is never silent)."""
    from . import cloud
    try:
        err = cloud.validate_credentials()
        app_state.cloud_creds = {"valid": err is None, "error": err, "checked_at": time.time()}
        if err:
            _log.warning(
                "cloud is enabled but the AWS credentials did not validate (STS GetCallerIdentity): %s "
                "— remote Duck launches will fail until this is fixed (Options → Cloud → Verify).", err)
    finally:
        with _cred_lock:
            app_state._cred_check_running = False


def refresh_credential_status(app_state, *, force: bool = False, background: bool = True) -> None:
    """Keep ``app_state.cloud_creds`` (``{valid, error, checked_at}`` | None) reasonably fresh WITHOUT
    ever blocking the request: when the gate is on and the cache is stale/unknown, kick a single
    background STS check; the request reads the cache. When the gate is off, clear the cache (no warning).
    ``force`` re-checks now (used right after creds / the data root change)."""
    if not _gate_enabled(app_state):
        app_state.cloud_creds = None
        return
    cur = getattr(app_state, "cloud_creds", None)
    fresh = bool(cur) and (time.time() - cur.get("checked_at", 0.0) < _CRED_TTL)
    if fresh and not force:
        return
    if not background:
        _validate_and_store(app_state)
        return
    with _cred_lock:
        if getattr(app_state, "_cred_check_running", False):
            return  # one background check at a time — avoid a thread storm from the 1 s status poll
        app_state._cred_check_running = True
    threading.Thread(target=_validate_and_store, args=(app_state,), daemon=True).start()


def set_credential_status(app_state, valid: bool, error: str | None) -> None:
    """Record an out-of-band validity result (the Verify button already made a live STS call), so the
    persistent banner updates immediately instead of waiting for the next background refresh."""
    app_state.cloud_creds = {"valid": valid, "error": error, "checked_at": time.time()}
