from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from . import auth
from .db import connect, ensure_identity, migrate
from .driver import Driver
from .launcher import NoopLauncher, SubprocessLauncher, load_launcher_class
from .routes import router

_STATIC_DIR = Path(__file__).parent / "static"


async def _scheduler(driver: Driver) -> None:
    """Drive clock processes (Tide deadlines, window boundaries, Wave-on-idle) at next_wake."""
    while True:
        nw = driver.next_wake()
        now = datetime.now(timezone.utc)
        delay = (nw - now).total_seconds() if nw else 1.0
        await asyncio.sleep(max(0.05, min(delay, 5.0)))
        try:
            driver.scheduler_tick()
        except Exception as exc:  # keep the loop alive
            print(f"[catchment] scheduler error: {exc}", flush=True)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    base_url = app.state.base_url
    if os.environ.get("DUCKSTRING_DISABLE_DUCKS"):
        launcher = NoopLauncher()
    else:
        # Ducks dial back over the duck channel with the internal worker token (decoupled from the user
        # keys, so rotating those never disrupts running Ducks).
        # base_url None = unknown (the platform picked the bind address): the launcher defers spawns
        # until the dial-back middleware learns the address from the first request.
        # DUCKSTRING_DUCK_LAUNCHER=module:Class (prereqs D6) swaps the implementation — same
        # constructor contract, same interface; the Duck still dials back over the duck channel.
        if os.environ.get("DUCKSTRING_DUCK_LAUNCHER"):
            # A fully-custom launcher (D6) takes over entirely — same constructor contract as before.
            launcher_cls = load_launcher_class(os.environ["DUCKSTRING_DUCK_LAUNCHER"])
            launcher = launcher_cls(
                app.state.root, base_url, token=app.state.duck_token, data_root=app.state.data_root
            )
        else:
            # The dispatching launcher: `catchment`-targeted Ponds on this box (subprocess), pool/
            # dedicated ones on EC2 — the same code whether the Catchment is local or hosted. The EC2
            # backend is only built when cloud is enabled (remote data root + AWS creds); otherwise
            # remote targets degrade to local (plans/cloud-config.md).
            from .cloud_backends import build_remote_backends
            from .launcher import DispatchingLauncher

            local = SubprocessLauncher(
                app.state.root, base_url, token=app.state.duck_token, data_root=app.state.data_root
            )
            # The remote backends (Fargate/EC2 + the shared dial-back) exist only when cloud is enabled
            # (remote data root + AWS creds); otherwise remote targets degrade to local. The same builder
            # re-attaches them live when creds / the data root change at runtime (see cloud_backends).
            remotes, dialback = build_remote_backends(
                app.state.root, base_url, app.state.duck_token, app.state.data_root, app.state.secret_store)
            launcher = DispatchingLauncher(local, remotes, dialback=dialback, default_provider="fargate")
    driver = Driver(app.state.db, app.state.root, base_url, launcher, data_root=app.state.data_root)
    app.state.driver = driver
    app.state.launcher = launcher

    # Boot credential check: the cloud gate is presence-based (deterministic, network-free), so a
    # present-but-rejected key would show "enabled" while every remote launch fails. Validate the creds
    # actually authenticate and WARN if not — non-blocking (a daemon thread), never fails boot. Validity
    # is surfaced, not folded into the gate (a transient STS blip must not strand a running setup).
    from . import cloud as _cloud
    if _cloud.is_remote(app.state.data_root) and _cloud.aws_configured(app.state.secret_store):
        import logging
        import threading

        def _probe_credentials():
            err = _cloud.validate_credentials()
            if err:
                logging.getLogger("duckstring.catchment").warning(
                    "cloud is enabled but the AWS credentials did not validate (STS GetCallerIdentity): "
                    "%s — remote Duck launches will fail until this is fixed (Options → Cloud → Verify).", err)

        threading.Thread(target=_probe_credentials, daemon=True).start()

    # Restore: resume any Pond Runs that were in flight when the Catchment last stopped.
    driver.resume_incomplete()

    # Data-serving wire adapters (plans/data-serving.md): the Postgres wire (default) + Arrow Flight SQL,
    # each on its own port when configured, sharing the sandboxed serving core. Put TLS + a network ACL
    # in front for a hosted Catchment (the password is an API key).
    app.state.serve_wires = []
    pg_port = os.environ.get("DUCKSTRING_SERVE_PG_PORT")
    if pg_port:
        from .pg_wire import PgWireServer

        pg = PgWireServer(driver, api_key=app.state.api_key,
                          host=os.environ.get("DUCKSTRING_SERVE_HOST", "127.0.0.1"), port=int(pg_port))
        pg.start()
        app.state.serve_wires.append(pg)
    flight_port = os.environ.get("DUCKSTRING_SERVE_FLIGHT_PORT")
    if flight_port:
        try:
            from .flight_sql import FlightSqlServer

            fl = FlightSqlServer(driver, api_key=app.state.api_key,
                                 host=os.environ.get("DUCKSTRING_SERVE_HOST", "127.0.0.1"), port=int(flight_port))
            fl.start()
            app.state.serve_wires.append(fl)
        except ImportError:
            pass  # Flight needs pyarrow.flight; skip if unavailable

    from .alert_worker import run_alert_worker
    from .egress_worker import run_egress_worker
    from .poller import run_poller

    # The poller wakes immediately when a Draw acquires demand (so it solicits its upstream at once),
    # instead of waiting for its next cycle. The driver signals across threads via the running loop.
    wake = asyncio.Event()
    loop = asyncio.get_running_loop()
    driver.set_notify(lambda: loop.call_soon_threadsafe(wake.set))

    # The egress worker wakes when a Pond publishes (its Spouts may have work) or a Spout is resynced.
    egress_wake = asyncio.Event()
    driver.set_egress_notify(lambda: loop.call_soon_threadsafe(egress_wake.set))

    # The alert worker wakes when a failure/freshness event enqueues a notification delivery.
    alert_wake = asyncio.Event()
    driver.set_alert_notify(lambda: loop.call_soon_threadsafe(alert_wake.set))

    from .state_sync import checkpoint_full, run_checkpoint_worker

    scheduler = asyncio.create_task(_scheduler(driver))
    poller = asyncio.create_task(run_poller(driver, app.state.root, wake))
    egress = asyncio.create_task(run_egress_worker(driver, app.state.root, egress_wake))
    alerts = asyncio.create_task(run_alert_worker(driver, alert_wake))
    # Tier-1 state backup: push a duck.db snapshot to DUCKSTRING_STATE_BACKUP_URI on an interval (no-op
    # when unset). The Tier-2 warm bundle is flushed once below, after the Ducks are stopped (quiescent).
    checkpointer = asyncio.create_task(
        run_checkpoint_worker(app.state.root, app.state.state_backup, app.state.checkpoint_every)
    )
    # Renew the data-root writer lease so a live Catchment's ownership never lapses (external data root only).
    tasks = [scheduler, poller, egress, alerts, checkpointer]
    if app.state.data_lease is not None:
        from .data_lease import run_lease_renewer

        lease_store, owner_id = app.state.data_lease
        tasks.append(asyncio.create_task(run_lease_renewer(lease_store, owner_id)))
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        launcher.shutdown_all()
        for wire in getattr(app.state, "serve_wires", []):
            wire.stop()
        if app.state.data_lease is not None:
            from .data_lease import release_lease

            lease_store, owner_id = app.state.data_lease
            try:
                release_lease(lease_store, owner_id)
            except Exception:  # pragma: no cover - releasing the lease must not break shutdown
                pass
        # Ducks are now stopped → the registries/ledgers are quiescent: flush the warm Tier-1+2 bundle so a
        # scaled-to-zero restart comes back warm (and engine state survives even a hard next crash).
        if app.state.state_backup:
            from fastapi.concurrency import run_in_threadpool

            await run_in_threadpool(checkpoint_full, app.state.root, app.state.state_backup)


def create_app(
    root: Path, api_key: str | None = None, base_url: str | None = None, name: str | None = None,
    *, data_root: str | None = None, state_backup: str | None = None, checkpoint_every: str | None = None,
) -> FastAPI:
    # Data plane location (object store / Volume / path) and the Tier-1 state-backup target. Explicit
    # arguments (the CLI passes the registration's values) win, else the platform-hosting env vars.
    data_root = data_root or os.environ.get("DUCKSTRING_DATA_ROOT") or None
    state_backup = state_backup or os.environ.get("DUCKSTRING_STATE_BACKUP_URI") or None
    checkpoint_every = checkpoint_every or os.environ.get("DUCKSTRING_CHECKPOINT_INTERVAL") or "60s"

    # Restore Tier-1 state (duck.db, ledgers) from the backup before opening the DB, if the state root is
    # empty (a fresh/scaled-to-zero node) and a backup exists.
    from .state_sync import restore_state_if_empty

    restore_state_if_empty(root, state_backup)

    root.mkdir(parents=True, exist_ok=True)
    con = connect(root / "duck.db")
    migrate(con)
    ensure_identity(con, name or os.environ.get("DUCKSTRING_CATCHMENT_NAME"))

    # The data root can be a persisted Catchment setting (attached via the API after the Catchment is
    # made — plans/cloud-config.md), behind an explicit argument and the env for platform hosting.
    if not data_root:
        from .cloud import DATA_ROOT_KEY, get_setting
        data_root = get_setting(con, DATA_ROOT_KEY)

    # Writer lease on an external data root — refuse to start if a *different* live Catchment owns it (two
    # Catchments racing one lake's Iceberg catalog would dangle its pointer). A same-id restart reclaims
    # instantly; only engaged for an external DUCKSTRING_DATA_ROOT, so the local default is untouched.
    data_lease = None
    if data_root:
        from ..storage import get_storage
        from .data_lease import acquire_lease

        cid = con.execute("SELECT value FROM catchment_meta WHERE key = 'id'").fetchone()
        owner_id = cid[0] if cid else "unknown"
        lease_store = get_storage(data_root)
        acquire_lease(lease_store, owner_id)  # raises LeaseConflict → the server does not start
        data_lease = (lease_store, owner_id)

    app = FastAPI(title="Duckstring Catchment", lifespan=_lifespan)
    app.state.root = root
    app.state.data_lease = data_lease
    app.state.data_root = data_root
    app.state.state_backup = state_backup
    app.state.checkpoint_every = checkpoint_every
    app.state.db = con
    # Built-in single API key (legacy / bare self-hosting): explicit argument or the environment. It
    # means full access. The tiered read/demand/full keys live in `catchment_key` (see auth.py); either
    # may gate the API. With neither configured, the Catchment is fully open.
    app.state.api_key = api_key or os.environ.get("DUCKSTRING_API_KEY") or None
    # The internal token Ducks present on the duck channel (persisted, decoupled from the user keys).
    app.state.duck_token = auth.ensure_duck_token(con)
    # The write-only secret store (at the root, archive-excluded). Injected into the egress credential
    # resolver so a ${secret:NAME} reference resolves at egress time.
    from ..egress import credentials
    from .secrets import SecretStore
    app.state.secret_store = SecretStore(root)
    credentials.set_secret_provider(app.state.secret_store.get)
    # Give the AWS_* secrets teeth: load them into the environment so botocore's chain uses them (the
    # enable-by-secret cloud flow). Real env wins.
    from . import cloud as _cloud
    _cloud.load_aws_env(app.state.secret_store)
    # The address Ducks dial back to: explicit argument (the CLI passes its bind address), or the
    # environment, or None — unknown, because the host platform picks the bind address (e.g. Posit
    # Connect). When None it is learned from the first request's ASGI scope below.
    app.state.base_url = base_url or os.environ.get("DUCKSTRING_CATCHMENT_URL") or None

    @app.middleware("http")
    async def _learn_dialback_address(request, call_next):
        launcher = getattr(app.state, "launcher", None)
        if launcher is not None and getattr(launcher, "base_url", "") is None:
            server = request.scope.get("server")  # the server's bound (host, port) per the ASGI spec
            if server and server[1]:  # a unix socket has port None — nothing TCP to dial
                host = "127.0.0.1" if server[0] in ("0.0.0.0", "::") else server[0]
                url = f"http://{host}:{server[1]}"
                app.state.base_url = url
                app.state.driver.base_url = url
                launcher.set_base_url(url)  # spawns any Ducks that were waiting on the address
        return await call_next(request)

    app.include_router(router, prefix="/api")
    auth.audit_routes(app)  # fail-closed: every /api route must declare an access level

    # Prometheus scrape endpoint at the ROOT (unauthenticated, the exporter convention) — mounted before
    # the static "/" catch-all so it resolves, and outside "/api" so the access-level audit doesn't cover it.
    from .routes.metrics import router as metrics_router
    app.include_router(metrics_router)

    if _STATIC_DIR.exists():
        app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="frontend")

    return app
