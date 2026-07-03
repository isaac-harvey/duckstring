"""Duck entrypoint: ``python -m duckstring.duck --pond NAME --version V --catchment URL --token T``.

Wires :class:`DuckCore` (engine + ledger) to the :class:`RippleExecutor` and the long-poll
:class:`CatchmentClient`, then serves until the Catchment tells it to shut down (Pond idle). The Duck
keeps draining in-flight Pond Runs and buffering events regardless of Catchment availability.
"""

from __future__ import annotations

import argparse
import queue
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path

from ..engine import NEVER
from ..engine import pond as ledger
from .client import CatchmentClient
from .core import DuckCore
from .executor import RippleExecutor, load_topology


def _now() -> datetime:
    return datetime.now(timezone.utc)


# An active Pond Run (the Duck has outstanding work) must have a Ripple actually executing. If none is
# for this long, the Run is wedged — report it as a Pond failure rather than hang forever. Generous,
# to absorb the momentary gap between one Ripple finishing and the next being launched.
_STUCK_GRACE_S = 30.0


def serve(core: DuckCore, executor: RippleExecutor, client: CatchmentClient) -> None:
    """Single-threaded event loop fed by a poll thread (jobs) and executor callbacks (completions)."""
    q: queue.Queue = queue.Queue()
    stop = threading.Event()
    inflight = 0  # Ripple Runs currently executing in the pool
    last_progress = _now()  # last time a Ripple was launched or finished

    def _poll_loop():
        while not stop.is_set():
            jobs = client.poll_jobs()
            for job in jobs:
                q.put(("job", job))
            if not jobs:
                stop.wait(0.1)  # short poll interval; avoids busy-spinning the Catchment

    def _launch(names):
        nonlocal inflight, last_progress
        for name in names:
            inflight += 1
            last_progress = _now()
            start_f = core.state.states[name].start_f  # the run's freshness, exposed as pond.f
            executor.submit(
                name,
                start_f,
                core.previous_f_for(start_f),  # the prior run's freshness, exposed as pond.previous_f
                on_done=lambda n, started, finished: q.put(("done", (n, started, finished))),
                on_error=lambda n, exc, started, finished: q.put(("error", (n, exc, started, finished))),
                sources_changed=core.sources_changed_for(start_f),  # backs pond.sources_changed()
                skip_sink=(lambda f=start_f: core.mark_skipped(f)),  # backs pond.skip() for this Run
            )

    poller = threading.Thread(target=_poll_loop, daemon=True)
    poller.start()

    shutdown_requested = False
    try:
        while True:
            try:
                kind, data = q.get(timeout=1.0)
            except queue.Empty:
                kind, data = None, None

            try:
                if kind == "job":
                    if data.get("kind") == "shutdown":
                        shutdown_requested = True
                    elif data.get("kind") == "begin_run":
                        prev = data.get("previous_f")
                        if data.get("refresh"):
                            executor.wipe()  # cold reset: the run rebuilds from scratch
                        _launch(core.begin_run(
                            datetime.fromisoformat(data["f"]), _now(),
                            retry_immediately=data.get("immediate_retries", 0),
                            force=data.get("force", False),
                            previous_f=datetime.fromisoformat(prev) if prev else NEVER,
                            contract=data.get("contract"),
                            sources_changed=data.get("sources_changed", True),
                        ))
                elif kind == "done":
                    inflight -= 1
                    last_progress = _now()
                    name, started, finished = data
                    _launch(core.ripple_completed(
                        name, _now(), started_at=started, finished_at=finished, export=executor.export
                    ))
                elif kind == "error":
                    inflight -= 1
                    last_progress = _now()
                    name, exc, started, finished = data
                    from ..core import MissingSourceAsset
                    if isinstance(exc, MissingSourceAsset):
                        # A read of an unpublished Source asset — waiting, not broken. Park it (no retry,
                        # no failure); the Catchment blocks the Pond with a reason. See plans/reset.md.
                        print(f"[duck:{core.pond_name}] ripple {name} waiting on {exc.source}.{exc.table}", flush=True)
                        _launch(core.ripple_missing_source(
                            name, exc.source, exc.table, _now(), started_at=started, finished_at=finished,
                        ))
                    else:
                        print(f"[duck:{core.pond_name}] ripple {name} failed: {exc}", flush=True)
                        _launch(core.ripple_failed(
                            name, _now(), started_at=started, finished_at=finished,
                            error=_msg(exc), traceback=_tb(exc),
                        ))

                core.flush(client.post_event)
            except Exception as exc:
                # A Pond-level error (e.g. a failed ledger write): report it against the most recent
                # Pond Run and exit. The Catchment fails the Pond (and may retry it on change).
                print(f"[duck:{core.pond_name}] pond-level failure: {exc}", flush=True)
                _report_pond_failure(core, client, _msg(exc), _tb(exc))
                break

            # Watchdog: outstanding work but nothing running, past the grace period → the Run is stuck.
            if not core.idle() and inflight == 0 and (_now() - last_progress).total_seconds() > _STUCK_GRACE_S:
                print(f"[duck:{core.pond_name}] stuck: active Pond Run with no running Ripple", flush=True)
                _report_pond_failure(core, client, "stuck: active Pond Run with no running Ripple")
                break

            if shutdown_requested and core.idle() and not core.events:
                break
    finally:
        stop.set()
        executor.shutdown()
        client.close()


def _msg(exc: BaseException) -> str:
    """A compact, single-line failure message for the UI/DB (type + first line, length-capped)."""
    text = f"{type(exc).__name__}: {exc}".strip().splitlines()[0]
    return text[:500]


def _tb(exc: BaseException) -> str | None:
    """The full formatted traceback for the failure (length-capped), or None if unavailable."""
    if exc.__traceback__ is None:
        return None
    text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return text[:8000]


def _report_pond_failure(
    core: DuckCore, client: CatchmentClient, error: str | None = None, tb: str | None = None
) -> None:
    """Best-effort: buffer + flush a Pond-level failure for the Catchment (the Duck then exits)."""
    try:
        core.pond_failed(error, tb)
        core.flush(client.post_event)
    except Exception:
        pass


def main() -> None:
    from ..catchment.registry import pond_major_dir

    ap = argparse.ArgumentParser(prog="duckstring.duck")
    ap.add_argument("--pond", required=True)
    ap.add_argument("--major", required=True, type=int)
    ap.add_argument("--version", required=True)
    ap.add_argument("--catchment", required=True)
    ap.add_argument("--token", default="")
    ap.add_argument("--root", required=True)
    ap.add_argument("--source-path", required=True, help="pond source dir relative to root")
    ap.add_argument("--data-root", default="", help="data-plane root URI (object store / Volume / path); "
                                                    "empty = under the state root")
    args = ap.parse_args()

    root = Path(args.root)
    data_root = args.data_root or None
    parents = load_topology(root / args.source_path)
    major_dir = pond_major_dir(root, args.pond, args.major)
    major_dir.mkdir(parents=True, exist_ok=True)
    con = ledger.connect(major_dir / "pond.db")
    core = DuckCore(f"{args.pond}@{args.major}", con, parents)  # name@major label for log lines
    executor = RippleExecutor(args.pond, args.major, args.version, args.source_path, root, data_root=data_root)
    client = CatchmentClient(args.catchment, args.pond, args.major, args.token)
    serve(core, executor, client)


if __name__ == "__main__":
    main()
