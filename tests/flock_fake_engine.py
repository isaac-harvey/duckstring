"""A fake Flock engine for the offline e2e (selected via ``DUCKSTRING_FLOCK_ENGINE=flock_fake_engine:
FakeFlockEngine``, with ``PYTHONPATH`` pointing at ``tests/`` so spawned Duck subprocesses can import it).

The real engine is Athena, which needs AWS — so without this the entire Flock path (dispatch, the
fallback contract, and the reporting that rides run completion) is only reachable on a live cloud box.
This stands in for it faithfully: it "dispatches" by computing the builder's own comprehensive relation
and materialising it, which is exactly the shape a remote engine returns.

Env knobs (read per construction, so a test can flip behaviour between runs):

- ``FAKE_FLOCK_FAIL=1``   — dispatch returns ``None`` (an engine-side failure, per the protocol), so the
  terminal degrades to local compute.
- ``FAKE_FLOCK_RAISE=1``  — dispatch RAISES instead, which no well-behaved engine should do: it covers
  the catch-all in ``flock._dispatch`` that keeps the fallback contract from depending on engine
  discipline.
- ``FAKE_FLOCK_DELAY_S``  — seconds to sleep inside dispatch (mimics a slow remote query).
"""

from __future__ import annotations

import os
import time
import uuid


class FakeFlockEngine:
    """A :class:`duckstring.flock.FlockEngine` that runs the "remote" side locally."""

    def __init__(self, env=None):
        self.env = env if env is not None else os.environ
        self.last_error: str | None = None

    def enabled(self) -> bool:
        return True

    def eligible(self, builder) -> str | None:
        return None  # everything is dispatchable — the fake has no SQL-shape restrictions

    def estimate_rows(self, builder) -> int:
        return 10_000_000  # "clearly over the envelope", so `upgrade` dispatches up front too

    def dispatch(self, builder, out_pk):
        delay = float(self.env.get("FAKE_FLOCK_DELAY_S") or 0)
        if delay:
            time.sleep(delay)
        if self.env.get("FAKE_FLOCK_RAISE") == "1":
            raise RuntimeError("fake flock: dispatch blew up")
        if self.env.get("FAKE_FLOCK_FAIL") == "1":
            self.last_error = "fake flock: dispatch failed"
            return None
        # The comprehensive relation, materialised — the same contract a remote engine's result honours.
        con = builder.ctx.con
        name = f"_ds_fake_flock_{uuid.uuid4().hex[:8]}"
        con.execute(f"CREATE TEMP TABLE {name} AS {builder._full_join().sql_query()}")
        return con.table(name)
