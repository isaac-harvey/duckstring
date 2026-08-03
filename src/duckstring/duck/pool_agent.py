"""The Pool agent — the supervisor on a named Pool's machine (plans/pool-agent.md).

A named Pool is ONE shared filesystem hosting many Ponds' Ducks as co-resident processes
(plans/persist.md). The agent is the machine-side half: it **dials back** to the Catchment (symmetric
with the Duck transport — never listens, NAT-safe, and a local agent is a faithful test double for a
remote one), polls ``GET /api/pool/{name}/jobs`` for supervision commands, spawns/terminates child Duck
processes against ITS root, and POSTs process events to ``/api/pool/{name}/events``.

The agent only supervises. Once spawned, a child Duck is exactly a Duck: it polls its own job channel,
fetches its artifact over the remote-boot path if the source dir is absent, publishes locally to this
machine's root (the Pool handoff), and persists to the data root. Job kinds:

- ``ensure``    {pond, major, version, source_path, env?} — spawn if not already running
- ``terminate`` {pond, major} — stop the child
- ``shutdown``  — stop every child and exit

Usage: ``python -m duckstring.duck.pool_agent --pool NAME --catchment URL --token T --root DIR
[--data-root URI] [--persist-root URI]``.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx


class PoolAgent:
    def __init__(self, pool: str, catchment: str, token: str, root: Path,
                 data_root: str | None = None, persist_root: str | None = None,
                 poll_interval: float = 0.2):
        self.pool = pool
        self.base = catchment.rstrip("/")
        self.token = token
        self.root = root
        self.data_root = data_root
        self.persist_root = persist_root
        self.poll_interval = poll_interval
        self._procs: dict[str, subprocess.Popen] = {}  # "{pond}@{major}" -> child Duck
        self._client = httpx.Client(timeout=10.0, headers={"X-Duck-Token": token})

    # ─── the dial-back channel ───────────────────────────────────────────────────

    def _poll_jobs(self) -> list[dict]:
        try:
            r = self._client.get(f"{self.base}/api/pool/{self.pool}/jobs")
            r.raise_for_status()
            return r.json().get("jobs", [])
        except Exception:
            return []  # best-effort: the Catchment may be restarting; children keep running regardless

    def _post_event(self, payload: dict) -> None:
        try:
            self._client.post(f"{self.base}/api/pool/{self.pool}/events", json=payload)
        except Exception:
            pass  # process events are advisory (liveness rides the children's own channels)

    # ─── supervision ─────────────────────────────────────────────────────────────

    def _spawn(self, job: dict) -> None:
        key = f"{job['pond']}@{job['major']}"
        proc = self._procs.get(key)
        if proc is not None and proc.poll() is None:
            return  # already running
        env = dict(os.environ)
        env.update(job.get("env") or {})
        argv = [
            sys.executable, "-m", "duckstring.duck",
            "--pond", job["pond"],
            "--major", str(job["major"]),
            "--version", job["version"],
            "--catchment", self.base,
            f"--token={self.token}",
            "--root", str(self.root),
            "--source-path", job["source_path"],
            f"--data-root={self.data_root or ''}",
            # A Pool Duck publishes locally (this machine's root — the shared-filesystem handoff its
            # co-resident Sinks read) and async-persists to the durable plane, exactly like a
            # Catchment-Pool Duck (plans/persist.md).
            f"--persist-root={self.persist_root or ''}",
        ]
        self._procs[key] = subprocess.Popen(argv, env=env)
        self._post_event({"kind": "duck_started", "pond": job["pond"], "major": job["major"]})
        print(f"[pool:{self.pool}] started duck {key} (pid {self._procs[key].pid})", flush=True)

    def _terminate(self, key: str, wait: bool = False) -> None:
        proc = self._procs.pop(key, None)
        if proc is not None and proc.poll() is None:
            proc.terminate()
            if wait:
                try:
                    proc.wait(timeout=10)
                except Exception:
                    proc.kill()

    def _reap(self) -> None:
        for key, proc in list(self._procs.items()):
            code = proc.poll()
            if code is not None:
                del self._procs[key]
                pond, major = key.rsplit("@", 1)
                self._post_event({"kind": "duck_exit", "pond": pond, "major": int(major),
                                  "returncode": code})
                print(f"[pool:{self.pool}] duck {key} exited ({code})", flush=True)

    def serve(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self._post_event({"kind": "agent_up"})
        try:
            while True:
                for job in self._poll_jobs():
                    kind = job.get("kind")
                    if kind == "ensure":
                        self._spawn(job)
                    elif kind == "terminate":
                        self._terminate(f"{job['pond']}@{job['major']}", wait=job.get("wait", False))
                        self._post_event({"kind": "duck_exit", "pond": job["pond"],
                                          "major": job["major"], "returncode": None})
                    elif kind == "shutdown":
                        return
                self._reap()
                time.sleep(self.poll_interval)
        finally:
            for key in list(self._procs):
                self._terminate(key, wait=True)
            self._post_event({"kind": "agent_down"})
            self._client.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", required=True)
    ap.add_argument("--catchment", required=True)
    ap.add_argument("--token", default="")  # `--token=` joined so a leading '-' isn't read as a flag
    ap.add_argument("--root", required=True)
    ap.add_argument("--data-root", default="")
    ap.add_argument("--persist-root", default="")
    args = ap.parse_args()
    PoolAgent(
        args.pool, args.catchment, args.token, Path(args.root),
        data_root=args.data_root or None, persist_root=args.persist_root or None,
    ).serve()


if __name__ == "__main__":
    main()
