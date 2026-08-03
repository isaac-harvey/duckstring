"""PoolLauncher — the launcher backend for a NAMED Pool: one shared machine, many co-resident Ducks
(plans/pool-agent.md, plans/persist.md phase 5).

The Catchment side of the Pool-agent protocol: ``ensure``/``terminate`` enqueue supervision jobs the
agent drains over its dial-back channel (``routes/pool.py``); ``is_running`` reflects agent-reported
process state (a pending spawn counts as up, like the other remote backends — the silent-Duck heartbeat
catches a dead machine). The **machine backend** brings up the ONE machine per Pool:

- :class:`LocalPoolMachine` — a subprocess agent with its own root dir: the offline test double AND a
  legitimate second Pool on this box for dev.
- :class:`FargatePoolMachine` / :class:`Ec2PoolMachine` — start one task/instance running the agent
  command, reusing the per-Pond launchers' config resolution (image/roles/networking from the pool's
  deploy_config over the env defaults). Real-AWS validation is the gate run's job.

A Pool never scales out (one filesystem, by definition — plans/persist.md); machine idle/scale-to-zero
scheduling is deferred (v1: the machine starts on first need, stops on shutdown_all)."""

from __future__ import annotations

import logging
import subprocess
import sys
import threading
import time
from pathlib import Path

log = logging.getLogger(__name__)

_PROBE_INTERVAL_S = 30.0


class _RemoteMachine:
    """Liveness for a remote Pool machine — shared by the Fargate and EC2 backends.

    A machine RECORD is not proof the machine exists. An instance/task can vanish underneath the
    Catchment (terminated by hand, reclaimed as spot, retired, stopped by an unrelated tool), and on the
    record alone ``start`` was a permanent no-op: every Pond on that Pool queued work at a machine that
    was gone. Measured on a live box — the Pool never relaunched, and each Pond took the full
    silent-Duck window to fail with a message blaming its own Duck. So a stale record is checked against
    the provider, cleared, and relaunched on the next spawn.

    Probes are throttled: this runs on the scheduler tick, and the cost is one describe per machine per
    :data:`_PROBE_INTERVAL_S` however many Ponds sit on the Pool."""

    _probed_at: float = 0.0

    def check(self) -> bool:
        """Is the machine up? Clears the record — so the next ``start`` relaunches — when it is gone."""
        if self._record() is None:
            return False
        now = time.monotonic()
        if now - self._probed_at < _PROBE_INTERVAL_S:
            return True
        self._probed_at = now
        try:
            alive = self._probe()
        except Exception:
            # A failed describe is not evidence of death (throttling, a transient API error, lost
            # credentials). Never tear down a live machine — and its Ducks — on it.
            log.exception("pool '%s': could not probe the machine; assuming it is still up", self.pool)
            return True
        if alive:
            # Don't let a past machine's epitaph outlive it: launch_error is what a Pond failure is
            # attributed to, so a stale "disappeared" would misattribute every later failure on the Pool.
            self._error = None
            return True
        # Name the machine: the id is what an operator takes to the console, and a probe that is WRONG
        # about a live machine is only diagnosable if we say which one it judged.
        record = self._record()
        log.warning("pool '%s': machine %s is gone; relaunching on the next spawn", self.pool, record)
        self._forget()
        self._error = (f"the '{self.pool}' Pool machine ({record}) disappeared (terminated or stopped "
                       "outside Duckstring); a replacement starts on the next run")
        return False


class LocalPoolMachine:
    """The Pool machine as a local subprocess agent with its OWN root dir — a genuinely separate
    filesystem namespace, so co-residency, local-first publish, and cross-Pool reads all behave exactly
    as on a remote machine."""

    def __init__(self, pool: str, root: Path, base_url: str, token: str,
                 data_root: str | None, persist_root: str | None):
        self.pool = pool
        self.root = root
        self.base_url = base_url
        self.token = token
        self.data_root = data_root
        self.persist_root = persist_root
        self._proc: subprocess.Popen | None = None

    def start(self) -> None:
        if self.is_up():
            return
        self.root.mkdir(parents=True, exist_ok=True)
        self._proc = subprocess.Popen([
            sys.executable, "-m", "duckstring.duck.pool_agent",
            "--pool", self.pool,
            "--catchment", self.base_url,
            f"--token={self.token}",
            "--root", str(self.root),
            f"--data-root={self.data_root or ''}",
            f"--persist-root={self.persist_root or ''}",
        ])

    def is_up(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def check(self) -> bool:
        return self.is_up()  # a local process needs no probe — poll() is the truth

    def stop(self) -> None:
        if self._proc is not None and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=10)
            except Exception:
                self._proc.kill()
        self._proc = None

    def error(self) -> str | None:
        return None


_LIVE_TASK = {"PROVISIONING", "PENDING", "ACTIVATING", "RUNNING"}


class FargatePoolMachine(_RemoteMachine):
    """One Fargate task running the Pool agent. Rides the per-Pond FargateLauncher's config resolution
    (task-def registration, cluster/subnets/roles from the pool's deploy_config over env defaults) with
    the AGENT command as the container override. Sizing = the pool's cpu/memory."""

    def __init__(self, pool: str, pool_spec: dict, fargate_launcher):
        self.pool = pool
        self.spec = pool_spec or {}
        self.fl = fargate_launcher  # the existing FargateLauncher (config + boto plumbing)
        self._task_arn: str | None = None
        self._error: str | None = None

    def _agent_command(self) -> list[str] | None:
        from .fargate_launcher import _REMOTE_ROOT

        if self.fl.remote_base_url is None:
            self.fl.dialback.request()  # bring up the auto-relay; the caller re-tries on the next ensure
            return None
        return [
            "duckstring.duck.pool_agent", "--pool", self.pool,
            "--catchment", self.fl.remote_base_url, f"--token={self.fl.token}",
            "--root", _REMOTE_ROOT,
            f"--data-root={self.fl.data_root or ''}",
            f"--persist-root={self.fl.data_root or ''}",  # a Pool Duck persists to the data root
        ]

    def _record(self) -> str | None:
        return self._task_arn

    def _forget(self) -> None:
        self._task_arn = None

    def _probe(self) -> bool:
        cluster = (self.spec.get("deploy_config") or {}).get("cluster") or self.fl.cluster
        tasks = (self.fl._ecs().describe_tasks(cluster=cluster, tasks=[self._task_arn])
                 .get("tasks") or [])
        if not tasks:
            return False  # ECS drops a stopped task from describe once it ages out
        return (tasks[0].get("lastStatus") or "").upper() in _LIVE_TASK

    def start(self) -> None:
        if self.check():
            return
        command = self._agent_command()
        if command is None:
            self._error = "fargate pool: waiting for a reachable Catchment URL (dial-back)"
            return
        dc = self.spec.get("deploy_config") or {}
        region = dc.get("region") or self.fl.region
        task_def = self.fl._task_def(
            image=dc.get("image") or self.fl.image,
            task_definition=dc.get("task_definition") or self.fl.task_definition,
            execution_role=dc.get("execution_role") or self.fl.execution_role,
            task_role=dc.get("task_role") or self.fl.task_role,
            cpu_arch=(dc.get("cpu_arch") or self.fl.cpu_arch or "X86_64").upper(),
            region=region,
        )
        if task_def is None:
            self._error = "fargate pool: no image configured"
            return
        from .fargate_launcher import _CONTAINER, _csv

        kwargs = {
            "cluster": dc.get("cluster") or self.fl.cluster,
            "launchType": "FARGATE", "taskDefinition": task_def, "count": 1,
            "overrides": {
                "cpu": str(self.spec.get("cpu") or dc.get("cpu") or 1024),
                "memory": str(self.spec.get("memory") or dc.get("memory") or 4096),
                "containerOverrides": [{"name": _CONTAINER, "command": command}],
            },
            "networkConfiguration": {"awsvpcConfiguration": {
                "subnets": _csv(dc.get("subnets")) or self.fl.subnets,
                "securityGroups": _csv(dc.get("security_groups")) or self.fl.security_groups,
                "assignPublicIp": dc.get("assign_public_ip") or self.fl.assign_public_ip,
            }},
            "tags": [{"key": "duckstring:pool", "value": self.pool},
                     {"key": "duckstring:role", "value": "pool-agent"}],
        }
        try:
            resp = self.fl._ecs().run_task(**kwargs)
            if resp.get("failures"):
                self._error = f"fargate pool: run_task failed: {resp['failures']}"
                return
            self._task_arn = resp["tasks"][0]["taskArn"]
            self._error = None
            log.info("fargate: started pool machine %s for pool '%s'", self._task_arn, self.pool)
        except Exception as exc:
            log.exception("fargate: failed to start the pool machine for '%s'", self.pool)
            self._error = f"fargate pool: {exc}"

    def is_up(self) -> bool:
        return self._task_arn is not None

    def stop(self) -> None:
        if self._task_arn is None:
            return
        try:
            self.fl._ecs().stop_task(cluster=(self.spec.get("deploy_config") or {}).get("cluster")
                                     or self.fl.cluster, task=self._task_arn,
                                     reason="duckstring pool shutdown")
        except Exception:
            log.exception("fargate: failed to stop the pool machine for '%s'", self.pool)
        self._task_arn = None

    def error(self) -> str | None:
        return self._error


class Ec2PoolMachine(_RemoteMachine):
    """One EC2 instance running the Pool agent via userdata — the escape-hatch provider, same shape as
    :class:`FargatePoolMachine` over the Ec2Launcher's plumbing."""

    def __init__(self, pool: str, pool_spec: dict, ec2_launcher):
        self.pool = pool
        self.spec = pool_spec or {}
        self.el = ec2_launcher
        self._instance_id: str | None = None
        self._error: str | None = None

    def _record(self) -> str | None:
        return self._instance_id

    def _forget(self) -> None:
        self._instance_id = None

    def _probe(self) -> bool:
        try:
            resp = self.el._ec2().describe_instances(InstanceIds=[self._instance_id])
        except Exception as exc:
            # A terminated instance is purged after a while and the id stops resolving at all — that is
            # a definite answer, not a probe failure, so don't let it reach the assume-alive fallback.
            if "NotFound" in str(exc):
                return False
            raise
        for reservation in resp.get("Reservations") or []:
            for instance in reservation.get("Instances") or []:
                return ((instance.get("State") or {}).get("Name")) in ("pending", "running")
        return False

    def start(self) -> None:
        if self.check():
            return
        if self.el.remote_base_url is None:
            self.el.dialback.request()
            self._error = "ec2 pool: waiting for a reachable Catchment URL (dial-back)"
            return
        # The root MUST be the directory the launcher's userdata creates (_REMOTE_ROOT) — a hardcoded
        # "/var/duckstring" here never exists on the box, so the agent died on boot and the Pond just
        # timed out as a silent Duck. The Fargate pool machine above already uses _REMOTE_ROOT.
        from .ec2_launcher import _REMOTE_ROOT as _EC2_ROOT

        agent_args = (
            f"--pool {self.pool} --catchment {self.el.remote_base_url} "
            f"--token={self.el.token} --root {_EC2_ROOT} "
            f"--data-root={self.el.data_root or ''} --persist-root={self.el.data_root or ''}"
        )
        try:
            self._instance_id = self.el.start_pool_instance(
                self.pool, self.spec, f"duckstring.duck.pool_agent {agent_args}")
            self._error = None
        except Exception as exc:
            log.exception("ec2: failed to start the pool machine for '%s'", self.pool)
            self._error = f"ec2 pool: {exc}"

    def is_up(self) -> bool:
        return self._instance_id is not None

    def stop(self) -> None:
        if self._instance_id is None:
            return
        try:
            self.el.terminate_instance(self._instance_id)
        except Exception:
            log.exception("ec2: failed to stop the pool machine for '%s'", self.pool)
        self._instance_id = None

    def error(self) -> str | None:
        return self._error


class PoolLauncher:
    """The launcher for one named Pool: supervision jobs to the agent, process state from its reports.
    Implements the same seam as every other launcher backend (ensure/terminate/is_running/launch_error/
    shutdown_all); the DispatchingLauncher routes named-Pool spawns here."""

    manages_processes = True

    def __init__(self, pool: str, machine):
        self.pool = pool
        self.machine = machine
        self._lock = threading.Lock()
        self._jobs: list[dict] = []
        self._state: dict[str, str] = {}  # pond_key -> pending | running | exited
        self._flock_env = {}  # per-spawn env carried on the ensure job

    # ── the launcher seam ─────────────────────────────────────────────────────

    def ensure(self, pond_key: str, version: str, source_path: str, duck: dict | None = None) -> None:
        from ..keys import split_pond_key

        with self._lock:
            if self._state.get(pond_key) in ("pending", "running"):
                return
            self.machine.start()
            name, major = split_pond_key(pond_key)
            env = {}
            if duck:
                env["DUCKSTRING_FLOCK_MODE"] = duck.get("flock_mode") or "off"
                if duck.get("flock_engine"):
                    env["DUCKSTRING_FLOCK_ENGINE"] = duck["flock_engine"]
                env["DUCKSTRING_FLOCK_OOM_POLICY"] = duck.get("oom_policy") or "fail_up"
            self._jobs.append({"kind": "ensure", "pond": name, "major": major,
                               "version": version, "source_path": source_path, "env": env})
            self._state[pond_key] = "pending"

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        from ..keys import split_pond_key

        with self._lock:
            if self._state.get(pond_key) not in ("pending", "running"):
                return
            name, major = split_pond_key(pond_key)
            self._jobs = [j for j in self._jobs
                          if not (j.get("pond") == name and j.get("major") == major)]
            self._jobs.append({"kind": "terminate", "pond": name, "major": major, "wait": wait})
            self._state[pond_key] = "exited"

    def is_running(self, pond_key: str) -> bool:
        with self._lock:
            live = self._state.get(pond_key) in ("pending", "running")
        if not live:
            return False
        # An agent-reported state is only as good as the machine under it: if that is gone, nothing on
        # it is running whatever the agent last said. Answering now fails the Pond promptly with the
        # machine's own reason, instead of every Pond on the Pool waiting out the silent-Duck window and
        # blaming its Duck. (Probed outside the lock — it is a network call.)
        if not self.machine.check():
            with self._lock:
                for key, state in list(self._state.items()):
                    if state in ("pending", "running"):
                        self._state[key] = "exited"
            return False
        return True

    def launch_error(self, pond_key: str) -> str | None:
        return self.machine.error()

    def shutdown_all(self) -> None:
        with self._lock:
            self._jobs = [{"kind": "shutdown"}]
            self._state.clear()
        self.machine.stop()

    # ── the agent protocol (drained by routes/pool.py) ────────────────────────

    def take_jobs(self) -> list[dict]:
        with self._lock:
            jobs, self._jobs = self._jobs, []
            return jobs

    def on_event(self, payload: dict) -> None:
        kind = payload.get("kind")
        if kind not in ("duck_started", "duck_exit"):
            return
        key = f"{payload.get('pond')}@{payload.get('major')}"
        with self._lock:
            if kind == "duck_started":
                self._state[key] = "running"
            else:
                self._state[key] = "exited"
