"""The Fargate Duck backend (plans/cloud-config.md §4b) — the **default** remote launcher. Runs each
Duck as a serverless ECS/Fargate task from the Duckstring container image: fast cold start (tens of
seconds vs EC2's minutes), pay-per-second, native task-role IAM, no AMI/userdata bootstrap.

Same launcher seam as everything else: ``ensure`` = ``RunTask`` (the container command override is the
Duck args, so it boots via the existing remote-boot artifact fetch), ``terminate`` = ``StopTask``,
``is_running`` = a task record. A pool carries only its **size** (`cpu`/`memory`); the Fargate **infra**
(cluster / subnets / security groups / task+execution roles / image) is Catchment-level env
(``DUCKSTRING_FARGATE_*``) — one VPC/cluster, not per-pool. The reachable Catchment URL comes from the
shared :class:`RemoteDialback` (the auto-relay bridges a local Catchment). boto3 is lazy; a fake ecs
client is injectable for tests. The real path (a live ECS cluster) is the cloud e2e.
"""

from __future__ import annotations

import logging
import os

from ..keys import split_pond_key

log = logging.getLogger("duckstring.fargate")

_REMOTE_ROOT = "/var/lib/duckstring"
_CONTAINER = "duck"


def _NOISE(message: str) -> bool:
    """Is this log line routine Duck chatter rather than a cause? A Duck's stream is dominated by its
    job poll; keeping those crowds the actual error out of a bounded tail."""
    return "/jobs" in message and " 200 " in message
_FARGATE_NO_IMAGE = (
    "Fargate cannot launch — no container image or task definition configured. A Fargate pool needs the "
    "Duck image + VPC + IAM set as Catchment env: DUCKSTRING_FARGATE_IMAGE (or DUCKSTRING_FARGATE_TASK_DEF), "
    "DUCKSTRING_FARGATE_SUBNETS, DUCKSTRING_FARGATE_SECURITY_GROUPS, DUCKSTRING_FARGATE_EXECUTION_ROLE, "
    "DUCKSTRING_FARGATE_TASK_ROLE, DUCKSTRING_FARGATE_CLUSTER. Without them, set the Pond's compute to the "
    "Catchment (local) target.")
_TASK_FAMILY = "duckstring-duck"


class FargateLauncher:
    manages_processes = True

    def __init__(self, root, base_url: str | None, token: str = "", data_root: str | None = None,
                 *, dialback=None, image: str | None = None, cluster: str | None = None,
                 subnets: str | None = None, security_groups: str | None = None,
                 task_role: str | None = None, execution_role: str | None = None,
                 assign_public_ip: str | None = None, task_definition: str | None = None,
                 region: str | None = None, ecs_client=None, logs_client=None):
        from .dialback import RemoteDialback

        self.root = root
        self.base_url = base_url
        self.dialback = dialback or RemoteDialback(base_url)
        self.dialback.register(self._drain_pending)
        self.token = token
        self.data_root = data_root
        self.image = image or os.environ.get("DUCKSTRING_FARGATE_IMAGE")
        self.cluster = cluster or os.environ.get("DUCKSTRING_FARGATE_CLUSTER") or "default"
        self.subnets = _csv(subnets or os.environ.get("DUCKSTRING_FARGATE_SUBNETS"))
        self.security_groups = _csv(security_groups or os.environ.get("DUCKSTRING_FARGATE_SECURITY_GROUPS"))
        self.task_role = task_role or os.environ.get("DUCKSTRING_FARGATE_TASK_ROLE")
        self.execution_role = execution_role or os.environ.get("DUCKSTRING_FARGATE_EXECUTION_ROLE")
        self.assign_public_ip = (assign_public_ip or os.environ.get("DUCKSTRING_FARGATE_ASSIGN_PUBLIC_IP")
                                 or "ENABLED")
        # A pre-registered task-definition family/ARN skips registration; else we register one from the
        # image on first use. CPU arch (X86_64 default | ARM64 for a native Graviton/Apple-silicon image).
        self.task_definition = task_definition or os.environ.get("DUCKSTRING_FARGATE_TASK_DEF")
        self.cpu_arch = (os.environ.get("DUCKSTRING_FARGATE_CPU_ARCH") or "X86_64").upper()
        # Region drives the ecs client + the awslogs config (so a Duck's output is visible in CloudWatch).
        self.region = region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        self._client = ecs_client
        self._logs_client = logs_client  # injectable so the diagnostic path is testable
        self._registered: dict = {}   # config signature → the registered task-def family:revision
        self._tasks: dict[str, str] = {}   # pond key → task ARN
        self._pending: dict[str, tuple] = {}
        self._launch_errors: dict[str, str] = {}  # pond key → last spawn-failure reason (surfaced by the driver)

    @property
    def remote_base_url(self) -> str | None:
        return self.dialback.url

    def _ecs(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("ecs", **({"region_name": self.region} if self.region else {}))
        return self._client

    def _logs(self):
        if self._logs_client is None:
            import boto3

            self._logs_client = boto3.client("logs", region_name=self.region)
        return self._logs_client

    def reset_client(self) -> None:
        """Drop the cached boto3 clients so added/rotated credentials take effect on the next spawn."""
        self._client = None
        self._logs_client = None

    # ─── the launcher interface ───────────────────────────────────────────────────

    def set_base_url(self, url: str) -> None:
        self.base_url = url
        self.dialback.set_base_url(url)

    def _drain_pending(self) -> None:
        pending, self._pending = self._pending, {}
        for pond_key, (version, source_path, duck) in pending.items():
            self.ensure(pond_key, version, source_path, duck=duck)

    def is_running(self, pond_key: str) -> bool:
        return pond_key in self._pending or pond_key in self._tasks

    def _task_def(self, *, image, task_definition, execution_role, task_role, cpu_arch, region) -> str | None:
        """The task-definition to run for the effective config: an explicit one, else register one from the
        image (cached per (image, roles, arch) signature so distinct pools/images get distinct task-defs)."""
        if task_definition:
            return task_definition
        sig = (image, execution_role, task_role, cpu_arch)
        if sig in self._registered:
            return self._registered[sig]
        if not image:
            log.error("%s", _FARGATE_NO_IMAGE)
            return None
        container = {
            "name": _CONTAINER, "image": image,
            "entryPoint": ["python", "-m"],
            "logConfiguration": {"logDriver": "awslogs", "options": {
                "awslogs-group": "/duckstring/duck", "awslogs-region": region or "",
                "awslogs-stream-prefix": "duck", "awslogs-create-group": "true",
            }} if region else None,
        }
        container = {k: v for k, v in container.items() if v is not None}
        kwargs = {
            "family": _TASK_FAMILY, "networkMode": "awsvpc",
            "requiresCompatibilities": ["FARGATE"], "cpu": "1024", "memory": "4096",
            "runtimePlatform": {"cpuArchitecture": cpu_arch, "operatingSystemFamily": "LINUX"},
            "containerDefinitions": [container],
        }
        if execution_role:
            kwargs["executionRoleArn"] = execution_role
        if task_role:
            kwargs["taskRoleArn"] = task_role
        resp = self._ecs().register_task_definition(**kwargs)
        td = resp["taskDefinition"]
        tdid = f"{td['family']}:{td['revision']}"
        self._registered[sig] = tdid
        return tdid

    def ensure(self, pond_key: str, version: str, source_path: str, duck: dict | None = None) -> None:
        if self.remote_base_url is None:
            self.dialback.request()   # bring up the auto-relay (local Catchment); defer meanwhile
            self._pending[pond_key] = (version, source_path, duck)
            return
        if pond_key in self._tasks:
            return
        name, major = split_pond_key(pond_key)
        pool = (duck or {}).get("pool") or {}
        target = (duck or {}).get("duck_target")
        # The per-spawn deployment config (a dedicated Duck's, else the pool's) coalesced over the env
        # defaults the launcher holds (plans/compute-config-ui.md).
        dc = (duck or {}).get("deploy_config") or pool.get("deploy_config") or {}
        region = dc.get("region") or self.region
        task_def = self._task_def(
            image=dc.get("image") or self.image,
            task_definition=dc.get("task_definition") or self.task_definition,
            execution_role=dc.get("execution_role") or self.execution_role,
            task_role=dc.get("task_role") or self.task_role,
            cpu_arch=(dc.get("cpu_arch") or self.cpu_arch or "X86_64").upper(),
            region=region,
        )
        if task_def is None:
            self._launch_errors[pond_key] = _FARGATE_NO_IMAGE  # surfaced to the Pond failure via the driver
            return
        cluster = dc.get("cluster") or self.cluster
        subnets = _csv(dc.get("subnets")) or self.subnets
        security_groups = _csv(dc.get("security_groups")) or self.security_groups
        assign_public_ip = dc.get("assign_public_ip") or self.assign_public_ip
        cpu = str(pool.get("cpu") or dc.get("cpu") or os.environ.get("DUCKSTRING_FARGATE_CPU") or 1024)
        memory = str(pool.get("memory") or dc.get("memory") or os.environ.get("DUCKSTRING_FARGATE_MEMORY") or 4096)
        command = [
            "duckstring.duck", "--pond", name, "--major", str(major), "--version", version,
            "--catchment", self.remote_base_url, f"--token={self.token}",
            "--root", _REMOTE_ROOT, "--source-path", source_path, f"--data-root={self.data_root or ''}",
        ]
        kwargs = {
            "cluster": cluster, "launchType": "FARGATE", "taskDefinition": task_def, "count": 1,
            "overrides": {
                "cpu": cpu, "memory": memory,
                "containerOverrides": [{"name": _CONTAINER, "command": command}],
            },
            "networkConfiguration": {"awsvpcConfiguration": {
                "subnets": subnets, "securityGroups": security_groups,
                "assignPublicIp": assign_public_ip,
            }},
            "tags": [
                {"key": "duckstring:pond", "value": f"{name}@{major}"},
                {"key": "duckstring:target", "value": target or "pool"},
                {"key": "duckstring:pool", "value": target if target not in (None, "dedicated") else ""},
            ],
        }
        try:
            resp = self._ecs().run_task(**kwargs)
            if resp.get("failures"):
                log.error("fargate: run_task failed for %s: %s", pond_key, resp["failures"])
                self._launch_errors[pond_key] = f"fargate: run_task failed: {resp['failures']}"
                return
            task_arn = resp["tasks"][0]["taskArn"]
        except Exception as exc:
            log.exception("fargate: failed to run a Duck task for %s", pond_key)
            self._launch_errors[pond_key] = f"fargate: failed to run a Duck task: {exc}"
            return
        self._launch_errors.pop(pond_key, None)  # launched cleanly
        self._tasks[pond_key] = task_arn
        log.info("fargate: started %s for %s (%s/%s)", task_arn, pond_key, cpu, memory)

    def launch_error(self, pond_key: str) -> str | None:
        """The reason the last spawn attempt for ``pond_key`` failed (missing config / an AWS error), so
        the driver can attribute the Pond failure to it instead of a generic "process not running"."""
        return self._launch_errors.get(pond_key)

    def diagnose(self, pond_key: str) -> str | None:
        """Ask ECS what happened to this Duck's task — called by the liveness sweep when a launched Duck
        never dialled back (the silent branch), so the Pond failure carries the provider's real reason
        (``TaskFailedToStart: … not authorized to perform: logs:CreateLogGroup …``) instead of only
        "lost contact". Best-effort: any DescribeTasks problem → None (the generic message stands)."""
        task_arn = self._tasks.get(pond_key)
        if task_arn is None:
            return None
        try:
            resp = self._ecs().describe_tasks(cluster=self.cluster, tasks=[task_arn])
            task = (resp.get("tasks") or [None])[0]
            if task is None:
                return None
            parts = []
            if task.get("lastStatus"):
                parts.append(f"task {task['lastStatus']}")
            if task.get("stopCode"):
                parts.append(str(task["stopCode"]))
            if task.get("stoppedReason"):
                parts.append(str(task["stoppedReason"]))
            for c in task.get("containers") or []:
                if c.get("exitCode") is not None:
                    parts.append(f"container exit {c['exitCode']}")
                if c.get("reason"):
                    parts.append(str(c["reason"]))
            logs = self.log_tail(pond_key)
            if logs:
                parts.append(f"logs: {logs}")
            return "fargate: " + "; ".join(parts) if parts else None
        except Exception:
            log.debug("fargate: describe_tasks failed for %s", pond_key, exc_info=True)
            return None

    def log_tail(self, pond_key: str, lines: int = 12) -> str | None:
        """The tail of this Duck's CloudWatch stream. The task definition already routes container output
        there (awslogs), but nothing ever read it back — so an operator saw "container exit 1" and had to
        go to the console to learn why. The Duck logs to stderr, so this is its traceback."""
        task_arn = self._tasks.get(pond_key)
        if task_arn is None or not self.region:
            return None
        try:
            task_id = task_arn.rsplit("/", 1)[-1]
            # Over-fetch, then drop the routine chatter: an idle Duck's log is almost entirely job polls,
            # and a raw tail of it is all poll lines and no cause. Measured on a live Fargate failure —
            # twelve identical GET /jobs lines, which is exactly where the traceback should have been.
            events = self._logs().get_log_events(
                logGroupName="/duckstring/duck", logStreamName=f"duck/{_CONTAINER}/{task_id}",
                limit=max(lines * 10, 100), startFromHead=False,
            ).get("events") or []
            messages = [e["message"] for e in events]
            useful = [m for m in messages if not _NOISE(m)]
            # If it is ALL chatter, show the raw tail rather than nothing — silence reads as "no logs".
            return " | ".join((useful or messages)[-lines:]) or None
        except Exception:
            log.debug("fargate: log fetch failed for %s", pond_key, exc_info=True)
            return None

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        self._pending.pop(pond_key, None)
        self._launch_errors.pop(pond_key, None)
        task_arn = self._tasks.pop(pond_key, None)
        if task_arn is None:
            return
        try:
            self._ecs().stop_task(cluster=self.cluster, task=task_arn, reason="duckstring: pond idle")
            log.info("fargate: stopped %s (%s)", task_arn, pond_key)
        except Exception:
            log.exception("fargate: failed to stop %s for %s", task_arn, pond_key)

    def shutdown_all(self) -> None:
        for pond_key in list(self._tasks):
            self.terminate(pond_key)
        self._pending.clear()


def _csv(value: str | None) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()] if value else []
