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
_TASK_FAMILY = "duckstring-duck"


class FargateLauncher:
    manages_processes = True

    def __init__(self, root, base_url: str | None, token: str = "", data_root: str | None = None,
                 *, dialback=None, image: str | None = None, cluster: str | None = None,
                 subnets: str | None = None, security_groups: str | None = None,
                 task_role: str | None = None, execution_role: str | None = None,
                 assign_public_ip: str | None = None, task_definition: str | None = None,
                 region: str | None = None, ecs_client=None):
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
        # image on first use.
        self.task_definition = task_definition or os.environ.get("DUCKSTRING_FARGATE_TASK_DEF")
        self.region = region
        self._client = ecs_client
        self._registered = None   # the task-def family:revision once ensured
        self._tasks: dict[str, str] = {}   # pond key → task ARN
        self._pending: dict[str, tuple] = {}

    @property
    def remote_base_url(self) -> str | None:
        return self.dialback.url

    def _ecs(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("ecs", **({"region_name": self.region} if self.region else {}))
        return self._client

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

    def _task_def(self) -> str | None:
        """The task-definition to run: an explicit one, else register one from the image (once)."""
        if self.task_definition:
            return self.task_definition
        if self._registered:
            return self._registered
        if not self.image:
            log.error("fargate: no image (DUCKSTRING_FARGATE_IMAGE) or task definition — cannot launch")
            return None
        container = {
            "name": _CONTAINER, "image": self.image,
            "entryPoint": ["python", "-m"],
            "logConfiguration": {"logDriver": "awslogs", "options": {
                "awslogs-group": "/duckstring/duck", "awslogs-region": self.region or "",
                "awslogs-stream-prefix": "duck", "awslogs-create-group": "true",
            }} if self.region else None,
        }
        container = {k: v for k, v in container.items() if v is not None}
        kwargs = {
            "family": _TASK_FAMILY, "networkMode": "awsvpc",
            "requiresCompatibilities": ["FARGATE"], "cpu": "1024", "memory": "4096",
            "containerDefinitions": [container],
        }
        if self.execution_role:
            kwargs["executionRoleArn"] = self.execution_role
        if self.task_role:
            kwargs["taskRoleArn"] = self.task_role
        resp = self._ecs().register_task_definition(**kwargs)
        td = resp["taskDefinition"]
        self._registered = f"{td['family']}:{td['revision']}"
        return self._registered

    def ensure(self, pond_key: str, version: str, source_path: str, duck: dict | None = None) -> None:
        if self.remote_base_url is None:
            self.dialback.request()   # bring up the auto-relay (local Catchment); defer meanwhile
            self._pending[pond_key] = (version, source_path, duck)
            return
        if pond_key in self._tasks:
            return
        task_def = self._task_def()
        if task_def is None:
            return
        name, major = split_pond_key(pond_key)
        pool = (duck or {}).get("pool") or {}
        target = (duck or {}).get("duck_target")
        cpu = str(pool.get("cpu") or os.environ.get("DUCKSTRING_FARGATE_CPU") or 1024)
        memory = str(pool.get("memory") or os.environ.get("DUCKSTRING_FARGATE_MEMORY") or 4096)
        command = [
            "duckstring.duck", "--pond", name, "--major", str(major), "--version", version,
            "--catchment", self.remote_base_url, f"--token={self.token}",
            "--root", _REMOTE_ROOT, "--source-path", source_path, f"--data-root={self.data_root or ''}",
        ]
        kwargs = {
            "cluster": self.cluster, "launchType": "FARGATE", "taskDefinition": task_def, "count": 1,
            "overrides": {
                "cpu": cpu, "memory": memory,
                "containerOverrides": [{"name": _CONTAINER, "command": command}],
            },
            "networkConfiguration": {"awsvpcConfiguration": {
                "subnets": self.subnets, "securityGroups": self.security_groups,
                "assignPublicIp": self.assign_public_ip,
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
                return
            task_arn = resp["tasks"][0]["taskArn"]
        except Exception:
            log.exception("fargate: failed to run a Duck task for %s", pond_key)
            return
        self._tasks[pond_key] = task_arn
        log.info("fargate: started %s for %s (%s/%s)", task_arn, pond_key, cpu, memory)

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        self._pending.pop(pond_key, None)
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
