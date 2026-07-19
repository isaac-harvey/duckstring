"""The Fargate Duck backend (plans/cloud-config.md §4b) — the default remote launcher. Logic tested
with a faked ECS client (no AWS in the suite; the real cluster is the cloud e2e). Provider routing
through the dispatcher is covered here too (fargate vs ec2 pools)."""

from __future__ import annotations

import pytest

from duckstring.catchment.dialback import RemoteDialback
from duckstring.catchment.fargate_launcher import FargateLauncher
from duckstring.catchment.launcher import DispatchingLauncher

pytestmark = pytest.mark.timeout(5)


@pytest.fixture(autouse=True)
def _clean_fargate_env(monkeypatch):
    for k in ("AWS_REGION", "AWS_DEFAULT_REGION", "DUCKSTRING_FARGATE_CPU_ARCH",
              "DUCKSTRING_FARGATE_CPU", "DUCKSTRING_FARGATE_MEMORY"):
        monkeypatch.delenv(k, raising=False)


class FakeEcs:
    def __init__(self):
        self.registered, self.ran, self.stopped = [], [], []
        self._n = 0

    def register_task_definition(self, **kw):
        self.registered.append(kw)
        return {"taskDefinition": {"family": kw["family"], "revision": 3}}

    def run_task(self, **kw):
        self._n += 1
        self.ran.append(kw)
        return {"tasks": [{"taskArn": f"arn:task/{self._n}"}], "failures": []}

    def stop_task(self, **kw):
        self.stopped.append(kw)
        return {}


def _fargate(ecs, **kw):
    defaults = dict(token="tok", data_root="s3://bucket/data", image="ghcr.io/x/duckstring:latest",
                    cluster="ducks", subnets="subnet-1,subnet-2", security_groups="sg-1",
                    task_role="arn:role/task", execution_role="arn:role/exec", ecs_client=ecs)
    defaults.update(kw)
    return FargateLauncher("/root", "http://cat:7474", **defaults)


def _duck(target="M", *, provider="fargate", cpu=1024, memory=4096):
    return {"duck_target": target, "remote": True,
            "pool": {"provider": provider, "cpu": cpu, "memory": memory}}


# ─── the Fargate launcher ────────────────────────────────────────────────────────


def test_ensure_registers_task_def_and_runs_task(tmp_path):
    ecs = FakeEcs()
    lch = _fargate(ecs)
    lch.ensure("sales@2", "1.2.0", "ponds/sales/1.2.0", duck=_duck("M", cpu=1024, memory=4096))
    # Registers one task def from the image (with the roles), then runs the task.
    assert len(ecs.registered) == 1
    td = ecs.registered[0]
    assert td["containerDefinitions"][0]["image"] == "ghcr.io/x/duckstring:latest"
    assert td["executionRoleArn"] == "arn:role/exec" and td["taskRoleArn"] == "arn:role/task"
    assert len(ecs.ran) == 1
    run = ecs.ran[0]
    assert run["cluster"] == "ducks" and run["launchType"] == "FARGATE" and run["taskDefinition"] == "duckstring-duck:3"
    assert run["overrides"]["cpu"] == "1024" and run["overrides"]["memory"] == "4096"
    net = run["networkConfiguration"]["awsvpcConfiguration"]
    assert net["subnets"] == ["subnet-1", "subnet-2"] and net["securityGroups"] == ["sg-1"]
    # The container command IS the Duck args, dialing the (reachable) Catchment + the S3 data root.
    cmd = run["overrides"]["containerOverrides"][0]["command"]
    assert cmd[:2] == ["duckstring.duck", "--pond"] and "sales" in cmd
    assert "--catchment" in cmd and "http://cat:7474" in cmd
    assert "--data-root=s3://bucket/data" in cmd
    tags = {t["key"]: t["value"] for t in run["tags"]}
    assert tags["duckstring:pond"] == "sales@2" and tags["duckstring:pool"] == "M"
    assert lch.is_running("sales@2")


def test_task_def_carries_arch_and_region_logs(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_REGION", "ap-southeast-2")
    monkeypatch.setenv("DUCKSTRING_FARGATE_CPU_ARCH", "arm64")
    ecs = FakeEcs()
    _fargate(ecs).ensure("a@1", "1", "ponds/a/1", duck=_duck())
    td = ecs.registered[0]
    assert td["runtimePlatform"] == {"cpuArchitecture": "ARM64", "operatingSystemFamily": "LINUX"}
    # A region → CloudWatch logs wired (so the Duck's output is visible), region-scoped.
    log = td["containerDefinitions"][0]["logConfiguration"]
    assert log["logDriver"] == "awslogs" and log["options"]["awslogs-region"] == "ap-southeast-2"


def test_second_spawn_reuses_the_registered_task_def(tmp_path):
    ecs = FakeEcs()
    lch = _fargate(ecs)
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck())
    lch.ensure("b@1", "1", "ponds/b/1", duck=_duck())
    assert len(ecs.registered) == 1 and len(ecs.ran) == 2  # register once, run per Pond


def test_explicit_task_definition_skips_registration(tmp_path):
    ecs = FakeEcs()
    lch = _fargate(ecs, task_definition="my-duck:7", image=None)
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck())
    assert ecs.registered == [] and ecs.ran[0]["taskDefinition"] == "my-duck:7"


def test_terminate_stops_the_task(tmp_path):
    ecs = FakeEcs()
    lch = _fargate(ecs)
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck())
    lch.terminate("a@1")
    assert ecs.stopped[0]["task"] == "arn:task/1" and not lch.is_running("a@1")


def test_defers_until_dialback_resolves(tmp_path):
    ecs = FakeEcs()
    dialback = RemoteDialback(None)  # unknown bind, no relay → URL resolves via set_base_url
    lch = _fargate(ecs, dialback=dialback)
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck())
    assert not ecs.ran and lch.is_running("a@1")  # pending
    lch.set_base_url("http://cat:7474")
    assert len(ecs.ran) == 1


# ─── provider routing through the dispatcher ─────────────────────────────────────


class Recorder:
    def __init__(self):
        self.ensured = []
        self.base_url = None

    def set_base_url(self, url):
        self.base_url = url

    def is_running(self, k):
        return False

    def ensure(self, k, v, s, duck=None):
        self.ensured.append(k)

    def terminate(self, k, wait=False):
        pass

    def shutdown_all(self):
        pass


def test_dispatcher_routes_by_pool_provider():
    local, fargate, ec2 = Recorder(), Recorder(), Recorder()
    d = DispatchingLauncher(local, {"fargate": fargate, "ec2": ec2}, default_provider="fargate")
    d.ensure("a@1", "1", "p/a", duck=_duck("M", provider="fargate"))
    d.ensure("b@1", "1", "p/b", duck=_duck("huge", provider="ec2"))
    d.ensure("c@1", "1", "p/c", duck={"duck_target": "catchment", "remote": False})
    # dedicated (no pool provider) → the default provider (fargate).
    d.ensure("e@1", "1", "p/e", duck={"duck_target": "dedicated", "remote": True, "pool": None})
    assert fargate.ensured == ["a@1", "e@1"] and ec2.ensured == ["b@1"] and local.ensured == ["c@1"]
