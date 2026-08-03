"""Cloud config: the AWS_*-secret → environment wiring (the enable-by-secret teeth), the live
instance-type discovery endpoint, and the enable-verification probe. AWS is faked (no network)."""

from __future__ import annotations

import os
import sys
import types

import pytest
from fastapi.testclient import TestClient

from duckstring.catchment import auth, cloud
from duckstring.catchment.app import create_app
from duckstring.catchment.routes import catchment as croute
from duckstring.catchment.secrets import SecretStore

pytestmark = pytest.mark.timeout(10)


@pytest.fixture(autouse=True)
def _clean_aws_env():
    """Snapshot/restore AWS_* env and the per-region instance-type cache so tests don't leak into each
    other (load_aws_env / the secret route mutate the real process environment by design)."""
    saved = {k: v for k, v in os.environ.items() if k.startswith("AWS_")}
    for k in list(os.environ):
        if k.startswith("AWS_"):
            del os.environ[k]
    croute._INSTANCE_TYPES.clear()
    cloud._chain_has_credentials.cache_clear()
    yield
    for k in list(os.environ):
        if k.startswith("AWS_"):
            del os.environ[k]
    os.environ.update(saved)
    croute._INSTANCE_TYPES.clear()
    cloud._chain_has_credentials.cache_clear()


# ─── Fake boto3 (injected via sys.modules so `import boto3` in a route returns it) ──

class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kw):
        return list(self._pages)


class _Ec2:
    def __init__(self, pages):
        self._pages = pages

    def get_paginator(self, _name):
        return _Paginator(self._pages)


class _Sts:
    def __init__(self, ident, error):
        self._ident, self._error = ident, error

    def get_caller_identity(self):
        if self._error:
            raise self._error
        return self._ident


class _S3:
    def __init__(self, error):
        self._error = error

    def put_object(self, **_kw):
        if self._error:
            raise self._error

    def delete_object(self, **_kw):
        pass


def _make_boto3(*, ident=None, sts_error=None, ec2_pages=None, s3_error=None):
    def client(service, **_kw):
        if service == "ec2":
            return _Ec2(ec2_pages or [])
        if service == "sts":
            return _Sts(ident or {"Account": "123456789012", "Arn": "arn:aws:iam::123456789012:user/tester"}, sts_error)
        if service == "s3":
            return _S3(s3_error)
        raise ValueError(service)
    return types.SimpleNamespace(client=client)


_EC2_PAGES = [{"InstanceTypes": [
    {"InstanceType": "m6i.large", "VCpuInfo": {"DefaultVCpus": 2}, "MemoryInfo": {"SizeInMiB": 8192}},
    {"InstanceType": "c6i.large", "VCpuInfo": {"DefaultVCpus": 2}, "MemoryInfo": {"SizeInMiB": 4096}},
    {"InstanceType": "g5.xlarge", "VCpuInfo": {"DefaultVCpus": 4}, "MemoryInfo": {"SizeInMiB": 16384},
     "GpuInfo": {"Gpus": [{"Count": 1}]}},
]}]


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path)) as c:  # open mode → full access
        yield c


# ─── AWS_* secrets → environment ────────────────────────────────────────────────

def test_load_aws_env_injects_only_aws_prefixed(tmp_path):
    store = SecretStore(tmp_path)
    store.set("AWS_ACCESS_KEY_ID", "AKIA_TEST")
    store.set("PGPASS", "hunter2")
    cloud.load_aws_env(store)
    assert os.environ.get("AWS_ACCESS_KEY_ID") == "AKIA_TEST"
    assert "PGPASS" not in os.environ  # a non-AWS_ secret is never leaked into the environment


def test_load_aws_env_real_env_wins(tmp_path):
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    store = SecretStore(tmp_path)
    store.set("AWS_DEFAULT_REGION", "eu-west-1")
    cloud.load_aws_env(store)
    assert os.environ["AWS_DEFAULT_REGION"] == "us-east-1"  # setdefault: ambient env is authoritative


def test_setting_aws_secret_via_api_populates_env(client):
    assert client.post("/api/secrets", json={"name": "AWS_SESSION_TOKEN", "value": "tok"}).status_code == 200
    assert os.environ.get("AWS_SESSION_TOKEN") == "tok"  # applied live so boto3 uses it without a restart


# ─── Instance-type discovery ────────────────────────────────────────────────────

def test_instance_types_needs_a_region(client):
    body = client.get("/api/catchment/instance-types").json()
    assert body["available"] is False and body["types"] == []


def test_instance_types_lists_and_sorts(client, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", _make_boto3(ec2_pages=_EC2_PAGES))
    body = client.get("/api/catchment/instance-types?region=us-east-1").json()
    assert body["available"] is True and body["region"] == "us-east-1"
    # sorted by (vcpu, memory, name): the 2-vCPU/4-GiB comes first, the GPU box carries its gpu count.
    assert [t["name"] for t in body["types"]] == ["c6i.large", "m6i.large", "g5.xlarge"]
    g5 = next(t for t in body["types"] if t["name"] == "g5.xlarge")
    assert g5["gpu"] == 1 and g5["memory_gib"] == 16.0


def test_instance_types_graceful_on_aws_error(client, monkeypatch):
    def _boom(_service, **_kw):
        raise RuntimeError("Unable to locate credentials")
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=_boom))
    body = client.get("/api/catchment/instance-types?region=us-east-1").json()
    assert body["available"] is False and "credentials" in body["error"]


# ─── The enable-verification probe ──────────────────────────────────────────────

def test_verify_ok_with_bucket_probe(client, monkeypatch):
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    monkeypatch.setitem(sys.modules, "boto3", _make_boto3(ec2_pages=_EC2_PAGES))
    body = client.post("/api/catchment/cloud/verify", json={"data_root": "s3://bucket/prefix"}).json()
    assert body["ok"] is True
    assert body["account"] == "123456789012" and body["bucket_ok"] is True


def test_verify_reports_bad_credentials_as_200(client, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3",
                        _make_boto3(sts_error=RuntimeError("The security token is invalid")))
    res = client.post("/api/catchment/cloud/verify", json={})
    assert res.status_code == 200
    assert res.json()["ok"] is False


def test_verify_flags_unwritable_bucket(client, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3",
                        _make_boto3(s3_error=RuntimeError("AccessDenied")))
    body = client.post("/api/catchment/cloud/verify", json={"data_root": "s3://bucket/prefix"}).json()
    assert body["ok"] is True and body["bucket_ok"] is False and "AccessDenied" in body["bucket_error"]


# ─── Live credential validation ─────────────────────────────────────────────────

def test_validate_credentials_ok(monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", _make_boto3())
    assert cloud.validate_credentials() is None


def test_validate_credentials_reports_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", _make_boto3(sts_error=RuntimeError("ExpiredToken")))
    assert cloud.validate_credentials() == "ExpiredToken"


# ─── Credential-validity surface (the persistent-warning source) ─────────────────

def test_cloud_status_merges_cred_status():
    base = cloud.cloud_status("s3://b/p", None)
    assert base["creds_valid"] is None and base["creds_error"] is None
    merged = cloud.cloud_status("s3://b/p", None, {"valid": False, "error": "ExpiredToken"})
    assert merged["creds_valid"] is False and merged["creds_error"] == "ExpiredToken"


def test_refresh_credential_status_gate_off_clears(tmp_path):
    from duckstring.catchment.cloud_backends import refresh_credential_status
    st = types.SimpleNamespace(data_root=None, secret_store=SecretStore(tmp_path), cloud_creds={"valid": True})
    refresh_credential_status(st)
    assert st.cloud_creds is None  # gate off → no cred status (and no stale warning)


def test_refresh_credential_status_validates_when_enabled(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", _make_boto3())
    from duckstring.catchment.cloud_backends import refresh_credential_status
    store = SecretStore(tmp_path)
    store.set("AWS_ACCESS_KEY_ID", "AKIA")
    st = types.SimpleNamespace(data_root="s3://b/p", secret_store=store)
    refresh_credential_status(st, background=False)
    assert st.cloud_creds["valid"] is True


def test_refresh_credential_status_records_failure(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", _make_boto3(sts_error=RuntimeError("ExpiredToken")))
    from duckstring.catchment.cloud_backends import refresh_credential_status
    store = SecretStore(tmp_path)
    store.set("AWS_ACCESS_KEY_ID", "AKIA")
    st = types.SimpleNamespace(data_root="s3://b/p", secret_store=store)
    refresh_credential_status(st, background=False)
    assert st.cloud_creds["valid"] is False and st.cloud_creds["error"] == "ExpiredToken"


def test_verify_updates_the_cred_cache(client, monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", _make_boto3())
    client.post("/api/catchment/cloud/verify", json={})
    assert client.app.state.cloud_creds["valid"] is True


def test_settings_carries_creds_field(client):
    # The cloud gate (with creds validity) lives on the settings endpoint now, not the status poll.
    assert "cloud" not in client.get("/api/status").json()
    cloud_gate = client.get("/api/catchment/settings").json()
    assert "creds_valid" in cloud_gate and cloud_gate["creds_valid"] is None  # gate off in the open test app


# ─── Runtime rebuild of the remote backends ─────────────────────────────────────

def _dispatching_app(tmp_path, store, base_url="http://catchment.example:7474"):
    from duckstring.catchment.launcher import DispatchingLauncher, SubprocessLauncher
    disp = DispatchingLauncher(SubprocessLauncher(tmp_path, base_url), {})
    return types.SimpleNamespace(state=types.SimpleNamespace(
        launcher=disp, data_root="s3://bucket/prefix", secret_store=store,
        root=tmp_path, duck_token="tok", base_url=base_url))


def test_build_remote_backends_empty_without_gate(tmp_path):
    from duckstring.catchment.cloud_backends import build_remote_backends
    store = SecretStore(tmp_path)  # no creds, local data root
    assert build_remote_backends(tmp_path, "http://x:1", "tok", None, store) == ({}, None)


def test_refresh_attaches_remotes_at_runtime_then_resets(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_RELAY", "off")  # never provision a relay in a unit test
    from duckstring.catchment.cloud_backends import refresh_cloud_backends
    store = SecretStore(tmp_path)
    store.set("AWS_ACCESS_KEY_ID", "AKIA_TEST")  # flips aws_configured → gate enabled (s3 root below)
    app = _dispatching_app(tmp_path, store)

    # No remotes yet (cloud was "disabled" at construction) → refresh builds + attaches them live.
    assert app.state.launcher.remotes == {}
    assert refresh_cloud_backends(app) is True
    assert set(app.state.launcher.remotes) == {"fargate", "ec2"}

    # A second refresh (a credential rotation) drops the cached boto client so new creds take effect.
    app.state.launcher.remotes["fargate"]._client = "stale"
    assert refresh_cloud_backends(app) is True
    assert app.state.launcher.remotes["fargate"]._client is None


def test_dispatching_launcher_propagates_data_root(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_RELAY", "off")
    from duckstring.catchment.cloud_backends import build_remote_backends
    from duckstring.catchment.launcher import DispatchingLauncher, SubprocessLauncher
    store = SecretStore(tmp_path)
    store.set("AWS_ACCESS_KEY_ID", "AKIA")
    remotes, dialback = build_remote_backends(tmp_path, "http://x:1", "tok", "s3://old", store)
    disp = DispatchingLauncher(SubprocessLauncher(tmp_path, "http://x:1", data_root="s3://old"), remotes, dialback=dialback)

    disp.set_data_root("s3://new")
    # The switch must reach the actual backends (the local + every remote) — not just the dispatcher —
    # or a live data-root switch wouldn't change where Ducks publish.
    assert disp.local.data_root == "s3://new"
    assert all(r.data_root == "s3://new" for r in disp.remotes.values())


def test_cloud_status_provider_readiness(monkeypatch):
    for k in ("DUCKSTRING_FARGATE_IMAGE", "DUCKSTRING_FARGATE_SUBNETS", "DUCKSTRING_FARGATE_EXECUTION_ROLE",
              "DUCKSTRING_FARGATE_TASK_ROLE", "DUCKSTRING_EC2_AMI", "DUCKSTRING_EC2_INSTANCE_PROFILE"):
        monkeypatch.delenv(k, raising=False)

    class _Secrets:
        def names(self):
            return [{"name": "AWS_ACCESS_KEY_ID", "set_at": "t"}]

    st = cloud.cloud_status("s3://b/p", _Secrets())
    assert st["cloud_enabled"] is True and st["fargate_enabled"] is False and st["ec2_enabled"] is False
    # Configuring the Fargate env flips fargate_enabled (which gates the presets), not ec2_enabled.
    for k, v in {"DUCKSTRING_FARGATE_IMAGE": "i", "DUCKSTRING_FARGATE_SUBNETS": "s",
                 "DUCKSTRING_FARGATE_EXECUTION_ROLE": "e", "DUCKSTRING_FARGATE_TASK_ROLE": "t"}.items():
        monkeypatch.setenv(k, v)
    st = cloud.cloud_status("s3://b/p", _Secrets())
    assert st["fargate_enabled"] is True and st["ec2_enabled"] is False
    # Not cloud-enabled (local root) → neither provider is enabled regardless of env.
    assert cloud.cloud_status(None, _Secrets())["fargate_enabled"] is False


def test_cloud_deploy_missing_fields():
    from duckstring.catchment import cloud_deploy

    # An empty EC2 config (and no env) is missing the required launch fields.
    assert set(cloud_deploy.missing_fields("ec2", {})) == {"ami", "instance_profile"}
    assert cloud_deploy.missing_fields("ec2", {"ami": "ami-1", "instance_profile": "p"}) == []
    # Fargate needs an image (or task-def) + subnets + both roles.
    assert "image" in cloud_deploy.missing_fields("fargate", {"subnets": "s", "execution_role": "e", "task_role": "t"})
    assert cloud_deploy.missing_fields("fargate",
                                       {"image": "i", "subnets": "s", "execution_role": "e", "task_role": "t"}) == []


def test_cloud_deploy_effective_coalesces_over_env(monkeypatch):
    from duckstring.catchment import cloud_deploy

    monkeypatch.setenv("DUCKSTRING_EC2_AMI", "ami-env")
    monkeypatch.setenv("DUCKSTRING_EC2_INSTANCE_PROFILE", "prof-env")
    # With env supplying the required fields, an empty blob is adequate; the blob overrides the env.
    assert cloud_deploy.missing_fields("ec2", None) == []
    assert cloud_deploy.effective("ec2", {"ami": "ami-ui"})["ami"] == "ami-ui"


def test_add_pool_rejects_and_accepts_by_deploy_config(tmp_path, monkeypatch):
    for k in ("DUCKSTRING_EC2_AMI", "DUCKSTRING_EC2_INSTANCE_PROFILE"):
        monkeypatch.delenv(k, raising=False)
    from duckstring.catchment.db import connect, migrate
    from duckstring.catchment.driver import Driver
    from duckstring.catchment.launcher import NoopLauncher

    db = connect(tmp_path / "duck.db")
    migrate(db)
    driver = Driver(db, tmp_path, "http://x", NoopLauncher())
    # A PARTIAL deploy config (ami, no instance_profile; nor env) is rejected.
    with pytest.raises(ValueError, match="instance_profile"):
        driver.add_pool("heavy", provider="ec2", instance_type="m6i.large", deploy_config={"ami": "ami-1"})
    # No blob supplied → validation deferred (the CLI / env path).
    driver.add_pool("nocfg", provider="ec2", instance_type="m6i.large")
    # A complete blob is accepted and round-trips.
    pool = driver.add_pool("heavy", provider="ec2", instance_type="m6i.large",
                           deploy_config={"ami": "ami-1", "instance_profile": "prof"})
    assert pool["deploy_config"] == {"ami": "ami-1", "instance_profile": "prof"}


def test_refresh_is_a_noop_for_a_non_dispatching_launcher(tmp_path):
    from duckstring.catchment.cloud_backends import refresh_cloud_backends
    from duckstring.catchment.launcher import NoopLauncher
    store = SecretStore(tmp_path)
    store.set("AWS_ACCESS_KEY_ID", "AKIA_TEST")
    app = types.SimpleNamespace(state=types.SimpleNamespace(
        launcher=NoopLauncher(), data_root="s3://bucket/prefix", secret_store=store,
        root=tmp_path, duck_token="tok", base_url="http://x:1"))
    assert refresh_cloud_backends(app) is False


# ─── Access gating ──────────────────────────────────────────────────────────────

def test_aws_routes_are_full_gated(tmp_path, monkeypatch):
    # Fake boto3 so the authorised call is deterministic + offline (a machine with an ambient AWS
    # profile would otherwise make a real STS call).
    monkeypatch.setitem(sys.modules, "boto3", _make_boto3())
    app = create_app(tmp_path)
    keys = auth.generate(app.state.db)
    with TestClient(app) as c:
        for level in ("read", "demand"):
            h = {"Authorization": f"Bearer {keys[level]}"}
            assert c.get("/api/catchment/instance-types?region=us-east-1", headers=h).status_code == 403
            assert c.post("/api/catchment/cloud/verify", headers=h, json={}).status_code == 403
        full = {"Authorization": f"Bearer {keys['full']}"}
        assert c.post("/api/catchment/cloud/verify", headers=full, json={}).status_code == 200
