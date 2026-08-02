"""Cloud config increment 3b (plans/cloud-config.md): the dispatching launcher (route each Pond by
duck_target: catchment→local box, pool/dedicated→EC2) and the EC2 backend logic (faked boto3 client —
no AWS in the suite; the real-metal path is the cloud e2e's)."""

from __future__ import annotations

import base64

import pytest

from duckstring.catchment.ec2_launcher import Ec2Launcher
from duckstring.catchment.launcher import DispatchingLauncher

pytestmark = pytest.mark.timeout(5)


@pytest.fixture(autouse=True)
def _clean_ec2_env(monkeypatch):
    """The launcher reads its defaults from the environment — clear them so a developer's shell can't
    change what these tests assert."""
    for k in ("DUCKSTRING_EC2_AMI", "DUCKSTRING_EC2_INSTANCE_PROFILE", "DUCKSTRING_EC2_PIP_SPEC",
              "DUCKSTRING_EC2_SUBNET", "DUCKSTRING_EC2_SECURITY_GROUPS",
              "DUCKSTRING_EC2_ASSIGN_PUBLIC_IP", "DUCKSTRING_EC2_INSTANCE_TYPE"):
        monkeypatch.delenv(k, raising=False)


class RecordingBackend:
    """A launcher backend that records calls — the dispatcher's routing target in tests."""

    def __init__(self, name):
        self.name = name
        self.base_url = "http://x"
        self.ensured, self.terminated = [], []
        self._live = set()

    def set_base_url(self, url):
        self.base_url = url

    def is_running(self, key):
        return key in self._live

    def ensure(self, key, version, source_path, duck=None):
        self.ensured.append((key, duck))
        self._live.add(key)

    def terminate(self, key, wait=False):
        self.terminated.append(key)
        self._live.discard(key)

    def shutdown_all(self):
        self._live.clear()


def _duck(target, *, pool=None, instance_type=None):
    return {"duck_target": target, "remote": target != "catchment", "pool": pool,
            "dedicated_instance_type": instance_type}


# ─── the dispatcher ─────────────────────────────────────────────────────────────


def test_catchment_target_routes_local():
    local, remote = RecordingBackend("local"), RecordingBackend("remote")
    d = DispatchingLauncher(local, remote)
    d.ensure("a@1", "1", "ponds/a/1", duck=_duck("catchment"))
    assert local.ensured and not remote.ensured
    assert d.is_running("a@1")


def test_pool_target_routes_remote():
    local, remote = RecordingBackend("local"), RecordingBackend("remote")
    d = DispatchingLauncher(local, remote)
    d.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool={"instance_type": "m6i.large"}))
    assert remote.ensured and not local.ensured


def test_remote_target_degrades_to_local_without_backend():
    local = RecordingBackend("local")
    d = DispatchingLauncher(local, None)  # cloud not enabled
    d.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool={"instance_type": "m6i.large"}))
    assert local.ensured  # a `duck = "heavy"` pond.toml still runs anywhere


def test_terminate_and_target_change_route_to_owner():
    local, remote = RecordingBackend("local"), RecordingBackend("remote")
    d = DispatchingLauncher(local, remote)
    d.ensure("a@1", "1", "ponds/a/1", duck=_duck("catchment"))
    # Re-ensure with a changed target tears down the stale (local) Duck, then spawns on remote.
    d.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool={"instance_type": "m6i.large"}))
    assert local.terminated == ["a@1"] and remote.ensured
    d.terminate("a@1")
    assert remote.terminated == ["a@1"]


def test_set_base_url_forwards_to_both():
    local, remote = RecordingBackend("local"), RecordingBackend("remote")
    DispatchingLauncher(local, remote).set_base_url("http://cat:7474")
    assert local.base_url == remote.base_url == "http://cat:7474"


# ─── the EC2 backend ────────────────────────────────────────────────────────────


class FakeEc2:
    def __init__(self):
        self.launched, self.terminated, self._n = [], [], 0

    def run_instances(self, **kw):
        self._n += 1
        iid = f"i-{self._n}"
        self.launched.append((iid, kw))
        return {"Instances": [{"InstanceId": iid}]}

    def terminate_instances(self, InstanceIds):
        self.terminated.extend(InstanceIds)
        return {}


def _launcher(tmp_path, client, **kw):
    return Ec2Launcher(tmp_path, "http://cat:7474", token="tok", data_root="s3://bucket/data",
                       ami="ami-123", ec2_client=client, **kw)


# ─── placement: the subnet + security groups a Duck needs to reach the Catchment ────


def test_placement_from_env_rides_the_launch(tmp_path, monkeypatch):
    """Without these, AWS puts the instance in the default VPC's DEFAULT security group, which normally
    cannot reach the Catchment — the Duck boots and is then failed as silent, pointing nowhere near the
    real cause. Found on the live gate box, where an EC2 pool could not have worked at all."""
    monkeypatch.setenv("DUCKSTRING_EC2_SUBNET", "subnet-abc")
    monkeypatch.setenv("DUCKSTRING_EC2_SECURITY_GROUPS", "sg-1, sg-2")
    ec2 = FakeEc2()
    lch = _launcher(tmp_path, ec2, instance_profile="ds-worker")
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy"))
    _iid, kw = ec2.launched[0]
    assert kw["SubnetId"] == "subnet-abc"
    assert kw["SecurityGroupIds"] == ["sg-1", "sg-2"]
    assert "NetworkInterfaces" not in kw  # the flat form while no public-IP override is asked for


def test_pool_deploy_config_overrides_env_placement(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKSTRING_EC2_SUBNET", "subnet-env")
    monkeypatch.setenv("DUCKSTRING_EC2_SECURITY_GROUPS", "sg-env")
    ec2 = FakeEc2()
    lch = _launcher(tmp_path, ec2, instance_profile="ds-worker")
    pool = {"instance_type": "m6i.large", "provider": "ec2",
            "deploy_config": {"subnet": "subnet-pool", "security_groups": "sg-pool"}}
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool=pool))
    _iid, kw = ec2.launched[0]
    assert kw["SubnetId"] == "subnet-pool" and kw["SecurityGroupIds"] == ["sg-pool"]


def test_public_ip_override_uses_the_network_interface_form(tmp_path, monkeypatch):
    """RunInstances only accepts a public-IP override inside a NetworkInterface spec, and then the subnet
    and groups must move in there too (mixing the flat form with it is an API error)."""
    monkeypatch.setenv("DUCKSTRING_EC2_SUBNET", "subnet-abc")
    monkeypatch.setenv("DUCKSTRING_EC2_SECURITY_GROUPS", "sg-1")
    monkeypatch.setenv("DUCKSTRING_EC2_ASSIGN_PUBLIC_IP", "ENABLED")
    ec2 = FakeEc2()
    lch = _launcher(tmp_path, ec2, instance_profile="ds-worker")
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy"))
    _iid, kw = ec2.launched[0]
    assert kw["NetworkInterfaces"] == [
        {"DeviceIndex": 0, "AssociatePublicIpAddress": True, "SubnetId": "subnet-abc", "Groups": ["sg-1"]}
    ]
    assert "SubnetId" not in kw and "SecurityGroupIds" not in kw


def test_no_placement_config_still_launches_and_warns(tmp_path, caplog):
    """Back-compat: an account whose default SG happens to allow the Catchment port keeps working — but
    the silence is what cost an afternoon, so it warns."""
    ec2 = FakeEc2()
    lch = _launcher(tmp_path, ec2, instance_profile="ds-worker")
    with caplog.at_level("WARNING", logger="duckstring.ec2"):
        lch.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy"))
        lch.ensure("b@1", "1", "ponds/b/1", duck=_duck("heavy"))
        warned = sum("no security group" in r.getMessage() for r in caplog.records)
    _iid, kw = ec2.launched[0]
    assert "SubnetId" not in kw and "SecurityGroupIds" not in kw and "NetworkInterfaces" not in kw
    assert warned == 1, "warn once, not once per spawn"


def test_pool_agent_launch_gets_the_same_placement(tmp_path, monkeypatch):
    """The Pool agent dials back exactly like a Duck, so it needs the same network placement."""
    monkeypatch.setenv("DUCKSTRING_EC2_SUBNET", "subnet-abc")
    monkeypatch.setenv("DUCKSTRING_EC2_SECURITY_GROUPS", "sg-1")
    ec2 = FakeEc2()
    lch = _launcher(tmp_path, ec2, instance_profile="ds-worker")
    lch.start_pool_instance("devpool", {"instance_type": "m6i.large"}, "duckstring.duck.pool_agent --x 1")
    _iid, kw = ec2.launched[0]
    assert kw["SubnetId"] == "subnet-abc" and kw["SecurityGroupIds"] == ["sg-1"]


def test_pool_deploy_config_supplies_ami_and_profile(tmp_path):
    # No env AMI/profile on the launcher — the pool's deploy_config supplies them (plans/compute-config-ui).
    ec2 = FakeEc2()
    lch = Ec2Launcher(tmp_path, "http://cat:7474", token="t", data_root="s3://b/d", ec2_client=ec2)
    pool = {"instance_type": "m6i.large", "provider": "ec2",
            "deploy_config": {"ami": "ami-xyz", "instance_profile": "prof"}}
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool=pool))
    assert len(ec2.launched) == 1
    _iid, kw = ec2.launched[0]
    assert kw["ImageId"] == "ami-xyz" and kw["IamInstanceProfile"] == {"Name": "prof"}


def test_ensure_launches_with_pool_instance_type_and_tags(tmp_path):
    ec2 = FakeEc2()
    lch = _launcher(tmp_path, ec2, instance_profile="ds-worker")
    lch.ensure("sales@2", "1.2.0", "ponds/sales/1.2.0",
               duck=_duck("heavy", pool={"instance_type": "c6i.2xlarge"}))
    assert len(ec2.launched) == 1
    iid, kw = ec2.launched[0]
    assert kw["InstanceType"] == "c6i.2xlarge" and kw["ImageId"] == "ami-123"
    assert kw["IamInstanceProfile"] == {"Name": "ds-worker"}  # worker IAM, not the Catchment's keys
    tags = {t["Key"]: t["Value"] for t in kw["TagSpecifications"][0]["Tags"]}
    assert tags["duckstring:pond"] == "sales@2" and tags["duckstring:pool"] == "heavy"
    # The userdata boots the Duck against the reachable Catchment URL + the S3 data root.
    ud = base64.b64decode(kw["UserData"]).decode()
    assert "duckstring.duck" in ud and "--catchment http://cat:7474" in ud
    assert "--data-root=s3://bucket/data" in ud and "--pond sales" in ud
    assert lch.is_running("sales@2")


def test_dedicated_uses_its_own_instance_type(tmp_path):
    ec2 = FakeEc2()
    lch = _launcher(tmp_path, ec2)
    lch.ensure("big@1", "1.0.0", "ponds/big/1.0.0",
               duck=_duck("dedicated", instance_type="r6i.4xlarge"))
    assert ec2.launched[0][1]["InstanceType"] == "r6i.4xlarge"


def test_terminate_kills_the_instance(tmp_path):
    ec2 = FakeEc2()
    lch = _launcher(tmp_path, ec2)
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool={"instance_type": "m6i.large"}))
    iid = ec2.launched[0][0]
    lch.terminate("a@1")
    assert ec2.terminated == [iid] and not lch.is_running("a@1")


def test_spawn_defers_until_reachable_url(tmp_path):
    ec2 = FakeEc2()
    lch = Ec2Launcher(tmp_path, None, token="t", data_root="s3://b/d", ami="ami-1", ec2_client=ec2)
    assert lch.remote_base_url is None  # a local/unknown bind with no relay → deferred
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool={"instance_type": "m6i.large"}))
    assert not ec2.launched and lch.is_running("a@1")  # pending → owned, so liveness won't fail it
    lch.set_base_url("http://cat:7474")
    assert len(ec2.launched) == 1


def test_missing_ami_does_not_launch(tmp_path):
    ec2 = FakeEc2()
    lch = Ec2Launcher(tmp_path, "http://cat:7474", token="t", data_root="s3://b/d", ec2_client=ec2)
    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool={"instance_type": "m6i.large"}))
    assert not ec2.launched
    # The spawn-failure reason is recorded so the driver can attribute the Pond failure to it (not a
    # generic crash), and it reaches through the dispatcher.
    assert "DUCKSTRING_EC2_AMI" in (lch.launch_error("a@1") or "")
    disp = DispatchingLauncher(RecordingBackend("local"), {"_any": lch})
    disp.ensure("a@1", "1", "ponds/a/1", duck=_duck("heavy", pool={"instance_type": "m6i.large"}))
    assert "DUCKSTRING_EC2_AMI" in (disp.launch_error("a@1") or "")
