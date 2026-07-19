"""The auto-relay (plans/cloud-config.md) — cloud Ducks from a local Catchment behind NAT. The
reachability logic, the RelayManager (provision + reverse tunnel + teardown, faked boto3 + ssh), and
its wiring into the EC2 launcher's pending/drain mechanism. The real ssh/EC2 path is the cloud e2e's."""

from __future__ import annotations

import pytest

from duckstring.catchment.ec2_launcher import Ec2Launcher
from duckstring.catchment.relay import RelayManager, needs_relay

pytestmark = pytest.mark.timeout(5)


# ─── reachability ────────────────────────────────────────────────────────────────


def test_needs_relay_for_local_and_private_binds():
    assert needs_relay("http://127.0.0.1:7474")
    assert needs_relay("http://localhost:7474")
    assert needs_relay("http://0.0.0.0:7474")
    assert needs_relay("http://192.168.1.20:7474")
    assert needs_relay("http://10.0.5.5:7474")
    assert needs_relay("http://172.16.3.4:7474")
    assert needs_relay(None)  # unknown bind → assume local
    # A public address dials back fine — no relay.
    assert not needs_relay("http://catchment.example.com:7474")
    assert not needs_relay("http://54.12.34.56:7474")
    assert not needs_relay("http://172.15.0.1:7474")  # just outside the 172.16–31 private range


# ─── the RelayManager ────────────────────────────────────────────────────────────


class FakeEc2:
    def __init__(self, ip="203.0.113.9"):
        self.ip, self.launched, self.terminated = ip, [], []

    def run_instances(self, **kw):
        self.launched.append(kw)
        return {"Instances": [{"InstanceId": "i-relay"}]}

    def describe_instances(self, InstanceIds):
        return {"Reservations": [{"Instances": [{"State": {"Name": "running"}, "PublicIpAddress": self.ip}]}]}

    def terminate_instances(self, InstanceIds):
        self.terminated.extend(InstanceIds)
        return {}


class FakeTunnel:
    def __init__(self):
        self.terminated = False

    def terminate(self):
        self.terminated = True


def _manager(ec2, ssh_calls, **kw):
    def runner(host, **rk):
        ssh_calls.append((host, rk))
        return FakeTunnel()

    defaults = dict(bind_url="http://127.0.0.1:7474", ami="ami-1", key_name="ds-key",
                    ssh_key="/keys/ds.pem", security_group="ds-relay-sg", ec2_client=ec2, ssh_runner=runner)
    defaults.update(kw)
    return RelayManager(**defaults)


def test_configured_requires_ami_key_and_sg():
    assert _manager(FakeEc2(), []).configured()
    assert not RelayManager(bind_url="http://127.0.0.1:7474", ami="ami-1").configured()  # no key/sg


def test_start_provisions_relay_and_opens_reverse_tunnel(monkeypatch):
    monkeypatch.delenv("DUCKSTRING_RELAY_TTL_MINUTES", raising=False)
    ec2, ssh = FakeEc2(ip="203.0.113.9"), []
    mgr = _manager(ec2, ssh)
    url = mgr.start()
    assert url == "http://203.0.113.9:7474" and mgr.public_url == url
    # Provisioned with the SSH key + security group + a tiny box + the GatewayPorts/TTL userdata.
    import base64
    kw = ec2.launched[0]
    assert kw["KeyName"] == "ds-key" and kw["SecurityGroups"] == ["ds-relay-sg"]
    assert kw["InstanceType"] == "t4g.nano"
    tags = {t["Key"]: t["Value"] for t in kw["TagSpecifications"][0]["Tags"]}
    assert tags["duckstring:role"] == "relay"
    ud = base64.b64decode(kw["UserData"]).decode()
    assert "GatewayPorts" in ud and "terminate-instances" in ud  # the TTL watchdog self-terminates
    # The outbound reverse tunnel forwards the relay's port back to the laptop's Catchment.
    host, rk = ssh[0]
    assert host == "203.0.113.9" and rk["relay_port"] == 7474 and rk["local_port"] == 7474
    assert rk["ssh_key"] == "/keys/ds.pem"


def test_start_is_idempotent():
    ec2 = FakeEc2()
    mgr = _manager(ec2, [])
    mgr.start()
    mgr.start()
    assert len(ec2.launched) == 1  # a second call returns the running relay, doesn't relaunch


def test_stop_tears_down_tunnel_and_instance():
    ec2, ssh = FakeEc2(), []
    mgr = _manager(ec2, ssh)
    mgr.start()
    tunnel = mgr._tunnel
    mgr.stop()
    assert tunnel.terminated and ec2.terminated == ["i-relay"]
    assert mgr.public_url is None


# ─── launcher wiring (the pending/drain path) ────────────────────────────────────


class FakeRelay:
    def __init__(self):
        self.cb = None
        self.stopped = False

    def configured(self):
        return True

    def start_async(self, on_ready):
        self.cb = on_ready  # captured, not auto-fired — the test drives readiness

    def stop(self):
        self.stopped = True


def _duck(target="heavy"):
    return {"duck_target": target, "remote": True, "pool": {"instance_type": "m6i.large"}}


def test_remote_spawn_defers_on_relay_then_drains_when_ready(tmp_path):
    ec2 = FakeEc2()
    relay = FakeRelay()
    # base_url is a local bind; with a relay, remote_base_url stays None until the relay is up.
    lch = Ec2Launcher(tmp_path, "http://127.0.0.1:7474", token="t", data_root="s3://b/d",
                      ami="ami-1", ec2_client=ec2, relay=relay)
    assert lch.remote_base_url is None

    lch.ensure("a@1", "1", "ponds/a/1", duck=_duck())
    assert relay.cb is not None          # the relay was kicked off
    assert not ec2.launched              # the spawn deferred (pending)
    assert lch.is_running("a@1")         # pending → owned, so liveness won't fail it

    relay.cb("http://203.0.113.9:7474")  # the tunnel came up → drain
    assert lch.remote_base_url == "http://203.0.113.9:7474"
    assert len(ec2.launched) == 1
    ud_ok = "http://203.0.113.9:7474"
    import base64
    assert ud_ok in base64.b64decode(ec2.launched[0]["UserData"]).decode()  # Duck dials the relay


def test_shutdown_stops_the_relay(tmp_path):
    relay = FakeRelay()
    lch = Ec2Launcher(tmp_path, "http://127.0.0.1:7474", ami="ami-1", ec2_client=FakeEc2(), relay=relay)
    lch.shutdown_all()
    assert relay.stopped
