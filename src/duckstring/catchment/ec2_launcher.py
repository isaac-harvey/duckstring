"""The EC2 Duck backend (plans/cloud-config.md increment 3b) — a launcher backend that brings a Duck
up on an EC2 instance instead of a local subprocess. Plugged behind :class:`DispatchingLauncher`, so a
Catchment (local or hosted) runs ``catchment``-targeted Ponds on its own box and pool/dedicated ones
here, on real iron.

**Bootstrap reuses the existing remote-Duck path** (`duck/__main__`): the instance runs
``python -m duckstring.duck --catchment <reachable-url> --root <local> --source-path <rel>``; because
that dir isn't on the box, the Duck fetches its source artifact over the duck channel and boots. Data
lives in the shared object store (``--data-root`` = the S3 URI); only hot state is local to the box.

**IAM:** the instance gets an **instance profile** (worker-side role) for S3/engine access — the
Catchment's own AWS creds (control plane) launch/terminate here, but are never copied onto the box.

**Placement:** ``DUCKSTRING_EC2_SUBNET`` / ``DUCKSTRING_EC2_SECURITY_GROUPS`` (or a pool's
``deploy_config``) put the instance where it can actually reach the Catchment. Omitting them is a
trap worth naming: AWS drops the instance into the default VPC's **default security group**, so the
Duck boots, installs, and is then failed by the silent-Duck heartbeat — a networking problem that
presents as a mysteriously dead worker. The launcher warns once when no group is configured.

**AMI:** the userdata runs ``pip3 install <pip_spec>`` then ``python3 -m duckstring.duck``, so the image's
*default* ``python3`` must be ≥3.10 (duckstring's floor) with a matching ``pip3`` — a stock AL2023 image
ships 3.9 and will not boot a Duck. Either bake an image that carries duckstring, or point the AMI at one
whose default interpreter is new enough.

**Scope (v1):** one instance per Pond spawn, honouring the pool's ``instance_type``/``region``. Warm-pool
reuse + the floor/ceiling/idle/keep-warm autoscaler are a deferred follow-up (the pool *config* exists;
the scheduler that pools instances across Ponds does not yet). Liveness leans on the Duck heartbeat: we
can't cheaply poll EC2 each tick, so ``is_running`` reports True while we hold a live instance record and
the Catchment's silent-Duck detector (no contact > 60s) fails a box whose Duck died. boto3 is imported
lazily; a fake client is injectable for tests.
"""

from __future__ import annotations

import base64
import logging
import os
import shlex
from pathlib import Path

from ..keys import split_pond_key

log = logging.getLogger("duckstring.ec2")

_REMOTE_ROOT = "/var/lib/duckstring"  # the Duck's local hot-state root on the EC2 box
_EC2_NO_AMI = (
    "EC2 cannot launch — no AMI configured (DUCKSTRING_EC2_AMI). An EC2 pool needs the Duck AMI + IAM set "
    "as Catchment env: DUCKSTRING_EC2_AMI, DUCKSTRING_EC2_INSTANCE_PROFILE, plus DUCKSTRING_EC2_SUBNET and "
    "DUCKSTRING_EC2_SECURITY_GROUPS so the Duck can reach the Catchment. Without them, set the Pond's "
    "compute to the Catchment (local) target.")


def _csv(value: str | None) -> list[str]:
    """A comma-separated config value → a list (blank entries dropped)."""
    return [v.strip() for v in value.split(",") if v.strip()] if value else []


# Mirror the boot script's output to the serial console, so ``get-console-output`` shows it. This is the
# ONLY window into a Duck that dies before it ever dials back — and that box has no inbound SSH (by
# design) and no log agent, so without this the failure is literally unobservable. Learned the hard way:
# a pool agent that never connected could not be diagnosed at all.
_CONSOLE_TEE = "exec > >(tee -a /dev/console /var/log/duckstring-boot.log) 2>&1"


def _aws_env(region: str | None) -> list[str]:
    """Export the AWS region into a worker's environment. Not cosmetic: without a region the data
    plane's S3 access falls back to an UNSIGNED request, which the bucket rejects with
    "No AWSAccessKey was presented" — the instance role is present and healthy, so the failure reads as
    a permissions problem rather than a missing setting. The Catchment's own Ducks inherit this from the
    service environment; a remote worker starts with an empty one and must be told.

    Credentials themselves are NEVER exported — the box uses its instance profile."""
    if not region:
        return []
    return [f"export AWS_REGION={shlex.quote(region)} AWS_DEFAULT_REGION={shlex.quote(region)}"]


def _userdata(*, pond: str, major: int, version: str, source_path: str, catchment_url: str,
              token: str, data_root: str | None, pip_spec: str | None,
              region: str | None = None) -> str:
    """The cloud-init shell that boots the Duck. It fetches its own source artifact over the duck
    channel (the dir won't exist on the box), so nothing but the reachable Catchment URL + token is
    needed. ``pip_spec`` installs duckstring when the AMI doesn't already carry it."""
    cmd = [
        "python3", "-m", "duckstring.duck",
        "--pond", pond, "--major", str(major), "--version", version,
        "--catchment", catchment_url, f"--token={token}",
        "--root", _REMOTE_ROOT, "--source-path", source_path,
        f"--data-root={data_root or ''}",
    ]
    lines = ["#!/bin/bash", "set -euxo pipefail", _CONSOLE_TEE, *_aws_env(region),
             f"mkdir -p {_REMOTE_ROOT}"]
    if pip_spec:
        lines.append(f"pip3 install --quiet {shlex.quote(pip_spec)}")
    lines.append("exec " + " ".join(shlex.quote(c) for c in cmd))
    return "\n".join(lines) + "\n"


class Ec2Launcher:
    manages_processes = True

    def __init__(self, root: Path, base_url: str | None, token: str = "", data_root: str | None = None,
                 *, ami: str | None = None, instance_profile: str | None = None,
                 dialback=None, pip_spec: str | None = None, subnet: str | None = None,
                 security_groups: str | None = None, assign_public_ip: str | None = None,
                 region: str | None = None, ec2_client=None):
        from .dialback import RemoteDialback

        self.root = root
        self.base_url = base_url                 # local bind (dispatcher keeps this in sync)
        # The reachable Catchment URL a Duck dials back to — shared across remote backends via the
        # RemoteDialback (which also owns the auto-relay for a local Catchment). Built standalone here
        # when none is injected (a bare Ec2Launcher, e.g. tests).
        self.dialback = dialback or RemoteDialback(base_url)
        self.dialback.register(self._drain_pending)
        self.token = token
        self.data_root = data_root
        self.ami = ami or os.environ.get("DUCKSTRING_EC2_AMI")
        self.instance_profile = instance_profile or os.environ.get("DUCKSTRING_EC2_INSTANCE_PROFILE")
        self.pip_spec = pip_spec if pip_spec is not None else os.environ.get("DUCKSTRING_EC2_PIP_SPEC")
        # Placement. Without these an instance lands in the default VPC's default subnet + DEFAULT
        # security group, which in any sensibly-configured account cannot reach the Catchment's port —
        # the Duck boots, installs, and is then failed by the silent-Duck heartbeat with no clue why.
        self.subnet = subnet or os.environ.get("DUCKSTRING_EC2_SUBNET")
        self.security_groups = _csv(security_groups or os.environ.get("DUCKSTRING_EC2_SECURITY_GROUPS"))
        self.assign_public_ip = assign_public_ip or os.environ.get("DUCKSTRING_EC2_ASSIGN_PUBLIC_IP")
        self._warned_no_sg = False
        self.region = region
        self._client = ec2_client
        self._instances: dict[str, dict] = {}   # pond_key → {"instance_id", "pool"}
        self._pending: dict[str, tuple] = {}     # spawns deferred until a reachable URL is known
        self._launch_errors: dict[str, str] = {}  # pond key → last spawn-failure reason (surfaced by the driver)

    # ─── client ──────────────────────────────────────────────────────────────────

    def _ec2(self):
        if self._client is None:
            import boto3  # lazy — only a cloud-enabled Catchment ever needs it
            self._client = boto3.client("ec2", **({"region_name": self.region} if self.region else {}))
        return self._client

    def reset_client(self) -> None:
        """Drop the cached boto3 client so added/rotated credentials take effect on the next spawn."""
        self._client = None

    @property
    def remote_base_url(self) -> str | None:
        return self.dialback.url

    # ─── the launcher interface ───────────────────────────────────────────────────

    def set_base_url(self, url: str) -> None:
        self.base_url = url
        self.dialback.set_base_url(url)  # resolves the dial-back URL when no relay is needed

    def _drain_pending(self) -> None:
        pending, self._pending = self._pending, {}
        for pond_key, (version, source_path, duck) in pending.items():
            self.ensure(pond_key, version, source_path, duck=duck)

    def is_running(self, pond_key: str) -> bool:
        # A live record → treat as up (provisioning included); the heartbeat detector catches a dead Duck.
        return pond_key in self._pending or pond_key in self._instances

    def ensure(self, pond_key: str, version: str, source_path: str, duck: dict | None = None) -> None:
        if self.remote_base_url is None:
            # A local Catchment: ask the shared dial-back to bring up the auto-relay (once, in the
            # background — EC2 boot is minutes); this spawn defers until a reachable URL resolves.
            self.dialback.request()
            self._pending[pond_key] = (version, source_path, duck)
            return
        if pond_key in self._instances:
            return
        name, major = split_pond_key(pond_key)
        pool = (duck or {}).get("pool") or {}
        target = (duck or {}).get("duck_target")
        # The per-spawn deployment config (a dedicated Duck's, else the pool's) coalesced over the env
        # defaults the launcher holds (plans/compute-config-ui.md).
        dc = (duck or {}).get("deploy_config") or pool.get("deploy_config") or {}
        instance_type = (
            (duck or {}).get("dedicated_instance_type") if target == "dedicated"
            else pool.get("instance_type")
        ) or dc.get("instance_type") or os.environ.get("DUCKSTRING_EC2_INSTANCE_TYPE") or "m6i.large"
        region = pool.get("region") or dc.get("region") or self.region
        ami = dc.get("ami") or self.ami
        instance_profile = dc.get("instance_profile") or self.instance_profile
        pip_spec = dc.get("pip_spec") if dc.get("pip_spec") is not None else self.pip_spec
        if not ami:
            log.error("%s (pond %s)", _EC2_NO_AMI, pond_key)
            self._launch_errors[pond_key] = _EC2_NO_AMI  # surfaced to the Pond failure via the driver
            return
        userdata = _userdata(
            pond=name, major=major, version=version, source_path=source_path,
            catchment_url=self.remote_base_url, token=self.token, data_root=self.data_root,
            pip_spec=pip_spec, region=region,
        )
        kwargs = {
            "ImageId": ami,
            "InstanceType": instance_type,
            "MinCount": 1, "MaxCount": 1,
            "UserData": base64.b64encode(userdata.encode()).decode(),
            "TagSpecifications": [{
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": f"duckstring-{name}@{major}"},
                    {"Key": "duckstring:pond", "Value": f"{name}@{major}"},
                    {"Key": "duckstring:target", "Value": target or "pool"},
                    {"Key": "duckstring:pool", "Value": target if target not in (None, "dedicated") else ""},
                ],
            }],
        }
        if instance_profile:  # worker-side IAM (never the Catchment's keys on the box)
            kwargs["IamInstanceProfile"] = {"Name": instance_profile}
        kwargs.update(self._placement(dc))
        if region and self.region is None:
            self.region = region
        try:
            resp = self._ec2().run_instances(**kwargs)
            instance_id = resp["Instances"][0]["InstanceId"]
        except Exception as exc:
            log.exception("ec2: failed to launch a Duck instance for %s", pond_key)
            self._launch_errors[pond_key] = f"ec2: failed to launch a Duck instance: {exc}"
            return
        self._launch_errors.pop(pond_key, None)  # launched cleanly
        self._instances[pond_key] = {"instance_id": instance_id, "pool": target}
        log.info("ec2: launched %s for %s (%s)", instance_id, pond_key, instance_type)

    def _placement(self, dc: dict) -> dict:
        """The network placement for a launch: subnet + security groups (per-spawn deploy_config over the
        launcher's env defaults), and a public IP when asked for.

        Given both, they go in a NetworkInterface spec — the only form ``RunInstances`` accepts when a
        public-IP override is set, and harmless otherwise. Given neither, the instance falls into the
        default VPC's default security group, which normally cannot reach the Catchment: warn once, since
        the symptom (a Duck that boots and is then failed as silent) points nowhere near the cause."""
        subnet = dc.get("subnet") or self.subnet
        groups = _csv(dc.get("security_groups")) or self.security_groups
        public = str(dc.get("assign_public_ip") or self.assign_public_ip or "").upper()
        if not groups and not self._warned_no_sg:
            self._warned_no_sg = True
            log.warning(
                "ec2: launching Ducks with no security group (DUCKSTRING_EC2_SECURITY_GROUPS / the pool's "
                "deploy_config) — the instance gets the VPC's DEFAULT security group, which usually cannot "
                "reach the Catchment. Ducks will boot and then fail as silent.")
        if not subnet and not groups and not public:
            return {}
        if public in ("ENABLED", "DISABLED", "TRUE", "FALSE"):
            nic: dict = {"DeviceIndex": 0,
                         "AssociatePublicIpAddress": public in ("ENABLED", "TRUE")}
            if subnet:
                nic["SubnetId"] = subnet
            if groups:
                nic["Groups"] = groups
            return {"NetworkInterfaces": [nic]}
        out: dict = {}
        if subnet:
            out["SubnetId"] = subnet
        if groups:
            out["SecurityGroupIds"] = groups
        return out

    def start_pool_instance(self, pool: str, pool_spec: dict, module_cmd: str) -> str:
        """Launch ONE instance running an arbitrary ``python3 -m <module> <args>`` command — the Pool
        machine (plans/pool-agent.md): the agent boots via userdata just like a Duck would, using the
        pool's deploy_config over the env defaults. Returns the instance id; raises on any failure (the
        caller records the error). Real-AWS validation is the gate run's job."""
        spec = pool_spec or {}
        dc = spec.get("deploy_config") or {}
        ami = dc.get("ami") or self.ami
        if not ami:
            raise RuntimeError(_EC2_NO_AMI)
        instance_type = (spec.get("instance_type") or dc.get("instance_type")
                         or os.environ.get("DUCKSTRING_EC2_INSTANCE_TYPE") or "m6i.large")
        pip_spec = dc.get("pip_spec") if dc.get("pip_spec") is not None else self.pip_spec
        region = spec.get("region") or dc.get("region") or self.region
        lines = ["#!/bin/bash", "set -euxo pipefail", _CONSOLE_TEE, *_aws_env(region),
                 f"mkdir -p {_REMOTE_ROOT}"]
        if pip_spec:
            lines.append(f"pip3 install --quiet {shlex.quote(pip_spec)}")
        lines.append(f"exec python3 -m {module_cmd}")
        kwargs = {
            "ImageId": ami, "InstanceType": instance_type, "MinCount": 1, "MaxCount": 1,
            "UserData": base64.b64encode(("\n".join(lines) + "\n").encode()).decode(),
            "TagSpecifications": [{
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": f"duckstring-pool-{pool}"},
                         {"Key": "duckstring:pool", "Value": pool},
                         {"Key": "duckstring:role", "Value": "pool-agent"}],
            }],
        }
        instance_profile = dc.get("instance_profile") or self.instance_profile
        if instance_profile:
            kwargs["IamInstanceProfile"] = {"Name": instance_profile}
        kwargs.update(self._placement(dc))  # a Pool agent dials back too — same placement rules
        if region and self.region is None:
            self.region = region
        resp = self._ec2().run_instances(**kwargs)
        return resp["Instances"][0]["InstanceId"]

    def terminate_instance(self, instance_id: str) -> None:
        """Terminate one instance by id (the Pool machine's stop)."""
        self._ec2().terminate_instances(InstanceIds=[instance_id])

    def launch_error(self, pond_key: str) -> str | None:
        """The reason the last spawn attempt for ``pond_key`` failed (missing config / an AWS error), so
        the driver can attribute the Pond failure to it instead of a generic "process not running"."""
        return self._launch_errors.get(pond_key)

    def diagnose(self, pond_key: str) -> str | None:
        """Ask EC2 what happened to this Duck's instance — the liveness sweep's silent branch, so a box
        that terminated/never booted surfaces its state ("terminated: Server.SpotInstanceTermination")
        instead of only "lost contact". Best-effort: any DescribeInstances problem → None.

        Includes the tail of the **serial console**, which the boot script tees to. A Duck that dies
        before dialling back leaves no other trace: the box has no inbound SSH and no log agent, so the
        console is the difference between "lost contact" and the actual Python traceback."""
        rec = self._instances.get(pond_key)
        if rec is None:
            return None
        parts = []
        try:
            resp = self._ec2().describe_instances(InstanceIds=[rec["instance_id"]])
            inst = resp["Reservations"][0]["Instances"][0]
            parts.append(f"instance {inst.get('State', {}).get('Name', 'unknown')}")
            reason = inst.get("StateReason", {}).get("Message")
            if reason:
                parts.append(str(reason))
        except Exception:
            log.debug("ec2: describe_instances failed for %s", pond_key, exc_info=True)
        console = self.console_tail(pond_key)
        if console:
            parts.append(f"console: {console}")
        return "ec2: " + "; ".join(parts) if parts else None

    def console_tail(self, pond_key: str, lines: int = 12) -> str | None:
        """The last ``lines`` of the instance's serial console (where the boot script tees its output),
        with the boot banner filtered out. ``None`` if unavailable — the console can lag a minute or two
        after boot, and is empty on some instance types."""
        rec = self._instances.get(pond_key)
        if rec is None:
            return None
        try:
            out = self._ec2().get_console_output(InstanceId=rec["instance_id"], Latest=True).get("Output") or ""
        except Exception:
            log.debug("ec2: get_console_output failed for %s", pond_key, exc_info=True)
            return None
        keep = [ln for ln in out.splitlines()
                if ln.strip() and not ln.startswith(("ci-info:", "[", "<"))]
        return " | ".join(keep[-lines:]) or None

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        self._pending.pop(pond_key, None)
        self._launch_errors.pop(pond_key, None)
        rec = self._instances.pop(pond_key, None)
        if rec is None:
            return
        try:
            self._ec2().terminate_instances(InstanceIds=[rec["instance_id"]])
            log.info("ec2: terminated %s (%s)", rec["instance_id"], pond_key)
        except Exception:
            log.exception("ec2: failed to terminate %s for %s", rec["instance_id"], pond_key)

    def shutdown_all(self) -> None:
        for pond_key in list(self._instances):
            self.terminate(pond_key)
        self._pending.clear()
        # The shared dial-back (and its relay) is torn down once by the dispatcher, not per backend.
