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


def _userdata(*, pond: str, major: int, version: str, source_path: str, catchment_url: str,
              token: str, data_root: str | None, pip_spec: str | None) -> str:
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
    lines = ["#!/bin/bash", "set -euo pipefail", f"mkdir -p {_REMOTE_ROOT}"]
    if pip_spec:
        lines.append(f"pip3 install --quiet {shlex.quote(pip_spec)}")
    lines.append("exec " + " ".join(shlex.quote(c) for c in cmd))
    return "\n".join(lines) + "\n"


class Ec2Launcher:
    manages_processes = True

    def __init__(self, root: Path, base_url: str | None, token: str = "", data_root: str | None = None,
                 *, ami: str | None = None, instance_profile: str | None = None,
                 dialback=None, pip_spec: str | None = None,
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
        self.region = region
        self._client = ec2_client
        self._instances: dict[str, dict] = {}   # pond_key → {"instance_id", "pool"}
        self._pending: dict[str, tuple] = {}     # spawns deferred until a reachable URL is known

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
        instance_type = (
            (duck or {}).get("dedicated_instance_type") if target == "dedicated"
            else pool.get("instance_type")
        ) or os.environ.get("DUCKSTRING_EC2_INSTANCE_TYPE") or "m6i.large"
        region = pool.get("region") or self.region
        if not self.ami:
            log.error("ec2: no AMI configured (DUCKSTRING_EC2_AMI) — cannot launch a Duck for %s", pond_key)
            return
        userdata = _userdata(
            pond=name, major=major, version=version, source_path=source_path,
            catchment_url=self.remote_base_url, token=self.token, data_root=self.data_root,
            pip_spec=self.pip_spec,
        )
        kwargs = {
            "ImageId": self.ami,
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
        if self.instance_profile:  # worker-side IAM (never the Catchment's keys on the box)
            kwargs["IamInstanceProfile"] = {"Name": self.instance_profile}
        if region and self.region is None:
            self.region = region
        try:
            resp = self._ec2().run_instances(**kwargs)
            instance_id = resp["Instances"][0]["InstanceId"]
        except Exception:
            log.exception("ec2: failed to launch a Duck instance for %s", pond_key)
            return
        self._instances[pond_key] = {"instance_id": instance_id, "pool": target}
        log.info("ec2: launched %s for %s (%s)", instance_id, pond_key, instance_type)

    def terminate(self, pond_key: str, wait: bool = False) -> None:
        self._pending.pop(pond_key, None)
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
