"""The auto-relay (plans/cloud-config.md "least-friction local against cloud Ducks") — the eventual,
now-built least-friction path for running cloud Ducks from a **local** Catchment (a laptop behind NAT).

An EC2 Duck must reach the Catchment to dial back; a laptop isn't reachable inbound. Rather than make
the operator stand up a tunnel/mesh, the Catchment auto-provisions one tiny always-reachable EC2
**relay**, holds an outbound **SSH reverse tunnel** to it (``ssh -R`` — outbound works through NAT), and
hands the Ducks the relay's public address, which forwards to the laptop. The Duck dial-back transport is
**unchanged** — the Ducks think they're dialing the Catchment. This is a self-hosted reverse tunnel, NOT
a state-replicating twin; it forwards bytes, nothing more.

Lifecycle safety: the relay is tagged for cost visibility and runs a **TTL watchdog** so a vanished
laptop (crash / sleep) can't leak a running box — if the reverse tunnel goes idle past the TTL, the
relay terminates itself.

Only ever built for a **local** Catchment (loopback/private bind) with **cloud enabled** and the relay
config present (AMI + an SSH key pair + a security group). A hosted Catchment has a public URL and needs
no relay. boto3 + the ssh subprocess are the real path (validated on AWS, like the EC2 launcher); the
manager's logic is offline-testable with an injected ec2 client + ssh runner.
"""

from __future__ import annotations

import base64
import logging
import os
import shlex
import subprocess
import threading
import time
from urllib.parse import urlparse

log = logging.getLogger("duckstring.relay")

_PRIVATE_PREFIXES = ("10.", "192.168.", "127.", "169.254.")


def _is_local_host(host: str) -> bool:
    """Whether a Catchment bind host is unreachable from EC2 (a laptop) — loopback, 0.0.0.0, or an
    RFC-1918 private address. A public IP / hostname dials back fine and needs no relay."""
    if not host or host in ("localhost", "0.0.0.0", "::1", "::"):
        return True
    if host.startswith(_PRIVATE_PREFIXES):
        return True
    if host.startswith("172."):  # 172.16.0.0 – 172.31.255.255
        try:
            return 16 <= int(host.split(".")[1]) <= 31
        except (ValueError, IndexError):
            return False
    return False


def needs_relay(bind_url: str | None) -> bool:
    """True when a Duck couldn't reach this Catchment directly (a local/NAT'd bind) — so cloud Ducks
    need the relay to bridge back."""
    if not bind_url:
        return True  # bind unknown → assume local (the safe default; the operator can force off)
    return _is_local_host(urlparse(bind_url).hostname or "")


def _watchdog_userdata(relay_port: int, ttl_minutes: int) -> str:
    """cloud-init for the relay box: allow the reverse forward to bind publicly (GatewayPorts) and run a
    TTL watchdog that self-terminates the instance if nothing is connected to the forwarded port for
    ``ttl_minutes`` — so a crashed/sleeping laptop never leaks the box."""
    return "\n".join([
        "#!/bin/bash",
        "set -euo pipefail",
        # Let sshd's reverse forward bind 0.0.0.0 so the Ducks can reach it.
        "echo 'GatewayPorts clientspecified' >> /etc/ssh/sshd_config",
        "systemctl restart sshd || service sshd restart",
        # TTL watchdog: terminate self once the forwarded port has had no established connection for the
        # TTL (the laptop's ssh -R died / went idle). Runs in the background from boot.
        "cat >/usr/local/bin/ds-relay-watchdog <<'EOF'",
        "#!/bin/bash",
        f"PORT={relay_port}; TTL=$(({ttl_minutes} * 60)); idle=0",
        "IMDS=http://169.254.169.254/latest",
        "TOKEN=$(curl -s -X PUT $IMDS/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 300' || true)",
        "meta() { curl -s -H \"X-aws-ec2-metadata-token: $TOKEN\" $IMDS/meta-data/$1; }",
        "while true; do",
        "  if ss -tnH state established \"( sport = :$PORT )\" | grep -q .; then idle=0; else idle=$((idle+30)); fi",
        "  if [ $idle -ge $TTL ]; then",
        "    IID=$(meta instance-id); REG=$(meta placement/region)",
        "    aws ec2 terminate-instances --instance-ids \"$IID\" --region \"$REG\" || shutdown -h now",
        "    exit 0",
        "  fi",
        "  sleep 30",
        "done",
        "EOF",
        "chmod +x /usr/local/bin/ds-relay-watchdog",
        "nohup /usr/local/bin/ds-relay-watchdog >/var/log/ds-relay-watchdog.log 2>&1 &",
    ]) + "\n"


def _default_ssh_runner(host: str, *, ssh_key: str | None, ssh_user: str, relay_port: int,
                        local_port: int) -> subprocess.Popen:
    """Launch the outbound ``ssh -R`` reverse tunnel: the relay's ``relay_port`` forwards to the
    laptop's Catchment (``local_port``). Kept alive with keepalives + auto-reconnect-friendly flags."""
    cmd = [
        "ssh", "-N", "-o", "StrictHostKeyChecking=accept-new", "-o", "ExitOnForwardFailure=yes",
        "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
    ]
    if ssh_key:
        cmd += ["-i", ssh_key]
    cmd += ["-R", f"0.0.0.0:{relay_port}:localhost:{local_port}", f"{ssh_user}@{host}"]
    log.info("relay: opening reverse tunnel: %s", " ".join(shlex.quote(c) for c in cmd))
    return subprocess.Popen(cmd)


class RelayManager:
    def __init__(self, *, bind_url: str | None, ami: str | None = None, key_name: str | None = None,
                 ssh_key: str | None = None, security_group: str | None = None,
                 instance_type: str | None = None, relay_port: int | None = None,
                 ttl_minutes: int | None = None, ssh_user: str | None = None, region: str | None = None,
                 ec2_client=None, ssh_runner=None):
        self.bind_url = bind_url
        self.local_port = (urlparse(bind_url).port if bind_url else None) or 7474
        self.ami = ami or os.environ.get("DUCKSTRING_RELAY_AMI") or os.environ.get("DUCKSTRING_EC2_AMI")
        self.key_name = key_name or os.environ.get("DUCKSTRING_RELAY_KEY_NAME")
        self.ssh_key = ssh_key or os.environ.get("DUCKSTRING_RELAY_SSH_KEY")
        self.security_group = security_group or os.environ.get("DUCKSTRING_RELAY_SECURITY_GROUP")
        self.instance_type = instance_type or os.environ.get("DUCKSTRING_RELAY_INSTANCE_TYPE") or "t4g.nano"
        self.relay_port = relay_port or int(os.environ.get("DUCKSTRING_RELAY_PORT") or self.local_port)
        self.ttl_minutes = ttl_minutes if ttl_minutes is not None else int(
            os.environ.get("DUCKSTRING_RELAY_TTL_MINUTES") or 30)
        self.ssh_user = ssh_user or os.environ.get("DUCKSTRING_RELAY_SSH_USER") or "ec2-user"
        self.region = region
        self._client = ec2_client
        self._ssh_runner = ssh_runner or _default_ssh_runner
        self.instance_id: str | None = None
        self.public_url: str | None = None
        self._tunnel = None
        self._started = False   # the tunnel is up
        self._starting = False  # a start is in flight (async) — don't kick another

    def configured(self) -> bool:
        """Whether the relay can actually be built — needs an AMI, an SSH key pair, and a security
        group. Absent any, we log once and skip (Ducks stay pending; the operator can set
        DUCKSTRING_CATCHMENT_PUBLIC_URL instead)."""
        return bool(self.ami and self.key_name and self.security_group)

    def _ec2(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("ec2", **({"region_name": self.region} if self.region else {}))
        return self._client

    def _await_public_ip(self, instance_id: str, *, poll=5.0, timeout=180.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r = self._ec2().describe_instances(InstanceIds=[instance_id])
            inst = r["Reservations"][0]["Instances"][0]
            if inst.get("State", {}).get("Name") == "running" and inst.get("PublicIpAddress"):
                return inst["PublicIpAddress"]
            time.sleep(poll)
        raise TimeoutError(f"relay {instance_id} did not become reachable within {timeout}s")

    def start(self) -> str:
        """Provision the relay, wait for it, open the reverse tunnel, and return the public URL the
        Ducks dial. Idempotent — a second call returns the running relay's URL."""
        if self._started and self.public_url:
            return self.public_url
        userdata = _watchdog_userdata(self.relay_port, self.ttl_minutes)
        kwargs = {
            "ImageId": self.ami, "InstanceType": self.instance_type, "MinCount": 1, "MaxCount": 1,
            "KeyName": self.key_name, "SecurityGroups": [self.security_group],
            "UserData": base64.b64encode(userdata.encode()).decode(),
            "TagSpecifications": [{"ResourceType": "instance", "Tags": [
                {"Key": "Name", "Value": "duckstring-relay"},
                {"Key": "duckstring:role", "Value": "relay"},
            ]}],
        }
        resp = self._ec2().run_instances(**kwargs)
        self.instance_id = resp["Instances"][0]["InstanceId"]
        log.info("relay: launched %s (%s) — awaiting reachability", self.instance_id, self.instance_type)
        ip = self._await_public_ip(self.instance_id)
        self._tunnel = self._ssh_runner(
            ip, ssh_key=self.ssh_key, ssh_user=self.ssh_user,
            relay_port=self.relay_port, local_port=self.local_port,
        )
        self.public_url = f"http://{ip}:{self.relay_port}"
        self._started = True
        log.info("relay: ready at %s → localhost:%s", self.public_url, self.local_port)
        return self.public_url

    def start_async(self, on_ready) -> None:
        """Provision in the background (EC2 boot is minutes); call ``on_ready(public_url)`` when the
        tunnel is up. Used by the launcher so the first remote spawn defers (pending) rather than blocks.
        A burst of spawns kicks off only one relay (the ``_starting`` guard)."""
        if self._started or self._starting:
            return
        self._starting = True

        def _run():
            try:
                on_ready(self.start())
            except Exception:
                log.exception("relay: failed to bring up the reverse tunnel — cloud Ducks stay pending")
            finally:
                self._starting = False

        threading.Thread(target=_run, name="ds-relay-start", daemon=True).start()

    def stop(self) -> None:
        if self._tunnel is not None:
            try:
                self._tunnel.terminate()
            except Exception:
                pass
            self._tunnel = None
        if self.instance_id is not None:
            try:
                self._ec2().terminate_instances(InstanceIds=[self.instance_id])
                log.info("relay: terminated %s", self.instance_id)
            except Exception:
                log.exception("relay: failed to terminate %s", self.instance_id)
            self.instance_id = None
        self.public_url = None
        self._started = False
