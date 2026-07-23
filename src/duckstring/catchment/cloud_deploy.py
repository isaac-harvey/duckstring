"""The compute deployment config (plans/compute-config-ui.md): the AWS settings a remote Duck needs to
launch, as a per-pool / per-dedicated JSON blob coalesced over the launcher's env defaults.

Kept dependency-light (no boto3, no DB) so the driver, the launchers, and the routes can all share the
field lists + validation.
"""

from __future__ import annotations

import os

# The deployment fields the UI collects, per provider (in addition to the size — Fargate cpu/memory,
# EC2 instance_type — and the shared region, which live on the pool row / duck config already).
FARGATE_FIELDS = (
    "image", "task_definition", "cluster", "subnets", "security_groups",
    "execution_role", "task_role", "assign_public_ip", "cpu_arch",
)
EC2_FIELDS = ("ami", "instance_profile", "pip_spec")

# What each Fargate/EC2 launcher reads from the environment (field → env var). The effective config
# coalesces the UI blob over these, so a Catchment configured purely by env stays valid with an empty blob.
_FARGATE_ENV = {
    "image": "DUCKSTRING_FARGATE_IMAGE", "task_definition": "DUCKSTRING_FARGATE_TASK_DEF",
    "cluster": "DUCKSTRING_FARGATE_CLUSTER", "subnets": "DUCKSTRING_FARGATE_SUBNETS",
    "security_groups": "DUCKSTRING_FARGATE_SECURITY_GROUPS",
    "execution_role": "DUCKSTRING_FARGATE_EXECUTION_ROLE", "task_role": "DUCKSTRING_FARGATE_TASK_ROLE",
    "assign_public_ip": "DUCKSTRING_FARGATE_ASSIGN_PUBLIC_IP", "cpu_arch": "DUCKSTRING_FARGATE_CPU_ARCH",
}
_EC2_ENV = {
    "ami": "DUCKSTRING_EC2_AMI", "instance_profile": "DUCKSTRING_EC2_INSTANCE_PROFILE",
    "pip_spec": "DUCKSTRING_EC2_PIP_SPEC",
}

# Fields required for a launch (a missing one → the Duck can't start). cluster defaults to "default";
# security_groups is recommended, not required.
_FARGATE_REQUIRED = ("subnets", "execution_role", "task_role")  # + image-or-task_definition (special-cased)
_EC2_REQUIRED = ("ami", "instance_profile")


def _provider(name: str | None) -> str:
    return (name or "fargate").lower()


def env_defaults(provider: str | None) -> dict:
    """The deployment fields this Catchment's environment already supplies (empty values dropped) — so the
    UI can show them as inherited and validation can account for them."""
    env_map = _EC2_ENV if _provider(provider) == "ec2" else _FARGATE_ENV
    return {field: os.environ[var] for field, var in env_map.items() if os.environ.get(var)}


def effective(provider: str | None, config: dict | None) -> dict:
    """The launch-time deployment config: the UI blob's non-empty values over the env defaults."""
    merged = dict(env_defaults(provider))
    for k, v in (config or {}).items():
        if v not in (None, ""):
            merged[k] = v
    return merged


def missing_fields(provider: str | None, config: dict | None) -> list[str]:
    """The required deployment fields still absent from the *effective* config (UI ?? env). Empty = ready
    to launch. ``config`` is the UI blob (env defaults are added here)."""
    eff = effective(provider, config)
    if _provider(provider) == "ec2":
        return [f for f in _EC2_REQUIRED if not eff.get(f)]
    missing = [f for f in _FARGATE_REQUIRED if not eff.get(f)]
    if not (eff.get("image") or eff.get("task_definition")):
        missing.insert(0, "image")  # (or task_definition)
    return missing


def clean(provider: str | None, config: dict | None) -> dict:
    """Keep only the known deployment fields for the provider, dropping empties — what we persist."""
    fields = EC2_FIELDS if _provider(provider) == "ec2" else FARGATE_FIELDS
    return {k: v for k, v in (config or {}).items() if k in fields and v not in (None, "")}
