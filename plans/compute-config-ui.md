# UI-entered, validated compute (Fargate/EC2) deployment config

Status: **to build.** Today the AWS deployment config a remote Duck needs (Fargate: image/task-def, VPC
subnets, security groups, execution+task roles, cluster; EC2: AMI, instance profile) comes ONLY from
Catchment env vars, read once at `create_app`. So a pool/dedicated Duck can be "selected" but fail at
launch with "no image/AMI configured" (surfaced now, but only after a run fails). This makes that config
**UI-entered, co-located with where the compute is configured, and validated up front**.

## Goals

1. **Enter the deployment vars in the UI** where you configure the compute — the Cloud menu (a pool) and
   a Pond's Dedicated Duck.
2. **Validate**: a Fargate/EC2 pool or dedicated Duck is only accepted if its *effective* config
   (UI-entered ?? env defaults) is adequate to launch; else reject with exactly what's missing.
3. **Dedicated reuses the pool form.** Selecting "Dedicated" on a Pond shows the SAME provider setup form
   the Cloud menu uses for a pool — identical work, scoped to that one Duck.

## The deployment config (per provider)

A JSON `deploy_config` blob (extensible; avoids a wide multi-column migration):

- **fargate**: `image`, `task_definition`, `cluster`, `subnets`, `security_groups`, `execution_role`,
  `task_role`, `assign_public_ip`, `cpu_arch` (+ the existing `cpu`/`memory` size).
- **ec2**: `ami`, `instance_profile`, `pip_spec` (+ the existing `instance_type`).
- common: `region`.

**Required to launch** (`cloud_deploy.missing_fields(provider, effective)`):
- fargate: (`image` OR `task_definition`) AND `subnets` AND `execution_role` AND `task_role` — the Duck
  needs the exec role (image pull + logs) and the task role (S3 data-plane access). `security_groups`
  recommended, `cluster` defaults to `default`.
- ec2: `ami` AND `instance_profile` (worker-side IAM for S3).

The **effective** config coalesces UI-entered over the launcher's env defaults, so a Catchment already
configured by env stays valid with an empty blob — validation runs against the effective view.

## Storage

- `duck_pool.deploy_config TEXT` (JSON) — a named pool's deployment config.
- `pond_duck.deploy_config TEXT` (JSON) — a Pond's dedicated Duck's deployment config.
- Migration `026`.

## Backend

- **`catchment/cloud_deploy.py`** (new): the field lists per provider, `missing_fields(provider, cfg)`,
  `effective(cfg, env_defaults)`, and `env_defaults(launcher)` (what the launchers read from env, so the
  UI can show "set via env" and validation can account for it).
- **Launchers**: `FargateLauncher`/`Ec2Launcher.ensure` read the per-spawn deploy config from the `duck`
  dict (dedicated `deploy_config` ?? `pool.deploy_config`) coalesced over their env defaults, instead of
  only `self.<env>`. Fargate task-def registration is keyed by the effective (image, roles, cpu_arch) so
  distinct pools/images get distinct task-defs (cache dict, not a single `_registered`).
- **Driver**: `add_pool` / `set_duck` accept `deploy_config`, **validate** the effective config for a
  remote provider and reject (`ValueError` → 422) listing the missing fields; `duck_config`/`list_pools`
  return it. `duck_config` threads `deploy_config` into the `duck` dict launchers receive.
- **Routes**: pass `deploy_config` through pool + duck endpoints; a `GET …/compute-defaults` exposes the
  env defaults + required-field lists so the UI form knows what's inherited/required.

## UI

- **`DeployConfigForm`** (new, shared): the provider setup fields (Fargate set / EC2 set), with the
  size controls (Fargate cpu/mem dropdowns, EC2 instance picker) already built — extended with the
  deployment fields. Fields with an env default show a muted "set via env" placeholder and are optional;
  the rest are marked required. Inline missing-field validation mirrors the backend.
- **Cloud menu** Duck Pools: the add-pool form uses `DeployConfigForm`.
- **DuckSection** (a Pond's compute): when **Dedicated** is selected, render the *same* `DeployConfigForm`
  (scoped to that Duck) — one Duck, same fields.

## Build order

1. Storage + `cloud_deploy` (fields/validate/effective) + launcher plumbing (read per-spawn config) —
   the foundation; validated at the driver.
2. Driver CRUD validation + the compute-defaults route.
3. `DeployConfigForm` + wire into the Cloud menu pool form and DuckSection dedicated.

## Not changing

The env-var path stays (the hosted platform + existing deploys rely on it) — UI config is an override/
addition, and the env values are the defaults the effective config coalesces over.
