# Cloud-configurable OSS (the Flock branch, phase 2)

**Thesis.** The OSS Catchment becomes *cloud-configurable*: point it at object storage, define
remote compute, and run Ducks and the Flock on real cloud iron — against your own AWS. Duckstring
**Cloud** is then not a feature-gated fork; it is (a) **managed operation** (no IAM/VPC/autoscale/
SSO to wire), (b) **cheaper compute** bought wholesale/spot, and (c) **Exaflock**, the proprietary
Flock engine that is genuinely faster/cheaper than DIY Athena/EMR and is the primary revenue line.

The design rule that follows: **capability is OSS; operation + Exaflock + management-grade features
are Cloud.** We never gate an already-open seam (the launcher seam `DUCKSTRING_DUCK_LAUNCHER`, the
data-plane registry, the Flock engine registry) — doing so is a rug-pull and torches trust. We sell
the work we remove, not features we withhold.

## The free/paid line (explicit)

| Concern | OSS | Cloud |
| --- | --- | --- |
| Point at S3 for the data layer | ✅ configure it | ✅ done for you |
| Remote Ducks (EC2 launcher, pools, pond ducks) | ✅ the seam + knobs | ✅ managed pools, wholesale/spot |
| Autoscaling | thin knobs (floor/ceiling/idle/keep-warm) | smart/predictive policy |
| Flock: Athena, EMR Serverless engines | ✅ (routes to AWS's own compute) | ✅ managed |
| **Exaflock** engine | ❌ | ✅ **the moat** |
| Cost **visibility** (resource tags + total runtime + engine on `/metrics`) | ✅ reputational floor | ✅ |
| Cost **management** (budgets, caps, per-pond/team attribution, forecasting) | ❌ | ✅ |
| SSO / SAML / SCIM | (can sit behind a platform-auth proxy) | ✅ wired for you |

The tell that the boundary is right: a self-hoster on their own AWS can run **everything** and still
think "I'd rather point at Duckstring Cloud and use Exaflock." If that sentence is true, we eat.

## 1. S3 data plane — a persisted Catchment setting, effectively set-once

Not startup-only. A **persisted Catchment setting**, settable via API/UI, so the onboarding path
"play locally → attach S3 → go cloud" needs no restart. But the data plane is where all data lives,
so switching it after ponds hold data is a **migration**, not a hot-swap. Semantics:

- Settable freely while the Catchment holds **no published data**.
- Once data exists: refuse the change (422) unless an explicit `migrate=true` rebuild is requested
  (deferred — v1 just refuses with a clear message).
- Env var (`DUCKSTRING_DATA_PLANE` / a new `DUCKSTRING_DATA_ROOT` + creds) still **seeds** it for
  platform hosting; the setting persists in `catchment_setting` and survives restarts via the root.

**Cloud-enabled gate.** Remote compute needs shared storage a remote box can read. So:

- local-disk data plane → only **Catchment** Ducks are launchable;
- S3 data plane configured **and** AWS creds present (secret store) → unlocks Duck Pools, Pond Ducks,
  Flock. The UI greys out remote options until both hold, showing the reason.

Region for storage **and** compute is a single Catchment-level setting. Cross-region is out of scope
by construction: crossing regions = crossing Catchments = a **Duct**. No new machinery.

## 2. Duck taxonomy: Catchment / Duck Pool / Pond Duck

Maps directly onto the existing launcher seam (`DUCKSTRING_DUCK_LAUNCHER=module:Class`, constructed
`(root, base_url, token=…, data_root=…)`, receives each Pond's duck config on `ensure`).

- **Catchment** (default) — the Duck runs on the box hosting the Catchment (today's
  `SubprocessLauncher`). Named "Catchment" not "Local" — it names *where*, regardless of whether that
  box is itself cloud-hosted.
- **Duck Pool** — a Catchment-level, **named** EC2 launcher config, **shared across Ponds**. Warm,
  autoscaled between a floor and ceiling, idle-timeout to scale down.
- **Pond Duck** — a **dedicated** box for one Pond, optionally **ephemeral** (auto-stop on Pond-run
  completion). The cost/isolation opposite of a pool: dedicated, pays cold-start per run if ephemeral.

A Pond selects one of the three. The EC2 launcher is one new `Launcher` implementation reading the
pool/pond-duck config; **Catchment Ducks are unchanged**.

**Cold-start vs liveness (the landmine).** EC2 boot is minutes; the silent-Duck detector fails an
in-flight Duck after 60 s. A provisioning Duck must not be killed before it dials back. Reuse the
existing deferral shape: the launcher reports a **provisioning** Duck as `is_running` (exactly as
pending-key spawns already do), so `_check_liveness` leaves it alone until it is truly up.

## 3. Duck Pools — config

A pool is Catchment-level named config (not `pond.toml` — it is environment-specific infra):

```
name            (unique)
instance_type   (e.g. m6i.large)
min_instances   (floor; keep-warm)
max_instances   (ceiling)
idle_timeout    (scale-down after N idle)
keep_warm       (spare capacity beyond current load, e.g. n+1; thin)
region          (defaults to the Catchment region)
```

**Autoscaling policy stays thin in OSS** — floor/ceiling/idle/keep-warm are honest primitives.
Predictive/spot-fleet/demand-curve scaling is a Cloud differentiator; do **not** build a clever
autoscaler into OSS.

CRUD: `duckstring duck pool add|ls|rm|show`, `POST/GET/DELETE /api/catchment/duck-pools`
(full-gated — it provisions billable infra). A pool with no EC2 launcher configured is inert
(defined, not runnable).

## 4. Pond Ducks — dedicated/ephemeral

Operator-only (see §6 on why not in `pond.toml`): a Pond may be assigned its own dedicated box with
`auto_stop` (terminate on Pond-run completion). Held in the per-Pond override (§6), not a pool.

## 4b. Remote compute providers — Fargate (default) + EC2 (escape) — BUILT

A remote backend is chosen **per pool** (`duck_pool.provider ∈ fargate|ec2`, default **fargate**), so a
Catchment can mix a fast Fargate `default` pool with an EC2 `huge` pool. **Fargate is the default**:
tens-of-seconds cold start (vs EC2's minutes — the biggest EC2 pain), pay-per-second serverless
(the thesis: don't pay for idle compute), native **task-role** IAM, and image-based (no AMI + userdata
`pip install`). **EC2 stays as the escape hatch** for jobs over Fargate's 16 vCPU / 120 GB cap or needing
GPU/warm-pools — genuinely huge work is the Flock's job anyway.

- **`FargateLauncher`** implements the launcher seam over ECS: `ensure` = `RunTask` (container command
  override = the Duck args), `terminate` = `StopTask`, `is_running` = a task record. Uses the
  **container image** (§ image) so there's no bootstrap. A pool carries only its **size** (`cpu`/`memory`
  — the Fargate task size); the Fargate **infra** (cluster / subnets / security groups / task+execution
  roles / image / assign-public-ip) is **Catchment-level** env (`DUCKSTRING_FARGATE_*`), like the EC2
  launcher's AMI — it's one VPC/cluster, not per-pool.
- **Shared dial-back**: both remote backends need a reachable Catchment URL, and the auto-relay is shared
  across them (a `RemoteDialback` holds the URL + relay; each backend registers a drain callback, fired
  once when the relay's tunnel comes up). The `DispatchingLauncher` routes a remote spawn to the backend
  for its pool's provider (`dedicated` → the Catchment's default provider).
- **The container image** (`Dockerfile`, GHCR via `release.yml`): `python -m` entrypoint over the
  installed wheel; a Duck runs `duckstring.duck <args>` as the task command. Ship an official image
  alongside the PyPI package.

### Preset pools — S/M/L/XL (Fargate, built-in on OSS)

OSS ships **built-in preset pools** `S/M/L/XL` (code-defined, `provider=fargate`, escalating `cpu/memory`)
that always appear in the pool list (flagged `managed`, not editable/removable) and resolve like any pool
— so `pond.toml duck = "M"` works out of the box with zero pool setup. This is the **seamless-DSC-transfer
move**: DSC ships the *same* preset names backed by its own serverless pools, so a `duck = "M"` project
runs identically on OSS-Fargate and on DSC — only the backend differs, never the config. User pools sit
alongside the presets; preset names are reserved.

## 5. Flock per Pond (rip out ripple-level)

Ripple-level `@ripple(flock=…)` is **removed**. The posture moves up to the **Pond**. If a user needs
to isolate a heavy chunk, they split it into its own Pond — the Pond is the unit of everything.

- Posture ladder (unchanged semantics, per-Pond source): **off** (default) / **upgrade** / **always**.
- **Flock engine is a registry** (`duckstring.flock.get_engine`, already scheme-selected). Keep it a
  named selection even though **Athena** is the only entry today; **EMR Serverless** and **Exaflock**
  slot in as registered engines. Exaflock becomes the default once stable.
- Per-Pond config: `flock_mode ∈ off|upgrade|always`, `flock_engine ∈ athena|…` (nullable → Catchment
  default → `athena`).

Precedence collapses to one level (Pond), so the old ripple-vs-pond puzzle is gone.

## 6. OOM fail-up — operation-level, upgrade-the-rest, configurable

The **posture** is a Pond setting; the **fail-up acts on the operation** (a builder terminal — finer
than a ripple). Mechanics (mostly the existing `flock.comprehensive`):

- Each comprehensive terminal decides independently. On a local `OutOfMemoryException`, that terminal
  fails **up** to the engine. Terminals **already completed in the run are not redone** — only the
  remainder of the run upgrades. (This already falls out of per-terminal dispatch; assert it in a
  test rather than assume it.)
- **Configurable policy** (`oom_policy`, per-Pond, nullable → Catchment default):
  - `fail_up` (default) — retry the over-envelope operation on the Flock in the same run.
  - `fail` — do not dispatch; surface the OOM as a Pond failure (for users who want a hard cap).
- The DuckDB memory cap the fail-up depends on stays Duck-level (`DUCKSTRING_MEMORY_LIMIT`, size-derived).

`flock.comprehensive(...)` stops reading `ripple_mode` from the ripple attr and takes the resolved
**Pond posture** threaded through the executor from the Duck config.

## 7. `pond.toml` declaration + coalesce override + portability

Duck/Flock are a **compute hint** — a property of the transform's needs, so unlike windows/spouts they
*belong* in `pond.toml`. **This reverses `018_duck.sql`'s "never pond.toml" note** — update that
rationale, don't leave two stories.

**`pond.toml` names a pool by name; it never specifies raw compute** (verbose, environment-specific,
and it would couple the transform's identity to infra). Example:

```toml
[pond]
duck = "heavy"          # a Duck Pool name, or "catchment" (default). NOT an instance type.

[flock]
mode = "upgrade"        # off | upgrade | always
engine = "athena"       # optional; defaults to the Catchment default engine
oom_policy = "fail_up"  # optional
```

- **A dedicated Pond Duck is NOT declarable in `pond.toml`** — it needs an instance spec (the verbose,
  environment-specific thing we keep out). It is an **operator override only** (UI/CLI). `pond.toml`
  only ever names a pool or `"catchment"`.
- **Unknown-pool fallback (portability).** `pond.toml` is portable code. If it names a pool this
  Catchment does not have (a laptop, a different inventory), **fall back to the Catchment Duck** — warn
  at deploy, never hard-fail. That keeps `duck = "heavy"` deployable anywhere; it just runs locally
  where `heavy` is undefined.

**Coalesce model (not a dirty flag).** Mirror the identity split:

- **Declared** compute (pool name / flock mode / engine / oom policy) lives on **`pond_version`**
  (immutable; rides the deployed artifact; updates automatically on redeploy).
- **Operator override** lives on **`pond_duck`** (nullable columns, keyed on the live `pond`).
- **Effective = override ?? declared ?? Catchment default** (`coalesce`).

So a redeploy updates the declared value automatically; an operator edit persists because it is a
separate, nullable override; "reset to declared" = null the override (`duck set --clear`, retargeted to
revert to *declared*, not raw Catchment default). Surface **declared vs effective** side by side in the
UI so a `pond.toml` bump silently ignored (because overridden) isn't confusing. This is strictly nicer
than a seed-once/dirty-flag scheme — no third state to keep coherent.

## 8. IAM

AWS creds in the secret store enable the **control plane** only: the Catchment launches EC2 and submits
Athena/Flock queries using them. **Worker boxes use instance profiles / assumed roles** for S3 and
engine access — never the Catchment's long-lived keys sprayed onto every Duck (that would make each
worker a credential-exfiltration surface and undercut the write-only secret store). Design remote-compute
access as roles; static keys live only at the Catchment.

## 9. Cost — visibility floor in OSS, management in Cloud

- **OSS floor (cheap, reputational insurance):** tag every AWS resource Duckstring creates
  (Catchment / pool / pond) and expose **total runtime per Duck** and the **Flock engine in use** on
  `/metrics`. A self-hoster who racks up EC2/Athena spend with zero visibility writes the
  "Duckstring cost me $4k overnight" post — the tag+metric floor prevents that class of story and costs
  almost nothing. Let them see spend in their own AWS Cost Explorer via the tags.
- **Cloud (paid):** in-product spend dashboard, per-pond/team attribution, budgets, caps, forecasting,
  anomaly alerts. The engine protocol keeps **no cost surface** — metering is out of band per the Flock
  design.

## Schema (migration `021_cloud.sql`)

```sql
-- Catchment-level persisted settings (data plane target, region, ...). Key/value, one row per key.
CREATE TABLE catchment_setting (key TEXT PRIMARY KEY, value TEXT);

-- Named Duck Pools (Catchment-level infra config; not pond.toml).
CREATE TABLE duck_pool (
    name          TEXT PRIMARY KEY,
    instance_type TEXT,
    min_instances INTEGER NOT NULL DEFAULT 0,
    max_instances INTEGER NOT NULL DEFAULT 1,
    idle_timeout  INTEGER,     -- seconds
    keep_warm     INTEGER NOT NULL DEFAULT 0,
    region        TEXT
);

-- Declared compute on the immutable artifact (from pond.toml; updates on redeploy).
ALTER TABLE pond_version ADD COLUMN duck_pool     TEXT;    -- pool name | 'catchment' | NULL
ALTER TABLE pond_version ADD COLUMN flock_mode    TEXT;    -- off|upgrade|always | NULL
ALTER TABLE pond_version ADD COLUMN flock_engine  TEXT;    -- athena|… | NULL
ALTER TABLE pond_version ADD COLUMN oom_policy    TEXT;    -- fail_up|fail | NULL

-- Widen the operator override (pond_duck) — all nullable, coalesced over declared then default.
ALTER TABLE pond_duck ADD COLUMN duck_target   TEXT;    -- 'catchment' | pool name | 'dedicated' | NULL
ALTER TABLE pond_duck ADD COLUMN dedicated_instance_type TEXT;  -- for duck_target='dedicated'
ALTER TABLE pond_duck ADD COLUMN dedicated_auto_stop     INTEGER;
ALTER TABLE pond_duck ADD COLUMN flock_mode    TEXT;
ALTER TABLE pond_duck ADD COLUMN flock_engine  TEXT;
ALTER TABLE pond_duck ADD COLUMN oom_policy    TEXT;
-- existing pond_duck.size/flock(bool) kept for back-compat; size still advisory. `flock` bool is
-- superseded by flock_mode (bool on ⇒ 'upgrade', off ⇒ 'off' on read for old rows).
```

## Build sequence

**Increment 1 — the config model (this commit set).** Foundational, no new infra runtime, fully
testable. Unblocks everything else.
1. Rip ripple-level `flock=` out of `core.py` `@ripple`; thread the Pond posture into
   `flock.comprehensive` via the executor's Duck config.
2. Migration `021`: `catchment_setting`, `duck_pool`, declared columns on `pond_version`, widened
   `pond_duck`.
3. `_pond_config` parses `[pond] duck` + `[flock] mode/engine/oom_policy`; deploy seeds the declared
   columns on `pond_version` (always — they ride the artifact) and leaves `pond_duck` as the override.
4. `Driver.duck_config` becomes the **coalesce**: override → declared → default, with the
   unknown-pool→catchment fallback. `set_duck` writes overrides; `--clear` reverts to declared.
5. CLI/route/UI: extend `duck show/set` and the `/api/ponds/{name}/duck` payload to the new fields.

**Increment 2 — Catchment S3 setting + cloud-enable gate.** `catchment_setting` read/write API + UI,
set-once semantics, the S3+creds gate exposed on `/api/status` (`cloud_enabled`).

**Increment 3 — Duck Pool CRUD + the EC2 launcher.** `duck_pool` CRUD (CLI/API/UI) + an
`Ec2Launcher` (`DUCKSTRING_DUCK_LAUNCHER` target) with the provisioning/liveness deferral, floor/
ceiling/idle/keep-warm, instance-profile IAM, and resource tagging. Pond Ducks (dedicated/ephemeral).

**Increment 4 — cost visibility.** Resource tags + total-runtime-per-Duck + engine on `/metrics`.

**Increment 5 — frontend.** The branch's "Duck" section on the Pond window reworked: Duck target
(Catchment / Pool picker / dedicated) + Flock (mode + engine + oom policy), declared-vs-effective,
and a Catchment-settings panel for S3 + pools. Fix the cloud-preset-sizes section that "doesn't work
for OSS".

## Least-friction local against cloud Ducks (the auto-relay — BUILT)

An EC2 Duck must *reach* the Catchment to dial back; a laptop Catchment behind NAT isn't reachable.
Local-with-cloud-Ducks is a **test-the-path** mode (nobody runs prod from a laptop), so the shipped
answer is the operator's `DUCKSTRING_CATCHMENT_PUBLIC_URL` (a tunnel / a Tailscale-style mesh address).

Inverting the transport (Catchment polls the Ducks) *would* remove the tunnel — outbound-to-EC2 works
where inbound-to-laptop doesn't — but it makes the Duck a listening server (ephemeral-TLS pain, a second
transport strictly worse for the hosted case), so it's rejected.

**The auto-provisioned relay (BUILT — `catchment/relay.py`):** when a local Catchment (loopback/private
bind) with cloud enabled first needs a remote Duck, `RelayManager` spins one tiny always-reachable EC2
box (`t4g.nano`), the laptop holds an outbound **`ssh -R` reverse tunnel** to it, and the Ducks dial the
relay's public address (which forwards to the laptop). The Duck dial-back transport is unchanged; it
forwards bytes, *not* a state-replicating twin. It hooks the `Ec2Launcher`'s existing **pending/drain**
mechanism: the first remote spawn kicks the relay off in the background (EC2 boot is minutes) and defers;
when the tunnel is up, `set_remote_base_url` drains the pending Ducks — the same deferral the
bind-unknown case uses. The relay box runs a **TTL watchdog** in its userdata (self-terminates if the
forwarded port is idle past the TTL — a crashed/sleeping laptop can't leak it), and `shutdown_all` tears
tunnel + box down. Gated: only for a local bind, cloud enabled, `DUCKSTRING_RELAY≠off`, no explicit
`DUCKSTRING_CATCHMENT_PUBLIC_URL`, and the config present (`DUCKSTRING_RELAY_{AMI,KEY_NAME,SSH_KEY,
SECURITY_GROUP,...}`). **Still to validate on real AWS** (like the EC2 launcher, the ssh/EC2/watchdog
path is offline-tested with fakes); a security-group scoped to the Duck instances + the laptop, and TLS
on the exposed endpoint, are the remaining hardening. DSC manages all of this for you. Tests:
`tests/test_relay.py`.

## Validated on real AWS (2026-07-20) + follow-ups

A cloud-enabled **local** Catchment ran a Fargate Duck end-to-end (laptop Catchment → `M` pool →
ARM64 task from the ECR image → Duck dialed back over a cloudflared tunnel → fetched its artifact →
published 15 MB to S3 via the Iceberg plane → idle task auto-terminated). Bugs found + fixed in the
run: (1) `create_app` didn't thread `DUCKSTRING_CATCHMENT_PUBLIC_URL` into `RemoteDialback` (a cloud
Duck dialed the local bind → connection-refused) — fixed + `tests/test_dialback.py`; (2) the container
image didn't install the `[aws]` extra, so the Duck lacked `s3fs` — the `Dockerfile` now installs
`duckstring[aws]`.

**Open follow-up (real gap): reap a self-stopped remote task's record.** `FargateLauncher`/`Ec2Launcher`
`is_running` returns True while a task/instance *record* exists; when a task **stops on its own** (the
Duck errors and exits) the record lingers, so `is_running` stays True and a `force`/on-change retry is
**skipped** (the launcher thinks a Duck is still up). Workaround: `control kill` (reaps the record via
`terminate`) then `force`. Proper fix: on a run **failure** (silent-Duck / error), the Catchment should
`launcher.terminate(pond)` to reap the record so a retry re-dispatches — mirroring the idle-completion
path that already terminates cleanly. Touches the liveness/retry path, so deliberately deferred.

## Deferred / Cloud-only

- The **Exaflock** engine (Cloud; the moat) — registered like any Flock engine.
- **EMR Serverless** engine (registered later; the registry is ready now).
- Cost **management** (budgets/caps/attribution/forecast) — Cloud.
- **Smart autoscaling** — Cloud.
- **SSO/SAML/SCIM** — Cloud (OSS can sit behind a platform-auth proxy today).
- Data-plane **migration** when switching the S3 setting on a populated Catchment (v1 refuses).
