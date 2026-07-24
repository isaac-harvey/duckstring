# Plan: the Pool agent — named Pools as shared machines (persist.md phase 5)

> **Status: design → building.** The launcher work that makes a named Pool what plans/persist.md defines:
> **one shared filesystem** — one machine, multiple co-resident Ducks, `end_f` handoffs between them.
> Offline-verifiable by construction (see "Offline verification"); the Fargate/EC2 machine-start wiring is
> validated in the real-AWS gate run.

## The shape

A named Pool today spawns one machine per Pond (a Pool-of-one). Phase 5 inverts it: **one machine per
Pool**, hosting all its Ponds' Ducks as child processes, supervised by a small **Pool agent**.

**The agent dials back — never listens.** Symmetric with the Duck transport (the settled architecture:
"the Duck always dials back, so the same code works local and remote"): the agent long-polls the
Catchment for supervision commands and POSTs process events. No inbound networking to a Fargate task, no
service discovery, NAT-safe — and exactly the property that lets a *local* agent be a faithful test
double for a remote one.

```
Catchment ──(PoolLauncher enqueues)──> pool job queue
    ^                                        |
    | POST /api/pool/{name}/events           v  GET /api/pool/{name}/jobs (long-poll)
    └──────────────── Pool agent (on the Pool machine) ── spawns/terminates child Ducks
                          |
                          └─ each child Duck dials the Catchment itself (unchanged duck transport)
```

The agent only supervises: `ensure` (spawn a Duck with the standard args), `terminate`, `shutdown`.
Once spawned, a Duck is exactly a Duck — it polls its own job channel, fetches its artifact over the
remote-boot path if the source dir is absent, publishes locally to the Pool machine's root, persists to
the data root. **A Pool Duck gets `--persist-root` like a Catchment-Pool Duck** — the Pool is a shared
filesystem with co-located consumers, so local-first publish + async Persist apply verbatim; the
Catchment (a different Pool) reads it from the durable plane at `persisted_f`, which phase 3 already
gates.

## Components

- **`duckstring/duck/pool_agent.py`** (`python -m duckstring.duck.pool_agent --pool NAME --catchment URL
  --token T --root ROOT --data-root … --persist-root …`): the supervisor loop. Holds `{pond_key: Popen}`;
  on `ensure` builds the same Duck argv a `SubprocessLauncher` would (against ITS root); reaps exits and
  reports them; heartbeats by polling. Reports each child's liveness transitions
  (`{"kind": "duck_exit", pond, major, code}`) so the Catchment can attribute failures.
- **`PoolLauncher`** (catchment-side, `catchment/pool_launcher.py`): a launcher backend keyed by pool
  name. `ensure(pond)` enqueues an agent `ensure` job (and, first use, asks its **machine backend** to
  start the Pool machine); `terminate` enqueues; `is_running` from agent-reported state (pending counts
  as up, like the other remote backends — the silent-Duck heartbeat catches a dead box);
  `launch_error` surfaces machine-start failures. One `PoolLauncher` instance per named pool, routed to
  by the `DispatchingLauncher`.
- **Machine backends**: how the one Pool machine comes up.
  - `LocalPoolBackend` — a subprocess agent with its **own root dir**: the offline test double AND a
    legitimate "second pool on this box" for dev.
  - Fargate/EC2 — reuse the existing launchers' machinery with the **agent command** as the task/
    instance command instead of duck args (one task per POOL, not per Pond). Config (image/AMI/IAM/
    networking) is the already-built deploy_config; sizing is the pool's instance_type/cpu/memory.
- **Routes** (`routes/pool.py`, duck-token channel like `/api/duck/*`): `GET /api/pool/{name}/jobs`
  (drain), `POST /api/pool/{name}/events`.
- **Driver**: `_pool_of` returns `pool:{target}` when the target is a named pool with a working
  machine backend — two Ponds on the same pool now SHARE a Pool identity (same-Pool `end_f` gating
  between them, the whole point). Degrade-to-local unchanged when no backend exists.

## Semantics carried over unchanged

- **Scale-up-only** (a Pool is one filesystem): the agent never spans machines; compute pressure =
  bigger instance_type or more Pools.
- **Presets/Dedicated stay task-per-Pond** (isolation on purpose) — the existing Fargate/EC2 per-Pond
  path is untouched.
- **Pool-loss rollback (phase 4)** applies to Pool Ducks as-is: their local publishes die with the
  machine; their claims regress to the persisted floor on reconciliation. Fate-sharing holds per Pool.
- **Registry eviction (phase 6)** runs where the disk is — a follow-up moves the pressure check into
  the agent for remote Pools; v1 documents the Catchment-Pool-only scope.

## Offline verification

The dial-back design makes the offline test REAL, not a mock: a `LocalPoolBackend` agent with root B
under a Catchment with root A exercises every seam — agent poll/spawn/report, two co-resident Ducks
publishing to root B, same-Pool `end_f` gating between them, the Catchment reading their output from
the durable plane (cross-Pool `persisted_f`), terminate/shutdown, agent-death detection. The only code
excluded is the ~30 lines that start a Fargate task / EC2 instance with the agent command — the gate
run's job.

## Deferred within phase 5

- Machine idle/scale-to-zero + keep-warm scheduling (the pool config fields exist; v1 starts the
  machine on first need and stops it on shutdown_all).
- Agent-side disk-pressure eviction (phase 6 on remote Pools).
- Per-Pool resource partitioning between co-resident Ducks (DuckDB memory_limit split).
- **Dead-machine respawn for Fargate/EC2 Pools**: a machine record counts as up (consistent with the
  per-Pond remote semantics — the silent-Duck heartbeat fails the *Ponds*), but the v1 machine object
  never re-starts a genuinely dead task/instance; the `LocalPoolMachine` does (is_up = a live process).
  The gate run informs the right recheck cadence (DescribeTasks polling costs money per tick).
- **Reset/remove scrub on the Pool machine**: a catchment-side reset scrubs the Catchment root; a
  Pond that ran on a named Pool keeps its registry + local publish on the Pool machine until a Refresh
  (which wipes through the Duck itself) or the machine recycles. Known v1 gap.
- `catchment archive` now includes `pools/` under the root for local-provider Pools (their agent roots
  live there) — state downloads grow accordingly.
