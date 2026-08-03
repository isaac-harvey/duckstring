---
title: The Catchment
description: The reference runtime — a server that receives deployed Ponds and executes them.
---

# The Catchment

The **Catchment** is Duckstring's reference runtime: a server that receives deployed [Ponds](ponds.md), decides when they run, executes them, and publishes their outputs. It bundles everything a team needs to operate a pipeline — orchestration engine, REST API, [web UI](../guides/web-ui.md), and data access — behind one process.

It's worth being precise about its place in the system: **Duckstring is the packaging standard; the Catchment is one runtime for it.** Ponds don't know or care what executes them. The Catchment is the batteries-included option for teams that want the full stack working out of the box.

## What it does

- **Holds the graph.** Every deployed Pond version, its declared Sources, and its Ripple topology — assembled from each Pond's `pond.toml`, never authored centrally.
- **Decides runs.** The Catchment runs the orchestration engine from [Theory](../theory.md): it tracks every Pond's and Ripple's freshness, holds [triggers](../guides/triggers.md) and [Windows](../guides/windows.md), and starts a Pond Run exactly when the freshness rules say so.
- **Executes Ponds.** Each running Pond gets a dedicated worker process (a *Duck*) that executes its Ripples and reports back. Workers are spawned on demand and survive Catchment restarts — see [Architecture](../reference/architecture.md).
- **Publishes data.** Each Pond's tables are exported as Parquet on every successful run, served via the [data API](../guides/querying-data.md) without ever touching a live computation.
- **Remembers.** Run history (per-attempt, with errors and tracebacks), freshness state, triggers, and windows all persist in the Catchment's database and survive restarts.

## Local and remote

A Catchment runs either as a local daemon or a remote server — the CLI treats both identically:

```bash
duckstring catchment init --name dev                              # create + start locally
duckstring catchment connect --name prod --path https://host:7474 # register a remote
```

Each registered Catchment has a name; one is the default, and any command takes `-c {name}` to target another. Starting locally and later pointing the same commands at a hosted server is the intended upgrade path — there are future plans for a managed Catchment service at [duckstring.com](https://duckstring.com).

Every Catchment also carries a stable identity (a UUID minted on first start), which lets Catchments reference each other unambiguously. A Catchment can **draw a Pond from another** over a duct — so the package graph can span teams and machines, not just one runtime. See [Connecting Catchments](../guides/connecting-catchments.md).

See [Running a Catchment](../guides/running-a-catchment.md) for operations: starting, connecting, what lives on disk, and restart behaviour.
