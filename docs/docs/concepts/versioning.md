---
title: Versioning
description: SemVer for transforms — atomic upgrades and concurrent major versions.
---

# Versioning

Duckstring's answer to the coordination problem is the one software packaging settled on long ago: **SemVer, with old majors kept alive until consumers move**. Versioning isn't a label on a Pond — it's the mechanism that lets teams ship breaking changes without organising anyone else.

## SemVer on Ponds

Every Pond version is a SemVer string in `pond.toml`. The contract is the Pond's published tables — their names, schemas, and semantics:

- **Patch / minor** (`1.0.0 → 1.0.1`, `1.1.0`) — compatible changes: bug fixes, new tables, new columns. Consumers don't need to do anything.
- **Major** (`1.x → 2.0.0`) — breaking changes: renamed or removed tables, changed semantics. Consumers opt in by updating their declared Source version.

## One selected version per major

Deploying a version registers it as an immutable artifact and atomically points "the Pond" for that major line at it. Deploying `sales 1.2.1` means every Sink consuming `sales` major 1 gets `1.2.1` from its next run onwards — no Sink changes anything, exactly as a compatible package release flows to users on their next resolve.

History survives upgrades: run records are keyed to the specific version that produced them, so an upgrade never rewrites what ran before.

## Concurrent majors

The load-bearing feature: **a new major doesn't replace the old one — it runs alongside it.**

Deploying `sales 2.0.0` creates a second live `sales` line. Sinks declaring `sales = "1.0.0"` keep consuming major 1, which keeps executing; Sinks that migrate declare `sales = "2.0.0"` and consume major 2. The owning team ships the breaking change the day it's ready, each consumer migrates on its own schedule, and major 1 is retired when nothing depends on it.

This is the step most mesh architectures stop short of. Splitting a pipeline into domain-owned pieces decentralises *ownership*, but without concurrent versions a breaking change still forces every consumer to move in lockstep — the coordination just happens across team boundaries instead of within one repo. Versioned boundaries plus concurrent execution removes the lockstep.

## Declaring a dependency

A Sink pins each Source in `pond.toml`:

```toml
[sources]
transactions = "1.0.0"   # major 1, at least 1.0.0 — required
products = "1.2.0?"      # the trailing ? marks the Source optional
```

The major selects the line to consume; the full string records the **minimum compatible version** the Sink was built against, enforced at deploy: a Sink whose pinned Source (within the pinned major) is selected *below* that version is rejected, and selecting a Source version that would regress below an existing downstream pin is rejected too. A major bump is the sanctioned escape hatch — a new line is independent of the old pin. A required Source that is failed or missing [blocks](../guides/fault-tolerance.md) the Sink; an optional one (`?`) lets it run regardless. Declarations are by *name and major*, not by artifact — so a Sink can deploy before its Source has (the pin is checked once the Source appears), and resolves it as soon as it does.

## The schema contract

A Pond's output schema is whatever its Ripples produce — so it can't be vetted before the Pond runs. Instead Duckstring checks it **at publish time**, before the new output overwrites the live tables. The rule for a major line is **additive-only**: a version that advances the line must keep every table and column the line already published (new tables and columns are fine; a dropped column, a removed table, or a changed type is a breaking change).

- On a Pond's first successful run, its output schema is captured as the line's contract.
- A later run (a new version on the same major) whose output isn't a superset of that contract is **rejected at publish**: the live tables keep their last-good data, the Pond's Run is failed with a contract message, and its Sinks are [blocked](../guides/fault-tolerance.md) rather than run against the broken output. Nothing downstream ever sees the incompatible schema. The failure is labelled as a contract violation everywhere it surfaces — the web UI's status card ("Failed · Contract violation"), the status API's `failure_kind`, and its own `contract` [alert kind](../reference/cli.md#duckstring-alert--notification-channels).
- A compatible run publishes and *extends* the contract.
- A deliberate **rollback** to an already-accepted lower version skips this check — it's governed by `min_version` instead (a Sink that needs a later version's columns pinned them, and the rollback is already rejected for that Sink).

To make a genuine breaking change — drop a column, change a type — **bump the major**. The new line carries the new shape; the old line keeps running for the Sinks that haven't re-pinned, so they migrate on their own schedule. (Because the data plane records schema metadata per version, this is cheap to enforce; it's strongest on the default [Iceberg data plane](../guides/running-a-catchment.md#the-data-plane), which captures the published schema directly.)

## Versioning workflow

Day to day this looks like releasing a library: bump the version in `pond.toml`, deploy, done. Compatible releases flow to consumers automatically; breaking releases open a new major and a migration window. Deploying a fixed version also auto-clears a failure on the Pond — shipping the fix *is* the recovery action. See [Deploying](../guides/deploying.md).
