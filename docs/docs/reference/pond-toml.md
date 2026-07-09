---
title: pond.toml
description: The Pond manifest — every field, with defaults.
---

# pond.toml Reference

`pond.toml` sits at a Pond project's root and declares the Pond's identity and its place in the package graph. It is read at deploy time; everything in it travels with the deployed version.

A complete example:

```toml
[pond]
name = "sales"
version = "1.2.0"
type = "pond"            # inlet | pond | outlet
immediate_retries = 1
source_retries = 2

[sources]
transactions = "1.0.0"
products = "1.1.0?"      # trailing ? — optional
```

## `[pond]`

| Field | Required | Default | Meaning |
|---|---|---|---|
| `name` | yes | — | The Pond's name — its identity across versions, and how Sinks refer to it. |
| `version` | yes | — | SemVer. The major selects the version line; see [Versioning](../concepts/versioning.md). |
| `type` | no | `"pond"` | `"inlet"` (no Sources, ingests external data), `"pond"`, or `"outlet"` (no Sinks, final data products). |
| `immediate_retries` | no | `0` | Default budget for Ripple retries within one Pond Run. |
| `source_retries` | no | `0` | Default budget for fresh Pond Runs attempted as Sources update, after a failure. |
| `ripples` | no | `"src/pond.py"` | The module defining the Pond's `@ripple` functions. |
| `puddles` | no | `"src/puddles.py"` | The module defining the Pond's `@puddle` functions for [local testing](../guides/local-testing.md). |
| `dbt_project` | no | — | Makes this a [dbt-mode Pond](../guides/dbt.md): the dbt project directory (each model becomes a Ripple). Mutually exclusive with `ripples`. Needs the dbt extra. |

The retry fields are **seeds, not settings**: they initialise the live budgets when the Pond is first deployed to a Catchment, after which the budgets are operator-owned (`duckstring control failure-budget`) and redeploys don't touch them. See [Fault Tolerance](../guides/fault-tolerance.md).

## `[sources]`

One entry per Source Pond — this section *is* the Pond's pipeline declaration:

```toml
[sources]
transactions = "1.0.0"
products = "1.1.0?"
```

| Part | Meaning |
|---|---|
| Key | The Source Pond's name. |
| Value, major digit | Which major line to consume — `"2.1.0"` consumes major 2. |
| Value, full string | The minimum compatible version of that line — enforced at deploy (see [Versioning](../concepts/versioning.md)). |
| Trailing `?` | The Source is **optional**: its absence or [failure](../guides/fault-tolerance.md) doesn't block this Pond. Without it, the Source is required. |

Sources are declared by name and major, not by deployed artifact — a Sink can deploy before its Source exists, and binds the moment the Source is deployed. An Inlet has no `[sources]` section.

## What does *not* belong here

By design, `pond.toml` carries only what's intrinsic to the Pond version. Operational configuration lives on the Catchment, set by operators, and survives redeploys:

- **[Triggers](../guides/triggers.md)** — demand is an operational decision, not a property of the code.
- **[Windows](../guides/windows.md)** — availability of the external source is environment-specific (`duckstring trigger window … add`).
- **Live retry budgets** — seeded from here once, then operator-owned.
