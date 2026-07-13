---
title: Lineage
description: Observed table lineage, column derivations, and row-level provenance — recorded, never inferred.
---

# Lineage

Most platforms *infer* lineage — parsing query logs and scraping metadata to reconstruct a graph the
system never recorded, which is why it drifts. Duckstring records it: dependencies are declared in
`pond.toml`, every cross-Pond read goes through the Pond handle, and every incremental row carries the
freshness of the run that wrote it. Lineage here is bookkeeping over facts, at four levels — and it
follows one rule throughout: **exact or absent, never inferred**. An edge you see is true; where the
system can't prove a derivation it says so (*opaque*) rather than guessing.

## Pond level — the graph you declared

The Pond graph is the `[sources]` declarations, validated at deploy — it can't drift from what runs.
It's on the [status API](../reference/http-api.md#status) (`edges`), the canvas, and recursively across
Catchments in the [lineage view](connecting-catchments.md#identity-and-the-lineage-view).

## Table level — what each Ripple actually touched

Every read and write the Pond handle brokers is recorded at the moment of the call and shipped with the
Ripple's run report, so this level is *observed per run*, not static:

```bash
duckstring lineage sales            # each Ripple: ← the tables it read, → the tables it wrote
duckstring lineage sales -t order   # only the Ripples touching one table
```

The web UI shows the same under **Lineage · observed** in a Pond's sidebar. Because it's observed, it
also answers the drift question declarations can't: a Source you declare but no longer read shows up as
exactly that.

## Column level — what each output column derives from

For [Trickle builder](trickle.md) pipelines, column derivations are captured **at deploy** by walking
the pipeline's own structure — no parsing, no execution: joins unify their key columns, `.mutate()` and
computed `.select()` items union the columns their expressions reference, aggregate and accumulate
metrics map to their input columns, and chained `.merge()` statements resolve transitively to the
external sources.

```bash
duckstring lineage sales --columns
# priced
#   revenue ← orders.order_line.qty, prices.price.unit
#   id      ← orders.order_line.id
```

A `.sql()` escape hatch resolves too when the `lineage` extra is installed (`pip install
'duckstring[lineage]'` — a SQL parser); without it, those outputs read **opaque**. Classic
`read_table`/`write_table` Ripples contribute table-level lineage only — their column derivations are
arbitrary code, and Duckstring won't pretend otherwise. Together with the
[schema contract](../concepts/versioning.md#the-schema-contract), column lineage is impact analysis:
which downstream columns depend on `orders.discount` is a query, not an investigation.

## Row level — which run produced this row

Every [Trickle](trickle.md) row is stamped with the freshness of the run that wrote it, and run history
keys on the same freshness — so a row traces to its producing run, that run's version, and the exact
window of each Source it was derived from:

```bash
duckstring trace revenue.by_product --where "product_id = 7"
# 1 row(s) · newest produced at f = 2026-07-13T09:30:00+00:00
#   run: revenue v1.2.0 · success · 09:30:00 → 09:30:04
#   input window: (2026-07-13T08:30:00+00:00, 2026-07-13T09:30:00+00:00]
#   sources read over that window: orders, catalog
```

That's provenance, not just a graph — "this number came from *that* run over *that* slice of the
inputs" — and it costs nothing extra to keep, because the freshness stamps are how the incremental
engine works in the first place.

## Feeding a catalog

Duckstring records lineage; it doesn't try to be the place your whole company browses metadata. For
that, subscribe an [alert channel](../reference/cli.md#duckstring-alert--notification-channels) to the
`openlineage` kind and point it at your catalog's endpoint:

```bash
duckstring alert add --name catalog --to https://marquez.internal/api/v1/lineage --on openlineage
```

Each completed Pond Run then emits a standard **OpenLineage** RunEvent — the run identity, the observed
input/output tables, and schema facets — delivered through the alert outbox (retried, audited, and a
catalog outage never touches a run). `openlineage` is never part of an `--on all` subscription, so an
ops channel can't receive raw catalog events by accident.
