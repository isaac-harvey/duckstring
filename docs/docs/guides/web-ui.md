---
title: Web UI
description: The Catchment's built-in monitoring and control surface.
---

# Web UI

Every Catchment serves a web UI at its root URL — `http://127.0.0.1:7474` for a default local Catchment. It's the live view of everything this documentation describes: the Pond graph with freshness flowing through it, run history down to individual Ripple attempts, and the same trigger and control surface as the CLI.

The UI works wherever the Catchment is reachable: behind a platform login (its session cookies flow automatically), under a path prefix (all references are relative — e.g. Posit Connect's `/content/{guid}/`), and on a Catchment started with an [API key](running-a-catchment.md#authentication), where it prompts for the key on first visit and keeps it in the browser. A tool that already holds a key can skip the prompt by deep-linking `{url}/#key={key}` — the key is adopted into browser storage on load and immediately stripped from the URL.

One thing it deliberately is not: a pipeline editor. **Topology comes from deploying code** — Ponds and their Sources are declared in `pond.toml` and `src/pond.py`, never authored in a UI. The UI operates the pipeline; the packages define it.

## The canvas

The main view is the package graph, live: Ponds as containers with their Ripples and intra-Pond edges inside, Source→Sink edges between them. It updates the instant the engine state changes (a long-poll on `/api/status`, not a timer), and everything on it is the real engine state:

- **Node state by colour** — running (the brand cyan), queued, idle; failure states take visual precedence (failed / killed / blocked) so a stalled lineage is unmissable.
- **Demand on the edges** — pull demand (amber) and push demand (green-yellow) are visibly distinct, so you can watch a Tap propagate upstream or a Pulse's targets resolve downstream.
- **Freshness at a glance** — each Pond shows the age of its data and its standing trigger, if any.
- **Upstream Catchments** — when you [draw Ponds over a duct](connecting-catchments.md), their source Catchments appear as labelled containers feeding your Draw nodes, recursing the full lineage up the mesh. The top-left box names this Catchment (and its stable id).

Selecting any Pond, Ripple, or trigger opens it in the sidebar.

## The sidebar

The selected Pond's operating panel:

- **State** — status, version, freshness, run counts, and the active trigger.
- **Trigger row** — Tap / Pulse / Wave / Tide, exactly as in [Triggers](triggers.md); standing triggers can be set and removed here.
- **Control row** — Force / Wake / Sleep / Kill, exactly as in [Control](control.md).
- **Failures** — the Pond's [retry budgets](fault-tolerance.md), editable live, and a **Clear Failure** action when the Pond is failed.
- **Windows** — the Pond's [availability windows](windows.md), with an editor for adding and removing rules.
- **Alerts** — the [notification channels](running-a-catchment.md#alerts) scoped to this Pond, with a form to add one (destination, event kinds, freshness SLA) and a per-channel test.

Two catchment-wide menus sit top-right, beside **Collapse all** (full access only): **🔑 Secrets** (the write-only [credential store](running-a-catchment.md#authentication)) and **Alerts** (every notification channel — add/test/remove, plus a delivery log).

## Run history and Run Detail

The bottom panel splits in two. On the left, the run feed: every Pond Run, newest first, with status, timing, and duration — click a run to open it. On the right, **Run Detail** for the selected run:

- The run's freshness and timing.
- The per-attempt Ripple list — each Ripple's duration and status, with retries marked `↻N`, making the [retry trace](fault-tolerance.md#reading-the-trail) legible at a glance.
- For failed runs, the failure itself: each failing source (`ripple · message`, or the Pond for worker-level errors) with its **full traceback** — debugging starts here, not in a log file.

## When to use which surface

The CLI and UI drive the same API, so this is preference rather than capability — but in practice: the CLI suits scripted and routine actions (deploys, triggers in CI, a quick `status`), while the UI earns its place when you're *watching* something — following a Wave settle into its cadence, diagnosing a blocked lineage back to the failed Pond at its root, or reading a traceback two clicks after noticing a red node.
