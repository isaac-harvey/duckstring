# Duckstring

Build data pipelines the way you build software: version each transform, declare its dependencies, and Duckstring resolves the execution DAG automatically.

Duckstring treats data transformations as software packages. Upstream dependencies are declared per Pond (unit operation), defining the DAG without the need for its direct management. 

Ponds are upgraded and deployed to Duckstring's pull-based Catchment (orchestrator) atomically - like upgrading a package - with the earlier version continuing to execute until there are no consumers dependent on it. Upstream defines constraints on what it can consume, downstream defines when it's needed, and the Catchment optimally executes the sequence of Ponds supplying it with the best currency and frequency as possible.

You should not need to manage the DAG. You should not need global governance. You should know yourself and your suppliers and trust that you'll get what you need when you need it.

## Core Concepts

The main elements:

- **Catchment**: Control environment (FastAPI + UI + CLI)
- **Pond**: Versioned container with declared upstream dependencies
- **Ripple**: Unit operation within a Pond (e.g. a single transformation producing a table)
- **Trickle**: DBSP-based incremental engine operating within a Ripple

Ponds are typed or referred to in context:

- **Source**: A parent Pond
- **Sink**: A child Pond
- **Inlet**: A Pond with external dependencies and no Sources
- **Outlet**: A Pond with no Sinks (e.g. outputs final data products)

To see the orchestration model in action without installing anything, try the [Duckstring Playground](https://playground.duckstring.com).

## Quickstart

```bash
pip install duckstring

# Start a local Catchment (the runtime + web UI) — leave it running
duckstring catchment init --name dev
```

Then, in another terminal:

```bash
# Create the demo pipeline (transactions, products → sales → reports) and deploy it
mkdir demo && cd demo
duckstring pond demo
duckstring pond deploy --all -y

# Run it end to end, once
duckstring trigger pulse reports

# Look around
duckstring status                          # live state of every Pond
duckstring query reports monthly_summary   # peek at an output table
```

The Catchment also serves a live web UI at `http://127.0.0.1:7474` — the Pond graph, freshness, run history, and the full trigger/control surface.

## Execution

Ponds execute on demand signals sent to an Outlet, in two flavours — **push** runs the lineage forward to a target freshness; **pull** propagates demand upstream so every Pond re-runs as its Sources update, naturally throttled to the bottleneck. Each comes as a one-shot or a standing trigger:

| | Once | Continuously |
|---|---|---|
| **Push** | Pulse | Tide |
| **Pull** | Tap | Wave |

A Tide keeps an Outlet no staler than a bound (`duckstring trigger tide reports 1d`); a Wave keeps it as fresh as the pipeline can supply. See [Triggers](https://docs.duckstring.com/guides/triggers) for the full semantics.

*There is no DAG.*

## Going further

Full documentation lives at **[docs.duckstring.com](https://docs.duckstring.com)**:

- [Quickstart](https://docs.duckstring.com/getting-started/quickstart) — the path above, with explanations
- [Theory](https://docs.duckstring.com/theory) — the freshness-based orchestration model in depth
- [Versioning](https://docs.duckstring.com/concepts/versioning) — SemVer on Ponds, concurrent major versions, atomic upgrades
- [Local testing](https://docs.duckstring.com/guides/local-testing) — Puddles: test a Pond before deploying it
- [Fault tolerance](https://docs.duckstring.com/guides/fault-tolerance) — retry budgets, failure states, recovery
- [Running a Catchment](https://docs.duckstring.com/guides/running-a-catchment) — hosting, authentication, platform deployment
- [CLI](https://docs.duckstring.com/reference/cli) / [HTTP API](https://docs.duckstring.com/reference/http-api) — full references

There are future plans for a hosted Catchment service at [duckstring.com](https://duckstring.com). If you're interested, please [get in touch](mailto:dev@duckstring.com).

## License

[Apache 2.0](LICENSE)
