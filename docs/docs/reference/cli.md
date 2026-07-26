---
title: CLI
description: Full command reference for the duckstring / ds CLI.
---

# CLI Reference

The CLI installs as both `duckstring` and `ds`; the two are identical. Every command and group prints detailed help with `--help`, `duckstring --version` prints the installed version, and shell completions install with `duckstring --install-completion`.

## Common options

Most commands that talk to a Catchment share these:

| Option | Meaning |
|---|---|
| `--catchment`, `-c {name}` | Target a registered Catchment (default: the configured default; if exactly one is registered, it's implicit) |
| `--major`, `-m {int}` | Target a specific major version line (default: the highest deployed) |
| `--version`, `-v {semver}` | Target a specific version, e.g. `1.2.3` — must be its major line's currently selected version |
| `--silent` | Submit without opening the live status view |
| `--watch` | Keep the status view open even after a one-shot settles |

## `duckstring catchment` — work with Catchments

| Command | Description |
|---|---|
| `catchment init -n {name} [--host H] [-p PORT] [--root DIR] [--key KEY \| --generate-key] [--header 'N: v']… [-y] [--no-start]` | Create and register a local Catchment, then start its server. Defaults: host `127.0.0.1`, port `7474`, root `~/.duckstring/{name}`, no API key (open). `--generate-key` mints the read/demand/full key ladder, prints all three once, and stores the full key (mutually exclusive with `--key`, which sets a single full-access key). Offers to set as default (`-y` accepts). `--no-start` registers (and mints keys) without starting the server — for scripted provisioning where systemd or a container runtime owns the process via `catchment start`. |
| `catchment start {name}` | Start the server for a registered local Catchment. |
| `catchment rotate-keys [-c NAME] [--level read\|demand\|full]… [-y]` | Reroll a Catchment's access keys (default all three; `--level` repeatable for a subset), printing the new keys once. The old key for each rerolled level stops working; the internal Duck token is untouched. If the full key is rerolled, the stored registration is updated. Requires a full-access key. |
| `catchment connect -n {name} --path {url} [--key KEY] [--header 'N: v']… [-y]` | Register a remote Catchment by URL; `--key` stores its API key (sent as a Bearer header — use a `demand` key for a downstream that only solicits and draws), `--header` stores arbitrary headers for platform auth (e.g. `'Authorization: Key …'` for Posit Connect) — both attached to every request. |
| `catchment list` | List registered Catchments; `●` marks the default. |
| `catchment download [-c NAME] [--path DIR] [-y]` | Download the Catchment's entire root (database, artifacts, data, ledgers) into a local directory — default `./.duckstring`, so it drops straight into a platform deploy bundle. Shows the state size and asks before transferring (`-y` skips); streams with a progress bar. |
| `catchment reset [-c NAME] [--clear-history] [-y]` | Reset the **whole Catchment** to a fresh-deploy state — scrub every Pond's registry, published data, and ledger and rewind all freshness — **keeping** the deployed code, operational config, secrets, and keys. The sanctioned replacement for deleting `.duckstring`; stop-the-world (every worker restarts). Ponds rebuild lazily from the Inlets down. |
| `catchment set-default {name}` | Set the default Catchment. |
| `catchment disconnect {name} [--purge]` | Unregister; for local Catchments, offers to delete the data directory (`--purge` deletes without asking). |
| `catchment open {pond} [-m M] [--tap-on-get]` | Mark a Pond open to demand from any source; `--tap-on-get` makes a [query](../guides/querying-data.md) read fire a Tap (snapshot served first). |
| `catchment close {pond} [-m M]` | Remove a Pond's open flag. |

Registrations and the default live in `~/.duckstring/config.toml`.

### `duckstring catchment duct` — draw Ponds from other Catchments

Conduits that draw a Pond from an upstream Catchment into the consuming one (`-c`, default). See [Connecting Catchments](../guides/connecting-catchments.md). `{upstream}` is a registered Catchment name.

| Command | Description |
|---|---|
| `catchment duct create {upstream} [--sync] [-c]` | Open a duct from `{upstream}` into the consuming Catchment (forwards the upstream's URL, credentials, and identity). `--sync` then draws every Pond it exposes. |
| `catchment duct destroy {upstream} [-c]` | Remove a duct and all the Pond Draws it created. |
| `catchment duct add {upstream} {pond} [-m M] [--incremental] [-c]` | Draw one upstream Pond (materialises a Pond Draw). `--incremental` is reserved for delta transfer (not yet implemented). |
| `catchment duct remove {upstream} {pond} [-m M] [-c]` | Stop drawing a Pond. |
| `catchment duct sync {upstream} [-c]` | Draw every Pond the upstream currently exposes. |
| `catchment duct ls [-c]` | List ducts and the Ponds each draws. |

## `duckstring pond` — manage Pond projects

| Command | Description |
|---|---|
| `pond init {name}` | Scaffold a new Pond project in the current (empty) directory. |
| `pond demo [--ripple \| --trickle \| --tpcds \| --gharchive \| --dbt]` | Create a demo pipeline as subdirectories. Default (or `--ripple`): the four-Pond overwrite-Ripple set (`transactions`, `products`, `sales`, `reports`). `--trickle`: the four-Pond incremental-[Trickle](../guides/trickle.md) set (`orders`, `catalog`, `priced`, `revenue`). `--tpcds` / `--gharchive`: six-Pond real-data Trickle pipelines — TPC-DS (generated via DuckDB's `tpcds` extension) and GHArchive (streamed from the public archive) — each with a cross-Pond join and **two independent Outlets** to run at different cadences. `--dbt`: a [dbt-mode Pond](../guides/dbt.md) (`shop_analytics`, a dbt project as a Pond) plus its Source (`shop_orders`) — needs the dbt extra (`pip install 'duckstring[dbt]'`). |
| `pond hydrate [-s SOURCE] [--from-catchment] [-c NAME]` | Materialise the project's [Puddles](../guides/local-testing.md) into `puddles/`. Sources without a definition are skipped with a warning; `--from-catchment` fills them from the Catchment's exported tables; `-s` restricts to specific Sources. |
| `pond run [--ripple NAME] [--fresh]` | Execute the Pond locally against its hydrated Puddles, output to `puddles/out/`. `--ripple` runs a single Ripple against the last run's state; `--fresh` ignores a self-puddle seed. |
| `pond deploy [-c NAME] [--git REF] [-y] [--all]` | Deploy the current Pond project (reads `pond.toml`). `--all` deploys every subdirectory containing a `pond.toml`; `--git` deploys from a git ref (branch/tag/commit) of the project's `origin` remote instead of uploading the working tree; `-y` skips confirmations. |
| `pond remove {name} [-m M] [-c NAME] [-y]` | Remove (retire) a deployed Pond major line — deletes its data, live state, and on-disk runtime plus its own Spouts and alert channels, **keeping** its deployment record + run history (a redeploy restores it). `--major` picks the line (default: highest deployed). Downstream Ponds that read it block on the missing Source until fixed. Requires the line idle with no demand (`control sleep` it first). |

## `duckstring puddle` — inspect local test data

See [Local Testing](../guides/local-testing.md). All three operate on the current project's `puddles/` directory, no Catchment involved.

| Command | Description |
|---|---|
| `puddle ls` | List hydrated Puddles and run output, with row counts, size, and age. |
| `puddle show {pond}.{table} [-n N]` | Preview a table (run output wins when a self-puddle shares the name). |
| `puddle query {sql}` | Run SQL across everything local — snapshots as `"{source}"."{table}"`, output under the Pond's own name. |

## `duckstring trigger` — demand signals

See [Triggers](../guides/triggers.md) for semantics.

| Command | Description |
|---|---|
| `trigger tap {pond}` | Pull once — a single resupply from Sources. |
| `trigger pulse {pond}` | Push once — run the lineage through to the Pond, to now. |
| `trigger wave {pond}` | Standing pull — free-run at the bottleneck's pace. |
| `trigger tide {pond} {bound}` | Standing push — keep staleness under `bound` (compound durations: `30s`, `90m`, `1d`, `1h30m`). |
| `trigger remove {pond}` | Remove the standing Wave/Tide (existing work drains). |

One-shots (tap/pulse) open the live status view and close when the target settles; standing triggers keep it open until `Ctrl+C` (the trigger persists).

### `duckstring trigger window` — availability windows

See [Windows](../guides/windows.md). The Pond name comes directly after `window`:

| Command | Description |
|---|---|
| `trigger window {pond} add -n {name} -e {every} [-s START] [-d DUR] [-o DAYS] [-u UNTIL]` | Add a recurring window. `--every` is a single-unit interval (`10s`, `12h`, `1d`, `1w`); `--start` is ISO 8601 or `HH:MM` UTC (default `00:00` today); `--duration` accepts compound durations and defaults to `--every` (back-to-back); `--on` restricts weekdays (`MON,WED,FRI`); `--until` expires the rule. |
| `trigger window {pond} list` | List the Pond's windows. |
| `trigger window {pond} remove {name}` | Remove a window rule. |

## `duckstring spout` — egress bindings

Publish a Pond's output to external systems. A Spout is operational config (persisted, survives redeploys), not declared in `pond.toml`. Credentials go in the destination URI as `${env:NAME}` (process environment) or `${secret:NAME}` ([secret store](#duckstring-secret--credential-store)) references, resolved only at egress time — never stored in the binding or logged. After each successful Pond Run, the egress worker delivers the Pond's published tables to the destination: by default (`--mode auto`/`full`) as snapshot Parquet (`{prefix}/{table}.parquet`); with **`--mode append`** an object-store destination instead **mirrors the table's published Duckstring collection** (the per-run parts, changelog and tiers, and the sidecar) — each delivery ships only the new files, so it stays cheap for large [Trickle](../guides/trickle.md) tables, and the destination is directly readable as a Duckstring layout (by another Catchment, or anything that reads the parts).

`file://`, `s3://`, `gs://`, and `postgres://` work today. Object-store credentials go in the URI query: `s3://bucket/prefix?key_id=${env:AWS_KEY}&secret=${env:AWS_SECRET}&region=us-east-1` (also `endpoint`, `url_style`, `use_ssl`, `session_token`); `s3://` with no key falls back to the AWS credential chain (env / instance profile); `gs://` needs HMAC `key_id`+`secret`.

`postgres://user:${env:PGPASS}@host/db?schema=public` syncs **incrementally**: a [merge Trickle](../guides/trickle.md)'s changelog applies as upserts + deletes inside one transaction, exactly-once. A transactional destination **requires a primary key**, so the source table must be a merge Trickle — a plain/overwrite table is refused at creation with a signpost error.

| Command | Description |
|---|---|
| `spout add {pond} --to {uri} [--table T \| --all] [--mode auto\|full\|append] [--name N]` | Bind a Spout. `--to` is a `file://`/`s3://`/`gs://`/`postgres://` URI (credentials as `${env:NAME}`); `--table` egresses one table, default all; `--mode` defaults `auto`; `--name` defaults to the table (or scheme), `-2`/`-3` on collision. |
| `spout ls {pond}` | List the Pond's Spouts with their delivery watermark and state (ok / retrying / failed). |
| `spout rm {pond} {name}` | Remove a Spout. |
| `spout resync {pond} {name}` | Force a full re-egress (clears the watermark + any failure). |
| `spout sleep \| wake {pond} {name}` | Disarm / re-arm the Spout's standing Wake (it delivers on each source advance). |
| `spout force {pond} {name}` | Re-arm and re-deliver the current freshness now. |
| `spout kill \| clear {pond} {name}` | Park the Spout (terminal) / clear a failed-or-killed Spout. |

A Spout is a **real Pond** hanging off its source with a standing **Wake** (the egress dual of a [Pond Draw](../guides/connecting-catchments.md)) — it delivers whenever the source's freshness advances, never pulls on the source, and never blocks anything (its runs and failures are its own, with full run history + tracebacks in [run history](#duckstring-status)). The **Control** verbs above apply to it; the **Demand** verbs (tap/wave/pulse/tide) do not. To **throttle** delivery to a cadence, put a [window](#duckstring-trigger--demand-triggers) on the Spout — it's a Pond, so `trigger window {source}#{spout} add -e 1h …` (or the UI) works directly: it delivers at most once per window.

## `duckstring secret` — credential store

A **write-only**, catchment-wide store for the credentials a Spout references as `${secret:NAME}`. An alternative to `${env:NAME}` when you'd rather not manage the Catchment's process environment. Secrets are stored at the catchment root (private file, `0600`), **never returned** by the API or CLI (you can list names, not values), and **excluded** from a [`catchment download`](#duckstring-catchment) bundle. Managing secrets requires **full access**.

| Command | Description |
|---|---|
| `secret set {name}` | Store (or overwrite) a secret. The value is **prompted and hidden** — it never appears in your shell history or process arguments. Name must match `[A-Za-z_][A-Za-z0-9_]*`. |
| `secret ls` | List secret names (and when each was set) — never the values. |
| `secret rm {name}` | Remove a secret. |

The value **is** sent to the Catchment over the wire when you set it (an HTTPS POST body) — use TLS, or set it via the server's environment with `${env:NAME}` instead. Encryption-at-rest is not applied: the store is a private plaintext file, secured by filesystem permissions. The same names appear as a picker in the web UI's Spout add form (under the 🔑 menu beside the catchment name).

## `duckstring alert` — notification channels

Deliver failures and staleness to the channels a team already watches. A **channel** is operational config (persisted, survives redeploys), not declared in `pond.toml`. It fires on the events you subscribe it to — `failure` (a Pond Run gave up), `contract` (a breaking schema change), `spout` (an egress delivery failed), `recovery` (a failed Pond/Spout cleared), and `freshness` (a Pond stayed stale past an SLA) — and **root-cause dedup** means one failed Source that blocks twenty downstream Ponds pages you once (about the root, with the blocked names as blast radius), not twenty times. Credentials in the destination URI are `${env:NAME}`/`${secret:NAME}` references, resolved only at send time. Managing channels requires **full access**.

Two destinations work today: a **webhook** (`https://…`/`http://…`, a Slack-incoming-webhook-compatible JSON POST — also any generic receiver) and **email** (`mailto:you@example.com?smtp=host:587&from=alerts@example.com`; SMTP settings from the URI query or the `DUCKSTRING_SMTP_*` environment). A channel may also subscribe to the machine-consumer kind **`openlineage`** (`--on openlineage` — never part of `all`): each completed Pond Run then posts a standard OpenLineage RunEvent to the destination verbatim, which is how Duckstring [feeds a data catalog](../guides/lineage.md#feeding-a-catalog).

| Command | Description |
|---|---|
| `alert add --to {uri} [--pond N [--major M]] [--on failure,…\|all] [--stale 1h] [--renotify 6h] [--name N]` | Add a channel. `--to` is an `https://`/`http://`/`mailto:` URI; `--pond` scopes it to one Pond line (`--major` picks the major, default: the Pond's highest deployed major; omit `--pond` for catchment-wide); `--on` is the event kinds (default `all`); `--stale` sets a freshness SLA (e.g. `1h`, `30m`) — required for `freshness` to fire; `--renotify` repeats the alert at that interval while the failure/staleness persists (default: once per episode); `--name` defaults to the scheme/scope. |
| `alert ls` | List channels with their scope, events, SLA, and destination. |
| `alert rm {name}` | Remove a channel. |
| `alert test {name}` | Send a test notification through the channel (validates connectivity + credentials). |
| `alert log [--limit N]` | Recent deliveries (channel, kind, pond, status, error) — the audit trail. |

**Freshness is the headline.** A pipeline can be green with zero failures and still be *wrong* because nothing has refreshed it — a `--stale` channel is how you find out. By default a channel fires **once per episode** (and once on recovery); add `--renotify` if a webhook that fires once and then stays silently red for a week isn't enough — recovery still fires exactly once either way. A delivery failure never affects a Pond: it is retried and, if a channel stays broken, parked as `failed` in `alert log`, never cascaded. Channels are also managed from the web UI — a catchment-wide **Alerts** menu (beside 🔑 Secrets) and a per-Pond **Alerts** section in the sidebar. See also the Prometheus [`/metrics`](../guides/running-a-catchment.md#monitoring) endpoint.

## `duckstring control` — execution & health

See [Control](../guides/control.md) and [Fault Tolerance](../guides/fault-tolerance.md).

| Command | Description |
|---|---|
| `control wake {pond}` | Run once when Sources hold fresher data (waits for it; no upstream solicit). Clears failed/killed. |
| `control force {pond}` | Recompute now at current freshness; doesn't propagate downstream. Clears failed/killed. |
| `control refresh {pond} [--clear]` | Flag the Pond so its *next* run is a cold wipe-and-rebuild (full recompute, clears the changelog so downstream reloads). Lazy — nothing runs now. `--clear` un-flags. See [Trickle](../guides/trickle.md). |
| `control repair {ponds}... [--downstream]` | Force-rebuild a **connected** set of Ponds now, in dependency order (each reads its freshly-rebuilt parents). For an immediate fix when no new upstream run is coming. `--downstream` extends the set to all descendants; a disconnected set (a skipped Pond in a sequence) is rejected. |
| `control reset {pond} [--clear-history] [-y]` | Reset a Pond to a fresh-deploy state — scrub its registry, published data, and ledger and rewind its freshness — **keeping** its code, operational config, and demand. Lazy: nothing runs now; it rebuilds from scratch when next demanded. Requires the Pond idle. |
| `control sleep {pond} [--upstream]` | Clear all demand (started runs complete). `--upstream` also sleeps every ancestor. |
| `control kill {pond}` | Terminate the Pond's worker and cancel its run; parks the Pond `killed` until wake/force/clear. |
| `control clear {pond}` | Reset a failed/killed Pond to idle and unblock downstream, without running. |
| `control failure-budget {pond} [-i N] [-o N]` | Show (no flags) or set the retry budgets: `--immediate` Ripple retries per run, `--on-change` Pond Runs retried as Sources update. |

## `duckstring duck` — per-Pond compute config

```bash
duckstring duck show [pond]
duckstring duck set {pond} [--duck catchment|POOL|dedicated] [--flock off|upgrade|always] \
                           [--engine NAME] [--oom fail_up|fail] [--instance-type T] [--auto-stop] [--clear]
duckstring duck pool ls
duckstring duck pool add {name} [--provider fargate|ec2] [--cpu N --memory MiB] [--instance-type T] \
                                [--min N] [--max N] [--keep-warm N] [--idle-timeout S]
duckstring duck pool rm {name}
```

Where a Pond's worker (Duck) runs and its over-envelope offload posture (the **Flock**). The Duck target is the Catchment's own box (`catchment`), a named **Duck Pool**, or a `dedicated` box; there is no abstract size — sizing is the pool's Fargate `cpu`/`memory` (or an EC2 `instance_type`). A pool picks its provider: **`fargate`** (default — fast serverless containers) or **`ec2`** (the escape hatch for big/GPU jobs). Built-in **`S`/`M`/`L`/`XL`** preset pools (Fargate) always exist, so `duck = "M"` works with zero setup — and Duckstring Cloud ships the same names, so a project transfers seamlessly. These may be **declared in `pond.toml`** (`[pond] duck`, `[flock] mode`/`engine`/`oom_policy`) and are re-read on every redeploy; an operator override set here **coalesces over the declaration** and survives redeploys (`--clear` reverts to the declared config, else the Catchment default). Inert on a stock local Catchment — the local worker is whatever the host is; pools/dedicated boxes are acted on by a remote launcher once **cloud is enabled** (a remote data root + AWS creds, under Options → Cloud or `duckstring catchment settings`).

## `duckstring status` — live monitor

```bash
duckstring status [pond] [-c NAME] [--once]
```

Live view of deployed Ponds: state, freshness, staleness, and standing triggers — open until `Ctrl+C`. With a `pond` argument, shows only that Pond and its upstream lineage. `--once` prints a snapshot and exits; `-m`/`-v` narrow a named Pond to one major line.

## `duckstring lineage` / `trace` — lineage & provenance

See [Lineage](../guides/lineage.md).

```bash
duckstring lineage [pond] [-t TABLE] [--columns] [-m M] [-c NAME]
duckstring trace {pond}.{table} [--where "id = 42"] [-m M] [-c NAME]
```

`lineage` prints what each Ripple actually read and wrote on its latest run (observed at the call —
exact, never inferred); `--columns` adds the deploy-captured column derivations (which source columns
each output column comes from; `opaque` where unprovable — install `duckstring[lineage]` to resolve
`.sql()` outputs). `trace` is row-level provenance: which run produced the matching row(s), its version
and timings, and the input window `(previous_f, f]` each Source was read over.

## `duckstring get` / `query` — data access

See [Querying Data](../guides/querying-data.md).

```bash
duckstring get {pond} {ripple} [--path DIR]
```

Download a Ripple's published output (default destination `./ponds/{pond}/{ripple}/`).

```bash
duckstring query {pond} [ripple] [--sql SQL | --sql @file.sql]
                 [--csv F | --json F | --parquet F] [--path DIR]
```

Run SQL against the Pond's exported tables. With just a `ripple` argument: `SELECT * FROM {pond}.{ripple} LIMIT 10`. Without a format flag, results print to the terminal; with one, they're written to `./ponds/{pond}/[{ripple}/]{filename}` or `--path`.

```bash
duckstring objects {pond}                     # list a Pond's non-tabular Objects
duckstring get-object {pond} {name} [-o PATH]  # download one (a file, or a directory Object unzipped)
```

List / download a Pond's [Objects](python-api.md#objects--non-tabular-outputs) — models, blobs, and other non-tabular outputs. A single-file Object writes to `./{name}` (or `--out`); a directory Object unzips into it.

```bash
duckstring delete-table {pond} {table} [-y]    # delete a table (data + state) — no run, stays gone
duckstring delete-object {pond} {name} [-y]     # delete an Object (returns only if a Ripple rewrites it)
```

Delete one published output from a Pond (full access; the Pond must be idle). A table delete removes its data **and** registry state **now** — no run, no freshness change; it reappears only when the Pond next genuinely runs, rebuilt whole if the code still produces it (an append Trickle warns first — its history is dropped). An Object delete removes it directly.
