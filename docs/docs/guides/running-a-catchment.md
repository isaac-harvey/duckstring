---
title: Running a Catchment
description: Start, connect to, and operate a Catchment.
---

# Running a Catchment

Everything else — deploying, triggering, querying — needs a [Catchment](../concepts/catchment.md) to talk to. This guide covers creating one locally, connecting to remote ones, and what operating it day-to-day looks like.

## Create a local Catchment

```bash
duckstring catchment init --name dev
```

This creates a Catchment named `dev`, registers it in your CLI config, offers to set it as the default, and starts the server in the foreground (`Ctrl+C` stops it). Defaults and their flags:

| Option | Default | Meaning |
|---|---|---|
| `--name`, `-n` | *(prompted)* | The name the Catchment is registered under |
| `--host` | `127.0.0.1` | Bind address |
| `--port`, `-p` | `7474` | Port — the web UI and API are served here |
| `--root` | `~/.duckstring/{name}` | Where the Catchment's data lives |
| `--key` | *(none — open)* | API key the server requires on every request; the CLI stores it and sends it automatically |
| `--yes`, `-y` | | Set as default without prompting |

Once created, start it again any time with:

```bash
duckstring catchment start dev
```

The server is fully restartable: state lives on disk, not in the process (see [Restart behaviour](#restart-behaviour)).

## Connect to a remote Catchment

A Catchment running elsewhere is registered by URL:

```bash
duckstring catchment connect --name prod --path https://catchment.example.com --key $PROD_KEY
```

`--key` is the server's API key (if it requires one); it is stored against the registration and attached to every request — including by the Ducks the server spawns. From then on `prod` works exactly like a local Catchment in every command. Local-vs-remote is a property of where the server runs, not of how you use it — start local, move to a hosted server later, and your commands don't change.

## Managing registrations

Registrations live in `~/.duckstring/config.toml` and are managed with:

```bash
duckstring catchment list                 # all registered Catchments (● marks the default)
duckstring catchment set-default prod    # change the default
duckstring catchment disconnect dev      # unregister (offers to delete local data; --purge skips the prompt)
```

Every command that talks to a Catchment accepts `--catchment`/`-c {name}`; without it, the default is used (and if exactly one Catchment is registered, it's implicitly the default).

## Authentication

A Catchment is open by default — fine on `127.0.0.1`. Beyond your machine there are two models, and which one applies depends on where the Catchment runs.

### Platform auth (hosted deployments)

If the Catchment runs behind a service that already gates requests — Posit Connect, an oauth2 proxy, a cloud platform's IAM — leave the Catchment itself open and let the platform do the auth. This works without configuration on two of the three surfaces:

- **The web UI** is same-origin, so the platform's login session (cookies) flows with every request automatically.
- **Ducks** are subprocesses next to the server, dialing it directly inside the sandbox — they never pass through the platform's gate.

Only the **CLI** enters through the front door, so it must present the platform's credential. Register it as custom headers attached to every request:

```bash
duckstring catchment connect --name prod --path https://connect.example.com/content/abc123/ \
    --header "Authorization: Key $POSIT_API_KEY"
```

`--header` is repeatable and takes any `'Name: value'` pair, so it covers whatever the platform in front expects. The UI also works when the Catchment is hosted **under a path prefix** (like Posit Connect's `/content/{guid}/`) — all of its asset and API references are relative.

### Built-in API key (self-hosted)

To expose a bare Catchment (a VM, a LAN box) without a platform in front, give it an API key:

```bash
duckstring catchment init --name prod --host 0.0.0.0 --generate-key
```

`--generate-key` mints a **three-tier key ladder**, prints all three once, and stores the **full** key against the registration so `catchment start prod` reuses it. The levels are a total order — each grants everything below it:

| Level | Can do | Hand it to |
|---|---|---|
| **read** | read & query data | a dashboard, a read-only consumer |
| **demand** | read + create demand (tap/wave/pulse/tide) + connect a downstream duct | a downstream Catchment's operator |
| **full** | everything: deploy, the control verbs, windows, ducts, key rotation | yourself / trusted operators |

With a key set, every `/api` request except the health check must carry `Authorization: Bearer {key}` and is rejected `401` if missing/invalid, or `403` if the key's level is too low for the route. Clients register a key once with `catchment connect --key`; the server's own Ducks authenticate on a **separate internal token** (so rotating a user key never disrupts a running Duck); the web UI prompts for a key on first visit and keeps it in the browser.

Only the keys' **hashes** are stored, so the plaintext is unrecoverable — keep the copy you were shown. Pass `--key` instead of `--generate-key` to supply a single full-access key of your own (the two are mutually exclusive), or set `DUCKSTRING_API_KEY` in the server's environment for the same single-key, full-access mode.

**Rotating keys.** Reroll without recreating the Catchment:

```bash
duckstring catchment rotate-keys                 # all three (asks first)
duckstring catchment rotate-keys --level demand  # just one level
```

The old key for each rerolled level stops working immediately; the new keys are printed once. If the full key is rerolled, the CLI updates your stored registration so your own commands keep working — re-distribute the read/demand keys to whoever holds them.

Transport security is yours to provide — put a keyed Catchment behind TLS (a reverse proxy) before sending the key over a network. Either way, keys and headers live in `~/.duckstring/config.toml`, which the CLI keeps private (`0600`).

## Hosting on a platform

Anywhere that runs an ASGI app can host a Catchment. The packaged entry is `duckstring.catchment.asgi:app`, configured entirely by environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `DUCKSTRING_STATE_ROOT` (alias: `DUCKSTRING_ROOT`) | `./.duckstring` | The local POSIX root for **hot state** (`duck.db`, ledgers, registries). The default is relative to the working directory; point it at a persistent path for durable state. **Must be a local path** — never an object store (SQLite/DuckDB need POSIX semantics). |
| `DUCKSTRING_DATA_ROOT` | *(under the state root)* | Where the data plane publishes/reads tables: a local path **or** an object-store / Volume URI (`s3://…`, `gs://…`, `abfss://…`, `/Volumes/…`). Credentials ride the URI query as `${env:NAME}` refs. See [Storage on a cloud node](#storage-on-a-cloud-node). |
| `DUCKSTRING_STATE_BACKUP_URI` | *(unset)* | Where Tier-1/2 hot-state checkpoints sync (object store / Volume / path), so an ephemeral / scale-to-zero node survives. Unset → no sync. |
| `DUCKSTRING_CHECKPOINT_INTERVAL` | `60s` | The Tier-1 (`duck.db`) backup cadence, e.g. `30s`. |
| `DUCKSTRING_API_KEY` | *(unset)* | The built-in API key. Leave unset when the platform already gates requests (see [Authentication](#authentication)). |
| `DUCKSTRING_CATCHMENT_URL` | *(unset)* | The Duck dial-back address. Normally unset: the Catchment learns its bound address from the first request it serves, and its Ducks dial that directly. |
| `DUCKSTRING_FLOCK_MODE` / `DUCKSTRING_FLOCK_ENGINE` / `DUCKSTRING_FLOCK_OOM_POLICY` | `off` / `athena` / `fail_up` | Catchment-wide default Flock posture (over-envelope offload). Per-Pond via `pond.toml` `[flock]` or `duckstring duck set`. Inert on a stock local Catchment (no engine configured). |
| `DUCKSTRING_DUCK_LAUNCHER` | *(unset)* | A `module:Class` import spec replacing the local subprocess worker launcher — how a hosting platform runs each Pond's worker elsewhere (its own container, another host). The worker still dials back over the same protocol and fetches its code from the Catchment. |
| `DUCKSTRING_DATA_PLANE` | `iceberg` | How Ponds publish their tables for each other — `iceberg` (default; snapshots + schema metadata) or `parquet` (whole-table snapshots, the lightest/offline opt-out). Both work on an object-store data root. See [the data plane](#the-data-plane). |

One rule applies everywhere: **run exactly one process of the app.** The Catchment is a single brain — one scheduler, one database, one set of Ducks. Multiple workers (a `--workers` flag, a platform's process autoscaling) would double-dispatch runs.

### A server or container

The simplest hosted Catchment is uvicorn behind whatever TLS proxy you already run (Caddy, nginx, Traefik):

```bash
DUCKSTRING_STATE_ROOT=/var/lib/duckstring DUCKSTRING_API_KEY=$KEY \
    uvicorn duckstring.catchment.asgi:app --host 127.0.0.1 --port 7474
```

or containerised, with the root on a volume:

```dockerfile
FROM python:3.13-slim
RUN pip install duckstring
ENV DUCKSTRING_STATE_ROOT=/data
VOLUME /data
EXPOSE 7474
CMD ["uvicorn", "duckstring.catchment.asgi:app", "--host", "0.0.0.0", "--port", "7474"]
```

```bash
docker run -p 7474:7474 -v duckstring-data:/data -e DUCKSTRING_API_KEY=$KEY my-catchment
```

Both of these use the built-in key; clients connect with `catchment connect --key`.

### Storage on a cloud node

A cloud node has two very different kinds of storage, and Duckstring splits along them:

- **Hot state** — `duck.db`, the `pond.db` ledgers, the `registry.duckdb` registries — needs POSIX semantics (byte-range writes, `fsync`, locking), so it lives on a **local disk** (`DUCKSTRING_STATE_ROOT`). **Never** put it on S3 or a Volume FUSE mount — SQLite-on-FUSE corrupts. If that local disk is **ephemeral** (a scale-to-zero container, Databricks Apps), set `DUCKSTRING_STATE_BACKUP_URI`: the Catchment syncs `duck.db` snapshots out and **restores automatically** on a fresh boot. Durability comes from syncing snapshots, never from relocating the live files.
- **Data blobs** — the published Parquet / Iceberg — are write-once / atomic-overwrite, so they belong in the **bucket or Volume** (`DUCKSTRING_DATA_ROOT`). Both data planes work there; Iceberg keeps its per-line catalog (`catalog.json`) and table metadata in the bucket alongside the data, read back over DuckDB's `httpfs`.

Object-store credentials are `${env:NAME}` references in the data-root URI query, resolved only at runtime (never stored); with no key the ambient credential chain (instance profile / managed identity) is used. **Run one Catchment per data root** — an external data root is held by a writer lease, so a second Catchment on the same lake refuses to start (give each a distinct prefix to share a bucket; `DUCKSTRING_FORCE_TAKEOVER=1` overrides). All of this is **operational config** set at the Catchment, never in `pond.toml` (where your output lands is the Catchment's concern, like [windows](windows.md) and [Spouts](../reference/cli.md#duckstring-spout--egress-bindings)); `duckstring pond deploy` is unchanged.

The three recipes below are the common shapes.

### Recipe: a Catchment backed by S3

A VM / container with a local disk and the data plane in an S3 bucket.

1. **Choose the two roots** — state on local disk, data in the bucket:
   - `DUCKSTRING_STATE_ROOT=/var/lib/duckstring` (a persistent local path; ephemeral is fine if you add the backup in step 4)
   - `DUCKSTRING_DATA_ROOT=s3://acme-lake/duckstring?region=eu-west-1`
2. **Provide credentials.** Reference env vars in the URI query and export them, or rely on the instance role:
   ```bash
   export AWS_KEY=… AWS_SECRET=…
   DUCKSTRING_DATA_ROOT='s3://acme-lake/duckstring?region=eu-west-1&key_id=${env:AWS_KEY}&secret=${env:AWS_SECRET}'
   ```
   With no `key_id`/`secret` in the query, the AWS credential chain (instance profile, `AWS_*` env, `~/.aws`) is used.
3. **Start it** — env-configured ASGI:
   ```bash
   DUCKSTRING_STATE_ROOT=/var/lib/duckstring \
   DUCKSTRING_DATA_ROOT='s3://acme-lake/duckstring?region=eu-west-1' \
   DUCKSTRING_API_KEY=$KEY \
       uvicorn duckstring.catchment.asgi:app --host 0.0.0.0 --port 7474
   ```
   …or from the CLI, which stores the same settings in the registration so `catchment start prod` reuses them:
   ```bash
   duckstring catchment init --name prod --host 0.0.0.0 --port 7474 \
       --root /var/lib/duckstring \
       --data-root 's3://acme-lake/duckstring?region=eu-west-1' \
       --generate-key
   ```
4. **(Ephemeral disk only) add a state backup** so a redeploy / scale-to-zero survives:
   ```bash
   DUCKSTRING_STATE_BACKUP_URI=s3://acme-lake/duckstring-state
   DUCKSTRING_CHECKPOINT_INTERVAL=30s
   ```
   A fresh node restores from it on boot; `duckstring catchment restore --from <uri> --path <state-root>` does it by hand.
5. **Connect a client:** `duckstring catchment connect --name prod --path https://… --key <demand-key>`.

### Recipe: Databricks Apps with a Volume

Databricks Apps gives an **ephemeral** local disk and an HTTPS-gated app; a Unity Catalog **Volume** is the durable store. State goes on the ephemeral disk + a Volume backup; data goes on the Volume (its FUSE path is mounted in the app container and treated as a local data root).

1. **Pick a Volume** you can write, e.g. `/Volumes/main/duckstring/`, with subpaths for data vs the state backup.
2. **Build the app bundle** — three files:
   ```text
   duckstring-app/
   ├── app.py            # from duckstring.catchment.asgi import app
   ├── requirements.txt  # duckstring
   └── app.yaml
   ```
   `app.yaml`:
   ```yaml
   command: ["sh", "-c", "uvicorn duckstring.catchment.asgi:app --host 0.0.0.0 --port $DATABRICKS_APP_PORT"]
   env:
     - name: DUCKSTRING_STATE_ROOT
       value: /tmp/duckstring                  # the app's ephemeral local disk (POSIX)
     - name: DUCKSTRING_DATA_ROOT
       value: /Volumes/main/duckstring/data    # the Volume (FUSE) — durable data plane
     - name: DUCKSTRING_STATE_BACKUP_URI
       value: /Volumes/main/duckstring/state   # hot-state snapshots survive scale-to-zero
     - name: DUCKSTRING_CHECKPOINT_INTERVAL
       value: "30s"
   ```
3. **Deploy** with the Databricks CLI (sync the source to a workspace path first, then deploy the app):
   ```bash
   databricks sync ./duckstring-app /Workspace/Users/you/duckstring-app
   databricks apps deploy duckstring --source-code-path /Workspace/Users/you/duckstring-app
   ```
   Databricks injects `DATABRICKS_APP_PORT`; the app must bind it — hence the `sh -c` command.
4. **One process.** Apps runs a single instance — exactly what the Catchment needs; don't scale it out.
5. **Auth.** The platform gates with its own login, so leave `DUCKSTRING_API_KEY` unset and connect the CLI through the platform credential as a header:
   ```bash
   duckstring catchment connect --name prod --path https://<app-url> \
       --header "Authorization: Bearer $DATABRICKS_TOKEN"
   ```

When Apps idles the instance to zero the ephemeral state root is lost; on the next start the Catchment **restores `duck.db` from the Volume backup automatically**, so the engine + history come back (registries rebuild on demand — recompute, not data loss; the durable incremental state is in the Volume). If you'd rather avoid the FUSE mount's weaker rename atomicity, point `DUCKSTRING_DATA_ROOT` at the Volume's **external location** (`s3://…` / `abfss://…`) instead of the `/Volumes/…` path.

### Recipe: a gated platform (Posit Connect)

Platforms that host an ASGI app behind their own login — Posit Connect, an oauth2-proxied PaaS — take a small bundle and gate it for you.

1. **Build the bundle** — two files:
   ```text
   catchment-deploy/
   ├── app.py            # from duckstring.catchment.asgi import app
   └── requirements.txt  # duckstring
   ```
2. **Deploy:** `rsconnect deploy fastapi . --title "Duckstring Catchment"`.
3. **Force a single process.** In the content settings set **Max processes = 1** and **Min processes = 1** (standing Waves/Tides need the scheduler ticking between visits), with a generous idle timeout.
4. **Auth.** Leave `DUCKSTRING_API_KEY` unset — the platform's login is the gate. The UI works through it (and under the content path prefix); the CLI connects with the platform credential as a header:
   ```bash
   duckstring catchment connect --name prod --path https://connect.example.com/content/<guid>/ \
       --header "Authorization: Key $POSIT_API_KEY"
   ```
5. **Durable state.** Connect replaces the content directory on every redeploy, so either keep the data plane external (`DUCKSTRING_DATA_ROOT` on S3, durable on its own) or carry the state root in the bundle — see [Surviving a redeploy](#surviving-a-redeploy-of-the-catchment-app).

### Surviving a redeploy of the Catchment app

Platforms like Connect **replace the content directory on every redeploy**, and the default state root lives inside it — so a redeploy of the Catchment app wipes deployed Ponds, history, and (when the data plane is local) data. The defaults are arranged so state can ride along in the bundle:

```bash
cd catchment-deploy/
duckstring catchment download -c prod      # pulls the state root into ./.duckstring (after a size confirmation)
rsconnect deploy fastapi .                 # redeploy WITH the state in the bundle
```

The new deployment starts from exactly the downloaded state. `catchment download` streams the whole **state** root with consistent SQLite snapshots; do it while the Catchment is quiescent (no runs in flight) so the DuckDB registries are coherent too. It doubles as a plain backup. With an external `DUCKSTRING_DATA_ROOT` the data plane is durable on its own and is **not** in the download — and with `DUCKSTRING_STATE_BACKUP_URI` set you don't need the download/redeploy dance at all, since the node restores its state automatically.

## What's in the root directory

The `--root` directory is the Catchment's entire state:

```text
~/.duckstring/dev/
├── duck.db                      # the Catchment database: graph, freshness, triggers, run history
└── ponds/
    └── sales/
        ├── 1.0.0/               # each deployed version's source, as uploaded
        └── m1/                  # runtime state of major line 1 (m2/ if a 2.x is live, …)
            ├── registry.duckdb  # the line's live working database
            ├── data/            # exported Parquet snapshots — the published output
            │   └── sale_line.parquet
            └── pond.db          # the line's worker run ledger
```

Back up the root and you've backed up the Catchment. Paths inside the database are relative to the root, so the directory is relocatable.

## The data plane

A Pond publishes its output tables into its line's `data/` directory; Sinks and [queries](querying-data.md) read from there. How that publishing happens is the **data plane**, set by `DUCKSTRING_DATA_PLANE`:

- **`iceberg`** (default) — an [Apache Iceberg](https://iceberg.apache.org/) base layer (a per-line catalog `catalog.json` + table metadata, **over Parquet data files** — it's a metadata/snapshot layer, not a file-format change). Each run is an overwrite commit recorded as a snapshot stamped with the run's [freshness](../concepts/freshness.md). It ships with Duckstring (`pyiceberg`; a lightweight file-backed catalog means **no SQLAlchemy**). DuckDB's `iceberg` extension is loaded on first read (a one-time download — so a fully offline Catchment should either pre-cache it or use `parquet`). A flat `{table}.parquet` copy is still written alongside, so [ducts](connecting-catchments.md) and direct downloads are unchanged.
- **`parquet`** — each table is one `{table}.parquet` file, overwritten wholesale per run. The lightest option (no catalog, no extension); the right choice for an offline Catchment or when you don't need snapshots.

Both are **behaviour-neutral** — same overwrite-per-run semantics, same query results — and both work whether the data plane is local or on an [object store / Volume](#storage-on-a-cloud-node) (Iceberg keeps its catalog and metadata alongside the data there). When the data plane is **under the state root**, `catchment download` captures it (catalog included; download while [quiescent](#surviving-a-redeploy-of-the-catchment-app)); when it's an external `DUCKSTRING_DATA_ROOT`, the data is durable on its own and download covers only the state root.

The `_duckstring_*` column-name prefix is **reserved** for framework system columns on either plane; a published table using it is rejected at write.

## Monitoring

```bash
duckstring status            # live view of every active Pond
duckstring status sales      # one Pond and its upstream lineage
duckstring status --once     # single snapshot, no live updates
```

The live view polls the Catchment and shows each Pond's state (idle / queued / running / failed / killed / blocked), freshness, and standing trigger, staying open until `Ctrl+C`. The [web UI](web-ui.md) at the Catchment's URL shows the same state graphically.

### Alerts

To be told about a problem instead of watching for it, add a notification **channel** — a binding to a webhook (Slack or generic) or email that fires on failures and, importantly, on **staleness**:

```bash
duckstring alert add --to 'https://hooks.slack.com/services/${secret:SLACK_HOOK}'
duckstring alert add --to 'mailto:oncall@example.com?smtp=smtp.example.com:587' --pond sales --stale 1h
duckstring alert ls
duckstring alert test <name>     # send a test notification
```

A channel is catchment-wide by default, or scoped to one Pond with `--pond`. The `--stale` bound is the one most pipelines are missing: a pipeline can be **green with zero failures and still wrong** because nothing refreshed it — a freshness alert catches that. Failures dedup by root cause (one failed Source that blocks downstream Ponds alerts once, not once per blocked Pond), and a broken channel never affects a Pond — deliveries are retried and audited in `duckstring alert log`. Full details in the [CLI reference](../reference/cli.md#duckstring-alert--notification-channels); channels are also managed from the [web UI](web-ui.md).

### Metrics

The Catchment exposes a Prometheus scrape endpoint at **`/metrics`** (at the root, unauthenticated — the exporter convention; network-restrict it if your Pond names are sensitive). Point Prometheus at it to graph and alert in your own Grafana/Alertmanager:

```
duckstring_pond_freshness_lag_seconds{pond="sales",major="1"}   # seconds since last fresh (the headline)
duckstring_pond_failed{pond="sales",major="1"}                  # 0/1
duckstring_pond_failures_total{pond="sales",major="1"}          # cumulative failed runs
duckstring_spout_delivery_lag_seconds{spout="sales#warehouse"}  # egress lag
duckstring_alert_deliveries_total{status="failed"}              # notification health
```

## Restart behaviour

The Catchment is designed to be stopped and started without ceremony:

- **State restores from disk.** On startup it rebuilds the engine state — freshness, demand, triggers, windows, failure states — from its database.
- **Interrupted runs resume.** Pond Runs that were in flight are re-dispatched; each Pond's worker reconciles against its own ledger and re-runs only the Ripples that hadn't completed.
- **Workers tolerate the gap.** Worker processes survive Catchment downtime: they finish their in-flight runs independently, buffer their progress events, and replay them (idempotently) when the Catchment returns.

The practical upshot: restarting the Catchment mid-pipeline loses nothing and re-computes almost nothing. Details in [Architecture](../reference/architecture.md).

## Hosted Catchments

There are future plans for a managed Catchment service at [duckstring.com](https://duckstring.com) — if you're interested, [get in touch](mailto:dev@duckstring.com).
