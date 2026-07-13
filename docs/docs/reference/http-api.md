---
title: HTTP API
description: The Catchment's REST surface.
---

# HTTP API Reference

Everything the CLI and [web UI](../guides/web-ui.md) do goes through this API, served by the Catchment under `/api`. All timestamps are UTC ISO-8601 strings; all bodies are JSON unless noted.

When the Catchment is started with API keys (`duckstring catchment init --key …` / `--generate-key`, or the `DUCKSTRING_API_KEY` environment variable), every `/api` request except `/api/health` must carry one — `Authorization: Bearer {key}` — and is `401` when missing/invalid. Keys come in a total-ordered ladder (**read ⊂ demand ⊂ full**); each route declares a minimum level, and a valid key whose level is too low gets `403`. Read routes (status, runs, data, draw) need `read`; demand routes (tap/wave/pulse/tide, the duct connection) need `demand`; deploy, the control verbs, windows, ducts and key rotation need `full`. A single `--key`/`DUCKSTRING_API_KEY` means `full`. The CLI sends the credentials registered against the Catchment (`catchment connect --key …`, or arbitrary `--header` pairs for a platform gate in front) automatically; the web UI prompts for a key on a `401`. The worker (`/api/duck/*`) channel uses a separate internal token, not a user key. See [Authentication](../guides/running-a-catchment.md#authentication).

## Health

```
GET /api/health
```

Returns `{"status": "ok"}` when the Catchment and its database are reachable.

## State download

```
GET /api/catchment/usage
```

`{"total_bytes", "file_count", "archive_bytes"}` — the root's state size and a close estimate of the archive below (the CLI's size confirmation and progress total).

```
GET /api/catchment/archive
```

Streams the whole Catchment root as an uncompressed tar (`application/x-tar`). SQLite files are added as consistent point-in-time snapshots (WAL content included); DuckDB registries are copied as-is, so download while quiescent if registry coherence matters. This is what `duckstring catchment download` consumes.

```
GET /api/catchment/identity
```

`{"id", "name"}` — this Catchment's stable UUID (minted once on first start) and optional display name. How a downstream resolves cross-Catchment identity over a [duct](../guides/connecting-catchments.md).

```
POST /api/catchment/keys/rotate     {"levels": ["read", "demand", "full"]?}    (full)
```

Reroll the access keys for the given levels (omit `levels` for all three), returning `{"keys": {level: plaintext}}` **once** — only hashes are stored. The internal Duck token is untouched. Backs `duckstring catchment rotate-keys`.

## Deploy

```
POST /api/deploy
```

Two forms, distinguished by content type:

- **Upload** (`multipart/form-data`): fields `name`, `version`, `type`, and `pond` — a zip of the project. This is what `duckstring pond deploy` sends.
- **Git** (`application/json`): `{"name", "version", "type", "git_ref", "repo_url"}` — the Catchment clones `repo_url` and checks out `git_ref`.

Registers the version, selects it for its major, validates the graph (`422` on inter-Pond cycles or a bad archive), and auto-clears any failure on the Pond. See [Deploying](../guides/deploying.md).

```
GET /api/ponds/{name}/versions/{version}
```

Whether that exact version exists, and whether it's the selected one: `{"name", "version", "is_active"}`. `404` if never deployed.

## Status

```
GET /api/status[?since={version}]
```

The full live state, as one document — this is what the UI and `duckstring status` read. Without `since` it returns immediately. With `since`, it **long-polls**: the request holds until the engine state moves past that `version` (or a heartbeat timeout), so the UI updates the instant anything changes rather than on a timer. Each response carries the current `version` to pass back as `since`.

```json
{
  "catchment": {"id": "b1f0…", "name": "main"},
  "version": 1287,
  "ponds": [
    {
      "id": "sales@1", "name": "sales", "major": 1, "kind": "pond", "version": "1.0.0",
      "status": "running", "is_draw": false,
      "gen": 12, "runs_completed": 11,
      "has_pull": true, "target_f": null,
      "start_f": "2026-06-11T09:30:00+00:00", "end_f": "2026-06-11T09:29:57+00:00",
      "d_ms": 0, "trigger": null,
      "is_failed": false, "is_blocked": false, "is_killed": false,
      "failed_f": null, "failures": 0,
      "immediate_retries": 1, "source_retries": 2,
      "ripples": [
        {"name": "join_lines", "status": "running", "gen": 12, "runs_completed": 11,
         "has_pull": true, "target_f": null, "start_f": "…", "end_f": "…"}
      ],
      "ripple_edges": [["daily_sales", "join_lines"], ["price_tiers", "join_lines"]]
    }
  ],
  "edges": [["transactions@1", "sales@1"], ["products@1", "sales@1"], ["sales@1", "reports@1"]]
}
```

Field notes:

- `id` — the pond key `name@major`: one entry per deployed **major line** ([concurrent majors](../concepts/versioning.md) appear as separate live Ponds). `edges` reference these ids.
- `status` — one of `failed | killed | blocked | running | queued | idle`, in that precedence (a failed Pond reads `failed` even if work is queued behind it).
- `start_f` / `end_f` — the node's [freshness](../concepts/freshness.md) at run start / completion; `null` means never-run. `target_f` is the nearest unsatisfied push target; `has_pull` is the pull token.
- `gen` / `runs_completed` — runs started / completed since the Catchment loaded.
- `d_ms` — the Pond's window-derived freshness duration (0 without [Windows](../guides/windows.md)).
- `trigger` — the standing trigger, e.g. `{"kind": "tide", "bound_ms": 14400000}`, or `null`.
- The fault fields (`is_failed`, `is_blocked`, `is_killed`, `failed_f`, `failures`) and live budgets are described in [Fault Tolerance](../guides/fault-tolerance.md). When blocked, `missing_sources` (declared Sources absent from this Catchment, as `name@major`) and `blocked_by` (Sources that are themselves down) explain why; `error` carries a failed Pond's message, and `failure_kind` its sub-reason — `"contract"` when the Duck refused to publish a breaking schema change (last-good data intact; see [Versioning](../concepts/versioning.md)), `"error"` otherwise, `null` when healthy.
- `catchment` — this Catchment's [stable identity](../guides/connecting-catchments.md#identity-and-the-lineage-view) `{id, name}`. `is_draw` marks a [Pond Draw](../guides/connecting-catchments.md) (fed by a duct, not run by a worker).
- `edges` — the inter-Pond graph as `[source, sink]` pairs; `ripple_edges` the intra-Pond graph as `[parent, child]`.

## Lineage

```
GET /api/lineage?pond={name}&major={int}&table={name}&columns=false
```

The [observed table-level lineage](../guides/lineage.md): per Pond, what each Ripple actually read and
wrote on its latest recorded run — `{"ponds": [{"id", "name", "major", "version", "ripples": [{"ripple",
"f", "reads": [{"source", "table"}], "writes": [table]}]}]}` (a read with `source: null` is an own
table). `pond` narrows to one Pond (`major` picks the line, default highest); `table` narrows to the
Ripples touching that name; `columns=true` adds the deploy-captured column derivations per pond —
`{table: {column: [{ref, column}] | "constant" | "opaque"}}`.

```
GET /api/ponds/{name}/trace?table={t}&where={sql}&major={int}
```

Row-level provenance: resolves the newest `_duckstring_f` among the published rows matching `where`
(the whole table when omitted; a plain overwrite table resolves to its publish `f`), and answers with
the producing run (`version`, timings, `status`), the input `window` `(previous_f, f]`, and the
declared `sources`. `{"matched": 0, "run": null}` when nothing matches; `422` on a bad predicate. The
predicate runs over the exported snapshot at the `/api/query` trust level (read access).

## Run history

```
GET /api/runs?pond={name}&major={int}&version={semver}&lineage=true&ripples=false&limit=100
```

Recent Pond Runs, newest first. `pond` filters to one Pond — and with `lineage=true` (the default) its upstream Sources too; `major`/`version` narrow to one major line (default: the highest deployed); `ripples=true` nests each run's Ripple Runs; `limit` clamps to [1, 1000].

```json
{
  "runs": [
    {
      "pond": "sales", "id": "sales@1", "major": 1, "version": "1.0.0", "f": "2026-06-11T09:30:00+00:00",
      "started_at": "…", "finished_at": "…",
      "status": "success", "error": null, "traceback": null,
      "ripples": [
        {"ripple": "daily_sales", "retry": 0, "status": "success",
         "started_at": "…", "finished_at": "…", "error": null, "traceback": null}
      ]
    }
  ]
}
```

Run `status` is `running | success | failed | killed`. Ripple Runs carry one record **per attempt** — `retry` is the attempt index, so a Ripple that needed its [immediate-retry budget](../guides/fault-tolerance.md) shows multiple rows. Failures carry `error` and, for exceptions, the full `traceback`.

## Triggers & control

All under `/api/ponds/{name}/…`, all returning `{"ok": true}`; `404` for unknown Ponds, `422` for invalid payloads. Every route takes optional `major` / `version` query params selecting the major line to act on: `major` picks the line (default: the highest deployed), `version` additionally requires that exact version to be the line's selected artifact (`422` if it isn't). Semantics in [Triggers](../guides/triggers.md) and [Control](../guides/control.md).

| Endpoint | Body | Action |
|---|---|---|
| `POST …/tap` | — | Pull once (optional `?m={iso}` mints that demand epoch — a duct forwards the downstream's) |
| `POST …/pulse` | — | Push once (optional `?at={iso}` targets that freshness instead of now — a duct forwards the downstream's) |
| `POST …/wave` | — | Standing pull |
| `POST …/tide` | `{"bound_seconds": 14400}` | Standing push with staleness bound |
| `POST …/untrigger` | — | Remove the standing trigger |
| `POST …/wake` | — | One-shot non-propagating pull |
| `POST …/force` | — | Recompute at current freshness |
| `POST …/sleep` | `{"upstream": false}` | Clear demand (optionally ancestors too) |
| `POST …/kill` | — | Terminate the worker; park `killed` |
| `POST …/clear` | — | Reset failed/killed; unblock downstream |
| `GET …/budget` | — | `{"immediate_retries", "source_retries"}` |
| `POST …/budget` | `{"immediate_retries": 1, "source_retries": 2}` | Set the live retry budgets |
| `GET …/duck` | — | The Pond's effective worker config: `{"size", "flock", "override", "defaults"}` |
| `POST …/duck` | `{"size": "m", "flock": false}` or `{"clear": true}` | Override (or reset) the Pond's worker preset size / offload flag |

## Windows

| Endpoint | Description |
|---|---|
| `GET /api/ponds/{name}/windows` | `{"windows": [...]}` |
| `POST /api/ponds/{name}/windows` | Add a rule: `{"name", "start_anchor", "duration_seconds", "freq_unit", "freq_interval", "valid_days", "until_time"}`. `freq_unit` ∈ `SECOND \| MINUTE \| HOUR \| DAY \| WEEK`; `valid_days` like `"MON,WED,FRI"` or `null`; `422` on overlap. |
| `POST /api/ponds/{name}/windows/{window}/remove` | Remove a rule (`404` if absent). |

## Spouts (egress)

All full-gated. A Spout publishes a Pond's output to an external destination; it is operational config (persisted, survives redeploys). Credentials live in the destination URI as `${env:NAME}` (process environment) or `${secret:NAME}` ([secret store](#secrets)) references, resolved only at egress time (for object stores, in the query: `?key_id=${env:..}&secret=${env:..}&region=..`). After each Pond Run the egress worker delivers to the destination — snapshot Parquet for object stores; `postgres://` syncs a merge Trickle's changelog **incrementally** (upserts + deletes in one transaction, exactly-once). A transactional destination requires a primary key (a merge Trickle), so a plain table to `postgres://` is `422` at creation.

| Endpoint | Description |
|---|---|
| `GET /api/ponds/{name}/spouts` | `{"spouts": [{"name", "table", "destination", "mode", "schedule", "watermark", "is_failed", "failures", "error"}]}` |
| `POST /api/ponds/{name}/spouts` | Bind a Spout: `{"destination", "name"?, "table"?, "mode"?}`. `destination` scheme ∈ `file/s3/gs/postgres`; `mode` ∈ `auto/full/append` (default `auto`); `table` null = all. Returns `{"name"}`. `422` on a bad destination/mode or duplicate name. |
| `POST /api/ponds/{name}/spouts/test` | Probe a destination's connection/credentials before binding (the UI's *Test* button). Body `{"destination"}`; **writes no data** (a write+delete probe for `file://`, an `ATTACH`+`SELECT 1` for `postgres://`, a prefix-list for `s3`/`gs`). Returns `{"ok": true}` or `{"ok": false, "error"}` — a connection problem is a `200` result, not a `5xx`; the error is sanitised (never echoes a credential). `name` is unused (the probe is destination-only). |
| `POST /api/ponds/{name}/spouts/{spout}/remove` | Remove a Spout (`404` if absent). |
| `POST /api/ponds/{name}/spouts/{spout}/{action}` | Control a Spout's standing Wake. `action` ∈ `wake`/`force` (re-arm; force re-delivers now), `sleep`/`kill` (disarm; kill parks), `clear` (reset a fault), `resync` (full re-egress). |

A Spout is a **real node**, so it appears in `GET /api/status` `ponds[]` with `"is_spout": true` (its key is `{source}#{spout}@{major}`), and its source→spout edge rides the normal `edges` list. Its run history (including failed runs with tracebacks) is in `GET /api/runs` like any Pond.

## Secrets

All full-gated. A **write-only** catchment-wide store for the credentials a Spout references as `${secret:NAME}`. Values are **never returned** — you read names, not secrets — and the store is excluded from `GET /api/catchment/archive`.

| Endpoint | Description |
|---|---|
| `GET /api/secrets` | `{"secrets": [{"name", "set_at"}]}` — names and set-times only, never values. |
| `POST /api/secrets` | Store (or overwrite) a secret: `{"name", "value"}`. `name` must match `[A-Za-z_][A-Za-z0-9_]*` (`422` otherwise). The value is accepted but never echoed back. |
| `DELETE /api/secrets/{name}` | Remove a secret (`404` if absent). |

The value is transmitted in the `POST` body — front the Catchment with TLS, or prefer `${env:NAME}` (set in the server's environment, never over the wire). Stored as a private (`0600`) plaintext file; no encryption-at-rest.

## Alerts

All full-gated. A notification **channel** delivers failures and staleness to an external destination (a webhook / Slack, or email); it is operational config (persisted, survives redeploys). It fires on subscribed event kinds — `failure`, `contract`, `spout`, `recovery`, `freshness` — with root-cause dedup (a blocked-downstream Pond raises no alert; the failing root's payload carries the blocked names). Credentials in the destination URI are `${env:NAME}`/`${secret:NAME}`, resolved only at send time. A delivery failure never affects a Pond — it is retried, then parked `failed` in the log.

| Endpoint | Description |
|---|---|
| `GET /api/alerts` | `{"channels": [{"name", "destination", "scope", "events", "stale_ms", "renotify_ms", "enabled", "created_at"}]}`. `scope` is a Pond name or `null` (catchment-wide). |
| `POST /api/alerts` | Create a channel: `{"name", "destination", "scope"?, "events"?, "stale_ms"?, "renotify_ms"?}`. `destination` scheme ∈ `https/http/mailto`; `events` is a CSV of kinds or `all`; `stale_ms` sets a freshness SLA; `renotify_ms` repeats the alert at that interval while an episode persists (default: once per episode). `422` on a bad destination/event or duplicate name. |
| `DELETE /api/alerts/{name}` | Remove a channel (`404` if absent). |
| `POST /api/alerts/{name}/test` | Send a test notification (validates connectivity/credentials). Returns `{"ok": true}` or `{"ok": false, "error"}` — a connection problem is a `200` result, sanitised (never a credential). |
| `GET /api/alerts/deliveries?limit=N` | Recent deliveries (`{"deliveries": [{"channel", "kind", "pond", "severity", "status", "attempts", "error", ...}]}`) — the audit log. |

## Metrics

```
GET /metrics
```

A Prometheus text-exposition endpoint at the **root** (not under `/api`) and **unauthenticated** — the exporter convention; a scraper sends no key. Pond names appear as labels, so network-restrict it if those are sensitive. Families (`duckstring_*`): `up`; `pond_freshness_lag_seconds` (the headline — seconds since a Pond last became fresh); `pond_failed`/`blocked`/`killed`; `pond_runs_completed_total`; `pond_failures_total`; `spout_delivery_lag_seconds`; `spout_failed`; `alert_deliveries_total{status}`.

## Data

See [Querying Data](../guides/querying-data.md). Reads always hit the exported Parquet snapshots, never live state.

```
POST /api/query
```

`{"pond", "major"?, "version"?, "ripple"?, "sql"?, "format"?}` — runs `sql` against the Pond's exported tables (default: `SELECT * … LIMIT 10` on `ripple`; `major`/`version` select the major line to read, default the highest). Without `format`, returns JSON rows; with `"csv" | "json" | "parquet"`, returns the file. `400` on SQL errors.

```
GET /api/ponds/{pond}/ripples/{ripple}?major={int}
```

The Ripple's published output, zipped. `404` if it has no export yet.

## Cross-Catchment (ducts)

See [Connecting Catchments](../guides/connecting-catchments.md). The producer side reuses the routes above (the consumer reads its data and forwards demand as an ordinary client); these are the duct-specific additions.

Producer side — expose and transfer:

| Endpoint | Description |
|---|---|
| `POST /api/ponds/{name}/open` | `{"tap_on_get": false}` — mark the Pond open (and optionally tap-on-get). |
| `POST /api/ponds/{name}/close` | Remove the open flag. |
| `GET /api/draw/{name}/{major}?tables={csv}` | The Pond line's full exported Parquet as a zip — what a Draw transfers. `tables` optionally restricts the set. |
| `GET /api/draw/{name}/{major}/wait?after={iso}` | Long-poll: blocks until the Pond's freshness advances past `after` (or it goes down, or a timeout), returning `{end_f, down}`. Lets a downstream transfer the instant the upstream is fresh. |

Consumer side — manage ducts (a duct lives on the consuming Catchment):

| Endpoint | Description |
|---|---|
| `POST /api/duct` | `{"origin", "remote_url", "auth_headers"?, "upstream_id"?}` — register a conduit from an upstream Catchment. |
| `GET /api/duct` | `{"ducts": [...]}` — ducts and the Ponds each draws (credentials redacted). |
| `DELETE /api/duct/{origin}` | Destroy a duct and its Pond Draws. |
| `POST /api/duct/{origin}/ponds` | `{"pond", "major", "incremental"?}` — draw one Pond. |
| `DELETE /api/duct/{origin}/ponds/{pond}?major={int}` | Stop drawing a Pond. |
| `POST /api/duct/{origin}/sync` | Draw every Pond the upstream currently exposes. |

```
GET /api/view?scope={csv pond keys}&visited={csv uuids}
```

The recursive upstream lineage: `{catchments: [{id, name, reachable, ponds, edges}], duct_edges: [{from, to}]}`, where each `duct_edge` is `from:(upstream_id, pond) → to:(consumer_id, draw)`. Each hop expands its own ducts (it holds their credentials), threading `visited` so a mesh cycle cuts cleanly; the merge de-dups Catchments by id. This is what the [web UI](../guides/web-ui.md) renders as upstream containers.

## Worker protocol (informational)

Two further endpoints exist for the Catchment's own worker processes (Ducks) — listed for completeness, not for external use: workers hold a short poll on `GET /api/duck/{name}/{major}/jobs` for commands (`begin_run` / `shutdown`), report progress to `POST /api/duck/{name}/{major}/events`, idempotently on freshness (one Duck per major line), and can fetch the deployed source bundle from `GET /api/duck/{name}/{major}/artifact` (how a worker on another host gets its code — the Catchment stays the artifact authority). All are worker-initiated — the dial-back design that lets local and remote workers share one protocol. See [Architecture](architecture.md).
