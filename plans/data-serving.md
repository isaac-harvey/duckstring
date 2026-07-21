# Data serving — the native query surface (Catalog)

**Thesis.** Duckstring already *transforms* data and *pushes* it out (Spouts). This adds the third leg:
*serving* it. The target segment (settled with the author): **high-value data, low-latency needs, queried
by tens of people a day** — the workload a single fast node dominates and the warehouse-industrial-complex
wildly overserves (the "there is no DAG" thesis applied to serving: *you've been upsold into distributed
compute*). The Catchment already computed and holds the data; serving it warm is nearly free. Anything
beefier (high concurrency, huge scans, an org's existing serving DB) → **egress** to a dedicated system.
So, symmetric with the compute story: **serve native (small/warm), egress to dedicated (big)** — one
seam, no lock-in.

`duckstring query` already proves the *engine* (DuckDB over columnar Parquet) is fast. The gap to a
"data service" is **warmth + connectivity + a governed surface**, not a new query engine.

## Exposure is pond-level; two mechanisms share one declaration

"Serviceable" — the pond's **public data products** — is declared in `pond.toml` and is the shared
default for **both** exposure mechanisms:

- **Serve** (pull): consumers query the served tables.
- **Egress** (push): Duckstring delivers a subset to an external system (Spouts).

They share the declaration and the served-major default, but are **independently governed** (a serving
promotion must never silently re-point an egress consumer).

### Serviceable = a hint, overridable both ways, reset on a new major

- **Declared** in `pond.toml`: `[serve] tables = ["revenue", …]` — the default-serviceable set. **Absent
  ⇒ nothing served** (safe-by-default: opt tables *in*, so an internal intermediate / PII table is never
  exposed by accident). Stored per `pond_version` (re-read every deploy).
- **Operational override** (the eye toggle): expose a non-declared table, or hide a declared one — live,
  no redeploy. Stored on the **pond line** (`pond_id` = `name@major`), so it **persists across minor
  redeploys** but a **new major line starts fresh** (a new `pond` row has no overrides). Emergency lever
  (PII leaked → hide now).
- **Effective(table) = override ?? declared** — the same coalesce as the Duck config. "New major resets
  to `pond.toml` defaults" falls straight out.

### The served-major pointer (blue-green for queries)

A serving consumer is a **sink** — a major bump is a *breaking change* they opt into, so the default
must never auto-advance (that's the destructive-upgrade the version contract exists to prevent).

- `pond.table` → the **served major** — an operator-controlled pointer, **defaults to the lowest
  deployed major** (first deploy auto-serves; no promotion friction for the common single-major case),
  and **only moves via `promote`**.
- `pond_vN.table` → an explicit major line — deterministic, every deployed major. An app pins here.
- **`promote pond -m N`** flips the alias (blue-green: deploy v2 → queryable as `pond_v2` → migrate →
  promote → `pond_v1` laggards untouched → retire). Reversible (promote back). **Validated** — refuse to
  promote to a major missing the current serviceable tables (the serving analog of the forward-only
  schema contract). Gated (full), recorded, survives redeploys.
- Introspection: single major → just `pond`; **2+ majors → `pond` (served, badged) + `pond_vN`** so the
  fork is visible exactly when it exists.

## Catalog convention — no metastore

`catalog = catchment`, `schema = pond`, `table = table`. Same as "there is no DAG": **there is no
catalog — the pond structure *is* the catalog.** Zero new machinery; maps onto both wires (pg
database=catchment, schemas=ponds; Flight catalog/schema metadata the same). Sets up **mesh serving**
later: a downstream Catchment presents its ducted upstreams as additional catalogs.

## The serving core (the one brain)

A **warm, resident, sandboxed, read-only** DuckDB executor over the published data — the single engine
every front door shares. Generalises today's `routes/data._open_pond` (ephemeral, per-pond, ungated) to
a persistent, multi-pond, gated surface.

- **Warm + resident**: a long-lived connection with the served tables materialised/attached locally
  (not re-opening in-memory DuckDB + hitting S3 per query). S3-resident data adds tens-to-hundreds of ms
  per query; local is sub-ms — *the* latency lever. Refreshes atomically on new freshness.
- **Snapshot-consistent, for free**: the data plane stamps `f` and does as-of reads → serve "as of the
  latest committed `f`", swap atomically on a new run; readers never see half-published state.
- **Sandboxed for read users (the teeth)**: arbitrary DuckDB SQL can otherwise escape the allowlist via
  `read_parquet('s3://…')` / `read_csv('/etc/passwd')`. A read connection runs `SET enable_external_access
  = false` + `lock_configuration = true` and presents **only** the serviceable tables (views/attached),
  so the user's SQL can touch nothing else. Full users get an unsandboxed connection (ops/debug).
- **Resource caps**: per-statement timeout + `memory_limit` on the read connection (one accidental
  cross-join can't stall the node).
- **Scope**: joins/interactions **within a single Catchment** only (matches discovery-only upstream).

## Protocol adapters (thin front doors over the core)

Both wire protocols, one core. Default **Postgres**.

- **HTTP / CLI** — consolidate `routes/data` query onto the gated core, so `duckstring query` obeys the
  same serviceable + sandbox rules as the wires (no governance hole). CLI stays HTTP (no new dep).
- **Postgres wire (default)** — universal tool reach (Metabase/Tableau/psql/ORMs). **The real work is
  catalog introspection**: BI tools probe `pg_catalog`/`information_schema` on connect and expect
  Postgres's exact shapes; executing the SQL is easy, faking the metadata is the effort.
- **Arrow Flight SQL** — columnar-native, app/engineer clients; its explicit metadata API
  (`GetTables`/`GetDbSchemas`) is *cleaner* than pg introspection.

(Order to ship — pg vs Flight first — is the one open call.)

## Egress, revisited (additive, not a rearchitecture)

A Spout stays **major-pinned** (a destination's schema is fixed; migration needs parallel majors), but:

- **Table subset** (default = the serviceable set) — replaces today's `table | *`.
- **Major defaults to the served major** at creation (ergonomics), then **independent** — promoting the
  serving pointer does **not** move existing Spouts (or you'd push a breaking change onto every egress
  consumer). Multiple Spouts on different majors are legal (blue-green egress).
- Presented pond-level in the Catalog ("expose this pond"), one Spout the common case, N allowed.
- **Redshift/Snowflake/etc. = egress drivers** (extend the Spout scheme registry), not something the
  Catchment "attaches".

## The Catalog UI (the consumer/owner lens)

A **separate** view from the orchestration canvas — canvas = *execution* (pond+major, operator lens);
**Catalog** = *exposure/consumption* (pond-level, consumer + data-owner lens). Menu name: **Catalog**.

- **Catchment selector** — the current Catchment + its **ducted upstreams** (the mesh chain). Upstream is
  **read-only / discovery-only** (browse serviceable tables + metadata; query happens at that Catchment;
  no cross-Catchment joins — see Deferred).
- **Connect panel** (catchment-level) — the pg + Flight endpoints + which key/user to auth as. *The*
  data-service front page ("point your BI tool here").
- **Pond list with search** (across ponds *and* tables).
- **Table list** for the active/served major: exposed = white + eye icon, hidden = greyed; **the eye is
  toggleable on every table** (the operational override, both directions).
- **Version selector** (dropdown) — flick to another major (shows its tables); the served one is badged;
  a non-served one gets **Promote to default** + **Stop exposing**; egress is a **pond-level list** (all
  Spouts, each labeled with its major — so migration state is visible).
- **Table detail** — the existing data viewer + schema + **freshness badge** + the **copyable qualified
  name** (`sales_v2.revenue`) — the "found it → querying it" handoff.
- **In-UI query console** — a write user runs SQL against the serving core, single-Catchment scope.
- **Access-level-aware**: a **read** consumer sees only the served major's serviceable tables + Connect +
  data viewer + console (no hidden tables, no version selector, no promote/egress); a **full** owner sees
  everything above. (Gates on `accessLevel` like the Sidebar.)

## Schema (migration `024_serving.sql`)

```sql
CREATE TABLE pond_version_serve (            -- DECLARED serviceable set (pond.toml [serve] tables)
    pond_version_id INTEGER NOT NULL REFERENCES pond_version(id),
    table_name      TEXT    NOT NULL,
    PRIMARY KEY (pond_version_id, table_name)
);
CREATE TABLE pond_serve_override (           -- operational eye toggle; on the pond LINE (persists across
    pond_id    INTEGER NOT NULL REFERENCES pond(id),   -- minor redeploys, fresh for a new major)
    table_name TEXT    NOT NULL,
    exposed    INTEGER NOT NULL,             -- 1 expose / 0 hide (an explicit override either way)
    PRIMARY KEY (pond_id, table_name)
);
CREATE TABLE pond_serve (                    -- the served-major pointer, per named pond
    pond_name_id  INTEGER PRIMARY KEY REFERENCES pond_name(id),
    served_major  INTEGER NOT NULL
);
```

## Build sequence

**Increment 1 — the foundation (this commit set), fully testable, no protocol/UI yet.**
1. Migration `024`.
2. `_pond_config` parses `[serve] tables`; deploy stores `pond_version_serve`; seed `pond_serve`
   (served_major = lowest deployed) on first deploy of a name.
3. `Driver`: `serviceable_tables(pond_key)` (effective = override ?? declared), `set_exposed(pond_key,
   table, exposed|None)`, `served_major(name)`, `promote(name, major)` (validated), reload loading.
4. **`catchment/serving.py`** — the serving core: build a sandboxed read-only connection presenting a
   Catchment's serviceable tables as `{pond}` (served) + `{pond}_vN` schemas over the data plane;
   `enable_external_access=false`; execute read-only SQL; resource caps.
5. Tests: declared→effective, override both directions, reset-on-new-major, promote (+ validation), and
   the core (serviceable query works; sandbox blocks external-file access + non-serviceable tables).

**Increment 2 — HTTP/CLI onto the core** (consolidate `routes/data` query; the gated query endpoint).
**Increment 3 — Postgres wire adapter** (the introspection is the work). **Increment 4 — Flight SQL.**
**Increment 5 — egress table-subset + served-major default.** **Increment 6 — the Catalog UI.**

**Increment 7 — unify the query surface + govern data access (this commit set).**
1. The custom-`sql` branch of `/api/query/count` + `/api/query/page` now runs through the **serving core**
   (`serving.py` `serving_count`/`serving_page`) — cross-pond joins within the Catchment, sandboxed to the
   serviceable surface unless the caller is full — the same brain `/serve/query` + the pg/Flight wires use.
   The per-pond `table`/`trickle` browse stays on the Pond's exported snapshot (freshness windows, history).
2. **Governance by role, everywhere:** a non-full caller sees only serviceable tables in the picker
   (`list_pond_tables`), may not browse/history/freshness a hidden table (`_require_serviceable` → 403), and
   its custom SQL is sandboxed. Full sees + manages everything.
3. **UI:** the Catalog and the Data Viewer are **one modal** (`DataViewerModal` → `CatalogModal`) — opened
   from Options → Catalog or a Pond's table icon (that Pond+Major preselected). A collapsible left pond-tree
   (per-pond version dropdown, served major bold; expandable table list with exposure eyes + copy-with-✓ that
   always emits the `_vN` name) drives the embedded viewer; Promote moved into the viewer's top row. The old
   full-page `CatalogView` + the `serveQuery` client are gone.

## Deferred

- **Federated cross-Catchment query** (query upstream tables *through* this Catchment over ducts) — the
  full catalog=catchment vision; v1 is discovery-only. Big (the core reaches upstream data).
- **Column masking / row-level security** — later layers on the same read gate (v1 is table-level).
- **Result caching / materialised pre-aggregates** — latency polish once the core is warm.
