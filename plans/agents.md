# Agent interface — making duckstring legible to AI agents

**Status: design/backlog, post-1.0. Nothing here blocks release.** Written 2026-07-11
(cloud-side design session; the customer-facing half lives in the private duckstring-cloud
repo's `plans/agents.md`).

## Premise

Within a few years, AI agents are the primary authors, operators, and consumers of data
pipelines. Duckstring's core design is accidentally agent-native — these properties already
exist and just need to be *said* (docs/positioning, not code):

- **Localized reasoning.** An agent has a context window, not an org chart. A Pond is a
  bounded unit with declared upstream deps — an agent reasons about one package and its
  suppliers, never a global DAG. ("You should know yourself and your suppliers.")
- **SemVer as a coordination protocol that needs no meetings.** Concurrent major lines +
  additive contracts + `min_version` pins mean one agent can ship a breaking change while
  downstream agents re-pin on their own schedule. When pond owners are agents, semver is the
  only language they share.
- **Reversibility.** Atomic publishes, last-good kept on contract violation, refresh/repair
  as recompute verbs, rollback via version selection. Agents fear irreversibility more than
  anything; duckstring's guarantees deserve explicit documentation as guarantees.
- **Declarative freshness.** An agent doesn't want cron; it wants "I tolerate 1 h staleness"
  (a Tide) or "as fresh as possible" (a Wave), and tap-on-get lets a *read* solicit recency
  without blocking.

What's missing is the mechanical layer. In leverage order:

## 1. `explain` — the engine's reasoning as an endpoint

`duckstring explain {pond} [--json]` + `GET /api/ponds/{name}/explain` (read-gated). The
engine already knows everything an agent burns context re-deriving; emit it as one structured
causal report:

- current status and **why** (the failure-precedence chain: failed → killed → blocked →
  running → queued → idle, with the blocking root named);
- what it is **waiting on** (source freshness vs own `start_f`, active window gaps, retry
  budgets remaining, `failed_f`, repair/refresh flags);
- **when it will next act** (the `next_wake` reasoning: tide bound expiry, window boundary,
  or "no standing demand");
- **suggested verb** (fault state → clear/wake/force/repair/redeploy mapping — the same table
  an experienced operator holds in their head).

All derivable from `PondState`/`pond_source_f`/`next_wake`/`derive_blocked` — no new engine
state. Emit a stable `kind` enum alongside each human string so machines don't parse prose.
This is the single highest-value item: the freshness model is subtle, and subtle semantics
are where agents make confident mistakes — don't make them simulate the engine, let them ask
it. (Humans debugging at 2am want exactly the same endpoint.)

## 2. `--json` everywhere

Every CLI verb gains `--json` with a stable schema (status, runs, trigger/control results,
deploy, duct, spout, alert, secret ls, puddle ls/show). Documented exit codes; `-y` on
anything interactive (mostly exists). Agents shell out; unstructured stdout is friction and
parsing bugs. Consider typed machine error codes on API error bodies (the duckflock
`ir::validate` pattern) at the same time.

## 3. `diff` — what did this run actually change?

`duckstring diff {pond} {table} --between f1 f2 [--json|--parquet]`. Trickle already records
every row change with a freshness stamp — the changelog **is** the answer:

- merge table → consolidated changelog window over `(f1, f2]` (the `read_delta` machinery);
- append table → the history window;
- overwrite table → snapshot compare where the plane retains snapshots (Iceberg plane: yes,
  via as-of reads; parquet plane: document the limitation).

Row-count caps + read-gating. This answers the agent's most important post-change question
("what did my change do to the data?") with machinery that exists; almost no other system
gives row-level change provenance without a bolted-on CDC stack.

## 4. Freshness metadata on query results

`/api/query` (and `duckstring query`) responses carry `{f, staleness_ms, window}` alongside
rows. An agent citing data needs its staleness bound; duckstring is rare in *having* the
number — attach it. Document tap-on-get as the agent pattern: a read that also solicits
recency for next time.

## 5. An MCP server

`duckstring mcp` (stdio, wraps the registered catchment's HTTP API; works against any
catchment, local or hosted). Keep the tool set small and semantic: `status`, `explain`,
`runs`, `query` (freshness-stamped), `trigger` (tap/pulse/tide/wave), `diff`, `deploy`, and
the control verbs behind an explicit opt-in. Outlets exposable as MCP **resources** with
freshness metadata. Authorization rides the existing key ladder — the key's level determines
which tools appear (read → status/explain/query/diff; demand → +trigger; full → +deploy/
control). No new auth model.

## 6. The agent handbook

A ~2-page `AGENTS.md` shipped in-repo + a docs page (and `llms.txt` on docs.duckstring.com):
concepts in one table, the verb table, the freshness semantics in five sentences, the
error→verb mapping, the reversibility guarantees, the four engine landmines stated as rules.
`theory.md` is the formal spec for deep study; agents working in-context need the operational
contract at minimum token cost. Cheapest item here, and it doubles as distribution — agents
adopt tools their harnesses already understand.

## Sequencing

4 and 6 are near-free and can ride any release. 2 is mechanical but wide. 1 and 3 are real
(small) features with tests — build 1 first. 5 wraps whatever exists, so it lands last and
inherits everything.
