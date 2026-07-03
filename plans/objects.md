# Objects: non-tabular Pond outputs

Status: **implemented (v1).** `src/duckstring/objects.py` is the module; `Pond.write_object` /
`read_object` / `object_path` (+ Puddle parity) are the surface; `RippleExecutor.export` / the local
runner commit staged Objects after the table export; the sidecar gains an `objects` section; the read API
(`/api/ponds/{name}/objects[/{obj}]`) + `has_objects` on `/api/status` + the Data Viewer **Objects** tab
+ `duckstring objects` / `get-object` + cross-Catchment duct/draw transfer are all wired. Tests:
`tests/test_objects.py`. Overwrite-only / ripple-only (Trickle Objects remain a future extension).

The original gap: a deployed Pond could only publish *tables* (`write_table` → the
DuckDB registry → the data plane). A Ripple that produces a non-tabular artifact — an ML model, a
serialised vectoriser, a rendered report, a blob — has **nowhere blessed to put it**. Only the *puddle*
handle has a general escape (`Puddle.path`, `core.py`); the deployed `Pond` handle has none.

This plan adds a first-class **Object**: a *named*, non-tabular artifact a Pond publishes alongside its
tables, resolved cross-Pond by the same `(name@major, as-of f)` contract that governs tables.

## Scope (v1)

- **Ripple-only, overwrite-only.** An Object is written by a Ripple with `write_table`-shaped semantics:
  wholesale overwrite, one current version per `name@major`. No Trickle/incremental Objects, no history,
  no retention. (Trickle Objects — a versioned model registry — are a possible later extension; explicitly
  out of scope.)
- **A name is the unit.** Objects are *named things*, not a browsable directory — the same reason a Sink
  pins to a table *name*, and the reason Objects map cleanly onto object stores (a name = a key/prefix; a
  bare directory has no atomic swap and re-introduces the filesystem navigation we keep opaque). See the
  design discussion in the chat that produced this plan.
- **A name's payload may be a single file OR a directory tree** — a lone `model.pkl`, or a HuggingFace
  model dir (config + weights + tokenizer). Either way it is published, read, transferred, and deleted as
  **one named unit**.
- **Immediate navigation.** The Data Viewer gains an Objects list (name, size, last-`f`, file/dir) from
  day one, plus a download endpoint.

## The addressing — Objects ride the data plane

An Object lives under the Pond line's existing `data_dir`
(`ponds/{name}/m{major}/data`, or the configured `data_root` bucket/Volume) in an **`objects/`
namespace**, keeping it out of the `*.parquet` glob:

```
{data_dir}/objects/{name}/…            # the object payload (one file, or a tree)
{data_dir}/_trickle.json               # sidecar — gains an `objects` section (below)
```

The whole point of putting Objects in `data_dir`: they inherit the machinery for free —
- **cross-Pond resolution** through `_source_data_dir` (`core.py`, honours the Sink's major pin + a bucket
  `data_root`);
- the `Storage` seam (`storage.py`) already does everything an Object needs on **both** local and object
  stores — `write_bytes`/`read_bytes`, `child`, `copy_to`, `remove`/`rmtree`, `names`/`subdir_names`,
  `size`, `duckdb_setup`. **No new storage primitives**, only a small `fetch(dest, *parts)` helper to
  materialise a remote file-or-subtree locally for `read_object`;
- **`reset`/`undeploy`** (see the reset/undeploy plan) sweep Objects automatically — they clear `data_dir`;
- the **duct/draw** transfer already ships `data_dir` files + sidecar; Objects slot into the same loop.

**Objects do NOT go through Iceberg** — the same decision already made for append-only Trickle tables
(`iceberg_plane.py`): a blob has nothing to gain from snapshot metadata. They ride the flat layer +
sidecar only.

### Sidecar entry

`_trickle.json` today is keyed by table name (`{mode, f, pk, floor, …}`, see `dataplane.publish_plan`).
Add a **separate `objects` section** (not a table entry) so an Object and a table may share a name without
collision and the read paths stay cleanly split:

```json
{ "sales": {"mode": "overwrite", "f": "..."},
  "objects": { "model": {"f": "...", "is_dir": false, "size": 20481, "ext": ".pkl"} } }
```

`f` is the run freshness that produced it (for the Viewer + advance-detection on transfer); `size` is the
total byte size (payload sum for a dir).

## Write path — staged, committed at export (abort-safe)

The subtlety: an Object is arbitrary bytes, **not** in the DuckDB registry, but publish must stay
**atomic** — a Ripple exception or contract violation later in the run must leave the *last-good* Object
intact (the same guarantee `_export_data` gives tables: the contract gate aborts before `export`, live
tables keep last-good).

So `write_object` **stages**, and the run's export **commits**:

1. `pond.write_object(name, src)` (Ripple, deployed `Pond` handle in `core.py`):
   - `src` = a `Path` (file or dir), `bytes`, or a file-like.
   - Validate `name` (regex as for tables; reject the `_duckstring_` prefix and path separators).
   - Write the payload into a **run-scoped staging dir** under the Duck's local state dir (never the
     bucket), and record `(name, staged_relpath, is_dir, ext, size)` in a registry meta table
     **`_duckstring_object`** (parallels `_duckstring_trickle`).
   - Returns `None` (overwrite, fire-and-forget), like `write_table`.
2. At run export (`duck/executor.py::_export_data`, after the contract gate passes): read
   `_duckstring_object`, and for each, **atomically commit** staging → `{data_dir}/objects/{name}/`
   (write to `objects/.staging/{name}` then swap: local `os.replace`, object store = PUT the key(s) then
   drop the old prefix). Then `publish_plan` writes the `objects` sidecar section.
3. On any run failure the export never runs → staging is orphaned (swept on the next run / `wipe`) and the
   published Object is **untouched**.

`Pond.write_object` needs the own-line `data_dir`, which the current handle doesn't carry — thread the
`major` (or the resolved own `data_dir`) into the `Pond` constructor. Staging keeps `write_object` itself
needing only a local scratch path; the commit lives in the executor where `data_dir` already is.

(Documented tradeoff: a write-through v1 — `write_object` writes straight to `objects/{name}/` — is
simpler but breaks last-good-on-abort. Objects carry no schema contract, so the only exposure is a
mid-run Ripple exception; we still choose staging for parity with tables. Revisit only if staging proves
heavy for large models.)

## Read path

Two accessors on the `Pond` handle (`core.py`, beside `read_table`), mirroring the `write_table` /
`write_path` and `read_object` / `object_path` naming split:

- **`pond.read_object("source.name") -> bytes`** — the object *itself*. The ergonomic default for the
  common single-blob case (`pickle.loads(pond.read_object("model"))`). A **directory** Object has no
  bytes, so this **raises** for one (with a message pointing at `object_path`) — single-file only.
- **`pond.object_path("source.name") -> Path`** — a **materialised local path**, valid for both a single
  file and a directory tree. The escape hatch for dir-artifacts and lazy/large loads
  (`Model.from_pretrained(pond.object_path("model"))`) where you don't want the whole payload in memory.

Both share resolution:

- **Foreign** (`source != self.name`): resolve `_source_data_dir(source)` (major pin + `data_root`), read
  the `objects` sidecar for `name`. `read_object` streams the single file's bytes; `object_path`
  **materialises** the file-or-subtree to a **run-scoped temp dir** via `Storage.fetch` (a local Object is
  copied/symlinked read-only; a remote one is pulled down once, cached by `f` so repeated reads in a run
  don't refetch) and returns the local `Path`.
- **Own** (`source == self.name` or a bare name): read from `objects/{name}/` (or the staging copy if
  written earlier this run).
- Overwrite-only ⇒ **reads latest** (no as-of history), exactly like an overwrite table on the Parquet
  plane. Raise a `FileNotFoundError` mirroring `read_table`'s ("has {source} completed a successful run?").
- Contract: an `object_path` result is **read-only** by convention (don't mutate; copy if you must).

## Cross-Catchment transfer (duct/draw)

`routes/draw.py` already zips `data_dir` files + the sidecar; the consumer's `poller._land_transfer`
drops them in. Extend both:

- **Producer** (`draw`): include `objects/{name}/…` for each sidecar Object whose `f` advanced past the
  consumer's held value (overwrite = wholesale; the same "ship only if advanced" gate the merge base
  uses). A dir-Object ships its whole subtree under `objects/{name}/`.
- **Consumer** (`poller`): wholesale-replace `objects/{name}/` on receipt (atomic swap). The sidecar
  already travels.

## API + Data Viewer (navigation from day one)

- **`GET /api/ponds/{name}/objects`** (read-gated, `routes/data.py`) → `[{name, size, f, is_dir, ext}]`
  from the sidecar `objects` section (via `_data_dir` + `load_sidecar`).
- **`GET /api/ponds/{name}/objects/{obj}`** (read-gated) → stream the Object: a single file inline, a dir
  zipped — mirror the existing per-Ripple zip-stream (`routes/data.py::get_ripple`).
- **`/api/status`** entries gain **`has_objects`** (beside `has_tables`, `driver.status()`) so the Viewer
  offers the Objects tab only when present.
- **`frontend/`**: `api.ts` gains `fetchObjects(pond)` + an object download URL + `has_objects` on the
  status type; **`DataViewerModal.tsx`** gains an **Objects** tab/list beside Tables — columns: name,
  humanised size, last-`f` (via `formatAge`), file/dir icon, a Download link. Read-only (Objects are code
  outputs, never UI-authored — same stance as topology).

## Local parity (puddles)

- `Puddle.write_object`/`read_object` (`core.py`) mirroring the deployed API, writing into
  `puddles/ponds/{source}/data/objects/{name}/` — the same catchment-root layout `read_table`'s foreign
  branch already relies on, so cross-puddle `read_object` works unchanged. Supersedes the vague
  `Puddle.path` for the model/blob use case.
- `local/runner.py`: commit staged Objects to `puddles/out/objects/` at end-of-run (mirror the table
  export); a full run resets it.
- `cli/puddle.py`: `puddle ls` surfaces Objects alongside tables (name, size, dir/file).

## CLI

- `duckstring data objects {pond} [--major]` — list (parity with the Viewer).
- `duckstring data get-object {pond} {name} [-o path]` — download. (Thin over the two new endpoints.)

## Open decisions

1. **Same-name table + Object in one Pond** — allowed by the split sidecar sections (disambiguated by
   `read_table` vs `read_object`), but confusing in the Viewer. Lean: **reject at write** (a Pond's table
   names and object names share one namespace) for clarity; cheap to relax later.
2. **`Storage.fetch` shape** — add a single `fetch(dest: Path, *parts)` (file-or-subtree) to the seam, vs
   composing `names` + `read_bytes` at the call site. Lean: add it to the seam (both planes implement it;
   object-store uses fsspec `get`).
3. **Large-model staging cost** — staging a multi-GB model doubles a local write transiently. Acceptable
   for v1 (atomic swap = a rename, no copy, when staging shares the `data_dir` filesystem — stage under
   `{data_dir}/objects/.staging` rather than the state root so the commit is a rename, not a cross-device
   copy). Note: this means staging *is* on the bucket for a remote `data_root` — fine, object PUT is the
   commit.

## Build order

1. `Storage.fetch` + the `objects` sidecar section + `_duckstring_object` meta table.
2. `Pond.write_object`/`read_object` + thread own `data_dir`/`major` into the handle; commit-at-export in
   `executor._export_data`; `publish_plan` writes the section.
3. Puddle parity + `local/runner` commit.
4. The two API endpoints + `has_objects` on status.
5. Data Viewer Objects tab.
6. Duct/draw transfer.
7. CLI + docs (`docs/docs/…` python-api + an Objects concept page) + tests.

## Tests

- Write→publish→own-read; cross-Pond read honouring the major pin; overwrite replaces cleanly.
- **Abort safety**: a Ripple that `write_object`s then raises leaves last-good intact (staging discarded).
- Dir-Object round-trips as a unit (write a tree, read it back).
- `reset`/`undeploy` sweep Objects (once those land).
- Duct/draw ships an Object only when its `f` advanced; consumer swaps it in.
- Viewer object-list + download endpoints; `has_objects` gating.
