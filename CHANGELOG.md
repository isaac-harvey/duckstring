# Changelog

Notable changes per release. Versions before 0.5.0 are recorded in the git history and the `v*` tags.

## 0.5.0 — unreleased

The cloud release: a Catchment can now run its Ducks on AWS, keep its data on S3, and serve that data
over standard wire protocols. Everything below is opt-in — a Catchment with no cloud configuration
behaves exactly as it did in 0.4.0, and a `pond.toml` asking for remote compute still runs anywhere.

### Cloud compute

- **Duck launchers**: a Pond's Duck runs on the Catchment's own box (default), on **Fargate** (the
  default remote backend — serverless containers, task-role IAM), or on **EC2** (the escape hatch, for
  sizes and GPUs Fargate does not offer). Built-in **S/M/L/XL preset pools** resolve with zero setup.
- **Duck Pools**: named remote-compute pools (`duckstring duck pool add`), with per-Pond compute
  declared in `pond.toml` and overridable operationally (`duckstring duck set`).
- **The Pool agent**: one shared machine per named Pool hosting many co-resident Ducks.
- **Auto-relay**: a Catchment on a laptop behind NAT can run cloud Ducks with no manual tunnel — the
  first remote spawn provisions a small always-reachable box and holds a reverse tunnel to it.
- **Remote failures are observable**: boot output is teed to the console (EC2) / CloudWatch (Fargate)
  and the tail is attached to the Pond's failure, so a Duck that dies before dialling back still says
  why. Spawn-failure reasons (missing image, missing AMI, bad IAM) surface on the Pond rather than as a
  generic crash.
- Per-provider **startup grace** so a cold machine is not judged by the steady-state silence window.

### Data plane on S3

- A Catchment's data root can be an object store (`duckstring catchment settings --data-root s3://…`),
  with an S3-compatible **endpoint override** for MinIO/Ceph/R2.
- **Switch, adopt, or migrate** an existing plane, with a background copy and live progress.
- **Local-first publish + async Persist**: a run publishes locally and mirrors durably in the
  background, so compute never waits on the object store; `persisted_f` is the durable watermark and
  freshness gating is Pool-aware.
- **S3-resident state**: a merge base is read as a view rather than hydrated, and retention pruning is
  floor-anchored rather than inferred from absence.

### The Flock — over-envelope compute

A comprehensive `pond.trickle(...)` recompute that exceeds the Duck's memory envelope can be dispatched
to a serverless engine (**Athena** ships first), while the Duck keeps merge/diff/publish.

**DuckDB is the authority.** Dispatch decides *where* work runs, never *what* is published, enforced
structurally: an engine-owned allow-list of expressions proven equivalent on both engines (division and
CAST are excluded — the two engines genuinely disagree), a `conform` step that casts the engine's result
to DuckDB's own schema and rejects a differing column set, and degradation to local compute on any
failure. Dispatch counters ride `/metrics`, since a silently degrading Flock is otherwise invisible.

### Data serving

- A sandboxed, warm, read-only query surface over published data: catalog = catchment, schema = pond.
- **Postgres wire** and **Arrow Flight SQL** adapters, plus `/api/serve` and a CLI.
- A **Catalog UI** unifying the query surface, with role-governed access.

### Lineage

- Table-level (observed per run), column-level (static per version, with a sqlglot upgrade for SQL
  outputs), and row-level temporal provenance (`duckstring trace`).
- **OpenLineage** events emitted on run completion for catalog integration.

### dbt-mode Ponds

Deploy a dbt project as a Pond with no `@ripple` code: each dbt model becomes a Ripple with its own
freshness, failure and retry tracking, and `ref()` becomes the intra-Pond graph. Opt in with the
`duckstring[dbt]` extra.

### Correctness and operability

- **Version contract**: a lossless widening of a column type is accepted (a data-dependent branch could
  legitimately wedge a live Pond permanently); `reset-contract` is the escape hatch for a genuine
  narrowing; contract failures carry a dedicated sub-reason end-to-end.
- **Reads reject a stale local publish** the Catchment knows is behind, rather than serving it.
- Pond Runs the Pond has moved past are closed rather than stranded at `running`.
- A Pool machine that disappears is detected and relaunched, instead of wedging every Pond on it.
- The **serving sandbox** no longer spills into the shared system temp — DuckDB exempts its temp
  directory from the external-access lock, which on Linux exposed everything under `/tmp` to a
  read-level user.
- An idle Duck backs off instead of polling ten times a second.
- Opt-in **re-notify cadence** for alert channels while a failure or freshness episode persists.
- Incremental object-store egress (`mode=append`) mirrors the published collection per delivery.

### Testing

- The object-store data plane runs against a **real S3 API** in CI (MinIO, which rejects unsigned
  requests — the class of bug moto cannot catch).
- Real-data demo sets: **TPC-DS** and **GHArchive** (the latter offline against a committed fixture).
- Python matrix, the dbt extra, real-Postgres egress, and frontend tests in CI.

### Documentation

- A full **AWS guide** (`guides/cloud.md`): the cloud-enable gate, the three IAM roles, security-group
  asymmetry, the AMI Python constraint, the build-your-own-image policy, and the Flock's authority rules.
- New guides for lineage, dbt-mode, and querying data.
