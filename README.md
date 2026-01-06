# Duckstring

Duckstring is a cloud-native ELT framework for orchestrating data preparation DAGs by breaking each stage into modular, potentially reusable nodes called **Ponds**. A **Basin** is a dependency-resolved graph of Ponds built from one or more terminal Ponds (targets) backward through upstream dependencies. Multiple Basins together form a **Lake**.

Duckstring focuses on batch and incremental workloads for tables typically on the order of tens of millions of rows (e.g. <50M), executed on a single compute node. It is intentionally **not** a streaming engine and does **not** provide distributed processing. The objective is to help small analytics teams do *everything decently* with room for progressive evolution, avoiding the overhead of perfection. There are plenty of specialist tools for millisecond latency and truly massive data; most are unnecessary for the majority of analytical workflows.

Duckstring’s philosophy:

- Storage is cheap, so don't hesitate to have lots of tables
- Most of the time you don't need true streaming — micro-batch is fine, conceptually simpler and "always works"
- Most of the time you don't need distributed compute — data is small enough to fit on one compute node
- Modularisation helps distribute work over teams
- Brains, delays, and governance requirements are more expensive than compute
- Communication is the core bottleneck to complexity

The default engine is DuckDB (optionally backed by MotherDuck) with Ibis-friendly semantics, though this is configurable.

## Glossary

- A **Lake** is the entire data ecosystem
- A Lake is broken into multiple **Basins**
- Each Basin consists of a collection of **Ponds**
- **Inlets** and **Outlets** are the first (root) and last (leaf) Ponds in a Basin
- A **Pulse** is one execution pass over a Basin, where each Pond runs when its upstream dependencies for that Pulse are satisfied

## Ponds

Each Pond is attached to a version-controlled repository. It should be treated like a code package, but includes:

- **Code** defining transformations (ideally incremental) from source tables (upstream Ponds or within this Pond) to sink tables (within this Pond)
- **Compute** defining engine requirements and compute settings
- **Storage** defining the location and naming/namespace of tables

All tables in a Pond must be uniquely named (within the Lake namespace).

### Inlet / Outlet

An **Inlet** is a type of Pond whose inputs come exclusively from sources external to the Lake, and includes specification of source details (e.g. connectors and ingestion tooling).

An **Outlet** is a type of Pond whose only intended consumers are external to the Lake, and includes specification of consumer details (e.g. intended customers, schema requirements, data contracts). Outlets are not strictly required (consumers may read from non-Outlet Ponds), but are strongly recommended for governance and clear ownership of consumer expectations.

### Public / Private

Tables in a Pond are either **Public** or **Private**.

- **Public** tables define the Pond’s contract. They are the only tables that downstream Ponds may depend on.
- **Private** tables are internal implementation details. They may change without notice and must not be consumed downstream.

## Basins

A Basin is a collection of Ponds arranged into a Directed Acyclic Graph (DAG). Each Pond belongs to at most one Basin.

If two Basins are separate, and a new Pond is added that depends on upstream Ponds from both, the Basins are merged.

### Mode

The Basin Mode determines how Pulses are triggered.

Basin Modes:

- **Schedule** (default): Pulses begin on a fixed cadence. Within a Pulse, each Pond executes when its upstream dependencies complete.
- **Wave**: A new Pulse begins immediately after the previous Pulse completes successfully.
- **Ripple**: Pulses begin as frequently as possible, subject to (a) completion of the prior Pulse and (b) availability of new upstream data.

**Schedule** is useful for a specific cadence where consumption frequency is lower than source update frequency (e.g. weekly reports, daily dashboard refreshes).

**Wave** is useful when updates should be frequent but you want to maintain one Pulse at a time in a Basin. In practice it is nearly as responsive as Ripple when one process dominates total execution time.

**Ripple** is useful when minimising latency is most important. It is best suited to incremental transformations and small, fast Ponds, and can approach streaming performance without the overhead.

## Versioning

Consider three Ponds: A, B, and C. Both B and C depend on A.
Both A and B update and introduce breaking changes. C is more complex and cannot run without the original A version. This creates a dilemma — do you:

- Delay deploying updates to A and B until C is upversioned?
- Switch off execution of C until it can be upversioned?
- Disallow such breaking changes by design, relying on upfront engineering and governance?
- Run both versions of A *simultaneously* until C is upversioned?

Duckstring asserts that, in practice, the final option is best. There is always cost pressure to avoid parallel execution, but often the cost of delay exceeds the added compute/storage. Similarly, always requiring perfect design and governance upfront introduces delays; most real problems are only identified once time has been spent building the solution.

By version controlling **storage** as well as **code**, Duckstring can execute multiple major versions in parallel without conflict. The imperative is to upversion *as soon as practical*, not necessarily before deployment.

### Major / Minor / Patch changes

Duckstring uses Semantic Versioning (SemVer) `Major.Minor.Patch`:

- **Major** when backwards compatibility is broken
- **Minor** when backwards compatibility is maintained, but significant changes are made
- **Patch** when backwards compatible fixes are made with minimal additional change

Parallel execution of a Pond occurs only when two downstream Ponds require different **Major** versions.

Major change examples (Public tables):

- Deletion of a table or a column
- Renaming of a table or a column
- Change to a column's data type
- Change to transformation logic that defines a column where the change modifies output data

Minor change examples (Public or Private tables):

- Addition of a table or a column
- Changes to table write order / clustering
- Changes to transformation logic where the change does **not** modify output data (e.g. performance refactor)

Patch changes are used where Minor would be used, but the change is trivial.

## Importing and dependencies

Importing an upstream Pond’s Public tables requires specifying:

- The Pond and its Version
- The Public tables in scope (by name)
- All required columns selected explicitly (no implicit `SELECT *`)

This allows changes that exclusively add scope via a Minor change to guarantee no breakage downstream.
