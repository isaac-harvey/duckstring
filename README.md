# Duckstring
A cloud-native ELT framework supporting orchestration of data preparation DAGs by breaking each stage into potentially-useful standalone nodes or "Ponds". The pipeline consists of deriving potentially multiple downstream Ponds from upstream Ponds, managing any version dependencies. The pipeline or "Basin" is built by dependency resolution from terminal ponds backwards up the stream. The set of Ponds, potentially in multiple Basins, are part of the overall Lake.

The focus is on batch or incremental jobs for tables of fewer than 50M rows, each operating on a single compute node. It is by design *not* streaming or distributed processing. The key objective of Duckstring is to enable small analytics teams to do *everything decently* with natural room for progressive evolution, and avoids the overhead of perfection. There are plenty of specialty tools for handling millisecond latency and truly massive data - most of them unnecessary for the majority of analytical workflows.

The philosophy of Duckstring is:

- Storage is cheap, so don't hesitate to have lots of tables
- Most of the time you don't need true streaming - micro-batch is fine, conceptually simpler and 'always works'
- Most of the time you don't need distributed compute - data is small enough to fit on one compute node
- Modularisation helps distribute work over teams
- Brains, delays and governance requirements are more expensive than compute
- Communication is the core bottleneck to complexity

The default engine is DuckDB with a MotherDuck backend using Ibis semantics, though this is configurable.

## Glossary

- A **Lake** is the entire data ecosystem 
- The Lake is broken into multiple **Basins** 
- Each Basin consists of a collection of **Ponds** 
- **Inlets** and **Outlets** are the first (root) and last (leaf) Ponds in a Basin
- A **Pulse** inside a Basin is the execution of each sequential Pond

## Ponds
Each Pond is attached to a version-controlled repository. It should be treated like a code package, but contains each of:

- **Code** defining transformations (ideally incremental) from source tables (upstream Ponds or within this Pond) to sink tables (within this Pond)
- **Compute** defining the engine requirements and compute settings
- **Storage** defining the location of tables

All tables in a Pond must be uniquely named.

### Inlet / Outlet
An **Inlet** is a type of Pond that has inputs exclusively from sources external to the Lake, and includes specification of their details (e.g. source and ingestion tools).

An **Outlet** is a type of Pond whose only consumers are external to the Lake, and includes specification of consumer details (e.g. intended customers, schema requirements, data contracts). Outlets are not strictly required (it may be preferred that customers can consume directly from any Pond), but are strongly recommended for handling governance for consumer expectations.

### Public / Private
Tables in a Pond are either Public or Private. 
Private tables are optional, but often very useful stages in data preparation, but do not contain any useful information at consumption that is not present in the final, Public tables.
Public tables are the 'result' of a Pond, and are the only source from which any downstream Ponds can consume.

## Basins
A Basin is a collection of Ponds arranged into a Directed Acyclic Graph (DAG). Each Pond belongs to at most one Basin. 

If two Basins were separate, but a new Pond is added with upstream Ponds from both Basins, the Basins are merged.

### Mode
The Basin Mode decides how Ponds in a Basin should be executed - the conditions under which a Pulse of execution passes over it to define their collection as a Wave. 

Basins Modes can be:

- **Schedule** (Default): Each Pulse begins at a set time, each Pond executes when upstream Ponds complete
- **Wave**: Each Pulse begins immediately after the last Pond in the Basin completes
- **Ripple**: Pulses are made as frequently as possible, given a bottleneck process

**Schedule** is useful for setting a specific cadence, where the required consumption frequency is lower than the update frequency for data sources. A common example is a weekly report or a daily update for a dashboard, where more frequent execution is an unnecessary cost.

**Wave** is useful wherever data should be updated frequently, but there is a need to ensure a fully successful run before starting a new one. It is conceptually simpler than Ripple and nearly the same in performance when one process dominates total execution time.

**Ripple** is useful wherever minimising latency is most important. It executes each Pond whenever both new data is available to consume from upstream and new data from the previous Pulse has been consumed downstream. This is the lowest-latency mode and can approach streaming speeds if Ponds are small and processing is incremental.

## Versioning
Consider three Ponds: A, B and C. Both B and C depend on (are downstream of) A. 
Both A and B update and introduce breaking changes. C is more complex and cannot run without the original A version. This creates a dilemma - do you:

- Delay deploying updates to A and B until C is upversioned?
- Switch off execution of C until it can be upversioned?
- Disallow such breaking changes by design, relying on good upfront engineering and governance?
- Run both versions of A *simultaneously* until C is upversioned?

Duckstring asserts that, in practice, the final option is best. There is always cost pressure to avoid simultaneous execution like this, but often the cost of delay is more than the added cost of compute. Similarly, always requiring good design and governance upfront introduces unnecessary delays in practice - most problems are only identified once time has been spent building the solution.

By version controlling **storage** as well as **code**, the pipeline can happily execute both in parallel without conflict. The imperative is to upversion *as soon as one can*, not necessarily before deployment.

### Major / Minor / Patch Changes
Version control uses the Semantic Versioning (Semver) convention `Major.Minor.Patch`:

- **Major** when backwards compatibility is broken
- **Minor** when backwards compatibility is maintained, but significant changes are made
- **Patch** when backwards compatible fixes are made with minimal additional changes

Parallel execution of a Pond is only when two downstream Ponds require different **Major** versions.

Major changes only ever apply to changes affecting *Public* tables. An example Major change would be: 

- Deletion of a table or a column
- Renaming of a table or a column
- Change to a column's data type
- Change to transformation logic that defines a column, where that change modifies the output data

Minor changes can apply to either *Public* or *Private* tables. An example Minor change would be:

- Addition of a table or a column
- Changes to table order on write
- Changes to transformation logic that defines a column, where that change *does not* modify the output data

Patches can be used in cases that Minor would be used, but where the change is trivial.

## Importing and Dependencies
Importing an upstream Pond for use of its Public tables requires specifying:

- The Pond and its Version
- The tables in scope it by name
- All required columns selected explicitly

This allows changes that exclusively *add scope* via a Minor change to guarantee no break downstream.