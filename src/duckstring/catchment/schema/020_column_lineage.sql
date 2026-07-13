-- Static column lineage (plans/lineage.md Phase 2): per deployed version, each output column's source
-- columns — walked from the captured plan IR at deploy time (best-effort: a non-capturable ripple simply
-- contributes no rows). kind: 'exact' rows carry one (src_ref, src_column) pair each; 'constant' = the
-- column derives from nothing (a literal); 'opaque' = captured but unprovable (a .sql() output, an
-- unenumerable star) — recorded so "we know we don't know" is distinguishable from "not captured".
-- A whole-table opaque is a single row with "column" = ''. Recomputed wholesale on every deploy.
CREATE TABLE pond_version_column_lineage (
    pond_version_id INTEGER NOT NULL REFERENCES pond_version(id),
    "table"         TEXT NOT NULL,
    "column"        TEXT NOT NULL DEFAULT '',   -- '' = the whole-table marker (with kind 'opaque')
    kind            TEXT NOT NULL CHECK (kind IN ('exact', 'constant', 'opaque')),
    src_ref         TEXT NOT NULL DEFAULT '',   -- "source.table" (or an own table); '' for non-exact rows
    src_column      TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (pond_version_id, "table", "column", kind, src_ref, src_column)
);
