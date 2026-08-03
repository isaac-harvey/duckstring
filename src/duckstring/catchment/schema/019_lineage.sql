-- Observed table-level lineage (plans/lineage.md Phase 1): the tables each Ripple Run actually read
-- and wrote, recorded by the Pond handle at the moment of the call and shipped on the ripple event —
-- exact, never inferred. Keyed on pond_version like all history (lineage is a property of a version's
-- run), one row per (attempt, direction, table). source_name is '' for an own-table read and for every
-- write (writes are always this Pond's own outputs) — '' not NULL, because NULLs are pairwise-distinct
-- in a unique constraint and a replayed event would duplicate the row.
CREATE TABLE ripple_run_lineage (
    pond_version_id INTEGER NOT NULL REFERENCES pond_version(id),
    ripple          TEXT NOT NULL,
    f               TEXT NOT NULL,               -- the Pond Run freshness (UTC ISO-8601)
    retry           INTEGER NOT NULL DEFAULT 0,  -- the attempt index (matches ripple_run)
    direction       TEXT NOT NULL CHECK (direction IN ('read', 'write')),
    source_name     TEXT NOT NULL DEFAULT '',    -- the Source pond, or '' (own table / any write)
    table_name      TEXT NOT NULL,
    PRIMARY KEY (pond_version_id, ripple, f, retry, direction, source_name, table_name)
);

CREATE INDEX idx_ripple_run_lineage_table ON ripple_run_lineage(table_name);
