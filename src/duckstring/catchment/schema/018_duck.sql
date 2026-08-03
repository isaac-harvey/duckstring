-- Per-Pond Duck config (prereqs D5): the worker's preset size + the Flock posture. An absent row
-- (or a NULL column) inherits. Keyed on pond (the selected version) like all live state; advisory
-- for the local SubprocessLauncher, sizing for remote launchers.
-- SUPERSEDED by 021_cloud.sql: this table becomes the operator OVERRIDE half of a coalesce over the
-- pond.toml-DECLARED config on pond_version (Duck/Flock are a compute hint, so they DO belong in
-- pond.toml now — plans/cloud-config.md). The `flock` bool here is superseded by `flock_mode`.
CREATE TABLE pond_duck (
    pond_id INTEGER PRIMARY KEY REFERENCES pond(id),
    size TEXT CHECK (size IN ('s', 'm', 'l', 'xl')),
    flock INTEGER
);
