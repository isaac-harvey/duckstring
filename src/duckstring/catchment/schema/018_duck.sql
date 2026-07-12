-- Per-Pond Duck config (prereqs D5): the worker's preset size + whether it may escalate to the
-- Flock (the co-resident DuckFlock driver's distribution). Operational config, operator-owned —
-- never pond.toml (sizes/escalation are environment decisions, like windows and spouts). An absent
-- row (or a NULL column) inherits the Catchment default (DUCKSTRING_DUCK_SIZE / DUCKSTRING_DUCK_FLOCK
-- env). Keyed on pond (the selected version) like all live state; advisory for the local
-- SubprocessLauncher, sizing for remote launchers.
CREATE TABLE pond_duck (
    pond_id INTEGER PRIMARY KEY REFERENCES pond(id),
    size TEXT CHECK (size IN ('s', 'm', 'l', 'xl')),
    flock INTEGER
);
