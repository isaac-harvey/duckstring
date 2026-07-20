-- Data serving (plans/data-serving.md), increment 1. The exposure model: a pond.toml-declared
-- serviceable set + an operational override, coalesced (override ?? declared), plus the served-major
-- pointer that the bare `pond.table` schema resolves to.

-- DECLARED serviceable tables (pond.toml [serve] tables) — per artifact, re-read every deploy. Absent
-- ⇒ nothing served (safe-by-default: opt tables IN, never expose an intermediate/PII table by accident).
CREATE TABLE pond_version_serve (
    pond_version_id INTEGER NOT NULL REFERENCES pond_version(id),
    table_name      TEXT    NOT NULL,
    PRIMARY KEY (pond_version_id, table_name)
);

-- Operational override (the eye toggle) — expose a non-declared table or hide a declared one, live.
-- Keyed on the pond LINE (pond_id = name@major), so it persists across minor redeploys of the same major
-- but a NEW major line (a new `pond` row) starts fresh from pond.toml. Effective = override ?? declared.
CREATE TABLE pond_serve_override (
    pond_id    INTEGER NOT NULL REFERENCES pond(id),
    table_name TEXT    NOT NULL,
    exposed    INTEGER NOT NULL,   -- 1 = expose, 0 = hide (an explicit override in either direction)
    PRIMARY KEY (pond_id, table_name)
);

-- The served-major pointer: which major the bare `pond.table` schema resolves to. Per named pond;
-- defaults to the lowest deployed major (first deploy auto-serves) and moves ONLY via `promote`
-- (a serving consumer is a sink — a major bump is opt-in, never auto-advanced).
CREATE TABLE pond_serve (
    pond_name_id INTEGER PRIMARY KEY REFERENCES pond_name(id),
    served_major INTEGER NOT NULL
);
