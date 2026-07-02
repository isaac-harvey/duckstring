-- Re-scope an alert channel to a specific major line (name@major), like every other Pond-attached
-- construct (spouts, windows, triggers, retry). Adds `scope_major` alongside `scope_name`:
--   (NULL,  NULL)  → catchment-wide (matches every Pond)
--   (name,  NULL)  → the whole name (any major) — the legacy name-scope, still honoured
--   (name,  major) → one line, name@major — the new default from the CLI/UI
-- A removal of name@major deletes the channels scoped to exactly (name, major); a name-wide or
-- catchment-wide channel survives. See plans/remove-pond.md.
ALTER TABLE alert_channel ADD COLUMN scope_major INTEGER;
