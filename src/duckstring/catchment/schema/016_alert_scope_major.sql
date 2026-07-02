-- Re-scope an alert channel to a specific major line (name@major), like every other Pond-attached
-- construct (spouts, windows, triggers, retry). Adds `scope_major` alongside `scope_name`:
--   (NULL,  NULL)  → catchment-wide (matches every Pond)
--   (name,  major) → one line, name@major — a Pond-scoped channel is always a specific major
-- A bare pond name resolves to its highest deployed major at add-time (like every other CLI/API surface);
-- "any major of a name" is not a thing. A removal of name@major deletes the channels scoped to exactly
-- (name, major). See plans/remove-pond.md.
ALTER TABLE alert_channel ADD COLUMN scope_major INTEGER;

-- Backfill any pre-existing name-scoped channel to its highest deployed major (no-op on a fresh DB, and on
-- a channel naming a not-yet-deployed Pond it stays NULL — inert until re-added).
UPDATE alert_channel SET scope_major = (
    SELECT MAX(p.major) FROM pond p JOIN pond_name pn ON pn.id = p.pond_name_id
    WHERE pn.name = alert_channel.scope_name
) WHERE scope_name IS NOT NULL AND scope_major IS NULL;
