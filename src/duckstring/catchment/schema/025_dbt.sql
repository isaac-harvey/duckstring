-- dbt-mode marker on a deployed version, so the UI can flag a dbt-mode Pond (its Ripples are dbt models)
-- without re-reading pond.toml. Set from `[pond] dbt_project` at deploy; 0 for a normal (Python) Pond.
ALTER TABLE pond_version ADD COLUMN dbt INTEGER NOT NULL DEFAULT 0;
