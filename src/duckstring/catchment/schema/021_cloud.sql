-- Cloud-configurable OSS, phase 2 (plans/cloud-config.md). Three moves:
--   1. Catchment-level persisted settings (the S3 data-plane target lands here in increment 2).
--   2. Named Duck Pools — Catchment-level remote-compute config (NOT pond.toml; environment infra).
--   3. Per-Pond compute config as a coalesce: DECLARED on the immutable artifact (pond_version, from
--      pond.toml, re-read every redeploy) with a nullable operator OVERRIDE (pond_duck). Effective =
--      override ?? declared ?? Catchment default. This reverses 018_duck.sql's "never pond.toml" note:
--      Duck/Flock are a compute *hint* (a property of the transform's needs), so they belong in
--      pond.toml — but as a POOL NAME, never raw instance specs (those are environment-specific).

-- 1. Catchment-wide settings, key/value.
CREATE TABLE catchment_setting (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- 2. Named Duck Pools (shared remote compute; inert until an EC2 launcher is configured).
CREATE TABLE duck_pool (
    name          TEXT PRIMARY KEY,
    instance_type TEXT,
    min_instances INTEGER NOT NULL DEFAULT 0,   -- floor (keep-warm baseline)
    max_instances INTEGER NOT NULL DEFAULT 1,   -- ceiling
    idle_timeout  INTEGER,                       -- seconds before scale-down
    keep_warm     INTEGER NOT NULL DEFAULT 0,    -- spare capacity beyond current load (thin; n+1)
    region        TEXT                           -- defaults to the Catchment region when NULL
);

-- 3a. DECLARED per-Pond compute, on the immutable artifact (from pond.toml; updates on every redeploy).
--     duck_pool: a Duck Pool name | 'catchment' | NULL (→ Catchment default). A name this Catchment
--     doesn't have falls back to the Catchment Duck at resolve time (pond.toml stays portable).
ALTER TABLE pond_version ADD COLUMN duck_pool    TEXT;
ALTER TABLE pond_version ADD COLUMN flock_mode   TEXT;   -- off | upgrade | always | NULL
ALTER TABLE pond_version ADD COLUMN flock_engine TEXT;   -- athena | … | NULL (→ Catchment default)
ALTER TABLE pond_version ADD COLUMN oom_policy   TEXT;   -- fail_up | fail | NULL

-- 3b. Operator OVERRIDE, on the live pond (all nullable; coalesced over declared then default).
--     duck_target: 'catchment' | a pool name | 'dedicated' | NULL. 'dedicated' = a Pond's own box
--     (operator-only — never pond.toml, since it needs an instance spec).
ALTER TABLE pond_duck ADD COLUMN duck_target             TEXT;
ALTER TABLE pond_duck ADD COLUMN dedicated_instance_type TEXT;
ALTER TABLE pond_duck ADD COLUMN dedicated_auto_stop     INTEGER;   -- terminate on Pond-run completion
ALTER TABLE pond_duck ADD COLUMN flock_mode              TEXT;
ALTER TABLE pond_duck ADD COLUMN flock_engine            TEXT;
ALTER TABLE pond_duck ADD COLUMN oom_policy              TEXT;
-- The legacy pond_duck.size (advisory preset) is kept. The legacy pond_duck.flock (bool on/off) is
-- superseded by flock_mode; on read, a legacy True ⇒ 'upgrade', False ⇒ 'off' when flock_mode is NULL.
