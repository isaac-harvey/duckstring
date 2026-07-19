-- Retire the abstract Duck size (s/m/l/xl) and the legacy flock bool from pond_duck. Sizing is now
-- CONCRETE everywhere (plans/cloud-config.md): a pool's instance_type, a dedicated instance_type, or
-- "whatever the host is" for the Catchment box; the Flock envelope no longer derives from an abstract
-- preset. On DSC the S/M/L/XL presets are just named pools — there is no separate size concept left.
--
-- A table rebuild (not DROP COLUMN): `size` carries a CHECK constraint that references it, which
-- SQLite won't let DROP COLUMN remove. The legacy `flock` bool (superseded by flock_mode since 021) is
-- folded into flock_mode on the way (True⇒upgrade, False⇒off) so no operator override is lost.
CREATE TABLE pond_duck_new (
    pond_id                 INTEGER PRIMARY KEY REFERENCES pond(id),
    duck_target             TEXT,      -- 'catchment' | a pool name | 'dedicated'
    dedicated_instance_type TEXT,
    dedicated_auto_stop     INTEGER,
    flock_mode              TEXT,      -- off | upgrade | always
    flock_engine            TEXT,
    oom_policy              TEXT       -- fail_up | fail
);
INSERT INTO pond_duck_new
    (pond_id, duck_target, dedicated_instance_type, dedicated_auto_stop, flock_mode, flock_engine, oom_policy)
SELECT pond_id, duck_target, dedicated_instance_type, dedicated_auto_stop,
       COALESCE(flock_mode, CASE WHEN flock IS NULL THEN NULL WHEN flock = 1 THEN 'upgrade' ELSE 'off' END),
       flock_engine, oom_policy
FROM pond_duck;
DROP TABLE pond_duck;
ALTER TABLE pond_duck_new RENAME TO pond_duck;
