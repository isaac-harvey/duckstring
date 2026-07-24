-- Persist timing (plans/persist.md): the mirror's wall-clock start, so the run history can show how
-- long a Persist took (finished_at already exists). Reported by the Duck on the persist event.
ALTER TABLE pond_persist ADD COLUMN started_at TEXT;
