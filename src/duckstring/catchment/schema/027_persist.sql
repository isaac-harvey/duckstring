-- Persist (plans/persist.md): local-first publish + async durable mirror.
-- persisted_f = the freshness through which this line's published output has been mirrored to the data
-- root (<= end_f; NULL = never persisted / no cloud). pond_persist is the per-mirror log — the Persist is
-- a separate log item from the Pond Run (the run closes at local publish).
ALTER TABLE pond_state ADD COLUMN persisted_f TEXT;

CREATE TABLE pond_persist (
    pond_id     INTEGER NOT NULL REFERENCES pond(id) ON DELETE CASCADE,
    f           TEXT    NOT NULL,           -- the run freshness this mirror covered (through at least)
    status      TEXT    NOT NULL,           -- success | failed
    error       TEXT,                       -- failure message (sanitised Duck-side)
    finished_at TEXT    NOT NULL,           -- UTC ISO-8601
    PRIMARY KEY (pond_id, f)
);
