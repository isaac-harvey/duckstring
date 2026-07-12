-- Alert re-notify cadence (opt-in, per channel): while a failure/freshness episode persists, the
-- channel is re-notified at most once per interval. NULL (the default) keeps the fire-once-per-episode
-- behaviour. Mechanism: a channel with renotify_ms gets a time-bucketed dedup key
-- ("{kind}:{pond}:{f}:r{bucket}"), so the scheduler tick's re-emission passes the UNIQUE fence once per
-- bucket. No ack surface needed — paging escalation stays PagerDuty's job; this is the "don't stay
-- silently red for a week" floor.
ALTER TABLE alert_channel ADD COLUMN renotify_ms INTEGER;
