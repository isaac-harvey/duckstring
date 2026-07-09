"""gh_events — an append Trickle over the real GHArchive public event stream.

GHArchive publishes one gzipped-JSON file of every public GitHub event per hour at
``https://data.gharchive.org/YYYY-MM-DD-H.json.gz`` — public, no auth. An hour *is* a delta, so this is
genuine streamed real data, not emulation. Each run advances an hour cursor (kept in the registry) and
appends that hour's events; the first run backfills a few hours so the chain starts with real volume.

``event_id`` is unique per GitHub event, so this is the trust-the-writer append fast path — but because a
re-read of the same hour would otherwise duplicate, incoming rows are anti-joined against the history
first, making ingestion idempotent (and a real gap/offline hour a harmless no-op).

**Config** (env): ``DUCKSTRING_GHARCHIVE_BASE`` (default the public bucket; point it at a local directory
of ``{hour}.json.gz`` files to run offline), ``DUCKSTRING_GHARCHIVE_START`` (first hour, ``YYYY-MM-DD-H``),
``DUCKSTRING_GHARCHIVE_BACKFILL`` (hours loaded on the first run). The test suite points BASE at a small
committed fixture so it never touches the network.
"""

import os
from datetime import datetime, timedelta

from duckstring import ripple

_BASE = os.environ.get("DUCKSTRING_GHARCHIVE_BASE", "https://data.gharchive.org").rstrip("/")
_START = os.environ.get("DUCKSTRING_GHARCHIVE_START", "2024-01-15-0")
_BACKFILL = int(os.environ.get("DUCKSTRING_GHARCHIVE_BACKFILL", "3"))

_JSON_COLS = "{id:'VARCHAR', type:'VARCHAR', actor:'JSON', repo:'JSON', payload:'JSON', created_at:'TIMESTAMP'}"


def _parse_hour(s: str) -> datetime:
    date, hour = s.rsplit("-", 1)
    return datetime.fromisoformat(date) + timedelta(hours=int(hour))


def _fmt_hour(dt: datetime) -> str:
    return f"{dt:%Y-%m-%d}-{dt.hour}"  # GHArchive uses an unpadded hour (…-15.json.gz, …-0.json.gz)


@ripple
def ingest(pond):
    if _BASE.startswith("http"):
        pond.con.execute("INSTALL httpfs; LOAD httpfs")

    # The cursor (next hour to read) persists in the registry. First run → backfill from the start hour;
    # steady state → the single next hour.
    try:
        (cursor,) = pond.con.execute("SELECT next_hour FROM gh_cursor").fetchone()
        hours = [cursor]
    except Exception:
        start = _parse_hour(_START)
        hours = [_fmt_hour(start + timedelta(hours=k)) for k in range(_BACKFILL)]

    # Read consecutive hours, stopping at the first one that isn't published yet: we HOLD on a gap rather
    # than marching past it (the next hour of a live stream simply hasn't landed — wait for it, don't skip
    # it). So the cursor only advances over hours actually ingested.
    frames = []
    last_ok = None
    for hour in hours:
        url = f"{_BASE}/{hour}.json.gz"
        try:
            frames.append(
                pond.con.sql(
                    f"""
                    SELECT
                      id                                     AS event_id,
                      type,
                      actor->>'login'                        AS actor_login,
                      TRY_CAST(actor->>'id' AS BIGINT)       AS actor_id,
                      actor->>'avatar_url'                   AS actor_avatar,
                      repo->>'name'                          AS repo_name,
                      TRY_CAST(repo->>'id' AS BIGINT)        AS repo_id,
                      created_at,
                      TRY_CAST(payload->>'size' AS INTEGER)  AS push_size
                    FROM read_json('{url}', columns={_JSON_COLS})
                    WHERE id IS NOT NULL
                    """
                )
            )
            last_ok = hour
        except Exception as exc:  # not published yet (or offline) — hold here, retry next run
            print(f"gh_events: holding at {hour} ({type(exc).__name__}: {exc})")
            break

    # Advance past the last hour we actually ingested; if we ingested nothing, hold on the first attempted
    # hour (retry it next run).
    next_hour = _fmt_hour(_parse_hour(last_ok) + timedelta(hours=1)) if last_ok else hours[0]
    pond.con.execute("CREATE TABLE IF NOT EXISTS gh_cursor (next_hour VARCHAR)")
    pond.con.execute("DELETE FROM gh_cursor")
    pond.con.execute("INSERT INTO gh_cursor VALUES (?)", [next_hour])

    if not frames:
        pond.skip()  # nothing fetched this run — hold freshness, don't publish an empty delta
        return

    incoming = frames[0]
    for f in frames[1:]:
        incoming = incoming.union(f)

    # Idempotent append: drop any event_id already in the history (safe re-read of a fetched hour).
    try:
        pond.con.execute("SELECT 1 FROM gh_event LIMIT 1")
        incoming.create_view("_incoming", replace=True)
        fresh = pond.con.sql(
            "SELECT * FROM _incoming WHERE event_id NOT IN (SELECT event_id FROM gh_event)"
        )
    except Exception:
        fresh = incoming

    pond.append_table("gh_event", fresh, pk="event_id", fail_on_conflict=False)
