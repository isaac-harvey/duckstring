"""gh_repo_activity — an Outlet aggregating enriched pushes per repository.

The end of Path A: reads the clean current state of ``gh_pushes.push`` and rolls it up per repo — total
pushes, total commits, and the distinct set of contributors. Merges comprehensively, so only repos whose
activity actually moved this run reach the changelog. Run it on its own cadence, independent of the
``gh_trending`` path (they share only the ``gh_events`` Inlet).
"""

from duckstring import ripple


@ripple
def by_repo(pond):
    pond.read_table("gh_pushes.push")  # registers the Source as the view `push`
    totals = pond.con.sql("""
        SELECT
            repo_name,
            COUNT(*)                       AS pushes,
            SUM(commits)                   AS commits,
            COUNT(DISTINCT actor_login)    AS contributors
        FROM push
        GROUP BY repo_name
    """)
    pond.merge_table("repo_activity", totals, pk="repo_name")
