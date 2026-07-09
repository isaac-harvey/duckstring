"""gh_trending — an Outlet ranking repos by stars and forks.

The end of Path B: reads the clean current state of ``gh_stars.star`` and counts stars vs forks per repo.
The second independent Outlet of the GHArchive demo — run it at a different cadence from
``gh_repo_activity`` to demo one Inlet feeding two entirely separate paths at different rates. Merges
comprehensively, so only repos whose counts moved reach the changelog.
"""

from duckstring import ripple


@ripple
def rank(pond):
    pond.read_table("gh_stars.star")  # registers the Source as the view `star`
    totals = pond.con.sql("""
        SELECT
            repo_name,
            COUNT(*) FILTER (WHERE event_type = 'WatchEvent') AS stars,
            COUNT(*) FILTER (WHERE event_type = 'ForkEvent')  AS forks
        FROM star
        GROUP BY repo_name
    """)
    pond.merge_table("trending", totals, pk="repo_name")
