"""tpcds_store_revenue — an Outlet aggregating priced sales by store.

The second, independent Outlet off ``tpcds_priced``: it reads the same enriched sale stream but rolls it
up per store instead of per category. Runs on its own cadence — the two Outlets sharing one upstream
builder is the "run one path at a different frequency to another" demo. Merges comprehensively, so only
the stores whose revenue actually moved reach the changelog.
"""

from duckstring import ripple


@ripple
def by_store(pond):
    pond.read_table("tpcds_priced.priced_sale")  # registers the Source as the view `priced_sale`
    totals = pond.con.sql("""
        SELECT
            store_name,
            ANY_VALUE(market_id)   AS market_id,
            ROUND(SUM(revenue), 2) AS total_revenue,
            SUM(quantity)          AS units_sold,
            COUNT(*)               AS sale_count
        FROM priced_sale
        GROUP BY store_name
    """)
    pond.merge_table("revenue_by_store", totals, pk="store_name")
