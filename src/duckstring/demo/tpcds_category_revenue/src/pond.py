"""tpcds_category_revenue — an Outlet aggregating priced sales by product category.

Reads the **clean current state** of ``tpcds_priced.priced_sale`` (a Trickle source — its system columns
are stripped on read), re-aggregates per category, and merges comprehensively: Duckstring diffs the new
totals against the prior ones so only the categories whose revenue actually moved hit the changelog. An
aggregate recomputes fully each run (the win is the small delta *out*, not less compute in) — the honest
scope of Trickle. This is one of two Outlets off ``tpcds_priced``; run it at its own cadence (a Tide/Wave
independent of ``tpcds_store_revenue``) to demo a single upstream feeding two paths at different rates.
"""

from duckstring import ripple


@ripple
def by_category(pond):
    pond.read_table("tpcds_priced.priced_sale")  # registers the Source as the view `priced_sale`
    totals = pond.con.sql("""
        SELECT
            category,
            ROUND(SUM(revenue), 2) AS total_revenue,
            SUM(quantity)          AS units_sold,
            COUNT(*)               AS sale_count
        FROM priced_sale
        GROUP BY category
    """)
    pond.merge_table("revenue_by_category", totals, pk="category")
