"""tpcds_priced — the ``pond.trickle(...)`` builder: incremental star enrichment (a 3-way join).

Joins the ``tpcds_sales`` fact stream to the ``tpcds_items`` and ``tpcds_stores`` dimensions. The builder
composes each changed Source's **Z-set delta** through the join: a new sale (a ``+1`` on the spine) or an
item whose price drifted (a ``-1``/``+1`` on the item dimension) both flow to exactly the affected output
rows — a price change re-prices only the sales of that item, never the whole fact. The store dimension is
stable, so its empty delta drops out for free. The output is itself a merge Trickle (clean main + Z-set
changelog) for the two Outlets to consume.
"""

from duckstring import ripple


@ripple
def enrich(pond):
    (
        pond.trickle("tpcds_sales.store_sale")
        .join(pond.trickle("tpcds_items.item"), on={"ss_item_sk": "i_item_sk"})
        .join(pond.trickle("tpcds_stores.store"), on={"ss_store_sk": "s_store_sk"})
        .select(
            "s0.sale_id, s1.category, s1.class, s1.brand, s2.store_name, s2.market_id, "
            "s0.ss_quantity AS quantity, s1.unit_price, "
            "round(s0.ss_quantity * s1.unit_price, 2) AS revenue"
        )
        .merge("priced_sale", pk="sale_id")
    )
