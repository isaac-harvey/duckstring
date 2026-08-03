"""priced — the ``pond.trickle(...)`` builder: incremental star enrichment.

Joins the ``orders`` order-line stream to the ``catalog`` product dimension. The builder composes each
changed Source's **Z-set delta** through the join: a new order line (a ``+1`` on the spine) or a product
whose price drifted (a ``-1``/``+1`` on the dimension) both flow to the right output rows, and a deletion
propagates as a full-row retraction — so the join needs no FK=PK constraint. The output is itself a merge
Trickle (clean main + Z-set changelog) for ``revenue`` to consume.
"""

from duckstring import ripple


@ripple
def priced_line(pond):
    (
        pond.trickle("orders.order_line")
        .join(pond.trickle("catalog.product"), on="product_id")
        .select(
            "s0.order_id, s0.product_id, s0.quantity, s1.unit_price, "
            "CAST(round(s0.quantity * s1.unit_price, 2) AS DECIMAL(14,2)) AS revenue"
        )
        .merge("priced_line", pk="order_id")
    )
