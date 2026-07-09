"""shop_orders — a plain Ripple inlet feeding the dbt-mode ``shop_analytics`` Pond.

A hand-written Pond can sit upstream of a dbt-mode one: this generates a ``sale`` table that
``shop_analytics``'s dbt models read as a cross-Pond ``source()``. Overwrite output (grows a little each
run), so the dbt models downstream recompute over the current snapshot. Size is env-overridable
(``DUCKSTRING_DBT_DEMO_SALES``; the test suite shrinks it).
"""

import os

from duckstring import ripple

_SALES = int(os.environ.get("DUCKSTRING_DBT_DEMO_SALES", "5000"))
_PRODUCTS = ["Laptop", "Phone", "Tablet", "Watch", "Headphones"]


@ripple
def generate(pond):
    # A fresh batch of sales each run. sale_id continues nothing (overwrite table); the point is to give
    # the dbt models a realistic snapshot to transform, not to be incremental.
    products = "[" + ", ".join(f"'{p}'" for p in _PRODUCTS) + "]"
    rel = pond.con.sql(
        f"""
        SELECT
          i                                                              AS sale_id,
          {products}[CAST(floor(random() * {len(_PRODUCTS)}) AS INTEGER) + 1] AS product,
          round(20 + random() * 480, 2)                                  AS amount,
          (CURRENT_DATE - CAST(floor(random() * 30) AS INTEGER))         AS sale_date
        FROM range({_SALES}) AS t(i)
        """
    )
    pond.write_table("sale", rel)
