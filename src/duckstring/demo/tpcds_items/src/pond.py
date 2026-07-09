"""tpcds_items — a merge Trickle over the TPC-DS ``item`` dimension (CDC on price drift).

Each run emits the **complete** current catalogue; ``merge_table`` diffs it against the prior main as a
full-row Z-set difference, so a price that drifts between runs surfaces as a ``-1`` of the old row and a
``+1`` of the new in the ``__changelog`` — a downstream Trickle then re-prices only the affected sales,
never the whole catalogue.

The item attributes come from DuckDB's ``tpcds`` extension (``dsdgen``, offline + deterministic). Because
``dsdgen`` is deterministic, the first run stashes the generated attributes in a private ``item_base``
table (kept in the registry, never published); every run then re-emits that stable base with a small
random subset of prices perturbed — so an unchanged item diffs to nothing and the changelog carries only
the ``_PRICE_CHANGES`` items that drifted this run (plus the handful reverting from last run's drift).
Sizes are env-overridable (``DUCKSTRING_TPCDS_*``; the test suite shrinks them).
"""

import os
import random

from duckstring import ripple

_SF = os.environ.get("DUCKSTRING_TPCDS_SF", "0.1")
_PRICE_CHANGES = int(os.environ.get("DUCKSTRING_TPCDS_PRICE_CHANGES", "50"))  # items that drift per run


@ripple
def ingest(pond):
    # Bootstrap the deterministic base once from dsdgen, then drop the raw TPC-DS tables. i_item_sk is
    # unique per row, so it is the merge key directly (no SCD dedup needed).
    try:
        (n_items,) = pond.con.execute("SELECT count(*) FROM item_base").fetchone()
    except Exception:
        n_items = 0

    if n_items == 0:
        pond.con.execute(f"INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf={_SF})")
        pond.con.execute(
            """
            CREATE TABLE item_base AS
            SELECT
              i_item_sk,
              i_product_name                       AS name,
              COALESCE(i_category, 'Unknown')      AS category,
              COALESCE(i_class, 'Unknown')         AS class,
              COALESCE(i_brand, 'Unknown')         AS brand,
              COALESCE(i_current_price, 1.00)      AS base_price
            FROM item
            WHERE i_item_sk IS NOT NULL
            """
        )
        for t in ("call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
                  "customer_address", "customer_demographics", "date_dim", "household_demographics",
                  "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
                  "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
                  "web_sales", "web_site"):
            pond.con.execute(f"DROP TABLE IF EXISTS {t}")
        (n_items,) = pond.con.execute("SELECT count(*) FROM item_base").fetchone()

    # Perturb a small random subset of prices; everything else re-emits at its deterministic base price
    # (identical row → no changelog churn). An item that drifted last run but not this one reverts to base
    # — itself a detected change — so the changelog carries ~2·_PRICE_CHANGES rows, small however large
    # the catalogue.
    changed = random.sample(range(1, n_items + 1), min(_PRICE_CHANGES, n_items))
    if changed:
        drift = ", ".join(f"({sk}, {random.choice([0.85, 0.9, 1.1, 1.25, 1.5])})" for sk in changed)
        cte = f"WITH drift(i_item_sk, factor) AS (VALUES {drift}) "
        factor, join = "COALESCE(d.factor, 1.0)", "LEFT JOIN drift d USING (i_item_sk)"
    else:
        cte, factor, join = "", "1.0", ""

    state = pond.con.sql(
        f"""
        {cte}
        SELECT
          b.i_item_sk, b.name, b.category, b.class, b.brand,
          round(b.base_price * {factor}, 2) AS unit_price
        FROM item_base b
        {join}
        """
    )
    pond.merge_table("item", state, pk="i_item_sk")
