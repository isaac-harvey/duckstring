"""tpcds_stores — a merge Trickle over the TPC-DS ``store`` dimension (a stable dimension).

A small, slowly-changing dimension: the store roster. Generated once from DuckDB's ``tpcds`` extension
(``dsdgen``), then re-emitted as the complete current state each run. Because the roster doesn't drift,
after the first run ``merge_table`` diffs it to nothing — the changelog stays empty and the table is a
free *stable operand* for the downstream join (its delta is empty, so a sales-only run reconstructs no
store state). That's the honest other half of the CDC story: not every dimension churns.
"""

import os

from duckstring import ripple

_SF = os.environ.get("DUCKSTRING_TPCDS_SF", "0.1")


@ripple
def ingest(pond):
    # Generate the store roster once, stash it, and drop the raw TPC-DS tables. dsdgen's store dimension is
    # SCD (multiple versions per store_id); the surrogate key s_store_sk is unique per row, so it is the
    # merge key directly.
    try:
        (n,) = pond.con.execute("SELECT count(*) FROM store_base").fetchone()
    except Exception:
        n = 0

    if n == 0:
        pond.con.execute(f"INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf={_SF})")
        pond.con.execute(
            """
            CREATE TABLE store_base AS
            SELECT
              s_store_sk,
              COALESCE(s_store_name, 'Store ' || s_store_sk) AS store_name,
              COALESCE(s_market_id, 0)                       AS market_id
            FROM store
            WHERE s_store_sk IS NOT NULL
            """
        )
        for t in ("call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
                  "customer_address", "customer_demographics", "date_dim", "household_demographics",
                  "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
                  "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
                  "web_sales", "web_site"):
            pond.con.execute(f"DROP TABLE IF EXISTS {t}")

    state = pond.con.sql("SELECT s_store_sk, store_name, market_id FROM store_base")
    pond.merge_table("store", state, pk="s_store_sk")
