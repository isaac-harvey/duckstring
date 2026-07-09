"""tpcds_sales — an append Trickle over the TPC-DS ``store_sales`` fact.

The canonical analytics fact table, streamed as deltas. TPC-DS has no native stream, so the streaming is
**emulated**: the first run generates the base fact with DuckDB's ``tpcds`` extension
(``CALL dsdgen(sf=…)``, fully offline and deterministic — no auth, no network); every run after that
appends a small synthetic ``_BATCH`` of new sales referencing the item/store surrogate keys already in
the history, so the foreign keys stay valid without regenerating the dimensions.

That contrast — a large standing fact, a tiny per-run delta — is the whole point of the incremental demo,
so the base is deliberately sizeable. Scale and batch size are env-overridable (``DUCKSTRING_TPCDS_SF`` /
``DUCKSTRING_TPCDS_BATCH``; the test suite shrinks them to keep the e2e quick). ``sale_id`` is unique by
construction (a continuing sequence), so this is the trust-the-writer append fast path — ``append_table``
does no diff and the single history table is at once the full read and the delta source.
"""

import os

from duckstring import ripple

_SF = os.environ.get("DUCKSTRING_TPCDS_SF", "0.1")  # dsdgen scale factor for the base fact
_BATCH = int(os.environ.get("DUCKSTRING_TPCDS_BATCH", "20000"))  # new sales appended every subsequent run

_COLS = "sale_id, ss_item_sk, ss_store_sk, ss_customer_sk, ss_quantity, ss_sales_price, ss_net_profit"


@ripple
def ingest(pond):
    # The history table persists in the registry, so the id sequence continues across runs. The first run
    # (no table yet) generates the base fact; every run after it appends a small synthetic batch.
    try:
        (next_id, max_item, max_store, max_cust) = pond.con.execute(
            "SELECT COALESCE(MAX(sale_id), -1) + 1, MAX(ss_item_sk), MAX(ss_store_sk), MAX(ss_customer_sk) "
            "FROM store_sale"
        ).fetchone()
    except Exception:
        next_id, max_item, max_store, max_cust = 0, None, None, None

    if next_id == 0:
        # Bootstrap: generate the full TPC-DS dataset, keep only store_sales (stamped with a fresh unique
        # sale_id), then drop the raw tables so the registry doesn't carry the whole schema.
        pond.con.execute(f"INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf={_SF})")
        batch = pond.con.sql(
            """
            SELECT
              (row_number() OVER () - 1)            AS sale_id,
              ss_item_sk, ss_store_sk, ss_customer_sk,
              ss_quantity, ss_sales_price, ss_net_profit
            FROM store_sales
            WHERE ss_item_sk IS NOT NULL AND ss_store_sk IS NOT NULL
            """
        )
        pond.append_table("store_sale", batch, pk="sale_id", fail_on_conflict=False)
        for t in ("call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
                  "customer_address", "customer_demographics", "date_dim", "household_demographics",
                  "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
                  "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
                  "web_sales", "web_site"):
            pond.con.execute(f"DROP TABLE IF EXISTS {t}")
        return

    # Subsequent runs: synthesise a small batch of new sales, drawing item/store/customer keys uniformly
    # from the range already present in the history so every foreign key resolves against the dimensions.
    batch = pond.con.sql(
        f"""
        SELECT
          {next_id} + i                                              AS sale_id,
          CAST(floor(random() * {max_item}) + 1 AS INTEGER)          AS ss_item_sk,
          CAST(floor(random() * {max_store}) + 1 AS INTEGER)         AS ss_store_sk,
          CAST(floor(random() * {max_cust}) + 1 AS INTEGER)          AS ss_customer_sk,
          CAST(floor(random() * 8) + 1 AS INTEGER)                   AS ss_quantity,
          round(5 + random() * 195, 2)                               AS ss_sales_price,
          round((random() - 0.4) * 50, 2)                            AS ss_net_profit
        FROM range({_BATCH}) AS t(i)
        """
    )
    pond.append_table("store_sale", batch, pk="sale_id", fail_on_conflict=False)
