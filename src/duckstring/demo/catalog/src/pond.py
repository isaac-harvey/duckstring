"""catalog — a merge Trickle (the history-preserving upsert path).

Each run emits the **complete** current catalogue; ``merge_table`` diffs it against the prior main as a
full-row Z-set difference to derive inserts/updates/deletes automatically (no enumerate-every-change
obligation). So when a price drifts between runs, the change shows up as a ``-1`` of the old row and a
``+1`` of the new in the changelog; a downstream Trickle then re-prices only the affected order lines,
never the whole catalogue.

The catalogue is **generated** at scale (``_PRODUCTS`` rows) from a deterministic base, so an unchanged
product diffs to nothing run to run and the changelog stays small: only the ``_PRICE_CHANGES``
products this run perturbs (plus the handful reverting from last run's drift) reach the ``__changelog``.
That tiny, *targeted* delta — rather than the all-rows churn you get from too few products — is what lets
``priced`` re-price an affected slice instead of the whole join. Sizes are env-overridable
(``DUCKSTRING_DEMO_*``; the test suite shrinks them); no ``time.sleep`` — the full-row diff over the
full catalogue is the real work.

The clean *main* table is the current catalogue (one row per product, no tombstones); the ``__changelog``
companion is the CDC stream a delta read consumes.
"""

import os
import random

from duckstring import ripple

_PRODUCTS = int(os.environ.get("DUCKSTRING_DEMO_PRODUCTS", "100000"))
_PRICE_CHANGES = int(os.environ.get("DUCKSTRING_DEMO_PRICE_CHANGES", "100"))  # products that drift per run
_CATEGORIES = ["Electronics", "Clothing", "Food", "Home", "Books"]


@ripple
def ingest(pond):
    # Pick a small random set of products to re-price this run; everything else regenerates at its
    # deterministic base price (identical hash → no changelog churn). A product that drifted last run but
    # not this one reverts to base — itself a detected change, so the changelog carries ~2·_PRICE_CHANGES
    # rows: small and bounded, however large the catalogue.
    changed = random.sample(range(1, _PRODUCTS + 1), min(_PRICE_CHANGES, _PRODUCTS))
    if changed:
        drift = ", ".join(f"({pid}, {random.choice([0.85, 0.9, 1.1, 1.2, 1.5])})" for pid in changed)
        cte = f"WITH drift(product_id, factor) AS (VALUES {drift}) "
        factor, join = "COALESCE(d.factor, 1.0)", "LEFT JOIN drift d USING (product_id)"
    else:
        cte, factor, join = "", "1.0", ""

    cats = "[" + ", ".join(f"'{c}'" for c in _CATEGORIES) + "]"
    state = pond.con.sql(
        f"""
        {cte}
        SELECT
          p.product_id,
          'Product ' || p.product_id                                  AS name,
          {cats}[CAST(p.product_id % {len(_CATEGORIES)} AS INTEGER) + 1] AS category,
          -- CAST to a declared type: without it the result type follows whichever branch {factor}
          -- took (a bare literal vs a COALESCE over the drift CTE), so the published schema would
          -- change with the DATA — see schema_contract.is_widening.
          CAST(round(p.base_price * {factor}, 2) AS DECIMAL(12,2))    AS unit_price
        FROM (
          SELECT (i + 1) AS product_id, 5.0 + ((i * 37) % 2000) * 0.5 AS base_price
          FROM range({_PRODUCTS}) AS t(i)
        ) p
        {join}
        """
    )
    pond.merge_table("product", state, pk="product_id")  # full current state → Duckstring diffs it for CDC
