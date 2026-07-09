-- The second Ripple: aggregate the cleaned sales per product. `ref()` makes orders_clean this model's
-- parent — the dbt DAG edge Duckstring tracks as a ripple_to_ripple edge.
select
    product,
    count(*)              as sales,
    round(sum(amount), 2) as revenue
from {{ ref('orders_clean') }}
group by product
