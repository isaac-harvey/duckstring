-- The third Ripple (this Pond's headline Outlet table): rank products by revenue.
select
    product,
    revenue,
    sales,
    rank() over (order by revenue desc) as revenue_rank
from {{ ref('revenue_by_product') }}
order by revenue_rank
