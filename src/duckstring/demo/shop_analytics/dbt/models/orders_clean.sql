-- The first Ripple: read the cross-Pond source and drop non-positive amounts.
select
    sale_id,
    product,
    amount,
    sale_date
from {{ source('shop_orders', 'sale') }}
where amount > 0
