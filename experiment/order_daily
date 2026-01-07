from duckstring import Pond

def pond():
    # Pond
    ## Needs the name and version, as this sets the schema location in the lake and the version of all tables within this pond
    pond = Pond(
        name="order_daily",
        description="Daily aggregated sales at product x region granularity.",
        version="1.2.3",
    )

    # Sources
    ## Needs the name and version of foreign ponds
    pond.source({
        "order": "1.0.1",
        "customer": "1.0.1",
        "product": "1.0.1",
        "region": "1.0.1",
    })

    # Flow
    ## Can sequence a set of actions in sequence (new flow() action) or in parallel (inside the flow() list)
    pond.flow([
        order_daily_staging(pond),
    ])

    pond.flow([
        order_daily(pond),
    ])

    # Return the pond itself
    return pond

def order_daily_staging(pond):
    # Abstract to Ibis
    ## The get operation requires explicit column selection and aliasing
    order = pond.upstream['order'].get(
        "order",
        {
            "order_id": "order_id",
            "order_date": "order_date",
            "customer_id": "customer_id",
            "product_id": "product_id",
            "region_id": "region_id",
            "sales_amount": "sales_amount"
        },
    )
    customer = pond.upstream['customer'].get(
        "customer",
        {
            "customer_id": "customer_id",
            "customer_name": "customer_name",
            "customer_segment": "customer_segment"
        },
    )
    product = pond.upstream['product'].get(
        "product",
        {
            "product_id": "product_id",
            "product_name": "product_name",
            "product_category": "product_category"
        },
    )
    region = pond.upstream['region'].get(
        "region",
        {
            "region_id": "region_id",
            "region_name": "region_name"
        },
    )

    # Transformations in python using Ibis
    order_daily_staging = (
        order
        .join(customer, order.customer_id == customer.customer_id)
        .join(product, order.product_id == product.product_id)
        .join(region, order.region_id == region.region_id)
        .group_by(order.order_date, product.product_category, region.region_name)
        .aggregate(sales_amount_sum=order.sales_amount.sum())
    )

    # Sink
    ## Use sink_private for internal tables not exposed outside the pond
    ## Here there's only one table being sunk, but sink_private can take multiple tables as a dict and write them in parallel
    pond.sink_private({
        "order_daily_staging": order_daily_staging,
    })

def order_daily(pond):
    # Abstract to Ibis
    order_daily_staging = pond.get(
        "order_daily_staging",
        {
            "order_date": "order_date", 
            "product_category": "product_category", 
            "region_name": "region_name", 
            "sales_amount_sum": "sales_amount_sum"
        },
    )

    # Sometimes a select + alias is all that's needed, so transformations can be skipped
    order_daily = order_daily_staging

    # Sink
    pond.sink({
        "order_daily": order_daily,
    })