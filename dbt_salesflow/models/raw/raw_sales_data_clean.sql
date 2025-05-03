with deduplicated_sales as (
    select
        *,
        row_number() over (
            partition by order_id
            order by order_date
        ) as row_num
    from {{ source('raw', 'raw_sales_data') }}
)

select
    region,
    country,
    item_type,
    sales_channel,
    order_priority,
    order_date,
    order_id,
    ship_date,
    units_sold,
    unit_price,
    unit_cost
from deduplicated_sales
where row_num = 1
