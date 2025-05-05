select
    case when is_online then 'Online' else 'Offline' end as sales_channel,
    count(distinct id) as order_count,
    sum(units_sold) as total_units,
    sum(units_sold * p.unit_price) as total_revenue,
    sum(units_sold * (p.unit_price - p.unit_cost)) as total_profit
from
    {{ ref('orders') }} o
join
    {{ ref('product') }} p on o.product_id = p.item_type
group by
    is_online
