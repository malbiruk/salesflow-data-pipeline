select
    o.order_priority,
    avg(datediff('day', o.order_date, o.ship_date)) as avg_days_to_ship,
    count(distinct o.id) as order_count,
    sum(o.units_sold * p.unit_price) as total_revenue,
    sum(o.units_sold * (p.unit_price - p.unit_cost)) as total_profit,
from
    {{ ref('orders') }} o
join
    {{ ref('product') }} p on o.product_id = p.item_type
group by
    o.order_priority
order by
    avg_days_to_ship
