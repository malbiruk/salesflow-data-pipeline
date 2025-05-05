select
    o.order_date,
    sum(o.units_sold * p.unit_price) as total_revenue,
    sum(o.units_sold * (p.unit_price - p.unit_cost)) as total_profit
from
    {{ ref('orders') }} o
join
    {{ ref('product') }} p on o.product_id = p.item_type
group by
    o.order_date
order by
    o.order_date
