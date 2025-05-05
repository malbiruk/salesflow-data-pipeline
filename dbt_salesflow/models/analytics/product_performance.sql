select
    p.item_type,
    sum(o.units_sold) as total_units,
    sum(o.units_sold * p.unit_price) as total_revenue,
    sum(o.units_sold * (p.unit_price - p.unit_cost)) as total_profit,
    (sum(o.units_sold * (p.unit_price - p.unit_cost)) / nullif(sum(o.units_sold * p.unit_price), 0)) * 100 as profit_margin
from
    {{ ref('orders') }} o
join
    {{ ref('product') }} p on o.product_id = p.item_type
group by
    p.item_type
order by
    total_profit desc
