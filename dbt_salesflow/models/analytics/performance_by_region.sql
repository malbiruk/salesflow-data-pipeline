select
    r.region,
    c.country,
    sum(o.units_sold * p.unit_price) as total_revenue,
    sum(o.units_sold * (p.unit_price - p.unit_cost)) as total_profit,
    count(distinct o.id) as order_count
from
    {{ ref('orders') }} o
join
    {{ ref('country') }} c on o.country = c.country
join
    {{ ref('region') }} r on c.region = r.region
join
    {{ ref('product') }} p on o.product_id = p.item_type
group by
    r.region, c.country
order by
    total_revenue desc
