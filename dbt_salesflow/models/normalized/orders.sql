select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'order_date']) }} as id,
    order_id as source_order_id,
    country,
    case
      when sales_channel = 'Online' then true
      when sales_channel = 'Offline' then false
    end as is_online,
    order_priority,
    item_type as product_id,
    units_sold,
    order_date,
    ship_date
from
    {{ ref('raw_sales_data_clean') }}

{% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
{% endif %}
