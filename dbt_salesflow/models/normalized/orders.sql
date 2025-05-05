{{ config(
    column_types = {
        'id': 'VARCHAR(255)',
        'source_order_id': 'INTEGER',
        'country': 'VARCHAR(255)',
        'is_online': 'BOOLEAN',
        'order_priority': 'VARCHAR(255)',
        'product_id': 'VARCHAR(255)',
        'units_sold': 'INTEGER',
        'order_date': 'DATE',
        'ship_date': 'DATE'
    },
    post_hook=[
        "{{ add_pk_constraint(this, 'id') }}",
        "{{ add_fk_constraint(this, 'country', ref('country'), 'country') }}",
        "{{ add_fk_constraint(this, 'order_priority', ref('order_priority'), 'order_priority') }}",
        "{{ add_fk_constraint(this, 'product_id', ref('product'), 'item_type') }}"
    ]
) }}

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
