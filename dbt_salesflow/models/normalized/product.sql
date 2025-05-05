{{ config(
    column_types = {
        'item_type': 'VARCHAR(255)',
        'unit_cost': 'FLOAT',
        'unit_price': 'FLOAT'
    },
    post_hook=[
        "{{ add_pk_constraint(this, 'item_type') }}"
    ]
) }}

select distinct
    item_type,
    unit_cost,
    unit_price
from
    {{ ref('raw_sales_data_clean') }}
