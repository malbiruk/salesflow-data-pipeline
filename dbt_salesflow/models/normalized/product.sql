{{ config(
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
