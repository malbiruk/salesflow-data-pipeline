{{ config(
    post_hook=[
        "{{ add_pk_constraint(this, 'order_priority') }}"
    ]
) }}

select distinct
    order_priority
from
    {{ ref('raw_sales_data_clean') }}
