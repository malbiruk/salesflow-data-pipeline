{{ config(
    post_hook=[
        "{{ add_pk_constraint(this, 'region') }}"
    ]
) }}

select distinct
    region
from
    {{ ref('raw_sales_data_clean') }}
