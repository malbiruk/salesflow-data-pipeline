{{ config(
    post_hook=[
        "{{ add_pk_constraint(this, 'country') }}",
        "{{ add_fk_constraint(this, 'region', ref('region'), 'region') }}"
    ]
) }}

select distinct
    country,
    region
from
    {{ ref('raw_sales_data_clean') }}
