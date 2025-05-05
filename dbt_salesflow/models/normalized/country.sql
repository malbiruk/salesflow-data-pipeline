select distinct
    country,
    region
from
    {{ ref('raw_sales_data_clean') }}
