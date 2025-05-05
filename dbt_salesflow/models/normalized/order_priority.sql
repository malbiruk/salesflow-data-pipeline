select distinct
    order_priority
from
    {{ ref('raw_sales_data_clean') }}
