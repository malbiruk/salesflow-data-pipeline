-- Ensure both online and offline channels have data

SELECT count(distinct sales_channel) as channel_count
FROM {{ ref('performance_by_channel') }}
HAVING channel_count != 2
