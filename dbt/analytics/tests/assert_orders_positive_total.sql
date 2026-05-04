-- Fail if any order has a non-positive total
select order_id
from {{ ref('fct_orders') }}
where order_total_usd <= 0