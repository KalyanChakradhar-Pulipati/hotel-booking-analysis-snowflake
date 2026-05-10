{{ config(materialized='table') }}

select
    o.order_status,
    count(distinct o.order_key) as order_count,
    sum(l.extended_price * (1 - l.discount_percentage)) as net_revenue
from {{ ref('stg_tpch_orders') }} o
join {{ ref('stg_tpch_lineitem') }} l
  on o.order_key = l.order_key
group by o.order_status
