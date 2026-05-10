select
    order_key
    ,sum(extended_price) as gross_items_sales_amount
from {{ref('int_order_items')}}
group by 
    order_key