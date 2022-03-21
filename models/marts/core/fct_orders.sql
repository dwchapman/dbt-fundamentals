with customers as (
    select * from {{ ref('stg_customers')}}
),

orders as (
    select * from {{ ref('stg_orders')}}
),

payments as (
    select * from {{ ref('stg_payments')}}
    where status = 'success'
)

select
    o.order_id,
    c.customer_id,
    sum(amount) as amount
from orders o
    left join customers c on c.customer_id = o.customer_id
    left join payments p on p.order_id = o.order_id
group by o.order_id, c.customer_id