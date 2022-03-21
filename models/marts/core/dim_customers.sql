with customers as (
    select * from {{ ref('stg_customers')}}
),

orders as (
    select * from {{ ref('stg_orders')}}
),

payments as (
    select * from {{ ref('stg_payments')}}
    where status = 'success'
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

lifetime_value as (
    select
        o.customer_id,
        sum(p.amount) as lifetime_value
    from orders o
        left join payments p on p.order_id = o.order_id
    group by o.customer_id
),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        lv.lifetime_value

    from customers

    left join customer_orders using (customer_id)
    left join lifetime_value lv using (customer_id)

)

select * from final