with payments as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod,
        status,
        amount,
        created,
        _batched_at

    from `dbt-tutorial`.stripe.payment

)

select * from payments