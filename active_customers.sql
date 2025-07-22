with raw_orders as (
    select *
    from {{ source('ecommerce', 'orders') }}
    where status != 'cancelled'
),

raw_customers as (
    select *
    from {{ source('ecommerce', 'customers') }}
),

joined_data as (
    select
        o.*,
        c.first_name,
        c.last_name,
        c.email,
        c.created_at as customer_created_at
    from raw_orders o
    left join raw_customers c on o.customer_id = c.customer_id
),

latest_orders as (
    select
        customer_id,
        max(created_at) as last_order_date
    from raw_orders
    group by customer_id
),

active_customers as (
    select
        jd.customer_id,
        jd.email,
        lo.last_order_date,
        case
            when lo.last_order_date >= current_date - interval '30 day' then true
            else false
        end as is_active
    from joined_data jd
    left join latest_orders lo on jd.customer_id = lo.customer_id
)

select * fro*
