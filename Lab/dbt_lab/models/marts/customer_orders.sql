-- Mart model: Customer orders aggregation
{{ 
  config(
    materialized='table',
    tags=['marts', 'customers']
  )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
    select
        customer_id,
        count(*) as order_count,
        sum(total_amount) as lifetime_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders
    where order_status = 'completed'
    group by 1
)

select
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customers.email,
    customers.country,
    coalesce(customer_orders.order_count, 0) as order_count,
    coalesce(customer_orders.lifetime_value, 0) as lifetime_value,
    customer_orders.first_order_date,
    customer_orders.last_order_date,
    current_timestamp as dbt_updated_at
from customers
left join customer_orders using (customer_id)

