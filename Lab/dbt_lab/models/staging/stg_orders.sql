-- Staging model: Clean và standardize order data từ raw schema
{{ 
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

select
    order_id,
    customer_id,
    order_date,
    total_amount,
    status as order_status,
    current_timestamp as dbt_loaded_at
from {{ source('raw', 'orders') }}

