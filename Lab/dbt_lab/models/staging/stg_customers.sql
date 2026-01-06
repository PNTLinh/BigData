-- Staging model: Clean và standardize customer data từ raw schema
{{ 
  config(
    materialized='view',
    tags=['staging', 'customers']
  )
}}

select
    customer_id,
    first_name,
    last_name,
    email,
    registration_date,
    country,
    current_timestamp as dbt_loaded_at
from {{ source('raw', 'customers') }}

