{{ config(materialized='table') }}

-- Average transaction open time by month

select
    transaction_month,
    count(*) as total_transactions,
    avg(hours_open) as avg_hours_open,
    avg(hours_open) / 24.0 as avg_days_open,
    min(hours_open) as min_hours_open,
    max(hours_open) as max_hours_open,
    median(hours_open) as median_hours_open
from {{ ref('fct_transactions') }}
where is_active = false  -- Only closed/terminated transactions
  and hours_open is not null
group by transaction_month
order by transaction_month desc

