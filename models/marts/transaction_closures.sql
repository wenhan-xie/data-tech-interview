{{ config(materialized='table') }}

-- Returns closed transactions with closure timestamps

select
    transaction_id,
    inserted_at as transaction_created_at,
    closed_at,
    state,
    transfer_method,
    gross_proceeds,
    hours_open,
    case
        when hours_open is not null then hours_open / 24.0
        else null
    end as days_open
from {{ ref('fct_transactions') }}
where closed_at is not null
order by closed_at desc

