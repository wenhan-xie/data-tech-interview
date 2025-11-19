{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'transactions_seed') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        id as transaction_id,
        bid_id,
        state,
        transfer_method,
        inserted_at,
        company_id,
        num_shares,
        price_per_share,
        gross_proceeds,
        _fivetran_deleted,
        _fivetran_synced,
        -- Calculate if transaction is active/open
        -- bid_accepted is also an active state
        case
            when state in ('cancelled', 'expired', 'closed_paid', 'approval_declined') then false
            else true
        end as is_active,
        -- Transaction month for time-based analysis
        date_trunc('month', inserted_at::timestamp) as transaction_month
    from source
)

select * from renamed

