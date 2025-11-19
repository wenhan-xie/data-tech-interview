{{ config(materialized='table') }}

-- Most common termination reasons with counts and percentages

with transactions_with_reasons as (
    select
        ft.*,
        tr.termination_reason as explicit_termination_reason,
        -- Fallback termination reason based on state if explicit reason is missing
        case 
            when ft.state = 'cancelled' then 'Cancelled (No specific reason)'
            when ft.state = 'expired' then 'Expired (No specific reason)'
            when ft.state = 'approval_declined' then 'Approval Declined (No specific reason)'
            when ft.state = 'closed_paid' then 'Closed Paid (Completed)'
            else 'Other'
        end as fallback_termination_reason
    from {{ ref('fct_transactions') }} ft
    left join {{ ref('stg_termination_reasons') }} tr 
        on ft.transaction_id = tr.transaction_id
    where ft.is_active = false  -- Only terminated transactions
),

final_termination_reasons as (
    select
        *,
        coalesce(explicit_termination_reason, fallback_termination_reason) as final_termination_reason
    from transactions_with_reasons
)

select
    final_termination_reason as termination_reason,
    count(*) as transaction_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 2) as percentage,
    sum(gross_proceeds) as total_gross_proceeds,
    avg(gross_proceeds) as avg_gross_proceeds,
    avg(hours_open) as avg_hours_open,
    avg(hours_open) / 24.0 as avg_days_open
from final_termination_reasons
group by final_termination_reason
order by transaction_count desc

