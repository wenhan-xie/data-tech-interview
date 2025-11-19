{{ config(materialized='table') }}

/*
Grain: 1 row per transaction

Purpose: Central fact table for transaction lifecycle & proceeds analytics.
         Provides enriched transaction data with calculated metrics including
         closure timestamps, open duration, and termination reasons.

Used by: 
  - Execution team dashboards (closure timing, duration, reasons, proceeds)
  - Monthly average open time analysis
  - Termination reason analysis
  - Transfer method revenue analysis

Key Metrics:
  - closed_at: Timestamp when transaction closed
  - hours_open: Duration transaction was open
  - termination_reason: Reason transaction was terminated
  - gross_proceeds: Revenue value
*/

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

transitions as (
    select * from {{ ref('stg_transaction_transitions') }}
),

-- Find when transactions closed (state = 'closed_paid')
transaction_closures as (
    select
        transaction_id,
        transitioned_at as closed_at,
        row_number() over (partition by transaction_id order by transitioned_at desc) as rn
    from transitions
    where new_state = 'closed_paid'
),

-- Find when transactions transitioned to terminal states
transaction_terminal_states as (
    select
        transaction_id,
        new_state,
        transitioned_at,
        row_number() over (partition by transaction_id order by transitioned_at desc) as rn
    from transitions
    where new_state in ('cancelled', 'expired', 'closed_paid', 'approval_declined')
),

-- Aggregate termination reasons (some transactions may have multiple reasons)
termination_reasons_agg as (
    select
        transaction_id,
        string_agg(termination_reason, '; ') as termination_reason
    from {{ ref('stg_termination_reasons') }}
    group by transaction_id
),

joined as (
    select
        t.transaction_id,
        t.bid_id,
        t.state,
        t.transfer_method,
        t.inserted_at,
        t.company_id,
        t.num_shares,
        t.price_per_share,
        t.gross_proceeds,
        t.is_active,
        t.transaction_month,
        
        -- Closure timestamp
        tc.closed_at,
        
        -- Calculate time transaction was open
        -- Handle negative values (data quality issues where closed_at < inserted_at)
        case
            when tc.closed_at is not null then 
                greatest(0, datediff('hour', t.inserted_at::timestamp, tc.closed_at::timestamp))
            when tts.transitioned_at is not null then
                greatest(0, datediff('hour', t.inserted_at::timestamp, tts.transitioned_at::timestamp))
            else null
        end as hours_open,
        
        -- Termination reason
        tr.termination_reason
        
    from transactions t
    left join transaction_closures tc 
        on t.transaction_id = tc.transaction_id 
        and tc.rn = 1
    left join transaction_terminal_states tts
        on t.transaction_id = tts.transaction_id
        and tts.rn = 1
        and tc.closed_at is null  -- Only use tts if no closure found
    left join termination_reasons_agg tr 
        on t.transaction_id = tr.transaction_id
)

select * from joined

