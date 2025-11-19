-- Test: Transactions cannot be both active and have a closed_at timestamp
-- A transaction marked as is_active = true should NOT have a closed_at
-- This validates the business logic for "active" transactions

select 
    transaction_id,
    is_active,
    state,
    closed_at
from {{ ref('fct_transactions') }}
where is_active = true 
  and closed_at is not null

-- Should return 0 rows
-- If this test fails, there's a logic error in is_active calculation

