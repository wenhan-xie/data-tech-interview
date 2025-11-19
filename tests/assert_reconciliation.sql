-- Test: Reconciliation - Record counts should match between source and fact table
-- This validates that no records were lost in transformation
-- Also validates that gross_proceeds sum matches (accounting for filtering)

with source_counts as (
    select 
        date_trunc('month', inserted_at::timestamp) as month,
        count(*) as source_count,
        sum(gross_proceeds) as source_proceeds
    from {{ source('raw_data', 'transactions_seed') }}
    where _fivetran_deleted = false
    group by month
),
fact_counts as (
    select 
        transaction_month as month,
        count(*) as fact_count,
        sum(gross_proceeds) as fact_proceeds
    from {{ ref('fct_transactions') }}
    group by month
)
select 
    coalesce(s.month, f.month) as month,
    coalesce(s.source_count, 0) as source_count,
    coalesce(f.fact_count, 0) as fact_count,
    coalesce(s.source_count, 0) - coalesce(f.fact_count, 0) as count_difference,
    coalesce(s.source_proceeds, 0) as source_proceeds,
    coalesce(f.fact_proceeds, 0) as fact_proceeds,
    abs(coalesce(s.source_proceeds, 0) - coalesce(f.fact_proceeds, 0)) as proceeds_difference
from source_counts s
full outer join fact_counts f on s.month = f.month
where coalesce(s.source_count, 0) != coalesce(f.fact_count, 0) 
   or abs(coalesce(s.source_proceeds, 0) - coalesce(f.fact_proceeds, 0)) > 0.01

-- Should return 0 rows
-- If this test fails, there's a discrepancy between source and transformed data

