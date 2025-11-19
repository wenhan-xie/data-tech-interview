{{ config(materialized='table') }}

-- Gross proceeds aggregated by transfer method

select
    transfer_method,
    count(*) as transaction_count,
    sum(gross_proceeds) as total_gross_proceeds,
    avg(gross_proceeds) as avg_gross_proceeds,
    median(gross_proceeds) as median_gross_proceeds,
    min(gross_proceeds) as min_gross_proceeds,
    max(gross_proceeds) as max_gross_proceeds,
    round(sum(gross_proceeds) * 100.0 / sum(sum(gross_proceeds)) over (), 2) as percentage_of_total_proceeds,
    -- Active vs terminated breakdown
    sum(case when is_active = true then 1 else 0 end) as active_transactions,
    sum(case when is_active = false then 1 else 0 end) as terminated_transactions,
    sum(case when is_active = true then gross_proceeds else 0 end) as active_gross_proceeds,
    sum(case when is_active = false then gross_proceeds else 0 end) as terminated_gross_proceeds
from {{ ref('fct_transactions') }}
where gross_proceeds is not null
group by transfer_method
order by total_gross_proceeds desc

