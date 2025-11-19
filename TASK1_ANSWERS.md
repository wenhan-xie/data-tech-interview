# Task 1: Model Data to Answer Business Questions

## Overview

This document answers the 5 questions from Task 1 and demonstrates how the dbt models address the Execution team's business questions.

---

## Business Questions & Solutions

### Q1: When did a specific transaction 'close'?

**Answer Model:** `models/marts/transaction_closures.sql`

This model provides the `closed_at` timestamp for each transaction that has closed.

**Query Example:**
```sql
SELECT 
    transaction_id,
    transaction_created_at,
    closed_at,
    days_open
FROM {{ ref('transaction_closures') }}
ORDER BY closed_at DESC
```

**Result:** A table with one row per closed transaction, showing when it closed and how long it was open.

---

### Q2: What is the average time a transaction is 'open', across months?

**Answer Model:** `models/marts/avg_open_time_by_month.sql`

This model calculates average open time by month for all terminated transactions.

**Query Example:**
```sql
SELECT 
    transaction_month,
    total_transactions,
    avg_days_open,
    min_hours_open,
    max_hours_open
FROM {{ ref('avg_open_time_by_month') }}
ORDER BY transaction_month DESC
```

**Result:** Monthly aggregates showing average, min, and max open time.

---

### Q3: What are the most common termination reasons for transactions?

**Answer Model:** `models/marts/termination_reasons_analysis.sql`

This model aggregates termination reasons with counts, percentages, and total proceeds.

**Query Example:**
```sql
SELECT 
    termination_reason,
    transaction_count,
    percentage,
    total_gross_proceeds
FROM {{ ref('termination_reasons_analysis') }}
ORDER BY transaction_count DESC
```

**Result:** Ranked list of termination reasons with transaction counts and percentages.

---

### Q4: Visualize the highest gross proceeds per transfer method

**Answer Model:** `models/marts/gross_proceeds_by_transfer_method.sql`

This model aggregates gross proceeds by transfer method for visualization.

**Query Example:**
```sql
SELECT 
    transfer_method,
    transaction_count,
    total_gross_proceeds,
    percentage_of_total_proceeds
FROM {{ ref('gross_proceeds_by_transfer_method') }}
ORDER BY total_gross_proceeds DESC
```

**Result:** Transfer methods ranked by total gross proceeds.

---

## Question 1: What dbt sources, models, and tests would you apply?

### Sources

Defined in `models/staging/__sources.yml`:

- **`raw_data.transactions_seed`**: Raw transaction records
  - Freshness checks: warn after 24 hours, error after 48 hours
  - Tests: unique `id`, not_null on key fields, accepted_values for `state`
  
- **`raw_data.transaction_transitions_seed`**: State change log table
  - Freshness checks: warn after 24 hours, error after 48 hours
  - Tests: unique `id`, not_null on transaction_id, new_state, transitioned_at
  
- **`raw_data.transaction_termination_reasons_seed`**: Termination reason lookup
  - Tests: not_null on transaction_id and termination_reason

### Models

**Staging Layer** (`models/staging/`):
- `stg_transactions.sql`: Cleans raw transaction data, calculates `is_active` flag
- `stg_transaction_transitions.sql`: Cleans state transition log
- `stg_termination_reasons.sql`: Cleans termination reason lookup
- **Materialization**: `view` (lightweight, frequently rebuilt)

**Marts Layer** (`models/marts/`):
- `fct_transactions.sql`: Fact table with all transaction details, calculated `closed_at`, `hours_open`, and `termination_reason`
- `transaction_closures.sql`: Q1 answer - closed transactions with timestamps
- `avg_open_time_by_month.sql`: Q2 answer - monthly average open time
- `termination_reasons_analysis.sql`: Q3 answer - termination reason analysis
- `gross_proceeds_by_transfer_method.sql`: Q4 answer - proceeds by transfer method
- **Materialization**: `table` (optimized for BI consumption, better query performance)

### Tests

**Schema Tests** (defined in `models/staging/schema.yml` and `models/marts/schema.yml`):
- `unique`: transaction_id, transition_id
- `not_null`: All key fields
- `accepted_values`: State values validation
- `dbt_utils.accepted_range`: gross_proceeds >= 0, hours_open >= 0

**Custom Business Logic Tests** (`tests/`):
- `assert_transaction_logic.sql`: Validates active transactions don't have closed_at
- `assert_reconciliation.sql`: Validates record counts match between source and fact table

**Total: 43 tests** (all passing)

---

## Question 2: How would you ensure data quality? Provide code examples.

### 1. Source-Level Tests

```yaml
# models/staging/__sources.yml
sources:
  - name: raw_data
    tables:
      - name: transactions_seed
        columns:
          - name: id
            tests:
              - unique
              - not_null
          - name: state
            tests:
              - not_null
              - accepted_values:
                  arguments:
                    values: ['cancelled', 'expired', 'closed_paid', 'approval_declined', 'pending_approval', 'open', 'bid_accepted']
          - name: gross_proceeds
            tests:
              - dbt_utils.accepted_range:
                  arguments:
                    min_value: 0
                    inclusive: true
```

### 2. Model-Level Tests

```yaml
# models/marts/schema.yml
models:
  - name: fct_transactions
    columns:
      - name: transaction_id
        tests:
          - unique
          - not_null
      - name: hours_open
        tests:
          - dbt_utils.accepted_range:
              arguments:
                min_value: 0
                inclusive: true
```

### 3. Custom Business Logic Tests

```sql
-- tests/assert_transaction_logic.sql
-- Validates that active transactions don't have closed_at timestamps
select 
    transaction_id,
    is_active,
    closed_at
from {{ ref('fct_transactions') }}
where is_active = true 
  and closed_at is not null
-- Should return 0 rows
```

### 4. Data Reconciliation Test

```sql
-- tests/assert_reconciliation.sql
-- Validates record counts and sums match between source and fact table
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
    coalesce(s.source_count, 0) - coalesce(f.fact_count, 0) as count_difference,
    abs(coalesce(s.source_proceeds, 0) - coalesce(f.fact_proceeds, 0)) as proceeds_difference
from source_counts s
full outer join fact_counts f on s.month = f.month
where coalesce(s.source_count, 0) != coalesce(f.fact_count, 0) 
   or abs(coalesce(s.source_proceeds, 0) - coalesce(f.fact_proceeds, 0)) > 0.01
-- Should return 0 rows
```

### 5. Freshness Monitoring

```yaml
# models/staging/__sources.yml
sources:
  - name: raw_data
    tables:
      - name: transactions_seed
        loaded_at_field: _fivetran_synced
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
```

---

## Question 3: How would you validate that the output of your model is correct?

### 1. Record Count Reconciliation

Compare total record counts between source and fact table:

```sql
-- Source count
SELECT COUNT(*) as source_count 
FROM main.transactions_seed 
WHERE _fivetran_deleted = false;

-- Fact table count
SELECT COUNT(*) as fact_count 
FROM main.fct_transactions;

-- Should match (validated by assert_reconciliation test)
```

### 2. Aggregate Validation

Compare sum of gross_proceeds:

```sql
-- Source sum
SELECT SUM(gross_proceeds) as source_proceeds 
FROM main.transactions_seed 
WHERE _fivetran_deleted = false;

-- Fact table sum
SELECT SUM(gross_proceeds) as fact_proceeds 
FROM main.fct_transactions;

-- Should match within 0.01 tolerance
```

### 3. Spot Checks

Manually verify sample transactions:

```sql
-- Check a specific transaction
SELECT 
    t.id as source_id,
    t.state as source_state,
    f.transaction_id,
    f.state,
    f.is_active,
    f.closed_at,
    f.hours_open
FROM main.transactions_seed t
LEFT JOIN main.fct_transactions f ON t.id = f.transaction_id
WHERE t.id = 'ca22b06c-a67d-47b7-ac26-e5947b231a24';
```

### 4. Business Logic Validation

- **Active transactions**: Verify `is_active = true` for transactions in `pending_approval`, `bid_accepted`, `open` states
- **Closed transactions**: Verify `closed_at` is populated for transactions with `state = 'closed_paid'`
- **Hours open**: Verify `hours_open` is calculated correctly (closed_at - inserted_at)
- **Termination reasons**: Verify termination reasons are correctly joined from `transaction_termination_reasons_seed`

### 5. Automated Test Suite

All validations are automated through dbt tests:
- Run `dbt test` to execute all 43 tests
- Tests run automatically in CI/CD pipeline
- Failures alert the team immediately

---

## Question 4: How would you help the Execution team visualize this data?

### Q1: When did transactions close?

**Visualization Type:** **Time Series Line Chart**

- **X-axis**: Date (closed_at)
- **Y-axis**: Number of transactions closed
- **Additional**: Add a secondary Y-axis for cumulative closed transactions
- **Filters**: Date range, transfer method, termination reason
- **Why**: Shows trends over time, identifies peak closing periods

**Alternative**: **Bar Chart** by month showing total closed transactions per month

---

### Q2: Average time transactions are open, across months

**Visualization Type:** **Line Chart with Confidence Intervals**

- **X-axis**: Transaction month
- **Y-axis**: Average days open
- **Additional**: 
  - Shaded area showing min/max range
  - Trend line showing overall direction
  - Annotations for significant changes
- **Filters**: Transfer method, termination reason
- **Why**: Shows trends and variability in transaction duration

**Alternative**: **Box Plot** by month showing distribution of open times

---

### Q3: Most common termination reasons

**Visualization Type:** **Horizontal Bar Chart** (ordered by count)

- **X-axis**: Transaction count
- **Y-axis**: Termination reason (ordered descending)
- **Additional**: 
  - Percentage labels on bars
  - Color coding by category (e.g., buyer-related, seller-related, system-related)
  - Tooltip showing total proceeds for each reason
- **Filters**: Date range, transfer method
- **Why**: Easy to read, clearly shows ranking and relative proportions

**Alternative**: **Pie/Donut Chart** for quick overview, but bar chart is better for comparing values

---

### Q4: Highest gross proceeds per transfer method

**Visualization Type:** **Horizontal Bar Chart** (ordered by proceeds)

- **X-axis**: Total gross proceeds
- **Y-axis**: Transfer method
- **Additional**: 
  - Percentage of total proceeds label
  - Stacked bars showing active vs terminated transactions (optional)
  - Secondary metric: Average proceeds per transaction
- **Filters**: Date range
- **Why**: Easy comparison, shows which transfer methods generate most revenue

**Alternative**: **Stacked Bar Chart** if showing breakdown by transaction status is important

---

### Recommended Dashboard Structure

**Executive Dashboard:**
1. **KPI Cards**: Total transactions, total proceeds, average open time
2. **Time Series**: Transaction closures over time (Q1)
3. **Trend Analysis**: Average open time by month (Q2)
4. **Termination Analysis**: Termination reasons breakdown (Q3)
5. **Revenue Analysis**: Gross proceeds by transfer method (Q4)

**Tools**: Tableau, Looker, Power BI, or even Excel for quick analysis

**Delivery Format**: 
- Interactive dashboard for ongoing monitoring
- Weekly/monthly Excel exports for stakeholders who prefer spreadsheets
- Automated email reports with key metrics

---

## Running the Models

To run all models and tests:

```bash
# Activate virtual environment
source venv/bin/activate

# Seed data
dbt seed

# Run all models
dbt run

# Run all tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

All models compile and run successfully. All 43 tests pass.

---

## Project Structure

```
data-tech-interview/
├── models/
│   ├── staging/
│   │   ├── __sources.yml          # Source definitions
│   │   ├── schema.yml             # Staging model tests
│   │   ├── stg_transactions.sql
│   │   ├── stg_transaction_transitions.sql
│   │   └── stg_termination_reasons.sql
│   └── marts/
│       ├── schema.yml             # Mart model tests
│       ├── fct_transactions.sql   # Fact table
│       ├── transaction_closures.sql
│       ├── avg_open_time_by_month.sql
│       ├── termination_reasons_analysis.sql
│       └── gross_proceeds_by_transfer_method.sql
├── tests/
│   ├── assert_transaction_logic.sql
│   └── assert_reconciliation.sql
├── seeds/
└── README.md
```

