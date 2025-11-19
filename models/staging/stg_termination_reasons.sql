{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'transaction_termination_reasons_seed') }}
),

renamed as (
    select
        transaction_id,
        termination_reason
    from source
)

select * from renamed

