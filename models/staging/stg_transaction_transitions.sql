{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'transaction_transitions_seed') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        id as transition_id,
        transaction_id,
        new_state,
        transitioned_at,
        inserted_at,
        updated_at,
        _fivetran_deleted,
        _fivetran_synced
    from source
)

select * from renamed

