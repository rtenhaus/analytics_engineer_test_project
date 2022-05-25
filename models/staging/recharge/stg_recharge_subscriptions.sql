{{
    config(materialized='table')
}}

with source as (
    
    select * from {{ source('recharge', 'subscriptions') }}

)

, staging as (

    select

        -- IDs
          safe_cast(id as int64) as id
        , safe_cast(customer_id as int64) as customer_id
        
        -- Timestamps
        , safe_cast(created_at as timestamp) as created_at_et
        , safe_cast(cancelled_at as timestamp) as cancelled_at_et

        -- Needed Fields
        , safe_cast(status as string) as status
        , safe_cast(cancellation_reason as string) as cancellation_reason

        -- No need to stage the rest of the data for this exercise, include the un-staged data anyway
        , * except(id, customer_id, created_at, cancelled_at, status, cancellation_reason)

    from source

)

select * from staging