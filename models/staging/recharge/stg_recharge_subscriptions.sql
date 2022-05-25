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
        , safe_cast(created_at as timestamp) as created_at
        , safe_cast(created_at as date) as created_date
        , if(status = 'CANCELLED',coalesce(safe_cast(cancelled_at as timestamp), safe_cast(updated_at as timestamp)), safe_cast(cancelled_at as timestamp)) as cancelled_at
        , if(status = 'CANCELLED',coalesce(safe_cast(cancelled_at as date), safe_cast(updated_at as date)), safe_cast(cancelled_at as date)) as cancelled_date

        -- Needed Fields
        , safe_cast(status as string) as status
        , safe_cast(cancellation_reason as string) as cancellation_reason
        , status = 'CANCELLED' 
            and (lower(cancellation_reason) not like '%max number of charge attempts%' 
                    or cancellation_reason is null) as is_valid_cancellation
        , row_number() over(partition by customer_id order by created_at asc) as customer_subscription_number

        -- No need to stage the rest of the data for this exercise, include the un-staged data anyway
        , * except(id, customer_id, created_at, cancelled_at, status, cancellation_reason)

    from source

)

select * from staging