{{
    config(
        materialized='table',
        partition_by = {
                "field": "cancelled_date",
                "data_type": "date",
                "granularity": "day"
        },               
        cluster_by = 'customer_id'
    )
}}

with

cancellations as (

    select * from {{ ref('stg_recharge_subscriptions') }}
    where is_valid_cancellation

)

, final as (

    select
          cancelled_at
        , cancelled_date
        , customer_id
        , 'Subscription Cancelled' as action
        , id
        , customer_subscription_number

    from cancellations

)

select * from final