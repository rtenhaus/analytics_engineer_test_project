{{
    config(
        materialized='table',
        partition_by = {
                "field": "created_date",
                "data_type": "date",
                "granularity": "day"
        },               
        cluster_by = 'customer_id'
    )
}}

with

subscriptions as (

    select * from {{ ref('stg_recharge_subscriptions') }}

)

, final as (

    select
          created_at
        , created_date
        , customer_id
        , 'Subscription Created' as action
        , id
        , customer_subscription_number

    from subscriptions

)

select * from final