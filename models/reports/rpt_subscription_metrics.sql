{{
    config(
        materialized='table',
        partition_by = {
                "field": "date",
                "data_type": "date",
                "granularity": "day"
        }
    )
}}

with 

new_subs as ( select * from {{ ref('int_subscriptions_new') }} )

, cancellations as ( select * from {{ ref('int_subscriptions_cancellations') }} )

, active as ( select * from {{ ref('int_subscriptions_active') }} )

, returning as ( select * from {{ ref('int_subscriptions_returning') }} )

, churned as ( select * from {{ ref('int_subscriptions_churned') }} )

, joined as (

    select

          new_subs.date
        , new_subs.* except(date)
        , cancellations.* except(date)
        , active.* except(date)
        , returning.* except(date)
        , churned.* except(date)

    from new_subs
    inner join cancellations using(date)
    inner join active using(date)
    inner join returning using(date)
    inner join churned using(date)

)

, rearranged as (

    select
        
          date
        
        -- Subscription Metrics
        , subscriptions_new
        , subscriptions_returning
        , subscriptions_cancelled
        , subscriptions_active
        , subscriptions_churned

        -- Subscriber Metrics
        , subscribers_new
        , subscribers_cancelled
        , subscribers_active
        , subscribers_churned

    from joined

)

select * from rearranged
