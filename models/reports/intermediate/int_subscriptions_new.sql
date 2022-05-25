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

events as ( 
    /* Bring in New Subscription Events */
    select * from {{ ref('int_subscription_events') }} 
    where action = 'Subscription Created'

)

, customer_date_spine as (select * from {{ ref('int_date_customer_spine') }} )

, calendar as ( select * from {{ ref('calendar') }} )

, subscription_events as (

    select
    
          customer_date_spine.*
        , events.id
        , events.customer_subscription_number

    from customer_date_spine
    left join events on customer_date_spine.date = events.date
        and customer_date_spine.customer_id  = events.customer_id

)

, new_subscriptions as (

    select
          date
        , count(distinct id) as subscriptions_new
    from subscription_events
    {{ dbt_utils.group_by(1) }}

)

, new_subscribers as (

    select
          date
        , count(distinct customer_id) as subscribers_new
    from subscription_events
    where customer_subscription_number = 1
    {{ dbt_utils.group_by(1) }}

)

, final as (

    select
          date
        , coalesce(subscriptions_new, 0) as subscriptions_new
        , coalesce(subscribers_new, 0) as subscribers_new
    from calendar
    left join new_subscriptions using(date)
    left join new_subscribers using(date)

)

select * from final
