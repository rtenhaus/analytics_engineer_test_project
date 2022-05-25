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
    /* Bring in Cancellation Events */
    select * from {{ ref('int_subscription_events') }} 
    where action = 'Subscription Cancelled'

)

, subscriptions as ( select * from {{ ref('stg_recharge_subscriptions') }} )

, customer_date_spine as (select * from {{ ref('int_date_customer_spine') }})

, calendar as ( select * from {{ ref('calendar') }} )

, daily_status as (
    /* Create a daily record for every customer_id & subscription id and determine if subscription is active on said day $*/
    select

          customer_date_spine.date
        , customer_date_spine.customer_id
        , subscriptions.id
        , customer_date_spine.date >= subscriptions.created_date 
            and customer_date_spine.date <= coalesce(subscriptions.cancelled_date, '2050-01-01') as is_active
    
    from customer_date_spine
    inner join subscriptions using(customer_id)

)

, cancellation_events as (

    select
    
          customer_date_spine.*
        , events.id

    from customer_date_spine
    left join events on customer_date_spine.date = events.date
        and customer_date_spine.customer_id  = events.customer_id

)

, cancelled_subscriptions as (

    select
          date
        , count(distinct id) as subscriptions_cancelled
    from cancellation_events
    {{ dbt_utils.group_by(1) }}

)

, subscriber_active_status as (
    /* Get active status for customer across subscriptions on a particular day
       This is needed because subscribers can only be 'cancelled; if they are otherwise inactive */
    select
          date
        , customer_id
        , logical_or(is_active) as is_active
    from daily_status
    {{ dbt_utils.group_by(2) }}

)

, subscriber_active_to_cancelled as (
    /* We want to identify subscribers who go from active to inactive the next day */
    select
          date
        , customer_id
        , is_active
        , lag(is_active,1) over(partition by customer_id order by date asc) as prev_is_active
    from subscriber_active_status

)

, cancelled_subscribers as (

    select
          date
        , count(distinct customer_id) as subscribers_cancelled
    from subscriber_active_to_cancelled
    where prev_is_active and not is_active  /* This is it! */
    {{ dbt_utils.group_by(1) }}

)

, final as (

    select
          date
        , coalesce(subscriptions_cancelled, 0) as subscriptions_cancelled
        , coalesce(subscribers_cancelled, 0) as subscribers_cancelled
    from calendar
    left join cancelled_subscriptions using(date)
    left join cancelled_subscribers using(date)

)

select * from final
