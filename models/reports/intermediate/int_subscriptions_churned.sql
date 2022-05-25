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

subscriptions as ( select * from {{ ref('stg_recharge_subscriptions') }} )

, customer_date_spine as (select * from {{ ref('int_date_customer_spine') }} )

, calendar as ( select * from {{ ref('calendar') }} )

, daily_status as (
    /* Create a daily record for every customer_id & subscription id and determine if subscription is active on said day */
    select

          customer_date_spine.date
        , customer_date_spine.customer_id
        , subscriptions.id
        , subscriptions.customer_subscription_number
        , customer_date_spine.date >= subscriptions.created_date 
            and customer_date_spine.date <= coalesce(subscriptions.cancelled_date, '2050-01-01') as is_active
    
    from customer_date_spine
    inner join subscriptions using(customer_id)

)

, churned_subscriptions_all as (
    /* Find subscriptions churned subscriptions (cancelled and not otherwise active) $*/
    select
          daily_status.date
        , subscriptions.id
    from daily_status
    left join subscriptions using(id)
    where daily_status.date > subscriptions.created_date
    and is_valid_cancellation
    and not is_active

)
    
, churned_subscriptions as (

    select
          date
        , count(distinct id) as subscriptions_churned
    from churned_subscriptions_all
    {{ dbt_utils.group_by(1) }}

)

, churned_subscribers_all as (
    /* Find subscribers who are went inactive after their first subscription $*/
    select
          daily_status.date
        , daily_status.customer_id
    from daily_status
    inner join subscriptions using(customer_id)
    where daily_status.date > subscriptions.created_date
    and daily_status.customer_subscription_number = 1
    and not daily_status.is_active
    
)

, churned_subscribers as (

    select
          date
        , count(distinct customer_id) as subscribers_churned
    from churned_subscribers_all
    {{ dbt_utils.group_by(1) }}
)

, final as (

    select
          date
        , coalesce(subscriptions_churned, 0) as subscriptions_churned
        , coalesce(subscribers_churned, 0) as subscribers_churned
    from calendar
    left join churned_subscriptions using(date)
    left join churned_subscribers using(date)

)

select * from final
