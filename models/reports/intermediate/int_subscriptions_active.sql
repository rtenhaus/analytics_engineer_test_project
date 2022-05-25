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

/* This model creates a metrics for active subscriptions */

with

subscriptions as ( select * from {{ ref('stg_recharge_subscriptions') }} )

, customer_date_spine as (select * from {{ ref('int_date_customer_spine') }} )

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

, active_subscriptions as (

    select
          date
        , count(distinct id) as subscriptions_active
    from daily_status
    where is_active
    {{ dbt_utils.group_by(1) }}

)

, active_subscribers as (

    select
          date
        , count(distinct customer_id) as subscribers_active
    from daily_status
    where is_active
    {{ dbt_utils.group_by(1) }}

)

, final as (

    select
          date
        , coalesce(subscriptions_active, 0) as subscriptions_active
        , coalesce(subscribers_active, 0) as subscribers_active
    from calendar
    left join active_subscriptions using(date)
    left join active_subscribers using(date)

)

select * from final