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


, subscriptions_all as (
    /* Get all subscriptions */
    select
          created_at
        , customer_id
        , id
    from subscriptions
)

, cancellations_all as (
    /* Get all cancellation events */
    select
          cancelled_at
        , customer_id
        , id
    from subscriptions
    where is_valid_cancellation

)

, returning_subscriptions_all as (
    /* Get subscriptions of customer's who subscribe after being cancelled */
    select
          subscriptions_all.created_at
        , subscriptions_all.customer_id
        , subscriptions_all.id
    from subscriptions_all
    inner join cancellations_all using(customer_id)
    where subscriptions_all.created_at > cancellations_all.cancelled_at

    qualify row_number() over(partition by id order by created_at asc) = 1  /* Get the first one */

)

, returning_subscriptions as (

    select
          date(created_at) as date
        , count(distinct id) as subscriptions_returning
    from returning_subscriptions_all
    {{ dbt_utils.group_by(1) }}

)

, final as (

    select
          date
        , coalesce(subscriptions_returning, 0) as subscriptions_returning
    from calendar
    left join returning_subscriptions using(date)

)

select * from final