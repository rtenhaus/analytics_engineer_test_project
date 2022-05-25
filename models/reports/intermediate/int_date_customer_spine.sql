{{
    config(materialized='ephemeral')
}}

with

calendar as ( select * from {{ ref('calendar') }} )

, customers as ( select distinct customer_id from {{ ref('stg_recharge_subscriptions') }} )

, customer_date_spine as (

    select
          calendar.date
        , customers.customer_id
    from calendar
    cross join customers

)

select * from customer_date_spine