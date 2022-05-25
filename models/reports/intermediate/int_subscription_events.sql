{{
    config(
        materialized='table',
        partition_by = {
                "field": "date",
                "data_type": "date",
                "granularity": "day"
        },               
        cluster_by = 'customer_id'
    )
}}

/* This model creates a unified event stream for subscription creation and cancellation events */

with

subscriptions as ( select * from {{ ref('fct_subscriptions') }} )

, cancellations as (select * from {{ ref('fct_cancellations') }} )

, unioned as (

    select created_at as event_ts, created_date as date, * except(created_at, created_date) from subscriptions

    union all

    select cancelled_at as event_ts, cancelled_date as date,  * except(cancelled_at, cancelled_date) from cancellations


)

select * from unioned