{{
    config(materialized='table')
}}

with date_spine as (

    -- For this exercise, table starts at 2020-01-01
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="current_date()"
        )
    }}  

)

, casted as (

    select cast(date_day as date) as date from date_spine

)

, basics as (

    -- Let's add some additional items typically found in a calendar table

    select 
          *
        , extract(year from date) as year
        , extract(month from date) as month
        , extract(week from date) as week
        , extract(day from date) as day

        , extract(dayofyear from date) as day_of_year
        , extract(dayofweek from date) as day_of_week

    from casted

)

, enhanced as (

    select
          *
        -- Some more macros for good measure
        , {{ quarter('month') }}
        , {{ month_name('month') }}
        , {{ weekday_name('day_of_week') }}

    from basics

)

select * from enhanced