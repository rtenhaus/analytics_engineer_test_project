{# This macro returns a string in the format of #Q given a month number #}
{% macro quarter(month_num) %}

        case
            when {{ month_num }} between 1 and 3 then '1Q'
            when {{ month_num }} between 4 and 6 then '2Q'
            when {{ month_num }} between 7 and 9 then '3Q'
            when {{ month_num }} between 10 and 12 then '4Q'
            else 'ERR'
        end as quarter

{% endmacro %}

{# This macro returns the month name given a month number #}
{% macro month_name(month_num) %}

        case
            when {{ month_num }} = 1 then 'January'
            when {{ month_num }} = 2 then 'February'
            when {{ month_num }} = 3 then 'March'
            when {{ month_num }} = 4 then 'April'
            when {{ month_num }} = 5 then 'May'
            when {{ month_num }} = 6 then 'June'
            when {{ month_num }} = 7 then 'July'
            when {{ month_num }} = 8 then 'August'
            when {{ month_num }} = 9 then 'September'
            when {{ month_num }} = 10 then 'October'
            when {{ month_num }} = 11 then 'November'
            when {{ month_num }} = 12 then 'December'
            else 'ERR'
        end as month_name

{% endmacro %}

{# This macro returns the weekday name given a weekday number #}
{% macro weekday_name(day_of_week) %}

        case
            when {{ day_of_week }} = 1 then 'Sunday'
            when {{ day_of_week }} = 2 then 'Monday'
            when {{ day_of_week }} = 3 then 'Tuesday'
            when {{ day_of_week }} = 4 then 'Wednesday'
            when {{ day_of_week }} = 5 then 'Thursday'
            when {{ day_of_week }} = 6 then 'Friday'
            when {{ day_of_week }} = 7 then 'Saturday'
            else 'ERR'
        end as weekday_name

{% endmacro %}