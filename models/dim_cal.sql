/* 
This model creates a date table to simplify daily reporting needs
*/


{{ config (materialized="table") }}

with dates as (

	select date_add('2020-10-01',interval param day) as date
	from unnest(GENERATE_ARRAY(0, 553, 1)) as param 

)

select * from dates

