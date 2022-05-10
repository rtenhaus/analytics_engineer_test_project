/* 
This model aggregates subscriptions at a daily granularity provding 
a flag for whether the subscription was active on the given day
*/


{{ config (materialized="table") }}

with cal as (
	
	select * from {{ ref('dim_cal')}}

),

daily_agg as ( 

	select 
	    cal.date,
	    id,
	    customer_id,
	    IF(cal.date >= date(rs.created_at) AND (cal.date <= date(rs.cancelled_at) OR rs.cancelled_at is null), true, false) active_flag

	from cal 

	left join `wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions` rs on 1=1
	group by 1,2,3,4	

)

select * from daily_agg
