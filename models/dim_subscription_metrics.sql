/* 
This model aggregates subscription metrics at a daily level 
*/


{{ config (materialized="table") }}


with subs as ( 
	
	select *
	
	from {{ ref('stg_subscriptions')}}

),

daily_subs as (

	select *

	from {{ ref('dim_daily_subscriptions')}}

),

subscriptions_new as (

	select 
	    date(created_at) as date,
	    count(id) as subscriptions_new

	from subs
	group by 1

),

subscriptions_cancelled as (

	select 
		date(cancelled_at) as date,
		count(id) as subscriptions_cancelled

	from subs 
	where cancel_flag	
	group by 1

),

-- isolates first additional subscription id following a cancellation	
next_sub as (
	select 
		subs.customer_id,
		subs.id,
		min(subs.created_at) created_at

	from subs c
	left join subs on c.customer_id = subs.customer_id and subs.created_at > c.cancelled_at
	where c.cancel_flag
	group by 1,2 
),

subscriptions_returning as (

	select
		date(created_at) date,
		count(id) as subscriptions_returning

	from next_sub
	group by 1

), 

subscriptions_active as ( 

	select 
		date,
		count(id) as subscriptions_active

	from daily_subs
	where active_flag
	group by 1 
	order by 1 desc

),

subscriptions_churned as ( 

	select 
		date,
		count(distinct subs.id) as subscriptions_churned

	from daily_subs
	left join subs on daily_subs.id = subs.id and daily_subs.date > date(subs.created_at)
	where not daily_subs.active_flag 
	and cancel_flag	
	group by 1 

), 

metrics as (

	select 
		sn.date,
		subscriptions_new, 
		subscriptions_returning,
		subscriptions_cancelled,
		subscriptions_active,
		subscriptions_churned

	from subscriptions_new sn
	full outer join subscriptions_cancelled sc on sn.date = sc.date
	full outer join subscriptions_active sa on sn.date = sa.date 
	full outer join subscriptions_churned sch on sn.date = sch.date
	full outer join subscriptions_returning sr on sn.date = sr.date
)

select * from metrics