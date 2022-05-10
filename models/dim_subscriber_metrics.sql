/* 
This model aggregates subscriber metrics at a daily level 
*/


{{ config (materialized="table") }}


with subs as ( 
	
	select *

	from {{ ref('stg_subscriptions')}}

),

subscriptions_active as (

	select
		date(date) as date,
		customer_id, 
		count(if(active_flag, id, NULL)) subscriptions_active,
		count(if(not active_flag, id, NULL)) subscriptions_inactive


	from {{ ref('dim_daily_subscriptions')}}
	group by 1,2
	order by 1 desc, 2
),


subscribers_new as ( 

	select 
		date(created_at) date,
		count(distinct customer_id) as subscribers_new

	from subs
	where sub_row_num = 1
	group by 1

),

 subscribers_cancelled as ( 

	select 
		active.date,
		count(distinct active.customer_id) subscribers_cancelled

	from subscriptions_active active
	left join subscriptions_active prior on date_sub(active.date, interval 1 day) = prior.date and active.customer_id = prior.customer_id
	where active.subscriptions_active = 0 
	and prior.subscriptions_active > 0
	group by 1

),

 subscribers_active as (

	select 
		date,
		count(distinct customer_id) as subscribers_active

	from subscriptions_active
	where subscriptions_active.subscriptions_active > 0
	group by 1 
	order by 1 desc 

),

subscribers_churned as ( 

	select 
		sa.date,
		count(distinct sa.customer_id) as subscribers_churned

	from subscriptions_active sa
	inner join subs fs on fs.customer_id = sa.customer_id and sa.date > date(fs.created_at) and fs.sub_row_num = 1
	where subscriptions_active = 0
	group by 1

),

metrics as ( 
	select 
		sn.date,
		sn.subscribers_new,
		sc.subscribers_cancelled,
		sa.subscribers_active,
		sch.subscribers_churned

	from subscribers_new sn 
	left join subscribers_cancelled sc on sn.date = sc.date
	left join subscribers_active sa on sn.date = sa.date
	left join subscribers_churned sch on sn.date = sch.date

)

select * from metrics 



