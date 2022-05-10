/* 
This model aggregates metrics at a daily level 
*/


{{ config (materialized="table") }}


with subscribers as ( 
	
	select *
	
	from {{ ref('dim_subscriber_metrics')}}

),

subscriptions as ( 

	select *

	from {{ ref('dim_subscription_metrics')}}

)

select 
	subscriptions.*,
	subscribers.subscribers_new,
	subscribers.subscribers_cancelled,
	subscribers.subscribers_active,
	subscribers.subscribers_churned

from subscribers
left join subscriptions on subscribers.date = subscriptions.date