/* 
This staging model adds several fields useful for reporting to the subscription source
*/


{{ config (materialized="table") }}


with subscriptions as ( 
	
	select *,
		row_number() over (partition by customer_id order by created_at) as sub_row_num,
		if(status='CANCELLED' and lower(cancellation_reason) not like '%max number of charge attempts%', true, false) cancel_flag 
	
	from `wise-weaver-282922.raw_data_sandbox.acme1_recharge_subscriptions` 

)

select * from subscriptions 