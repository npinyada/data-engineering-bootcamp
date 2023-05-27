
with

orders as (

		select * from {{ ref('stg_greenery__orders') }}

)

, final as (

		select
				count(distinct order_guid) number_of_orders

		from orders

)

select * from final