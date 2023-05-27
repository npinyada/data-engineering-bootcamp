with

orders as (

		select * from {{ ref('stg_greenery__orders') }}

)

, products as (

    select * from {{ ref('stg_greenery__products') }}

)
, order_items as (

    select * from {{ ref('stg_greenery__order_items') }}

)

, joined as (

    select distinct
        o.order_guid
        , user_guid
        , promo_guid
        , p.product_guid
        , order_created_at_utc
        , order_cost_usd
        , shipping_cost_usd
        , order_total_usd
        , tracking_guid
        , shipping_service
        , estimated_delivery_at_utc
        , delivered_at_utc
        , order_status
        , p.product_name
        , price
        , inventory
        , ot.quantity as quantity
    
    from order_items as ot
    join orders as o on o.order_guid = ot.order_guid
    join products as p on p.product_guid = ot.product_guid
    

)

select * from joined