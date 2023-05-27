with source as (

    select * from {{ source('greenery', 'products') }}

),

renamed_recasted as (

    select
        productid as product_guid
        , name
        , price
        , inventory

    from source

)

select * from renamed_recasted