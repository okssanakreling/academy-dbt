with 
    source_data as (
        select *
        from {{ source('sap_adw', 'salesorderdetail') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["salesorderdetailid"]) }} as sk_sales_detail
            , salesorderid
            , salesorderdetailid
            , carriertrackingnumber
            , orderqty
            , productid
            , specialofferid
            , unitprice
            , unitpricediscount
        from source_data
    )

select *
from sk_key
