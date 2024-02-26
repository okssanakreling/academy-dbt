with 
    source_data as (
        select *
        from {{ source('sap_adw', 'purchaseorderheader') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["purchaseorderid"]) }} as sk_purchase
            , purchaseorderid
            , status as purchasestatus
            , vendorid
            , date(orderdate) as orderdate
            , subtotal
            , taxamt
            , freight
        from source_data
    )

select *
from sk_key
