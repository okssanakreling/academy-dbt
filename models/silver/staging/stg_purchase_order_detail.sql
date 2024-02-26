with 
    source_data as (
        select *
        from {{ source('sap_adw', 'purchaseorderdetail') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["purchaseorderid"]) }} as sk_purchase_detail
            , purchaseorderid
            , purchaseorderdetailid
            , duedate
            , orderqty
            , productid
            , cast(unitprice as decimal) as unitprice
            , receivedqty
            , rejectedqty
        from source_data
    )

select *
from sk_key
