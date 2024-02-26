with
    source_data as (
        select *
        from {{ source('sap_adw', 'salesorderheader') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["salesorderid"]) }} as sk_sale
            , salesorderid
            , revisionnumber
            , date(orderdate) as orderdate
            , date(duedate) as duedate
            , date(shipdate) as shipdate
            , status as salestatus
            , onlineorderflag
            , purchaseordernumber
            , accountnumber
            , customerid
            , salespersonid
            , territoryid
            , billtoaddressid
            , shiptoaddressid
            , shipmethodid
            , creditcardid
            , creditcardapprovalcode
            , currencyrateid
            , subtotal
            , taxamt
            , freight
            , totaldue
            , comment
        from source_data
    )

select *
from sk_key
