with 
    source_data as (
        select *
        from {{ source('sap_adw', 'customer') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["customerid"]) }} as sk_customer
            , customerid
            , personid
            , storeid
            , territoryid
        from source_data
    )

select * 
from sk_key