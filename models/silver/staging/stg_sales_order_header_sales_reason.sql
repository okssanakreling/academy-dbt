with 
    source_data as (
        select *
        from {{ source('sap_adw', 'salesorderheadersalesreason') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["salesorderid"]) }} as sk_sales_reason
            , salesorderid
            , salesreasonid
        from source_data
    )

select *
from sk_key
