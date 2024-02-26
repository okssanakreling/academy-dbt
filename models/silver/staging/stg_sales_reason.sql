with 
    source_data as (
        select *
        from {{ source('sap_adw', 'salesreason') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["salesreasonid"]) }} as sk_reason
            , salesreasonid
            , name
            , reasontype
        from source_data
    )

select *
from sk_key
