with 
    source_data as (
        select *
        from {{ source('sap_adw', 'store') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["businessentityid"]) }} as sk_store
            , businessentityid as storeid
            , name
        from source_data
    )

select *
from sk_key
