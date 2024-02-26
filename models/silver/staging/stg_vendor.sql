with
    source_data as (
        select *
        from {{ source("sap_adw", "vendor") }}
    )

    , vendor_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["businessentityid"]) }} as sk_vendor
            , businessentityid as vendorid
            , name
        from source_data
    )

select *
from vendor_data