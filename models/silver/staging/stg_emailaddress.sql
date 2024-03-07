with
    source_data as (
        select *
        from {{ source("sap_adw", "emailaddress") }}
    )

    , stg_data as (
        select
            BusinessEntityID as personid
            , EmailAddressID
            , EmailAddress
        from source_data
    )

select *
from stg_data