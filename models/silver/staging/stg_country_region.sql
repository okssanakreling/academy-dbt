with
    source_data as (
        select *
        from {{ source('sap_adw', 'countryregion') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["countryregioncode"]) }} as sk_country_region
            , countryregioncode
            , name
        from source_data
    )

select *
from sk_key
