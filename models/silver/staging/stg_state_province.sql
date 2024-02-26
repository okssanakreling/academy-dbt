with
    source_data as (
        select *
        from {{ source('sap_adw', 'stateprovince') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["stateprovinceid"]) }} as sk_state_province
            , stateprovinceid
            , stateprovincecode
            , countryregioncode
            , name
            , territoryid
        from source_data
    )

select *
from sk_key