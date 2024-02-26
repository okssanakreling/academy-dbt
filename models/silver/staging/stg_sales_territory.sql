with 
    source_data as (
        select *
        from {{ source('sap_adw', 'salesterritory') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["territoryid"]) }} as sk_territory
            , territoryid
            , name
            , countryregioncode
            , `group` as territorygroup
            , salesytd
            , saleslastyear
            , costytd
            , costlastyear
        from source_data
    )

select *
from sk_key
