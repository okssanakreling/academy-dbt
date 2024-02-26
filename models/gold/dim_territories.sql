with
    stg_territory as (
        select *
        from {{ ref("stg_sales_territory") }}
    )

    , stg_country_code as (
        select *
        from {{ ref("stg_country_region") }}
    )

    , territory_data as (
        select
            stg_territory.sk_territory
            , stg_territory.name as territoryname
            , stg_territory.countryregioncode as countrycode
            , stg_country_code.name as countryname
            , stg_territory.territorygroup
        from stg_territory
        join stg_country_code
            on stg_country_code.countryregioncode = stg_territory.countryregioncode
    )

select *
from territory_data