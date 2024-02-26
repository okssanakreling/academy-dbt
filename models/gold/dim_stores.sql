with
    stg_store as (
        select *
        from {{ ref("stg_store") }}
    )

    , stg_customer as (
        select *
        from {{ ref("stg_customer") }}
    )

    , stg_territory as (
        select *
        from {{ ref("stg_sales_territory") }}
    )

    , store_data as (
        select distinct
            stg_store.sk_store
            , stg_store.name as storename
            , stg_territory.sk_territory as fk_territory
        from stg_store
        left join stg_customer
            on stg_store.storeid = stg_customer.storeid
        left join stg_territory
            on stg_customer.territoryid = stg_territory.territoryid
    )

select *
from store_data