with
    stg_product_category as (
        select *
        from {{ ref("stg_product_category") }}
    )

    , categories as (
        select
            sk_product_category
            , name as categoryname
        from stg_product_category
    )

select *
from categories