with
    stg_product as (
        select *
        from {{ ref("stg_product") }}
    )

    , stg_product_subcategory as (
        select *
        from {{ ref("stg_product_subcategory") }}
    )

    , product_data as (
        select
            stg_product.sk_product
            , stg_product_subcategory.sk_product_subcategory as fk_product_subcategory
            , stg_product.name as productname
            , stg_product.listprice
        from stg_product
        join stg_product_subcategory
            on stg_product.productsubcategoryid = stg_product_subcategory.productsubcategoryid
    )

select *
from product_data