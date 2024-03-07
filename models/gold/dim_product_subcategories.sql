with
    stg_product_category as (
        select *
        from {{ ref("stg_product_category") }}
    )

    , stg_product_subcategory as (
        select *
        from {{ ref("stg_product_subcategory") }}
    )

    , subcategory as (
        select
            stg_product_subcategory.sk_product_subcategory
            , stg_product_category.sk_product_category as fk_product_category
            , stg_product_subcategory.name as subcategoryname
        from stg_product_subcategory
        join stg_product_category
            on stg_product_subcategory.productcategoryid = stg_product_category.productcategoryid
    )

select *
from subcategory