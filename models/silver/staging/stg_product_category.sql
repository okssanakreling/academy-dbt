with
    source_data as (
        select *
        from {{ source("sap_adw", "productcategory") }}
    )

    , productcategory as (
        select
            {{ dbt_utils.generate_surrogate_key(["productcategoryid"]) }} as sk_product_category
            , productcategoryid
            , name
        from source_data
    )

select *
from productcategory