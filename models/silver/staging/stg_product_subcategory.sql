with
    source_data as (
        select *
        from {{ source("sap_adw", "productsubcategory") }}
    )

    , productsubcategory as (
        select
            {{ dbt_utils.generate_surrogate_key(["productsubcategoryid"]) }} as sk_product_subcategory
            , productsubcategoryid
            , productcategoryid
            , name
        from source_data
    )

select *
from productsubcategory