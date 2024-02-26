with
    source_data as(
        select *
        from {{ source('sap_adw', 'product') }}
    )

    , sk_key as (
        select
            {{ dbt_utils.generate_surrogate_key(["productid"]) }} as sk_product
            , productid
            , name
            , productnumber
            , safetystocklevel
            , productsubcategoryid
            , productmodelid
            , date(sellstartdate) as sellstartdate
            , date(sellenddate) as sellenddate
        from source_data
    )

select *
from sk_key
