with
    stg_product as (
        select *
        from {{ ref("stg_product") }}
    )

    , product_data as (
        select
            sk_product
            , name as productname
            , sellstartdate
            , sellenddate
        from stg_product
    )

select *
from product_data