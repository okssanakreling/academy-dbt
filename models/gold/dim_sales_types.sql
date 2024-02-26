with
    stg_sales_type as (
        select *
        from {{ ref("stg_sales_type") }}
    )

    , type_data as (
        select
            sk_type
            , saletype
        from stg_sales_type
    )

select *
from type_data