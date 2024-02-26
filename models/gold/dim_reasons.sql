with
    stg_sales_reason as (
        select *
        from {{ ref("stg_sales_reason") }}
    )

    , reason_data as (
        select
            sk_reason
            , name as reasonname
            , reasontype
        from stg_sales_reason
    )

select *
from reason_data