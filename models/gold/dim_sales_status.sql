with
    stg_sales_status as (
        select *
        from {{ ref("stg_sales_status") }}
    )

    , status_data as (
        select
            sk_status
            , salesstatusname
        from stg_sales_status
    )

select *
from status_data