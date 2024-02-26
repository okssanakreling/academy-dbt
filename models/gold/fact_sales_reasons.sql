with
    stg_sales_order_header_sales_reason as (
        select *
        from {{ ref("stg_sales_order_header_sales_reason") }}
    )

    , stg_sales_reason as (
        select *
        from {{ ref("stg_sales_reason") }}
    )

    , stg_sales_order_header as (
        select *
        from {{ ref("stg_sales_order_header") }}
    )

    , reason_data as (
        select
            sk_sales_reason
            , stg_sales_order_header.sk_sale as fk_sale
            , stg_sales_reason.sk_reason as fk_reason
        from stg_sales_order_header_sales_reason
        join stg_sales_order_header
            on stg_sales_order_header_sales_reason.salesorderid = stg_sales_order_header.salesorderid
        join stg_sales_reason
            on stg_sales_order_header_sales_reason.salesreasonid = stg_sales_reason.salesreasonid
    )

select *
from reason_data