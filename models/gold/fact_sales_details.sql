with
    stg_sales_order_detail as (
        select *
        from {{ ref("stg_sales_order_detail") }}
    )

    , stg_sales_order_header as (
        select *
        from {{ ref("stg_sales_order_header") }}
    )

    , stg_product as (
        select *
        from {{ ref("stg_product") }}
    )

    , sales_details as (
        select
            stg_sales_order_detail.sk_sales_detail
            , stg_sales_order_header.sk_sale as fk_sale
            , stg_product.sk_product as fk_product
            , stg_sales_order_detail.orderqty
            , cast(stg_sales_order_detail.unitprice as decimal) as unitprice
            , cast(stg_sales_order_detail.unitpricediscount as decimal) as unitpricediscount
        from stg_sales_order_detail
        join stg_sales_order_header
            on stg_sales_order_detail.salesorderid = stg_sales_order_header.salesorderid
        join stg_product
            on stg_sales_order_detail.productid = stg_product.productid
    )

select *
from sales_details