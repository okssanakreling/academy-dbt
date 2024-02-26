with
    stg_purchase_order_detail as (
        select *
        from {{ ref("stg_purchase_order_detail") }}
    )

    , stg_purchase_order_header as (
        select *
        from {{ ref("stg_purchase_order_header") }}
    )

    , stg_product as (
        select *
        from {{ ref("stg_product") }}
    )

    , purchase_details as (
        select 
            stg_purchase_order_detail.sk_purchase_detail
            , stg_purchase_order_header.sk_purchase as fk_purchase
            , stg_product.sk_product as fk_product
            , orderqty
            , unitprice
            , receivedqty
            , rejectedqty
            , receivedqty - rejectedqty as stockedqty
        from stg_purchase_order_detail
        join stg_purchase_order_header
            on stg_purchase_order_header.purchaseorderid = stg_purchase_order_detail.purchaseorderid
        join stg_product
            on stg_purchase_order_detail.productid = stg_product.productid
    )

select *
from purchase_details