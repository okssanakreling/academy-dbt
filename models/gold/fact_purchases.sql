with
    stg_purchase_order_header as (
        select *
        from {{ ref("stg_purchase_order_header") }}
    )

    , stg_vendor as (
        select *
        from {{ ref("stg_vendor") }}
    )

    , dim_dates as (
        select *
        from {{ ref("dim_dates") }}
    )

    , purchases as (
        select
            stg_purchase_order_header.sk_purchase
            , stg_vendor.sk_vendor as fk_vendor
            , dim_dates.sk_date as fk_orderdate
            , cast(stg_purchase_order_header.subtotal as decimal) as subtotal
            , cast(stg_purchase_order_header.taxamt as decimal) as taxamt
            , cast(stg_purchase_order_header.freight as decimal) as freight
            , cast(
                coalesce(
                    stg_purchase_order_header.subtotal
                    + stg_purchase_order_header.taxamt
                    + stg_purchase_order_header.freight
                    , 0
                ) as decimal
            ) as totaldue
        from stg_purchase_order_header
        join stg_vendor
            on stg_purchase_order_header.vendorid = stg_vendor.vendorid
        join dim_dates
            on stg_purchase_order_header.orderdate = dim_dates.date
    )

select *
from purchases