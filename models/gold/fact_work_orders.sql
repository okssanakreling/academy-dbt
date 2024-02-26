with
    stg_work_order as (
        select *
        from {{ ref("stg_work_order") }}
    )

    , stg_products as (
        select *
        from {{ ref("stg_product") }}
    )

    , dim_dates as (
        select *
        from {{ ref("dim_dates") }}
    )

    , work_order as (
        select
            stg_work_order.sk_work_order
            , stg_products.sk_product as fk_product
            , stg_work_order.orderqty
            , stg_work_order.scrappedqty
            , dim_dates_start.sk_date as fk_startdate
            , dim_dates_end.sk_date as fk_enddate
            , dim_dates_due.sk_date as fk_duedate
        from stg_work_order
        join stg_products
            on stg_work_order.productid = stg_products.productid
        join dim_dates as dim_dates_start
            on stg_work_order.startdate = dim_dates_start.date
        join dim_dates as dim_dates_end
            on stg_work_order.startdate = dim_dates_end.date
        join dim_dates as dim_dates_due
            on stg_work_order.startdate = dim_dates_due.date
    )

select *
from work_order