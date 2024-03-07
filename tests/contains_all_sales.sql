with
    fact_sales as (
        select *
        from {{ ref("fact_sales") }}
    )

    , stg_sales_order_header as (
        select *
        from {{ ref("stg_sales_order_header") }}
    )

    , gold_sales as (
        select count(1) salescount, sum(totaldue) salestotal
        from fact_sales
    )

    , staging_sales as (
        select count(1) salescount, sum(totaldue) salestotal
        from stg_sales_order_header
    )

    , test as (
        select *
        from gold_sales
        cross join staging_sales
        where
            gold_sales.salescount != staging_sales.salescount
            or round(gold_sales.salestotal, 4) != round(staging_sales.salestotal, 4)
    )

select *
from test

