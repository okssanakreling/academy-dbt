with
    stg_sales as (
        select *
        from {{ ref("stg_sales_order_header") }}
    )

    , stg_purchase as (
        select *
        from {{ ref("stg_purchase_order_header") }}
    )

    , stg_work_order as (
        select *
        from {{ ref("stg_work_order") }}
    )

    , date_limits as (
        (
            select
                min(orderdate) as min_date
                , max(orderdate) as max_date
            from stg_sales
        ) union all (
            select
                min(orderdate) as min_date
                , max(orderdate) as max_date
            from stg_purchase
        ) union all (
            select
                min(enddate) as min_date
                , max(enddate) as max_date
            from stg_work_order
        )
    )

    , generated_dates as (
        select generate_date_array(
            date(min(min_date))
            , date_add(date(max(max_date), 90, days)
            , INTERVAL 1 DAY
        ) date_array
        from date_limits
    )

    , date_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["date"]) }} sk_date
            , date
            , extract(year from date) as year
            , extract(month from date) as month
            , extract(week from date) as week
            , extract(day from date) as day
        from generated_dates
        cross join unnest(generated_dates.date_array) date
    )

select *
from date_data