with
    stg_sales as (
        select *
        from {{ ref("stg_sales_order_header") }}
    )

    , date_limits as (
        select
            min(orderdate) as min_date
            , max(orderdate) as max_date
        from stg_sales
    )

    , generated_dates as (
        select generate_date_array(
            date(min(min_date))
            , date_add(date(max(max_date)), interval 90 day)
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