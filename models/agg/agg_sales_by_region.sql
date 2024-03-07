with
    fact_sales as (
        select *
        from {{ ref("fact_sales") }}
    )

    , sales_by_region as (
        select
            fk_territory,
            fk_salesperson,
            sum(totaldue) sales, 
            count(1) orders
        from fact_sales
        group by fk_territory, fk_salesperson
    )

select *
from sales_by_region