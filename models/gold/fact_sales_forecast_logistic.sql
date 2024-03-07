with
    sales_forecast_logistic as (
        select *
        from {{ source("ml", "sales_forecast_logistic") }}
    )

    , sales_forecast as (
        select
            dim_dates.sk_date as fk_date
            , stg_product.sk_product as fk_product
            , stg_store.sk_store as fk_store
            , stg_sales_territory.sk_territory as fk_territory
            , stg_sales_type.sk_type as fk_type
            , sales_forecast_logistic.predicted as orderqty
            , sales_forecast_logistic.predicted * stg_product.listprice as sales
        from sales_forecast_logistic
        join staging.stg_product
            on stg_product.productid = sales_forecast_logistic.productid
        left join staging.stg_store
            on stg_store.storeid = sales_forecast_logistic.storeid
        join staging.stg_sales_territory
            on stg_sales_territory.territoryid = sales_forecast_logistic.territoryid
        join gold.dim_dates
            on dim_dates.date = CAST(sales_forecast_logistic.date AS DATE FORMAT 'YYYYMMDD')
        join staging.stg_sales_type
            on stg_sales_type.onlineorderflag = sales_forecast_logistic.onlineorderflag
    )

select *
from sales_forecast