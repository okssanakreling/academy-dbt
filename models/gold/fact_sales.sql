with
    stg_sales as (
        select *
        from {{ ref("stg_sales_order_header") }}
    )

    , stg_customer as (
        select *
        from {{ ref("stg_customer") }}
    )

    , stg_store as (
        select *
        from {{ ref("stg_store") }}
    )

    , stg_credit_card as (
        select *
        from {{ ref("stg_creditcard") }}
    )

    , stg_sales_status as (
        select *
        from {{ ref("stg_sales_status") }}
    )

    , stg_sales_type as (
        select *
        from {{ ref("stg_sales_type") }}
    )

    , stg_territory as (
        select *
        from {{ ref("stg_sales_territory") }}
    )

    , dim_payment_method as (
        select *
        from {{ ref("dim_payment_method") }}
    )

    , dim_dates as (
        select *
        from {{ ref("dim_dates") }}
    )

    , stg_person as (
        select *
        from {{ ref("stg_person") }}
    )

    , sales_data as (
        select
            stg_sales.sk_sale
            , dim_dates.sk_date as fk_orderdate
            , stg_sales_type.sk_type as fk_type
            , stg_sales_status.sk_status as fk_status
            , cast(stg_sales.subtotal as decimal) as subtotal
            , cast(stg_sales.taxamt as decimal) as taxamt
            , cast(stg_sales.freight as decimal) as freight
            , cast(stg_sales.totaldue as decimal) as totaldue
            , stg_customer.sk_customer as fk_customer
            , dim_payment_method.sk_payment_method as fk_payment_method
            , stg_store.sk_store as fk_store
            , stg_territory.sk_territory as fk_territory
            , stg_person.sk_person as fk_salesperson
        from stg_sales
        left join stg_customer
            on stg_sales.customerid = stg_customer.customerid
        left join stg_store
            on stg_customer.storeid = stg_store.storeid
        left join stg_credit_card
            on stg_sales.creditcardid = stg_credit_card.creditcardid
        left join dim_payment_method
            on coalesce(stg_credit_card.cardtype, "Other") = dim_payment_method.paymentmethod
        left join dim_dates
            on stg_sales.orderdate = dim_dates.date
        left join stg_sales_type
            on stg_sales.onlineorderflag = stg_sales_type.onlineorderflag
        left join stg_sales_status
            on stg_sales.salestatus = stg_sales_status.salestatus
        left join stg_territory
            on stg_customer.territoryid = stg_territory.territoryid
        left join stg_person
            on stg_sales.salespersonid = stg_person.personid
    )

select *
from sales_data