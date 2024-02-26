with
    stg_credit_card as (
        select *
        from {{ ref("stg_creditcard") }}
    )

    , stg_sales_order_header as (
        select *
        from {{ ref("stg_sales_order_header") }}
    )

    , other_payment_method as (
        select "Other" as paymentmethod
    )

    , card_types as (
        select distinct cardtype as paymentmethod
        from stg_credit_card
        join stg_sales_order_header
            on stg_credit_card.creditcardid = stg_sales_order_header.creditcardid
    )

    , all_data as (
        select
            {{ dbt_utils.generate_surrogate_key(["paymentmethod"]) }} as sk_payment_method
            , paymentmethod
        from (
            (select * from card_types)
            union all (select * from other_payment_method)
        )
    )

select *
from all_data