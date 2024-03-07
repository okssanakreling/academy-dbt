with
    person as (
        select *
        from {{ ref("stg_person") }}
    )

    , stg_sales_order_header as (
        select *
        from {{ ref("stg_sales_order_header") }}
    )

    , salesperson_data as (
        select
            person.sk_person as sk_salesperson
            , person.title
            , person.firstname
            , person.middlename
            , person.lastname
            , person.suffix
            , array_to_string(
                [
                    person.title
                    , person.firstname
                    , person.middlename
                    , person.lastname
                    , person.suffix
                ]
                , ' '
            ) as fullname
        from person
        join stg_sales_order_header
            on stg_sales_order_header.salespersonid = person.personid
    )

select *
from salesperson_data
