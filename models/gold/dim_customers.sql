with
    person as (
        select *
        from {{ ref("stg_person") }}
    )

    , customers as (
        select *
        from {{ ref("stg_customer") }}
    )

    , customer_data as (
        select
            customers.sk_customer as sk_customer
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
        left join customers
            on customers.personid = person.personid
        where customers.personid is not null
    )

select *
from customer_data
